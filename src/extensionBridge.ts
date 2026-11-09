import { randomBytes, timingSafeEqual } from "node:crypto";
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { WebSocketServer, type WebSocket } from "ws";
import { buildBootstrapSnippet } from "./bootstrap.js";
import type { Bridge, ConnectionStatus } from "./bridge.js";
import { config, USER_ENV_FILE } from "./config.js";
import { ProjectionLabError } from "./errors.js";
import { log } from "./logger.js";
import type { ExportedData } from "./types/projectionlab.js";
import {
  CLOSE_CODES,
  HEARTBEAT_INTERVAL_MS,
  PROTOCOL_VERSION,
  type ClientFrame,
  type RequestFrame,
  type ResponseFrame,
  type WireErrorCode,
  type WireMethod,
} from "./types/wire.js";

/** The ProjectionLab app origin, resolved once at load. Empty when
 * PL_APP_URL is malformed: originAllowed runs inside the WebSocket
 * "connection" handler, where a thrown TypeError is an uncaught exception, so
 * letting `new URL` throw here would mean any peer carrying an Origin header
 * could crash the process. Falling back to "" just narrows the allowlist. */
const APP_ORIGIN = (() => {
  try {
    return new URL(config.appUrl).origin;
  } catch {
    return "";
  }
})();

/** Origins allowed to open the WebSocket. Extensions plus the PL app itself
 * (Tier 0 bootstrap snippet runs in page context, so the browser sends the
 * app origin). Sockets with no Origin header (native clients, e.g. the fake
 * test executor) are allowed; the hello token still gates them. */
function originAllowed(origin: string | undefined): boolean {
  if (!origin) return true;
  return (
    origin.startsWith("chrome-extension://") ||
    origin.startsWith("moz-extension://") ||
    (APP_ORIGIN !== "" && origin === APP_ORIGIN)
  );
}

const HELLO_TIMEOUT_MS = 5_000;
/** Bytes a connection may send before its hello is read. A hello frame is a
 * few hundred bytes; the cap stops an unauthenticated peer from making the
 * server buffer a large frame (ws allows 100 MiB by default). */
const MAX_PRE_HELLO_BYTES = 64 * 1024;
/** How often to retry the WebSocket port while another instance holds it. */
const PORT_RETRY_MS = 3_000;
const EXTENSION_HELP =
  "Install the MCP Bridge for ProjectionLab extension (zips on the GitHub Releases page of " +
  "https://github.com/Backroads4Me/projectionlab-mcp), enter the token from " +
  "~/.projectionlab-mcp/ws-token on its options page, and open https://app.projectionlab.com. " +
  "Or call projectionlab_get_bootstrap and paste the snippet into the ProjectionLab tab's console. " +
  "If this worked before in Firefox, Firefox removed the temporary add-on when it restarted: load " +
  "it again from about:debugging (This Firefox > Load Temporary Add-on > manifest.json in the same " +
  "folder) and re-enter the port and token if its options page is empty.";
const REQUEST_TIMEOUT_MS = 60_000;

interface Pending {
  resolve: (result: unknown) => void;
  reject: (err: Error) => void;
  timer: NodeJS.Timeout;
}

/** Best-effort chmod; a file the process does not own keeps its mode. */
function restrictMode(path: string, mode: number): void {
  try {
    chmodSync(path, mode);
  } catch {
    // Not the owner, or a filesystem without POSIX modes.
  }
}

/**
 * Constant-time token comparison. Both tokens are short and compared once per
 * connection, so the fixed-cost comparison is free; the length check keeps
 * timingSafeEqual from throwing and leaks only the length.
 *
 * An empty expected token always fails: a blank PL_WS_TOKEN must never make
 * every peer authenticate, whatever the config layer resolved it to.
 */
function tokenMatches(expected: string, presented: unknown): boolean {
  if (expected.length === 0 || typeof presented !== "string") return false;
  const expectedBuf = Buffer.from(expected, "utf8");
  const actualBuf = Buffer.from(presented, "utf8");
  if (expectedBuf.length !== actualBuf.length) return false;
  return timingSafeEqual(expectedBuf, actualBuf);
}

export class ExtensionBridge implements Bridge {
  private wss: WebSocketServer | null = null;
  private conn: WebSocket | null = null;
  private connExecutor = "";
  private readonly pending = new Map<string, Pending>();
  private nextId = 1;
  private heartbeat: NodeJS.Timeout | null = null;
  private awaitingPong = false;
  private readonly token: string;
  private portBusy = false;
  private retryTimer: NodeJS.Timeout | null = null;
  private closed = false;

  // Serialize all operations and cache the last full export until a write
  // invalidates it.
  private opChain: Promise<unknown> = Promise.resolve();
  private exportCache: { data: ExportedData; at: number } | null = null;

  constructor() {
    // Defense in depth: never run with an empty shared secret. A blank
    // PL_WS_TOKEN means "unset" - generate one rather than let every peer
    // authenticate and receive the Plugin API key in exportData/validateApiKey.
    const configured = config.wsToken?.trim();
    this.token = configured ? configured : this.loadOrCreateToken();
    if (this.token.length === 0) {
      throw new Error("Refusing to start: the WebSocket token resolved to an empty string");
    }
    this.listen();
  }

  /** Reads or creates `~/.projectionlab-mcp/ws-token`, keeping the folder
   * 0700 and the file 0600 even when they already existed with looser modes. */
  private loadOrCreateToken(): string {
    const dir = dirname(USER_ENV_FILE);
    const file = join(dir, "ws-token");
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    restrictMode(dir, 0o700);
    try {
      const existing = readFileSync(file, "utf8").trim();
      if (existing) {
        restrictMode(file, 0o600);
        return existing;
      }
    } catch {
      // No token yet.
    }
    const token = randomBytes(24).toString("base64url");
    writeFileSync(file, token + "\n", { mode: 0o600 });
    restrictMode(file, 0o600);
    log.info("extension", `Generated WebSocket token at ${file}`);
    return token;
  }

  /** Binds the WebSocket port. While another instance (another agent on
   * this machine) holds it, retries every PORT_RETRY_MS and takes over once
   * that instance exits; the extension reconnects on its own schedule. */
  private listen(): void {
    if (this.closed) return;
    const wss = new WebSocketServer({
      host: "127.0.0.1",
      port: config.wsPort,
    });
    this.wss = wss;
    wss.on("listening", () => {
      if (this.portBusy) {
        log.info("extension", `Port ${config.wsPort} is free; took over the listener`);
      }
      this.portBusy = false;
      log.info("extension", `Listening on ws://127.0.0.1:${config.wsPort}`);
    });
    wss.on("error", (err: NodeJS.ErrnoException) => {
      if (err.code === "EADDRINUSE") {
        if (!this.portBusy) {
          log.warn(
            "extension",
            `Port ${config.wsPort} is held by another process (usually another agent's ` +
              `projectionlab-mcp). Retrying every ${PORT_RETRY_MS / 1000}s until it is free.`,
          );
        }
        this.portBusy = true;
        if (this.wss === wss) this.wss = null;
        wss.close();
        this.retryTimer = setTimeout(() => {
          this.retryTimer = null;
          this.listen();
        }, PORT_RETRY_MS);
        this.retryTimer.unref();
        return;
      }
      log.error("extension", `WebSocket server error: ${err.message}`);
    });
    wss.on("connection", (ws, req) => {
      // A socket emits "error" on a malformed frame (RFC 6455 5.1: client
      // frames must be masked), and an EventEmitter with no "error" listener
      // throws, which would end the process. Listen from the first moment, so
      // a peer that never authenticates cannot take the server down; an
      // adopted executor's errors are logged by adopt().
      ws.on("error", (err) => {
        if (this.conn !== ws) {
          log.warn("extension", `Socket error before authentication: ${err.message}`);
        }
      });
      const origin = req.headers.origin;
      if (!originAllowed(origin)) {
        log.warn("extension", `Rejected connection from origin ${origin}`);
        ws.close(CLOSE_CODES.AUTH_FAILED, "origin not allowed");
        return;
      }
      const socket = req.socket;
      let preHelloBytes = 0;
      const countPreHelloBytes = (chunk: Buffer) => {
        preHelloBytes += chunk.length;
        if (preHelloBytes > MAX_PRE_HELLO_BYTES) {
          log.warn("extension", `Dropped connection from ${origin ?? "(no origin)"}: too much data before hello`);
          socket.off("data", countPreHelloBytes);
          socket.destroy();
        }
      };
      socket.on("data", countPreHelloBytes);
      this.awaitHello(ws, origin ?? "(no origin)", () =>
        socket.off("data", countPreHelloBytes),
      );
    });
  }

  /** New sockets must send a valid hello before anything else. `helloRead`
   * runs once the first message arrives, whatever it holds. */
  private awaitHello(ws: WebSocket, origin: string, helloRead: () => void): void {
    const timer = setTimeout(() => {
      ws.close(CLOSE_CODES.AUTH_FAILED, "hello timeout");
    }, HELLO_TIMEOUT_MS);

    ws.once("message", (raw) => {
      clearTimeout(timer);
      helloRead();
      let hello: ClientFrame;
      try {
        hello = JSON.parse(String(raw)) as ClientFrame;
      } catch {
        ws.close(CLOSE_CODES.AUTH_FAILED, "malformed hello");
        return;
      }
      if (hello.type !== "hello" || !tokenMatches(this.token, hello.token)) {
        log.warn("extension", `Auth failed for connection from ${origin}`);
        ws.close(CLOSE_CODES.AUTH_FAILED, "bad token");
        return;
      }
      if (hello.protocolVersion !== PROTOCOL_VERSION) {
        log.warn(
          "extension",
          `Protocol mismatch: executor ${hello.executor} speaks v${hello.protocolVersion}, server v${PROTOCOL_VERSION}`,
        );
        ws.close(CLOSE_CODES.VERSION_MISMATCH, "protocol version mismatch");
        return;
      }
      this.adopt(ws, hello.executor);
    });
  }

  /** Promote an authenticated socket to the active connection (newest wins). */
  private adopt(ws: WebSocket, executor: string): void {
    if (this.conn && this.conn !== ws) {
      log.info("extension", `Replacing ${this.connExecutor} with ${executor}`);
      this.conn.close(CLOSE_CODES.REPLACED, "replaced by newer connection");
    }
    this.conn = ws;
    this.connExecutor = executor;
    log.info("extension", `Executor connected: ${executor}`);
    ws.send(JSON.stringify({ type: "helloAck", protocolVersion: PROTOCOL_VERSION }));

    ws.on("message", (raw) => this.onFrame(ws, raw));
    ws.on("close", () => {
      if (this.conn === ws) {
        log.warn("extension", `Executor disconnected: ${this.connExecutor}`);
        this.dropConnection("executor disconnected");
      }
    });
    ws.on("error", (err) => {
      log.warn("extension", `Socket error from ${executor}: ${err.message}`);
    });
    this.startHeartbeat();
  }

  private onFrame(ws: WebSocket, raw: unknown): void {
    if (this.conn !== ws) return;
    let frame: ClientFrame;
    try {
      frame = JSON.parse(String(raw)) as ClientFrame;
    } catch {
      log.warn("extension", "Dropping malformed frame");
      return;
    }
    if (frame.type === "pong") {
      this.awaitingPong = false;
      return;
    }
    if (frame.type === "response") {
      this.settle(frame);
      return;
    }
  }

  private settle(frame: ResponseFrame): void {
    const pending = this.pending.get(frame.id);
    if (!pending) return;
    this.pending.delete(frame.id);
    clearTimeout(pending.timer);
    if (frame.ok) {
      pending.resolve(frame.result);
    } else {
      const code = frame.error?.code ?? "UNKNOWN";
      const message = frame.error?.message ?? "executor reported an error";
      pending.reject(this.wireError(code, message));
    }
  }

  private wireError(code: WireErrorCode, message: string): ProjectionLabError {
    switch (code) {
      case "NO_TAB":
        return new ProjectionLabError(
          "PAGE_NOT_LOADED",
          `No ProjectionLab tab: ${message}`,
          "Open https://app.projectionlab.com in the browser running the bridge extension",
        );
      case "API_NOT_AVAILABLE":
        return new ProjectionLabError(
          "API_NOT_AVAILABLE",
          message,
          "Make sure you are logged in to ProjectionLab and plugins are enabled in Account Settings",
        );
      case "API_KEY_INVALID":
        return new ProjectionLabError(
          "API_KEY_INVALID",
          message,
          "Check that PL_API_KEY matches your key in Account Settings > Plugins",
        );
      case "TIMEOUT":
        return new ProjectionLabError(
          "TIMEOUT",
          message,
          "The ProjectionLab page did not respond in time - check the tab and try again",
        );
      default:
        return new ProjectionLabError(
          "UNKNOWN",
          message,
          "The Plugin API rejected the operation. Verify the payload shape against a fresh export and try again.",
        );
    }
  }

  private startHeartbeat(): void {
    if (this.heartbeat) return;
    this.awaitingPong = false;
    this.heartbeat = setInterval(() => {
      if (!this.conn) return;
      if (this.awaitingPong) {
        log.warn("extension", "Heartbeat missed; dropping executor");
        this.conn.terminate();
        this.dropConnection("heartbeat missed");
        return;
      }
      this.awaitingPong = true;
      this.conn.send(JSON.stringify({ type: "ping" }));
    }, HEARTBEAT_INTERVAL_MS);
  }

  private dropConnection(reason: string): void {
    // Close the socket if it's still live - wss.close() waits for all
    // clients, so leaving it open would hang shutdown.
    try {
      this.conn?.terminate();
    } catch {
      // Already gone.
    }
    this.conn = null;
    this.connExecutor = "";
    this.awaitingPong = false;
    this.exportCache = null;
    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(
        new ProjectionLabError(
          "EXTENSION_NOT_CONNECTED",
          `Request ${id} aborted: ${reason}`,
          "The browser-side executor disconnected mid-request. Check the browser and retry.",
        ),
      );
    }
    this.pending.clear();
  }

  private serialize<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.opChain.then(fn, fn);
    this.opChain = next.catch(() => {});
    return next;
  }

  /** Why no executor can be reached, with the fix, for error messages. */
  private notConnectedReason(): string {
    if (this.portBusy) {
      return (
        `Port ${config.wsPort} is held by another projectionlab-mcp instance (another agent on this ` +
        "machine is using ProjectionLab), so the browser extension is connected to that one. This " +
        "server takes over automatically within a minute of that instance exiting. To run both at " +
        "once, set PL_WS_PORT to a free port here and point the extension at it."
      );
    }
    return `No browser-side executor is connected to ws://127.0.0.1:${config.wsPort}. ${EXTENSION_HELP}`;
  }

  private requireApiKey(): string {
    if (!config.apiKey) {
      throw new ProjectionLabError(
        "API_KEY_MISSING",
        "PL_API_KEY is not set",
        "Copy the Plugin API Key from ProjectionLab Account Settings > Plugins and set PL_API_KEY " +
          `in this MCP server's env block or in ${USER_ENV_FILE}, then restart the MCP host`,
      );
    }
    return config.apiKey;
  }

  private request(
    method: WireMethod,
    params: unknown[],
    timeoutMs = REQUEST_TIMEOUT_MS,
  ): Promise<unknown> {
    const conn = this.conn;
    if (!conn) {
      throw new ProjectionLabError(
        "EXTENSION_NOT_CONNECTED",
        "No browser-side executor is connected",
        this.notConnectedReason(),
      );
    }
    const id = String(this.nextId++);
    const frame: RequestFrame = { type: "request", id, method, params };
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(
          new ProjectionLabError(
            "TIMEOUT",
            `${method} timed out after ${timeoutMs}ms`,
            "The browser did not respond - check that the ProjectionLab tab is open and responsive",
          ),
        );
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      conn.send(JSON.stringify(frame));
    });
  }

  /** Tier 0: console snippet that turns a PL tab into the executor. */
  bootstrapSnippet(): string {
    return buildBootstrapSnippet(config.wsPort, this.token);
  }

  invalidateCache(): void {
    this.exportCache = null;
  }

  async checkConnection(): Promise<ConnectionStatus> {
    const result: ConnectionStatus = {
      browser: false,
      pageLoaded: false,
      apiAvailable: false,
      apiKeyValid: false,
      errors: [],
    };
    return this.serialize(async () => {
      if (!config.apiKey) {
        try {
          this.requireApiKey();
        } catch (err) {
          const e = err as ProjectionLabError;
          result.errors.push(`${e.code}: ${e.message}. ${e.suggestion}`);
        }
      }
      if (!this.conn) {
        result.errors.push(`EXTENSION_NOT_CONNECTED: ${this.notConnectedReason()}`);
        return result;
      }
      result.browser = true;
      try {
        await this.request(
          "waitForApi",
          [config.apiReadyTimeoutMs],
          config.apiReadyTimeoutMs + 5_000,
        );
        result.pageLoaded = true;
        result.apiAvailable = true;
      } catch (err) {
        if (err instanceof ProjectionLabError && err.code === "API_NOT_AVAILABLE") {
          result.pageLoaded = true;
        }
        result.errors.push(
          err instanceof ProjectionLabError
            ? `${err.code}: ${err.message}. ${err.suggestion}`
            : String(err),
        );
        return result;
      }
      if (!config.apiKey) return result;
      try {
        await this.request("validateApiKey", [{ key: config.apiKey }]);
        result.apiKeyValid = true;
      } catch {
        result.errors.push(
          "API key rejected. Check PL_API_KEY matches your Plugin API Key in Account Settings.",
        );
      }
      return result;
    });
  }

  async describePluginApi(): Promise<Record<string, string>> {
    return this.serialize(async () => {
      return (await this.request("describeApi", [])) as Record<string, string>;
    });
  }

  async exportData(opts?: { fresh?: boolean }): Promise<ExportedData> {
    const cached = (): ExportedData | undefined => {
      if (
        !opts?.fresh &&
        this.exportCache &&
        Date.now() - this.exportCache.at < config.exportCacheMs
      ) {
        log.debug("extension", "exportData served from cache");
        return this.exportCache.data;
      }
      return undefined;
    };
    const hit = cached();
    if (hit) return hit;
    return this.serialize(async () => {
      // A read queued behind another export reuses the result it just cached.
      const queuedHit = cached();
      if (queuedHit) return queuedHit;
      log.info("extension", "Calling exportData");
      const start = Date.now();
      const data = (await this.request("exportData", [
        { key: this.requireApiKey() },
      ])) as ExportedData;
      log.info("extension", `exportData completed in ${Date.now() - start}ms`);
      this.exportCache = { data, at: Date.now() };
      return data;
    });
  }

  async updateAccount(
    accountId: string,
    data: Record<string, unknown>,
    force?: boolean,
  ): Promise<void> {
    await this.serialize(async () => {
      log.info(
        "extension",
        `Calling updateAccount for ${accountId}${force ? " (force)" : ""}`,
      );
      const opts: { key: string; force?: string } = { key: this.requireApiKey() };
      if (force) opts.force = "true";
      await this.request("updateAccount", [accountId, data, opts]);
    });
    this.invalidateCache();
  }

  private async restoreCall(method: WireMethod, payload: unknown): Promise<void> {
    await this.serialize(async () => {
      log.info("extension", `Calling ${method}`);
      await this.request(method, [payload, { key: this.requireApiKey() }]);
    });
    this.invalidateCache();
  }

  async restoreCurrentFinances(state: unknown): Promise<void> {
    await this.restoreCall("restoreCurrentFinances", state);
  }

  async restorePlans(plans: unknown[]): Promise<void> {
    await this.restoreCall("restorePlans", plans);
  }

  async restoreProgress(progress: unknown): Promise<void> {
    await this.restoreCall("restoreProgress", progress);
  }

  async restoreSettings(settings: unknown): Promise<void> {
    await this.restoreCall("restoreSettings", settings);
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    this.dropConnection("server shutting down");
    if (this.wss) {
      log.info("extension", "Closing WebSocket server");
      await new Promise<void>((resolve) => this.wss?.close(() => resolve()));
      this.wss = null;
    }
  }
}
