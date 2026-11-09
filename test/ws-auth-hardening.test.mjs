// Auth hardening on the localhost WebSocket.
//
// 1. A blank PL_WS_TOKEN must mean "unset", not "empty secret". `.env` files
//    routinely carry `PL_WS_TOKEN=` with nothing after it (the shape shown in
//    .env.example), dotenv parses that to "", and an empty string is not
//    nullish - so a `??` fallback left the server with an empty shared secret
//    that every peer matched. Because every wire method carries the Plugin API
//    key (see src/types/wire.ts), that let an unauthenticated local process
//    adopt the executor role and read the key in cleartext.
// 2. A malformed PL_APP_URL must not crash the server: originAllowed runs
//    inside the "connection" handler, where a thrown TypeError is an uncaught
//    exception, and any peer carrying an Origin header would kill the process.

import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import WebSocket from "ws";
import { freePort } from "./helpers/free-port.mjs";

const home = mkdtempSync(join(tmpdir(), "plmcp-auth-home-"));
process.env.HOME = home;
process.env.PL_API_KEY = "plk-secret-value";
process.env.PL_WS_PORT = String(await freePort());
// Blank, not unset - exactly what `PL_WS_TOKEN=` parses to.
process.env.PL_WS_TOKEN = "";
// Deliberately malformed: no scheme.
process.env.PL_APP_URL = "app.projectionlab.com";
process.env.PL_SNAPSHOT_DIR = join(mkdtempSync(join(tmpdir(), "plmcp-auth-snap-")), "snapshots");
process.env.LOG_LEVEL = "error";

const PORT = Number(process.env.PL_WS_PORT);
const { ExtensionBridge } = await import("../dist/extensionBridge.js");
const { PROTOCOL_VERSION, CLOSE_CODES } = await import("../dist/types/wire.js");

const bridge = new ExtensionBridge();
await new Promise((r) => setTimeout(r, 150)); // let the server start listening

/** Opens a socket, sends one hello, and resolves with the close/ack outcome. */
function attempt({ token, origin } = {}) {
  return new Promise((resolve) => {
    const ws = new WebSocket(`ws://127.0.0.1:${PORT}`, {
      headers: origin === undefined ? {} : { Origin: origin },
    });
    const timer = setTimeout(() => {
      try { ws.close(); } catch { /* already closed */ }
      resolve({ acked, closed: null });
    }, 1500);
    let acked = false;
    ws.on("open", () => {
      ws.send(JSON.stringify({ type: "hello", token, protocolVersion: PROTOCOL_VERSION, executor: "probe/1.0" }));
    });
    ws.on("message", (raw) => {
      if (JSON.parse(String(raw)).type === "helloAck") acked = true;
    });
    ws.on("close", (code) => { clearTimeout(timer); resolve({ acked, closed: code }); });
    ws.on("error", () => { /* the close event follows and resolves */ });
  });
}

test("a blank PL_WS_TOKEN generates a token instead of an empty secret", (t) => {
  if (process.platform === "win32") return t.skip("POSIX home layout only");
  const generated = readFileSync(join(home, ".projectionlab-mcp", "ws-token"), "utf8").trim();
  assert.ok(generated.length >= 32, "expected a generated token, got an empty one");
});

test("an empty token is rejected, not accepted as the shared secret", async () => {
  const { acked, closed } = await attempt({ token: "" });
  assert.equal(acked, false, "server accepted an empty token");
  assert.equal(closed, CLOSE_CODES.AUTH_FAILED);
});

test("the generated token still authenticates", async () => {
  if (process.platform === "win32") return;
  const generated = readFileSync(join(home, ".projectionlab-mcp", "ws-token"), "utf8").trim();
  const { acked } = await attempt({ token: generated });
  assert.equal(acked, true, "the generated token was rejected");
});

test("a wrong token is rejected", async () => {
  const { acked, closed } = await attempt({ token: "not-the-token" });
  assert.equal(acked, false);
  assert.equal(closed, CLOSE_CODES.AUTH_FAILED);
});

test("a malformed PL_APP_URL does not crash the server on an Origin header", async () => {
  // A browser always sends Origin, so before the fix this killed the process.
  const web = await attempt({ token: "x", origin: "https://app.projectionlab.com" });
  assert.equal(web.acked, false);
  assert.equal(web.closed, CLOSE_CODES.AUTH_FAILED, "a web origin should be refused");

  // Survives, and still accepts extension origins: the server is alive.
  const ext = await attempt({
    token: "x",
    origin: "chrome-extension://abcdefghijklmnop",
  });
  assert.equal(ext.acked, false, "a wrong token must still fail");
  assert.equal(ext.closed, CLOSE_CODES.AUTH_FAILED);
});

test.after(async () => {
  await bridge.close();
});
