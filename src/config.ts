import { config as loadDotenv } from "dotenv";
import { resolve, dirname, join } from "node:path";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

/** Per-user config file shared by every MCP host on the machine. */
export const USER_ENV_FILE = join(homedir(), ".projectionlab-mcp", ".env");

// Precedence: the MCP host's `env` block, then the per-user file, then a
// `.env` at the package root (a development checkout). dotenv never
// overrides a variable that is already set, so load order is precedence.
loadDotenv({ path: USER_ENV_FILE });
loadDotenv({ path: resolve(__dirname, "../.env") });

function optional(name: string, fallback: string): string {
  return process.env[name] || fallback;
}

function optionalInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const value = parseInt(raw, 10);
  if (Number.isNaN(value) || value < 0) {
    throw new Error(`Invalid value for ${name}: "${raw}" (expected a non-negative integer)`);
  }
  return value;
}

function optionalBoolean(name: string): boolean | undefined {
  const value = process.env[name];
  if (value === undefined) return undefined;
  return value.toLowerCase() === "true";
}

function expandHome(p: string): string {
  if (p === "~") return homedir();
  if (p.startsWith("~/")) return resolve(homedir(), p.slice(2));
  return resolve(p);
}

export const config = {
  // Optional at startup so the server always connects and can tell the agent
  // what is missing; operations that need the key reject without it.
  apiKey: process.env.PL_API_KEY || undefined,
  // Localhost WebSocket the browser extension (or bootstrap snippet) connects
  // to. The token is a shared secret the executor must present; if unset,
  // ExtensionBridge generates one and persists it under the snapshot dir.
  // A blank value is treated as unset: `.env` files routinely carry
  // `PL_WS_TOKEN=` with nothing after it, and an empty string is not nullish,
  // so without the trim-or-undefined an empty secret would be adopted and
  // every peer would authenticate.
  wsPort: optionalInt("PL_WS_PORT", 8788),
  wsToken: process.env.PL_WS_TOKEN?.trim() || undefined,
  appUrl: optional("PL_APP_URL", "https://app.projectionlab.com"),
  apiReadyTimeoutMs: optionalInt("PL_API_READY_TIMEOUT_MS", 30000),
  // How long a full export may be served from cache. Writes always invalidate,
  // so this can be generous for read-heavy analysis sessions.
  exportCacheMs: optionalInt("PL_EXPORT_CACHE_MS", 90000),
  // Pretty-print JSON tool output (costs ~25-40% more tokens; off by default).
  prettyJson: optionalBoolean("PL_PRETTY_JSON") ?? false,
  // Writes stay disabled unless PL_READ_ONLY is exactly "false".
  readOnly: (process.env.PL_READ_ONLY ?? "").trim().toLowerCase() !== "false",
  snapshotDir: expandHome(optional("PL_SNAPSHOT_DIR", "~/.projectionlab-mcp/snapshots")),
  logLevel: optional("LOG_LEVEL", "info") as "error" | "warn" | "info" | "debug",
} as const;

export type Config = typeof config;
