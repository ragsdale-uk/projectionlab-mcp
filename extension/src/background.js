import browser from "webextension-polyfill";
import { executePageRequest } from "./page-main.js";
import { makeResponseFrame } from "./wire.js";

const DEFAULT_PORT = 8788;
const PROTOCOL_VERSION = 1;
const REQUEST_TIMEOUT_MS = 60_000;
const RECONNECT_ALARM = "plmcp-reconnect";
let socket;
let reconnectTimer;
let retryMs = 2_000;
let generation = 0;
let authBlocked = false;
const inFlight = new Map();

const EXTENSION_NAME = "MCP Bridge for ProjectionLab";
// Toolbar label per connection status: authed = token accepted by the server,
// connected = socket open and awaiting the server's helloAck. Only authed
// shows a badge: the server runs only while an MCP host does, so being
// disconnected is the normal idle state.
const STATUS_LABELS = { authed: "connected", connected: "connecting", disconnected: "not connected" };

async function showBadge(connectionStatus, lastError) {
  const authed = connectionStatus === "authed";
  const label = STATUS_LABELS[connectionStatus] ?? STATUS_LABELS.disconnected;
  const title = `${EXTENSION_NAME}: ${label}${lastError ? ` - ${lastError}` : ""}`;
  try {
    await browser.action.setBadgeText({ text: authed ? "✓" : "" });
    if (authed) {
      await browser.action.setBadgeBackgroundColor({ color: "#188038" });
      await browser.action.setBadgeTextColor?.({ color: "#ffffff" });
    }
    await browser.action.setTitle({ title });
  } catch {
    // The badge is cosmetic; a failure here must not interrupt the connection.
  }
}

const setStatus = (connectionStatus, lastError = "") => {
  void showBadge(connectionStatus, lastError);
  return browser.storage.local.set({ connectionStatus, lastError });
};
const codedError = (code, message) => Object.assign(new Error(message), { code });

function scheduleReconnect(myGeneration) {
  if (authBlocked || myGeneration !== generation || reconnectTimer) return;
  const delay = retryMs;
  retryMs = Math.min(retryMs * 2, 30_000);
  reconnectTimer = setTimeout(() => { reconnectTimer = undefined; void connect(); }, delay);
}

async function connect(force = false) {
  if (!force && (socket?.readyState === WebSocket.OPEN || socket?.readyState === WebSocket.CONNECTING)) return;
  const myGeneration = ++generation;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = undefined;
  if (socket) { socket.onclose = null; socket.close(); }
  const config = await browser.storage.local.get({ port: DEFAULT_PORT, token: "" });
  if (!config.token) {
    await setStatus("disconnected", "Configure the shared token to connect.");
    return;
  }
  // The port is interpolated into ws://127.0.0.1:<port>, so a non-integer
  // value such as "@evil.com/" would change the host the socket dials and
  // hand the token to that host instead. The options page stores Number(...),
  // but validate here too so a hand-edited or otherwise poisoned storage
  // value can never redirect the connection.
  const port = Number.isInteger(config.port) && config.port >= 1 && config.port <= 65535
    ? config.port
    : DEFAULT_PORT;
  authBlocked = false;
  await setStatus("disconnected");
  const ws = new WebSocket(`ws://127.0.0.1:${port}`);
  socket = ws;
  ws.onopen = () => {
    if (myGeneration !== generation) return;
    void setStatus("connected");
    ws.send(JSON.stringify({ type: "hello", token: config.token, protocolVersion: PROTOCOL_VERSION, executor: `extension/${browser.runtime.getManifest().version}` }));
  };
  ws.onmessage = (event) => {
    if (myGeneration !== generation) return;
    let frame;
    try { frame = JSON.parse(event.data); }
    catch { void setStatus("connected", "Server sent invalid JSON."); return; }
    if (frame.type === "helloAck") { retryMs = 2_000; void setStatus("authed"); }
    else if (frame.type === "ping") ws.send(JSON.stringify({ type: "pong" }));
    else if (frame.type === "request") void handleRequest(frame, ws);
  };
  ws.onerror = () => void setStatus("disconnected", "WebSocket connection failed.");
  ws.onclose = (event) => {
    if (myGeneration !== generation) return;
    socket = undefined;
    authBlocked = event.code === 4001 || event.code === 4002;
    const error = event.code === 4001 ? "Authentication failed. Check the shared token."
      : event.code === 4002 ? "Wire protocol version mismatch."
        : event.reason || `Connection closed (${event.code}).`;
    void setStatus("disconnected", error);
    scheduleReconnect(myGeneration);
  };
}

async function findProjectionLabTab() {
  const tabs = await browser.tabs.query({ url: "https://app.projectionlab.com/*" });
  if (tabs.length) return tabs[0];
  const { autoOpen } = await browser.storage.local.get({ autoOpen: true });
  if (!autoOpen) throw codedError("NO_TAB", "No ProjectionLab tab is open and auto-open is disabled.");
  try { return await browser.tabs.create({ url: "https://app.projectionlab.com/", active: false }); }
  catch { throw codedError("NO_TAB", "Could not open a ProjectionLab tab."); }
}

async function executeInTab(request) {
  const tab = await findProjectionLabTab();
  if (typeof tab.id !== "number") throw codedError("NO_TAB", "ProjectionLab tab has no usable id.");
  try {
    const injection = await browser.scripting.executeScript({
      target: { tabId: tab.id },
      world: "MAIN",
      func: executePageRequest,
      args: [request],
    });
    if (!injection.length || !injection[0].result) throw codedError("NO_TAB", "ProjectionLab tab returned no result.");
    return injection[0].result;
  } catch (error) {
    if (/tab|receiving end|closed/i.test(error?.message || "")) throw codedError("NO_TAB", "ProjectionLab tab closed while processing the request.");
    throw error;
  }
}

async function handleRequest(request, ws) {
  if (typeof request.id !== "string" || inFlight.has(request.id)) return;
  let timer;
  const operation = executeInTab(request);
  // Promise.race may settle on the timeout first; consume a later operation
  // rejection so it cannot become an unhandled service-worker rejection.
  operation.catch(() => {});
  const timeout = new Promise((_, reject) => { timer = setTimeout(() => reject(codedError("TIMEOUT", "Request timed out after 60 seconds.")), REQUEST_TIMEOUT_MS); });
  inFlight.set(request.id, operation);
  try {
    const response = await Promise.race([operation, timeout]);
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(makeResponseFrame(request.id, response)));
  } catch (error) {
    const code = ["NO_TAB", "API_NOT_AVAILABLE", "API_KEY_INVALID", "TIMEOUT"].includes(error?.code) ? error.code : "UNKNOWN";
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "response", id: request.id, ok: false, error: { code, message: error instanceof Error ? error.message : String(error) } }));
  } finally {
    clearTimeout(timer);
    inFlight.delete(request.id);
  }
}

browser.storage.onChanged.addListener((changes, area) => {
  if (area === "local" && (changes.port || changes.token)) void connect(true);
});
browser.action.onClicked.addListener(() => void browser.runtime.openOptionsPage());
browser.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RECONNECT_ALARM && !authBlocked && !socket) void connect();
});
async function createReconnectAlarm() {
  // Firefox MV3 uses an event page and does not treat WebSocket traffic as
  // activity, so a sub-30-second alarm prevents it from being unloaded.
  const isFirefox = typeof browser.runtime.getBrowserInfo === "function";
  await browser.alarms.create(RECONNECT_ALARM, { periodInMinutes: isFirefox ? 0.25 : 1 });
}
void createReconnectAlarm();
void connect();
