// Exfiltration hardening. The extension is loaded unpacked / in developer
// mode, so the bundle is unsigned and the only thing standing between the
// data it can see (the shared token, and every plan figure that crosses the
// socket) and a remote host is what the code itself chooses to dial.
//
// These tests pin three properties:
//   1. connect-src is locked to 127.0.0.1 in BOTH manifests, so a tampered
//      bundle cannot open a socket to an arbitrary origin.
//   2. The port is validated before it is interpolated into
//      ws://127.0.0.1:<port>, so a poisoned storage value cannot retarget the
//      connection - and with it the token - at another host.
//   3. No source file references a network sink or a non-loopback origin.

import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import * as esbuild from "esbuild";
import { WebSocketServer } from "ws";

/** Node 22's own WebSocket, kept so one test can use a real socket. */
const REAL_WEBSOCKET = globalThis.WebSocket;

const readManifest = (name) =>
  readFile(new URL(`../${name}`, import.meta.url), "utf8").then(JSON.parse);

test("both manifests restrict connect-src to loopback", async () => {
  const [chromium, firefox] = await Promise.all([
    readManifest("manifest.json"),
    readManifest("manifest.firefox.json"),
  ]);
  for (const [label, manifest] of [["chromium", chromium], ["firefox", firefox]]) {
    const csp = manifest.content_security_policy?.extension_pages;
    assert.ok(csp, `${label}: extension_pages CSP is missing`);
    assert.match(
      csp,
      /connect-src\s+ws:\/\/127\.0\.0\.1:\*/,
      `${label}: connect-src must allow only ws://127.0.0.1:*`,
    );
  }
});

test("background.js validates the port instead of interpolating it raw", async () => {
  const source = await readFile(new URL("../src/background.js", import.meta.url), "utf8");
  assert.match(source, /Number\.isInteger\(config\.port\)/);
  assert.match(source, /config\.port >= 1 && config\.port <= 65535/);
  // The raw, unvalidated value must never reach the socket constructor.
  assert.equal(source.includes("ws://127.0.0.1:${config.port}"), false);
});

test("no extension source dials a non-loopback origin", async () => {
  for (const file of ["background.js", "options.js", "page-main.js", "wire.js"]) {
    const source = await readFile(new URL(`../src/${file}`, import.meta.url), "utf8");
    for (const sink of ["fetch(", "XMLHttpRequest", "sendBeacon", "EventSource", "importScripts"]) {
      assert.equal(source.includes(sink), false, `${file} uses ${sink}`);
    }
    for (const url of source.match(/(https?|wss?):\/\/[^\s"'`)]+/g) ?? []) {
      assert.match(
        url,
        /^(wss?:\/\/127\.0\.0\.1|https:\/\/app\.projectionlab\.com)/,
        `${file} references ${url}`,
      );
    }
  }
});

// ---- behavioural: run the real connect() and observe the URL it dials ------

/** Bundles src/background.js once, with the browser platform stubbed. esbuild
 * keeps a long-lived service process, so it is built a single time and stopped
 * in the after hook. */
const bundle = (async () => {
  const result = await esbuild.build({
    entryPoints: [new URL("../src/background.js", import.meta.url).pathname],
    bundle: true,
    write: false,
    format: "iife",
    platform: "browser",
    plugins: [{
      name: "stub-browser",
      setup(b) {
        b.onResolve({ filter: /^webextension-polyfill$/ }, (a) => ({ path: a.path, namespace: "stub" }));
        b.onLoad({ filter: /.*/, namespace: "stub" }, () => ({
          contents: "export default globalThis.browser;",
          loader: "js",
        }));
      },
    }],
  });
  return result.outputFiles[0].text;
})();
test.after(() => esbuild.stop());

/** Loads the bundle with `port` in storage and returns every URL dialed.
 * With `realSocket` the bundle uses Node's own global WebSocket so it talks to
 * a real server instead of recording the URL. */
async function dialWith(port, { realSocket = false } = {}) {
  const dialed = [];
  const source = await bundle;
  globalThis.WebSocket = realSocket
    ? REAL_WEBSOCKET
    : class {
        // connect() short-circuits on socket?.readyState === WebSocket.OPEN,
        // so the statics must exist or `undefined === undefined` makes it
        // return before dialing.
        static CONNECTING = 0;
        static OPEN = 1;
        static CLOSING = 2;
        static CLOSED = 3;
        constructor(url) { dialed.push(url); this.readyState = 0; }
        send() {}
        close() {}
        set onopen(_) {}
        set onmessage(_) {}
        set onerror(_) {}
        set onclose(_) {} // never fires, so no reconnect loop is scheduled
      };
  globalThis.browser = {
    storage: {
      local: { get: async (d) => ({ ...d, port, token: "test-token" }), set: async () => {} },
      onChanged: { addListener: () => {} },
    },
    runtime: { id: "t@t", getManifest: () => ({ version: "0.0.0" }), openOptionsPage: async () => {} },
    action: {
      setBadgeText: async () => {}, setBadgeBackgroundColor: async () => {},
      setBadgeTextColor: async () => {}, setTitle: async () => {},
      onClicked: { addListener: () => {} },
    },
    alarms: { create: async () => {}, onAlarm: { addListener: () => {} } },
    tabs: { query: async () => [], create: async () => ({}) },
    scripting: { executeScript: async () => [] },
  };
  new Function(source)();
  await new Promise((r) => setTimeout(r, 60));
  return dialed;
}

test("a hostile stored port cannot retarget the connection", async () => {
  for (const hostile of ["@evil.example/", "8788.evil.example", "80", {}, null, NaN, 70000, -1]) {
    const dialed = await dialWith(hostile);
    assert.ok(dialed.length > 0, `stored port ${JSON.stringify(hostile)} dialed nothing`);
    for (const url of dialed) {
      assert.match(url, /^ws:\/\/127\.0\.0\.1:\d+$/, `stored port ${JSON.stringify(hostile)} dialed ${url}`);
    }
  }
});

test("a valid stored port is honoured", async () => {
  const dialed = await dialWith(9911);
  assert.ok(dialed.length > 0, "extension never dialed");
  assert.equal(dialed[0], "ws://127.0.0.1:9911");
});

// A real socket, proving the loopback hello path still works end to end.
test("a loopback server still receives the hello", async () => {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  await new Promise((resolve) => server.once("listening", resolve));
  const port = server.address().port;
  const hello = await new Promise((resolve, reject) => {
    const giveUp = setTimeout(() => reject(new Error("no hello received")), 15_000);
    server.on("connection", (ws) => {
      ws.on("message", (raw) => {
        clearTimeout(giveUp);
        resolve(JSON.parse(String(raw)));
        // 4001 sets authBlocked in the extension, so it does not schedule a
        // reconnect and keeps this test from leaving timers behind.
        ws.close(4001, "test complete");
      });
    });
    dialWith(port, { realSocket: true }).catch(reject);
  });
  assert.equal(hello.type, "hello");
  assert.equal(hello.protocolVersion, 1);
  assert.equal(typeof hello.token, "string");
  server.close();
});
