import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const root = new URL("..", import.meta.url);
const swSource = readFileSync(new URL("src/sw.js", root), "utf8");
const bundledFile = swSource.match(/BUNDLED_QUARTILES_FILE = "([^"]+)"/)[1];
const bundled = JSON.parse(readFileSync(new URL(bundledFile, root), "utf8"));

// Runs sw.js in a sandbox with a fake chrome, fires every onInstalled listener for an update, and returns storage.
async function runUpdate(initialLocal) {
  const local = structuredClone(initialLocal);
  const listeners = [];
  const event = () => ({ addListener() {}, removeListener() {}, hasListener: () => false });
  const area = (store) => ({
    get: async (q) => {
      if (q == null) return { ...store };
      if (typeof q === "string") return q in store ? { [q]: store[q] } : {};
      if (Array.isArray(q)) return Object.fromEntries(q.filter((k) => k in store).map((k) => [k, store[k]]));
      return Object.fromEntries(Object.entries(q).map(([k, d]) => [k, k in store ? store[k] : d]));
    },
    set: async (o) => { Object.assign(store, o); },
    remove: async (ks) => { for (const k of [].concat(ks)) delete store[k]; },
    setAccessLevel: async () => {},
    onChanged: event(),
  });
  const onInstalled = { addListener: (f) => listeners.push(f) };
  const chrome = new Proxy({
    runtime: new Proxy({ onInstalled, getURL: (p) => `chrome-extension://test/${p}`, getManifest: () => ({ version: "1.1.0" }), id: "test" }, { get: (t, k) => (k in t ? t[k] : event()) }),
    storage: { local: area(local), sync: area({}), session: area({}), onChanged: event() },
  }, { get: (t, k) => (k in t ? t[k] : new Proxy({}, { get: () => event() })) });
  const fetch = async (url) => {
    const path = String(url).replace("chrome-extension://test/", "");
    try {
      const body = readFileSync(new URL(path, root), "utf8");
      return { ok: true, status: 200, json: async () => JSON.parse(body), text: async () => body };
    } catch {
      return { ok: false, status: 404, json: async () => ({}), text: async () => "" };
    }
  };
  const sandbox = {
    chrome, fetch, console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, URL, Date, Promise, JSON,
    ExtPay: () => ({ startBackground() {}, getUser: async () => ({ paid: false }), onPaid: event(), onTrialStarted: event() }),
    SUEntitlement: { decideGrandfathered: (_r, g) => g },
  };
  sandbox.self = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(swSource, sandbox);
  for (const f of listeners) await Promise.resolve(f({ reason: "update", previousVersion: "1.0.2" })).catch(() => {});
  return local;
}

describe("service worker: bundled SJR quartiles on update", () => {
  it("seeds an empty install", async () => {
    const out = await runUpdate({});
    assert.equal(out.qualityQuartilesMeta?.year, bundled.meta.year);
    assert.ok(Object.keys(out.qualityQuartilesIndex).length > 1000);
  });
  it("replaces an older bundled snapshot with the newer bundled one", async () => {
    const out = await runUpdate({ qualityQuartilesIndex: { "old journal": "Q1" }, qualityQuartilesMeta: { ...bundled.meta, year: bundled.meta.year - 1 } });
    assert.equal(out.qualityQuartilesMeta.year, bundled.meta.year);
    assert.ok(!("old journal" in out.qualityQuartilesIndex));
  });
  it("never replaces quartiles the user imported in Options", async () => {
    const mine = { qualityQuartilesIndex: { "my journal": "Q2" }, qualityQuartilesMeta: { source: "SCImago (SJR export)", year: 2000 } };
    const out = await runUpdate(mine);
    assert.deepEqual(out.qualityQuartilesIndex, mine.qualityQuartilesIndex);
  });
});
