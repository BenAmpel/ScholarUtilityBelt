import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import zlib from "node:zlib";

// Regression for the uninstall feedback "Q1, Q2 and IF are not visible": a bundled list that failed to load once was
// remembered as empty for the rest of the page, and quartile lists written by the service worker after the page
// rendered never reached the rows already on screen.

globalThis.atob = (b64) => Buffer.from(b64, "base64").toString("binary");
globalThis.window = {};
globalThis.chrome = { runtime: { getURL: (p) => `chrome-ext://${p}` } };

let failing = true;
globalThis.fetch = async () => {
  if (failing) return { ok: false, status: 500, body: null, text: async () => "" };
  const gz = zlib.gzipSync(Buffer.from("Journal Name,Impact\nNature,64.8\n"));
  return new Response(gz);
};

const { loadImpactIndex } = await import("../src/content/data-loader.js");

describe("bundled list loaders", () => {
  it("retry after a failed load instead of caching an empty list", async () => {
    const first = await loadImpactIndex();
    assert.equal(first.size, 0);
    failing = false;
    const second = await loadImpactIndex();
    assert.equal(second.size, 1);
    assert.equal(second.get("nature"), 64.8);
  });
});

describe("content script storage listener", () => {
  const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
  it("re-renders when the quartile or impact lists change in storage", () => {
    const start = src.indexOf("onChanged.addListener(async (changes, area)");
    assert.ok(start > 0, "listener not found");
    const body = src.slice(start, start + 1500);
    for (const key of ["qualityQuartilesIndex", "qualityQuartilesMeta", "qualityJcrIndex", "qualityJcrMeta"]) {
      assert.ok(body.includes(`changes.${key}`), `${key} not watched`);
    }
  });
  it("does not keep a compiled index built without its bundled lists", () => {
    assert.match(src, /listsLoaded \? \{ qIndex: state\.qIndex, settingsHash \} : null/);
  });
  it("re-checks the stored lists once the listener exists, to catch a seed that landed during the first render", () => {
    const start = src.indexOf("window.__suRecheckLists = afterListener");
    assert.ok(start > 0, "re-check not defined");
    const defn = src.slice(start - 700, start);
    assert.ok(defn.includes("qualityQuartilesMeta") && defn.includes("qualityJcrMeta"), "re-check does not read list meta");
    const reg = src.indexOf("onChanged.addListener(async (changes, area)");
    const call = src.indexOf("window.__suRecheckLists?.()");
    assert.ok(call > reg, "re-check must run after the listener is registered");
  });
});
