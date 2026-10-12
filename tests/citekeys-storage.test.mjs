import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

const store = {};
globalThis.chrome = {
  storage: {
    local: {
      get: (d) => Promise.resolve(Object.fromEntries(Object.entries(d || {}).map(([k, v]) => [k, k in store ? JSON.parse(JSON.stringify(store[k])) : v]))),
      set: (items) => { for (const [k, v] of Object.entries(items)) store[k] = JSON.parse(JSON.stringify(v)); return Promise.resolve(); },
    },
  },
};
const { saveCitekeys, getSavedPapers } = await import("../src/common/storage.js");

describe("saveCitekeys", () => {
  beforeEach(() => { for (const k of Object.keys(store)) delete store[k]; });
  it("adds keys, never overwrites an existing one and does not recreate removed papers", async () => {
    store.savedPapers = { a: { key: "a", title: "A" }, b: { key: "b", title: "B", citekey: "keep2020" } };
    const n = await saveCitekeys({ a: "a2020x", b: "changed", gone: "ghost2020" });
    assert.equal(n, 1);
    const saved = await getSavedPapers();
    assert.equal(saved.a.citekey, "a2020x");
    assert.equal(saved.b.citekey, "keep2020");
    assert.equal(saved.gone, undefined);
    assert.equal(saved.a.title, "A");
  });
  it("accepts a Map and skips the write when nothing changes", async () => {
    store.savedPapers = { a: { key: "a", citekey: "k" } };
    assert.equal(await saveCitekeys(new Map([["a", "other"]])), 0);
    assert.equal(store.savedPapers.a.citekey, "k");
    assert.equal(await saveCitekeys(null), 0);
  });
});
