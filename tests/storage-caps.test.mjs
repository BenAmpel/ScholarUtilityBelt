import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

let store;
beforeEach(() => {
  store = {};
  globalThis.chrome = { storage: { local: {
    async get(d) { const o = {}; for (const [k, v] of Object.entries(d || {})) o[k] = k in store ? structuredClone(store[k]) : v; return o; },
    async set(i) { for (const [k, v] of Object.entries(i)) store[k] = structuredClone(v); },
  } } };
});
const s = await import("../src/common/storage.js");

describe("capRecordCount", () => {
  it("returns the same object when it is small enough", () => {
    const m = { a: 1, b: 2 };
    assert.equal(s.capRecordCount(m, 5, (v) => v), m);
  });
  it("keeps the highest scores and drops the rest", () => {
    const m = { a: 1, b: 5, c: 3, d: 4 };
    assert.deepEqual(Object.keys(s.capRecordCount(m, 2, (v) => v)).sort(), ["b", "d"]);
  });
  it("on a tie keeps the later key, and with no score keeps the keys added last", () => {
    assert.deepEqual(Object.keys(s.capRecordCount({ a: 0, b: 0, c: 0 }, 2, null)).sort(), ["b", "c"]);
  });
  it("copes with missing or odd input", () => {
    assert.deepEqual(s.capRecordCount(null, 3, null), {});
    assert.deepEqual(s.capRecordCount("x", 3, null), {});
    assert.deepEqual(s.capRecordCount({ a: undefined, b: null, c: 1 }, 2, (v) => v), { b: null, c: 1 });
  });
});

describe("the maps stop growing", () => {
  it("pageVisitCache keeps the most recently visited pages", async () => {
    for (let i = 0; i < s.PAGE_VISIT_CACHE_MAX + 25; i++) {
      await s.setPageVisitCacheEntry(`page${i}`, [`k${i}`], new Date(2026, 0, 1, 0, 0, i).toISOString());
    }
    const keys = Object.keys(store.pageVisitCache);
    assert.equal(keys.length, s.PAGE_VISIT_CACHE_MAX);
    assert.ok(!keys.includes("page0"), "the oldest was dropped");
    assert.ok(keys.includes(`page${s.PAGE_VISIT_CACHE_MAX + 24}`), "the newest was kept");
  });
  it("citationSnapshots are capped", async () => {
    store.citationSnapshots = Object.fromEntries(Array.from({ length: s.CITATION_SNAPSHOTS_MAX + 10 }, (_, i) => [`c${i}`, { citations: i, date: new Date(2026, 0, 1, 0, 0, i).toISOString(), history: [] }]));
    await s.setCitationSnapshot("fresh", 1, "2027-01-01T00:00:00.000Z");
    assert.equal(Object.keys(store.citationSnapshots).length, s.CITATION_SNAPSHOTS_MAX);
    assert.ok("fresh" in store.citationSnapshots);
    assert.ok(!("c0" in store.citationSnapshots));
  });
  it("authorHIndexSnapshots keeps the most recently seen profiles", async () => {
    store.authorHIndexSnapshots = Object.fromEntries(Array.from({ length: s.AUTHOR_HINDEX_PROFILES_MAX }, (_, i) => [`https://x/${i}`, [{ date: new Date(2026, 0, 1, 0, 0, i).toISOString(), hIndex: 5 }]]));
    await s.setAuthorHIndexSnapshot("https://x/new", 12);
    const keys = Object.keys(store.authorHIndexSnapshots);
    assert.equal(keys.length, s.AUTHOR_HINDEX_PROFILES_MAX);
    assert.ok(keys.includes("https://x/new"));
    assert.ok(!keys.includes("https://x/0"));
  });
});

describe("isDue", () => {
  const H = 60 * 60 * 1000;
  it("is due when never run, and after the interval", () => {
    assert.equal(s.isDue(0), true);
    assert.equal(s.isDue(undefined), true);
    assert.equal(s.isDue(1000, 24 * H, 1000 + 24 * H), true);
  });
  it("is not due inside the interval", () => {
    assert.equal(s.isDue(1000, 24 * H, 1000 + 23 * H), false);
  });
  it("treats a last-run time in the future (clock change) as due", () => {
    assert.equal(s.isDue(10 * H, 24 * H, H), true);
  });
});
