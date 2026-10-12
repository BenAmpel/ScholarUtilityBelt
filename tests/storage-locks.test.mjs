import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

// An async chrome.storage.local stand-in: every call yields to the event loop, which is
// what lets parallel read-modify-write calls interleave in the real browser.
let store;
const tick = () => new Promise((r) => setTimeout(r, 0));
function installChrome() {
  store = {};
  globalThis.chrome = {
    storage: {
      local: {
        async get(defaults) {
          await tick();
          const out = {};
          for (const [k, v] of Object.entries(defaults || {})) out[k] = k in store ? structuredClone(store[k]) : v;
          return out;
        },
        async set(items) {
          await tick();
          for (const [k, v] of Object.entries(items)) store[k] = structuredClone(v);
        },
        async remove(keys) { await tick(); for (const k of [].concat(keys)) delete store[k]; },
      },
    },
  };
}
beforeEach(installChrome);

const s = await import("../src/common/storage.js");
const N = 10;
const ids = Array.from({ length: N }, (_, i) => i);

describe("withStorageLock", () => {
  it("runs callers one at a time, in order, and keeps going after a failure", async () => {
    const order = [];
    const run = (name, ms, fail) => s.withStorageLock("k", async () => {
      order.push(`start ${name}`);
      await new Promise((r) => setTimeout(r, ms));
      order.push(`end ${name}`);
      if (fail) throw new Error("boom");
      return name;
    });
    const results = await Promise.allSettled([run("a", 15), run("b", 1, true), run("c", 1)]);
    assert.deepEqual(order, ["start a", "end a", "start b", "end b", "start c", "end c"]);
    assert.equal(results[0].value, "a");
    assert.equal(results[1].status, "rejected");
    assert.equal(results[2].value, "c");
  });

  it("does not make different keys wait for each other", async () => {
    const order = [];
    await Promise.all([
      s.withStorageLock("one", async () => { await new Promise((r) => setTimeout(r, 20)); order.push("one"); }),
      s.withStorageLock("two", async () => { order.push("two"); }),
    ]);
    assert.deepEqual(order, ["two", "one"]);
  });

  it("uses Web Locks when the browser has them", async () => {
    const seen = [];
    const original = Object.getOwnPropertyDescriptor(globalThis, "navigator");
    Object.defineProperty(globalThis, "navigator", {
      configurable: true,
      value: { locks: { request: async (name, fn) => { seen.push(name); return fn(); } } },
    });
    try {
      assert.equal(await s.withStorageLock("savedPapers", async () => 42), 42);
      assert.deepEqual(seen, ["su-storage:savedPapers"]);
    } finally {
      if (original) Object.defineProperty(globalThis, "navigator", original); else delete globalThis.navigator;
    }
  });
});

describe("parallel writes keep every entry", () => {
  it("upsertPaper", async () => {
    await Promise.all(ids.map((i) => s.upsertPaper({ key: `p${i}`, title: `Paper ${i}` })));
    assert.equal(Object.keys(store.savedPapers).length, N);
  });
  it("setCitationSnapshot", async () => {
    await Promise.all(ids.map((i) => s.setCitationSnapshot(`c${i}`, 10 + i, "2026-10-12")));
    assert.equal(Object.keys(store.citationSnapshots || store[Object.keys(store)[0]]).length, N);
  });
  it("setReadingLoadPageCount", async () => {
    await Promise.all(ids.map((i) => s.setReadingLoadPageCount(`p${i}`, i + 1)));
    const map = Object.values(store)[0];
    assert.equal(Object.keys(map).length, N);
  });
  it("addHiddenPaper and addHiddenVenue", async () => {
    await Promise.all(ids.flatMap((i) => [s.addHiddenPaper(`p${i}`), s.addHiddenVenue(`v${i}`)]));
    assert.equal(store.hiddenPapers.length, N);
    assert.equal(store.hiddenVenues.length, N);
  });
  it("setPageVisitCacheEntry, setStorageMapEntry and saveCitekeys", async () => {
    await Promise.all(ids.map((i) => s.setPageVisitCacheEntry(`page${i}`, [`a${i}`], "2026-10-12")));
    await Promise.all(ids.map((i) => s.setStorageMapEntry("someMap", `e${i}`, { i })));
    assert.equal(Object.keys(store.someMap).length, N);
    store.savedPapers = Object.fromEntries(ids.map((i) => [`p${i}`, { key: `p${i}` }]));
    await Promise.all(ids.map((i) => s.saveCitekeys({ [`p${i}`]: `key${i}` })));
    assert.equal(Object.values(store.savedPapers).filter((p) => p.citekey).length, N);
  });
  it("mixed upserts and removals on the same map stay consistent", async () => {
    store.savedPapers = Object.fromEntries(ids.map((i) => [`old${i}`, { key: `old${i}` }]));
    await Promise.all([
      ...ids.map((i) => s.upsertPaper({ key: `new${i}`, title: "n" })),
      ...ids.map((i) => s.removePaper(`old${i}`)),
    ]);
    const keys = Object.keys(store.savedPapers).sort();
    assert.deepEqual(keys, ids.map((i) => `new${i}`).sort());
  });
});
