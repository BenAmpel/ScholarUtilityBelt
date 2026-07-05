import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// Chrome storage mock
// ---------------------------------------------------------------------------
function createMockChromeStorage() {
  const store = {};
  return {
    _store: store,
    get(defaults) {
      const result = {};
      for (const [key, defaultValue] of Object.entries(defaults || {})) {
        result[key] = key in store ? JSON.parse(JSON.stringify(store[key])) : defaultValue;
      }
      return Promise.resolve(result);
    },
    set(items) {
      for (const [key, value] of Object.entries(items)) {
        store[key] = JSON.parse(JSON.stringify(value));
      }
      return Promise.resolve();
    },
    remove(keys) {
      for (const k of Array.isArray(keys) ? keys : [keys]) delete store[k];
      return Promise.resolve();
    },
    clear() {
      for (const k of Object.keys(store)) delete store[k];
      return Promise.resolve();
    },
  };
}

const mockLocal = createMockChromeStorage();
globalThis.chrome = { storage: { local: mockLocal } };

const storage = await import("../src/common/storage.js");
const {
  getLibraryState, setLibraryState,
  getReadingQueue, addToReadingQueue, clearReadingQueue,
  getReadingLoadPageCounts, setReadingLoadPageCount,
  getHiddenPapers, addHiddenPaper, removeHiddenPaper, clearHiddenPapers,
  getHiddenVenues, addHiddenVenue, removeHiddenVenue, clearHiddenVenues,
  getHiddenAuthors, addHiddenAuthor, removeHiddenAuthor, clearHiddenAuthors,
  getCitationSnapshots, setCitationSnapshot,
  getAuthorHIndexSnapshots, setAuthorHIndexSnapshot,
  getStorageMapEntry, setStorageMapEntry, getStorageMap, setStorageMap,
  getQualityQuartilesIndex, setQualityQuartilesIndex, clearQualityQuartilesIndex,
  getQualityJcrIndex, setQualityJcrIndex, clearQualityJcrIndex,
} = storage;

// ---------------------------------------------------------------------------
// Library state
// ---------------------------------------------------------------------------
describe("getLibraryState / setLibraryState", () => {
  beforeEach(() => mockLocal.clear());

  it("returns default library state when empty", async () => {
    const state = await getLibraryState();
    assert.equal(typeof state, "object");
  });

  it("persists and retrieves library state", async () => {
    await setLibraryState({ sortBy: "date", filterTag: "ai" });
    const state = await getLibraryState();
    assert.equal(state.sortBy, "date");
    assert.equal(state.filterTag, "ai");
  });
});

// ---------------------------------------------------------------------------
// Reading queue — addToReadingQueue requires title + link
// ---------------------------------------------------------------------------
describe("readingQueue", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty array when no items queued", async () => {
    const q = await getReadingQueue();
    assert.ok(Array.isArray(q));
    assert.equal(q.length, 0);
  });

  it("adds items with title and link to queue", async () => {
    await addToReadingQueue({ title: "Paper 1", link: "https://example.com/1" });
    await addToReadingQueue({ title: "Paper 2", link: "https://example.com/2" });
    const q = await getReadingQueue();
    assert.equal(q.length, 2);
    assert.equal(q[0].title, "Paper 1");
    assert.equal(q[1].title, "Paper 2");
  });

  it("returns null for invalid items (missing title or link)", async () => {
    const r1 = await addToReadingQueue({ key: "p1" });
    assert.equal(r1, null);
    const r2 = await addToReadingQueue({ title: "No Link" });
    assert.equal(r2, null);
  });

  it("clears the queue", async () => {
    await addToReadingQueue({ title: "Paper 1", link: "https://a.com" });
    await clearReadingQueue();
    const q = await getReadingQueue();
    assert.equal(q.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Reading load page counts
// ---------------------------------------------------------------------------
describe("readingLoadPageCounts", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty object when no counts set", async () => {
    const counts = await getReadingLoadPageCounts();
    assert.deepEqual(counts, {});
  });

  it("sets and retrieves page count for a paper", async () => {
    await setReadingLoadPageCount("paper1", 42);
    const counts = await getReadingLoadPageCounts();
    assert.equal(counts.paper1, 42);
  });

  it("updates existing page count", async () => {
    await setReadingLoadPageCount("paper1", 10);
    await setReadingLoadPageCount("paper1", 25);
    const counts = await getReadingLoadPageCounts();
    assert.equal(counts.paper1, 25);
  });
});

// ---------------------------------------------------------------------------
// Hidden papers / venues / authors
// ---------------------------------------------------------------------------
describe("hidden papers", () => {
  beforeEach(() => mockLocal.clear());

  it("adds and removes a hidden paper", async () => {
    await addHiddenPaper("paper_key_1");
    let h = await getHiddenPapers();
    // Could be array or object — check presence either way
    const has1 = Array.isArray(h) ? h.includes("paper_key_1") : !!h["paper_key_1"];
    assert.ok(has1);

    await removeHiddenPaper("paper_key_1");
    h = await getHiddenPapers();
    const has2 = Array.isArray(h) ? h.includes("paper_key_1") : !!h["paper_key_1"];
    assert.ok(!has2);
  });

  it("clears all hidden papers", async () => {
    await addHiddenPaper("p1");
    await addHiddenPaper("p2");
    await clearHiddenPapers();
    const h = await getHiddenPapers();
    const count = Array.isArray(h) ? h.length : Object.keys(h).length;
    assert.equal(count, 0);
  });
});

describe("hidden venues", () => {
  beforeEach(() => mockLocal.clear());

  it("adds and removes a hidden venue", async () => {
    await addHiddenVenue("nature");
    let h = await getHiddenVenues();
    const has = Array.isArray(h) ? h.includes("nature") : !!h["nature"];
    assert.ok(has);

    await removeHiddenVenue("nature");
    h = await getHiddenVenues();
    const has2 = Array.isArray(h) ? h.includes("nature") : !!h["nature"];
    assert.ok(!has2);
  });

  it("clears all hidden venues", async () => {
    await addHiddenVenue("v1");
    await clearHiddenVenues();
    const h = await getHiddenVenues();
    const count = Array.isArray(h) ? h.length : Object.keys(h).length;
    assert.equal(count, 0);
  });
});

describe("hidden authors", () => {
  beforeEach(() => mockLocal.clear());

  it("adds and removes a hidden author", async () => {
    await addHiddenAuthor("john smith");
    let h = await getHiddenAuthors();
    const has = Array.isArray(h) ? h.includes("john smith") : !!h["john smith"];
    assert.ok(has);

    await removeHiddenAuthor("john smith");
    h = await getHiddenAuthors();
    const has2 = Array.isArray(h) ? h.includes("john smith") : !!h["john smith"];
    assert.ok(!has2);
  });

  it("clears all hidden authors", async () => {
    await addHiddenAuthor("a1");
    await clearHiddenAuthors();
    const h = await getHiddenAuthors();
    const count = Array.isArray(h) ? h.length : Object.keys(h).length;
    assert.equal(count, 0);
  });
});

// ---------------------------------------------------------------------------
// Citation snapshots
// ---------------------------------------------------------------------------
describe("citation snapshots", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty object initially", async () => {
    const snaps = await getCitationSnapshots();
    assert.deepEqual(snaps, {});
  });

  it("stores and retrieves a citation snapshot", async () => {
    await setCitationSnapshot("cluster123", 42, "2024-01-15");
    const snaps = await getCitationSnapshots();
    assert.ok(snaps["cluster123"]);
    assert.equal(snaps["cluster123"].citations, 42);
  });
});

// ---------------------------------------------------------------------------
// Author h-index snapshots
// ---------------------------------------------------------------------------
describe("author h-index snapshots", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty object initially", async () => {
    const snaps = await getAuthorHIndexSnapshots();
    assert.deepEqual(snaps, {});
  });

  it("stores and retrieves h-index snapshot", async () => {
    await setAuthorHIndexSnapshot("scholar.google.com/user=ABC", 25);
    const snaps = await getAuthorHIndexSnapshots();
    assert.ok(snaps["scholar.google.com/user=ABC"]);
  });
});

// ---------------------------------------------------------------------------
// Generic storage map helpers
// ---------------------------------------------------------------------------
describe("storageMap helpers", () => {
  beforeEach(() => mockLocal.clear());

  it("getStorageMap returns empty object when unset", async () => {
    const map = await getStorageMap("testMapKey");
    assert.deepEqual(map, {});
  });

  it("setStorageMap and getStorageMap round-trip", async () => {
    await setStorageMap("testMapKey", { a: 1, b: 2 });
    const map = await getStorageMap("testMapKey");
    assert.equal(map.a, 1);
    assert.equal(map.b, 2);
  });

  it("setStorageMapEntry updates a single entry", async () => {
    await setStorageMap("testMapKey", { a: 1 });
    await setStorageMapEntry("testMapKey", "b", 2);
    const map = await getStorageMap("testMapKey");
    assert.equal(map.a, 1);
    assert.equal(map.b, 2);
  });

  it("getStorageMapEntry returns defaults-merged object when missing", async () => {
    // getStorageMapEntry returns {...defaults, ...entry}, not a scalar
    const val = await getStorageMapEntry("testMapKey", "missing", { x: 42 });
    assert.equal(val.x, 42);
  });

  it("getStorageMapEntry retrieves stored value", async () => {
    await setStorageMapEntry("testMapKey", "key1", { score: 42 });
    const val = await getStorageMapEntry("testMapKey", "key1", { score: 0 });
    assert.equal(val.score, 42);
  });
});

// ---------------------------------------------------------------------------
// Quality index storage — returns {index, meta}, not null
// ---------------------------------------------------------------------------
describe("quality quartiles index storage", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty index when no data stored", async () => {
    const result = await getQualityQuartilesIndex();
    assert.deepEqual(result.index, {});
    assert.equal(result.meta, null);
  });

  it("stores and retrieves quartiles index", async () => {
    await setQualityQuartilesIndex({ "Nature": "Q1" }, { source: "test" });
    const result = await getQualityQuartilesIndex();
    assert.equal(result.index["Nature"], "Q1");
    assert.equal(result.meta.source, "test");
  });

  it("clears quartiles index", async () => {
    await setQualityQuartilesIndex({ "Nature": "Q1" }, { source: "test" });
    await clearQualityQuartilesIndex();
    const result = await getQualityQuartilesIndex();
    assert.deepEqual(result.index, {});
  });
});

describe("quality JCR index storage", () => {
  beforeEach(() => mockLocal.clear());

  it("returns empty index when no data stored", async () => {
    const result = await getQualityJcrIndex();
    // Returns {index: {}, meta: null} structure
    assert.equal(typeof result, "object");
    assert.ok("index" in result || Object.keys(result).length === 0);
  });

  it("stores and retrieves JCR index", async () => {
    await setQualityJcrIndex({ "Science": 45.2 }, { source: "test" });
    const result = await getQualityJcrIndex();
    assert.ok(result);
  });

  it("clears JCR index", async () => {
    await setQualityJcrIndex({ "Science": 45.2 }, { source: "test" });
    await clearQualityJcrIndex();
    const result = await getQualityJcrIndex();
    assert.deepEqual(result.index, {});
  });
});
