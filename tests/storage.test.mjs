import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// Mock chrome.storage.local before importing the module
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
      for (const k of Array.isArray(keys) ? keys : [keys]) {
        delete store[k];
      }
      return Promise.resolve();
    },
    clear() {
      for (const k of Object.keys(store)) delete store[k];
      return Promise.resolve();
    },
  };
}

// Inject a global chrome stub so the module can load.
const mockLocal = createMockChromeStorage();
globalThis.chrome = { storage: { local: mockLocal } };

const storage = await import("../src/common/storage.js");
const {
  csvToTags,
  uniqTags,
  DEFAULT_SETTINGS,
  getSettings,
  setSettings,
  getSavedPapers,
  upsertPaper,
  removePaper,
  getStorageValue,
  setStorageValue,
  removeStorageKeys,
  batchGetStorage,
} = storage;

// ---------------------------------------------------------------------------
// csvToTags (pure function)
// ---------------------------------------------------------------------------
describe("csvToTags", () => {
  it("splits comma-separated values and trims", () => {
    assert.deepEqual(csvToTags("ai, ml, nlp"), ["ai", "ml", "nlp"]);
  });

  it("filters out empty entries", () => {
    assert.deepEqual(csvToTags(",a,,b,"), ["a", "b"]);
  });

  it("handles empty/null input", () => {
    assert.deepEqual(csvToTags(""), []);
    assert.deepEqual(csvToTags(null), []);
    assert.deepEqual(csvToTags(undefined), []);
  });

  it("handles single tag", () => {
    assert.deepEqual(csvToTags("machine-learning"), ["machine-learning"]);
  });
});

// ---------------------------------------------------------------------------
// uniqTags (pure function)
// ---------------------------------------------------------------------------
describe("uniqTags", () => {
  it("removes duplicate tags case-insensitively", () => {
    assert.deepEqual(uniqTags(["AI", "ai", "ML"]), ["AI", "ML"]);
  });

  it("preserves original casing of first occurrence", () => {
    const result = uniqTags(["NLP", "nlp", "Nlp"]);
    assert.deepEqual(result, ["NLP"]);
  });

  it("filters out empty/null entries", () => {
    assert.deepEqual(uniqTags(["a", "", null, "b", undefined]), ["a", "b"]);
  });

  it("handles empty/null input", () => {
    assert.deepEqual(uniqTags([]), []);
    assert.deepEqual(uniqTags(null), []);
    assert.deepEqual(uniqTags(undefined), []);
  });
});

// ---------------------------------------------------------------------------
// DEFAULT_SETTINGS
// ---------------------------------------------------------------------------
describe("DEFAULT_SETTINGS", () => {
  it("is a non-empty object", () => {
    assert.equal(typeof DEFAULT_SETTINGS, "object");
    assert.ok(Object.keys(DEFAULT_SETTINGS).length > 0);
  });

  it("has expected keys", () => {
    assert.equal(DEFAULT_SETTINGS.highlightSaved, true);
    assert.equal(DEFAULT_SETTINGS.showQualityBadges, true);
    assert.equal(typeof DEFAULT_SETTINGS.qualityBadgeKinds, "object");
  });
});

// ---------------------------------------------------------------------------
// Storage functions (with chrome.storage.local mock)
// ---------------------------------------------------------------------------
describe("getSettings / setSettings", () => {
  beforeEach(() => {
    mockLocal.clear();
  });

  it("returns defaults when storage is empty", async () => {
    const settings = await getSettings();
    assert.equal(settings.highlightSaved, true);
    assert.equal(settings.showQualityBadges, true);
  });

  it("persists and retrieves settings", async () => {
    await setSettings({ highlightSaved: false });
    const settings = await getSettings();
    assert.equal(settings.highlightSaved, false);
    // Other defaults still present
    assert.equal(settings.showQualityBadges, true);
  });

  it("merges with existing settings", async () => {
    await setSettings({ highlightSaved: false });
    await setSettings({ showQualityBadges: false });
    const settings = await getSettings();
    assert.equal(settings.highlightSaved, false);
    assert.equal(settings.showQualityBadges, false);
  });
});

describe("getSavedPapers / upsertPaper / removePaper", () => {
  beforeEach(() => {
    mockLocal.clear();
  });

  it("returns empty object when no papers saved", async () => {
    const papers = await getSavedPapers();
    assert.deepEqual(papers, {});
  });

  it("upserts and retrieves a paper", async () => {
    const paper = { key: "abc123", title: "Test Paper", authors: "A Smith" };
    const result = await upsertPaper(paper);
    assert.equal(result.key, "abc123");
    assert.equal(result.title, "Test Paper");
    assert.ok(result.savedAt);
    assert.ok(result.updatedAt);

    const papers = await getSavedPapers();
    assert.ok(papers["abc123"]);
    assert.equal(papers["abc123"].title, "Test Paper");
  });

  it("merges fields on re-upsert, preserves savedAt", async () => {
    await upsertPaper({ key: "p1", title: "V1" });
    const first = (await getSavedPapers())["p1"];

    await upsertPaper({ key: "p1", title: "V2", venue: "Nature" });
    const second = (await getSavedPapers())["p1"];

    assert.equal(second.title, "V2");
    assert.equal(second.venue, "Nature");
    assert.equal(second.savedAt, first.savedAt); // preserved
  });

  it("removes a paper", async () => {
    await upsertPaper({ key: "r1", title: "Remove Me" });
    const removed = await removePaper("r1");
    assert.equal(removed, true);
    const papers = await getSavedPapers();
    assert.equal(papers["r1"], undefined);
  });

  it("returns false when removing non-existent paper", async () => {
    const removed = await removePaper("nonexistent");
    assert.equal(removed, false);
  });

  it("throws when paper has no key", async () => {
    await assert.rejects(() => upsertPaper({}), { message: /paper\.key is required/ });
    await assert.rejects(() => upsertPaper(null), /paper\.key is required|Cannot read/);
  });
});

describe("batchGetStorage", () => {
  beforeEach(() => {
    mockLocal.clear();
  });

  it("returns defaults for missing keys", async () => {
    const result = await batchGetStorage({ foo: "default" });
    assert.equal(result.foo, "default");
  });

  it("returns stored values", async () => {
    await mockLocal.set({ foo: "bar" });
    const result = await batchGetStorage({ foo: "default" });
    assert.equal(result.foo, "bar");
  });
});

describe("getStorageValue / setStorageValue / removeStorageKeys", () => {
  beforeEach(() => {
    mockLocal.clear();
  });

  it("round-trips a value", async () => {
    await setStorageValue("testKey", 42);
    const val = await getStorageValue("testKey", 0);
    assert.equal(val, 42);
  });

  it("returns default when key missing", async () => {
    const val = await getStorageValue("missing", "fallback");
    assert.equal(val, "fallback");
  });

  it("removes keys", async () => {
    await setStorageValue("k1", "v1");
    await removeStorageKeys("k1");
    const val = await getStorageValue("k1", null);
    assert.equal(val, null);
  });
});
