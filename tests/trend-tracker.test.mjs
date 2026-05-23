import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { detectMethods, METHOD_KEYWORDS } = await import("../src/content/trend-methods.js");

describe("METHOD_KEYWORDS", () => {
  it("contains at least 10 keywords", () => {
    assert.ok(METHOD_KEYWORDS.length >= 10);
  });

  it("includes known methods", () => {
    const set = new Set(METHOD_KEYWORDS.map(k => k.toLowerCase()));
    assert.ok(set.has("meta-analysis"));
    assert.ok(set.has("deep learning"));
    assert.ok(set.has("llm"));
  });
});

describe("detectMethods", () => {
  function mockElements(texts) {
    return texts.map(t => ({ textContent: t }));
  }

  it("returns empty map for no matches", () => {
    const result = detectMethods(mockElements(["Some random text"]));
    assert.equal(result.size, 0);
  });

  it("detects a single method keyword", () => {
    const result = detectMethods(mockElements(["A systematic review of recent advances"]));
    assert.equal(result.get("systematic review"), 1);
  });

  it("counts multiple occurrences across elements", () => {
    const result = detectMethods(mockElements([
      "Deep learning for image classification",
      "Using deep learning with transformers"
    ]));
    assert.equal(result.get("deep learning"), 2);
  });

  it("is case-insensitive", () => {
    const result = detectMethods(mockElements(["A META-ANALYSIS of trials"]));
    assert.equal(result.get("meta-analysis"), 1);
  });

  it("detects multiple different methods", () => {
    const result = detectMethods(mockElements([
      "A survey using machine learning and NLP techniques"
    ]));
    assert.ok(result.has("survey"));
    assert.ok(result.has("machine learning"));
    assert.ok(result.has("nlp"));
  });

  it("handles empty input", () => {
    const result = detectMethods([]);
    assert.equal(result.size, 0);
  });

  it("handles elements with no textContent", () => {
    const result = detectMethods([{ textContent: null }, { textContent: undefined }]);
    assert.equal(result.size, 0);
  });
});

const { normalizeCacheKey, extractYearsFromResults, computeTrend } = await import("../src/content/trend-tracker.js");

describe("normalizeCacheKey", () => {
  it("lowercases and trims", () => {
    assert.equal(normalizeCacheKey("  Deep Learning  "), "deep learning");
  });

  it("sorts words alphabetically", () => {
    assert.equal(normalizeCacheKey("machine learning survey"), "learning machine survey");
  });

  it("deduplicates words", () => {
    assert.equal(normalizeCacheKey("learning learning deep"), "deep learning");
  });

  it("returns empty string for empty input", () => {
    assert.equal(normalizeCacheKey(""), "");
    assert.equal(normalizeCacheKey(null), "");
  });
});

describe("extractYearsFromResults", () => {
  function mockResults(yearStrings) {
    return yearStrings.map(y => ({
      querySelector: (sel) => {
        if (sel === ".gs_a") return { textContent: `B Ampel - Journal of AI, ${y} - Springer` };
        return null;
      }
    }));
  }

  it("extracts years from Scholar result metadata", () => {
    const counts = extractYearsFromResults(mockResults(["2023", "2023", "2021"]));
    assert.equal(counts["2023"], 2);
    assert.equal(counts["2021"], 1);
  });

  it("handles results with no year", () => {
    const counts = extractYearsFromResults([{
      querySelector: () => ({ textContent: "B Ampel - Some Journal - Publisher" })
    }]);
    assert.deepEqual(counts, {});
  });

  it("returns empty object for empty input", () => {
    assert.deepEqual(extractYearsFromResults([]), {});
  });

  it("ignores years before 1900 and after 2100", () => {
    const counts = extractYearsFromResults(mockResults(["1800", "2200", "2020"]));
    assert.equal(counts["2020"], 1);
    assert.equal(counts["1800"], undefined);
    assert.equal(counts["2200"], undefined);
  });
});

describe("computeTrend", () => {
  it("returns 'up' for increasing counts", () => {
    assert.equal(computeTrend([10, 20, 30, 40, 50]), "up");
  });

  it("returns 'down' for decreasing counts", () => {
    assert.equal(computeTrend([50, 40, 30, 20, 10]), "down");
  });

  it("returns 'flat' for stable counts", () => {
    assert.equal(computeTrend([100, 101, 99, 100, 102]), "flat");
  });

  it("returns 'flat' for empty or single-element arrays", () => {
    assert.equal(computeTrend([]), "flat");
    assert.equal(computeTrend([42]), "flat");
  });

  it("returns 'up' when slope > 10% of mean", () => {
    assert.equal(computeTrend([100, 100, 100, 100, 130]), "up");
  });

  it("returns 'down' when slope < -10% of mean", () => {
    assert.equal(computeTrend([130, 100, 100, 100, 100]), "down");
  });
});

const { readCache, writeCache, evictCache, CACHE_MAX, CACHE_TTL_MS } = await import("../src/content/trend-tracker.js");

describe("cache logic", () => {
  let storage;

  function mockChromeStorage() {
    storage = {};
    globalThis.chrome = {
      storage: {
        local: {
          get: (keys) => Promise.resolve(
            typeof keys === "string" ? { [keys]: storage[keys] } :
            Array.isArray(keys) ? Object.fromEntries(keys.map(k => [k, storage[k]])) :
            Object.fromEntries(Object.entries(keys).map(([k, def]) => [k, storage[k] ?? def]))
          ),
          set: (obj) => { Object.assign(storage, obj); return Promise.resolve(); }
        }
      }
    };
  }

  it("CACHE_MAX is 200", () => {
    assert.equal(CACHE_MAX, 200);
  });

  it("CACHE_TTL_MS is 7 days", () => {
    assert.equal(CACHE_TTL_MS, 7 * 24 * 60 * 60 * 1000);
  });

  it("readCache returns null for missing key", async () => {
    mockChromeStorage();
    const result = await readCache("nonexistent query");
    assert.equal(result, null);
  });

  it("writeCache stores and readCache retrieves", async () => {
    mockChromeStorage();
    const data = { concepts: [{ name: "AI", trend: "up", worksCount: 100 }], yearlyWorks: { "2023": 5 } };
    await writeCache("test query", data);
    const result = await readCache("test query");
    assert.deepEqual(result.concepts, data.concepts);
    assert.deepEqual(result.yearlyWorks, data.yearlyWorks);
  });

  it("readCache returns null for expired entries", async () => {
    mockChromeStorage();
    const data = { concepts: [], yearlyWorks: {} };
    await writeCache("old query", data);
    const cache = storage.trendCache;
    const key = normalizeCacheKey("old query");
    cache[key].ts = Date.now() - CACHE_TTL_MS - 1000;
    storage.trendCache = cache;
    const result = await readCache("old query");
    assert.equal(result, null);
  });

  it("evictCache removes oldest-accessed entry when over limit", async () => {
    mockChromeStorage();
    storage.trendCache = {};
    for (let i = 0; i < CACHE_MAX + 5; i++) {
      storage.trendCache[`query${i}`] = {
        ts: Date.now(),
        lastAccess: Date.now() - (CACHE_MAX + 5 - i) * 1000,
        concepts: [],
        yearlyWorks: {}
      };
    }
    await evictCache();
    const keys = Object.keys(storage.trendCache);
    assert.ok(keys.length <= CACHE_MAX);
  });
});

const { fetchConceptTrends, OPENALEX_RATE_LIMIT_MS } = await import("../src/content/trend-tracker.js");

describe("fetchConceptTrends", () => {
  it("OPENALEX_RATE_LIMIT_MS is 1000", () => {
    assert.equal(OPENALEX_RATE_LIMIT_MS, 1000);
  });

  it("returns concepts with trend arrows from mock API response", async () => {
    const currentYear = new Date().getFullYear();
    const mockResponse = {
      results: [
        {
          display_name: "Machine Learning",
          counts_by_year: [
            { year: currentYear, works_count: 500 },
            { year: currentYear - 1, works_count: 400 },
            { year: currentYear - 2, works_count: 300 },
            { year: currentYear - 3, works_count: 200 },
            { year: currentYear - 4, works_count: 100 }
          ]
        },
        {
          display_name: "Data Mining",
          counts_by_year: [
            { year: currentYear, works_count: 100 },
            { year: currentYear - 1, works_count: 100 },
            { year: currentYear - 2, works_count: 100 },
            { year: currentYear - 3, works_count: 100 },
            { year: currentYear - 4, works_count: 100 }
          ]
        }
      ]
    };

    globalThis.fetch = () => Promise.resolve({
      ok: true,
      json: () => Promise.resolve(mockResponse)
    });

    const concepts = await fetchConceptTrends("machine learning");
    assert.ok(concepts.length >= 1);
    const ml = concepts.find(c => c.name === "Machine Learning");
    assert.ok(ml);
    assert.equal(ml.trend, "up");
    const dm = concepts.find(c => c.name === "Data Mining");
    assert.ok(dm);
    assert.equal(dm.trend, "flat");
  });

  it("returns empty array on fetch failure", async () => {
    globalThis.fetch = () => Promise.reject(new Error("network error"));
    const concepts = await fetchConceptTrends("anything");
    assert.deepEqual(concepts, []);
  });

  it("limits to 8 concepts", async () => {
    const currentYear = new Date().getFullYear();
    const results = Array.from({ length: 15 }, (_, i) => ({
      display_name: `Concept ${i}`,
      counts_by_year: [{ year: currentYear, works_count: 100 }]
    }));
    globalThis.fetch = () => Promise.resolve({
      ok: true,
      json: () => Promise.resolve({ results })
    });
    const concepts = await fetchConceptTrends("query");
    assert.ok(concepts.length <= 8);
  });
});
