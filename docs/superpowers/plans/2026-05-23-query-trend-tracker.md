# Query Trend Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a collapsible sidebar panel to Scholar search results that shows publication year distribution, rising concepts from OpenAlex, and methodology signals detected from page content.

**Architecture:** Two new lazy-loaded ES modules (`trend-tracker.js`, `trend-methods.js`) following the existing `importModuleWithRetry` pattern. All data cached in `chrome.storage.local` with 7-day TTL and 200-entry LRU eviction. The panel injects after the Scholar results column and is gated by a `showTrendTracker` setting.

**Tech Stack:** Vanilla JS (no frameworks), CSS-only bar charts, OpenAlex REST API, `chrome.storage.local`, Node built-in test runner.

---

### File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/content/trend-methods.js` | Create | Method keyword detection — `detectMethods(elements)` and `METHOD_KEYWORDS` |
| `src/content/trend-tracker.js` | Create | Main module — panel DOM, year extraction, OpenAlex API, caching, concept chips |
| `src/content/content.css` | Modify | Add CSS for `.su-trend-*` panel, bars, chips, toggle button |
| `src/common/storage.js` | Modify | Add `showTrendTracker: true` to `DEFAULT_SETTINGS` |
| `src/options/options.html` | Modify | Add toggle row for `showTrendTracker` |
| `src/options/options.js` | Modify | Load/save `showTrendTracker` checkbox |
| `src/content/content.js` | Modify | Add lazy-load of trend-tracker module in `processAll` |
| `manifest.template.json` | Modify | Add trend modules to `web_accessible_resources` |
| `build.js` | Modify | Add two new build targets |
| `tests/trend-tracker.test.mjs` | Create | Unit tests for pure logic functions |

---

### Task 1: Create `trend-methods.js` (method keyword detection)

**Files:**
- Create: `src/content/trend-methods.js`
- Test: `tests/trend-tracker.test.mjs`

- [ ] **Step 1: Write the failing tests for `detectMethods`**

Create `tests/trend-tracker.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: FAIL — module `../src/content/trend-methods.js` does not exist yet.

- [ ] **Step 3: Implement `trend-methods.js`**

Create `src/content/trend-methods.js`:

```javascript
export const METHOD_KEYWORDS = [
  "meta-analysis",
  "systematic review",
  "randomized controlled trial",
  "survey",
  "case study",
  "qualitative",
  "longitudinal",
  "cross-sectional",
  "machine learning",
  "deep learning",
  "nlp",
  "llm"
];

const patterns = METHOD_KEYWORDS.map(kw => ({
  keyword: kw,
  regex: new RegExp("\\b" + kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b", "i")
}));

export function detectMethods(elements) {
  const counts = new Map();
  for (const el of elements) {
    const text = String(el?.textContent || "");
    if (!text) continue;
    for (const { keyword, regex } of patterns) {
      if (regex.test(text)) {
        counts.set(keyword, (counts.get(keyword) || 0) + 1);
      }
    }
  }
  return counts;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/content/trend-methods.js tests/trend-tracker.test.mjs
git commit -m "feat: add method keyword detection module for trend tracker"
```

---

### Task 2: Add trend-tracker pure logic (cache key, year extraction, trend computation)

**Files:**
- Create: `src/content/trend-tracker.js`
- Modify: `tests/trend-tracker.test.mjs`

- [ ] **Step 1: Write failing tests for pure logic functions**

Append to `tests/trend-tracker.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: FAIL — `trend-tracker.js` doesn't exist or doesn't export these functions.

- [ ] **Step 3: Create `trend-tracker.js` with pure logic functions (no DOM yet)**

Create `src/content/trend-tracker.js`:

```javascript
export function normalizeCacheKey(query) {
  const s = String(query || "").toLowerCase().trim();
  if (!s) return "";
  const words = s.split(/\s+/).filter(Boolean);
  return [...new Set(words)].sort().join(" ");
}

export function extractYearsFromResults(resultElements) {
  const counts = {};
  for (const el of resultElements) {
    const meta = el?.querySelector?.(".gs_a");
    if (!meta) continue;
    const text = meta.textContent || "";
    const m = text.match(/\b(19\d{2}|20\d{2})\b/);
    if (m) {
      const year = m[1];
      const y = parseInt(year, 10);
      if (y >= 1900 && y <= 2100) {
        counts[year] = (counts[year] || 0) + 1;
      }
    }
  }
  return counts;
}

export function computeTrend(yearlyCountsArray) {
  const arr = yearlyCountsArray || [];
  if (arr.length < 2) return "flat";
  const n = arr.length;
  const mean = arr.reduce((a, b) => a + b, 0) / n;
  if (mean === 0) return "flat";
  let sumXY = 0;
  let sumX2 = 0;
  const xMean = (n - 1) / 2;
  for (let i = 0; i < n; i++) {
    const dx = i - xMean;
    sumXY += dx * arr[i];
    sumX2 += dx * dx;
  }
  const slope = sumX2 === 0 ? 0 : sumXY / sumX2;
  const threshold = mean * 0.1;
  if (slope > threshold) return "up";
  if (slope < -threshold) return "down";
  return "flat";
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: All tests PASS (both method tests from Task 1 and new logic tests).

- [ ] **Step 5: Commit**

```bash
git add src/content/trend-tracker.js tests/trend-tracker.test.mjs
git commit -m "feat: add trend tracker pure logic — cache keys, year extraction, trend computation"
```

---

### Task 3: Add LRU cache logic to `trend-tracker.js`

**Files:**
- Modify: `src/content/trend-tracker.js`
- Modify: `tests/trend-tracker.test.mjs`

- [ ] **Step 1: Write failing tests for cache logic**

Append to `tests/trend-tracker.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: FAIL — `readCache`, `writeCache`, `evictCache` not exported.

- [ ] **Step 3: Add cache functions to `trend-tracker.js`**

Append to `src/content/trend-tracker.js`:

```javascript
export const CACHE_MAX = 200;
export const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

async function getCache() {
  try {
    const data = await chrome.storage.local.get({ trendCache: {} });
    return data.trendCache || {};
  } catch {
    return {};
  }
}

async function setCache(cache) {
  try {
    await chrome.storage.local.set({ trendCache: cache });
  } catch {}
}

export async function readCache(query) {
  const key = normalizeCacheKey(query);
  if (!key) return null;
  const cache = await getCache();
  const entry = cache[key];
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) return null;
  entry.lastAccess = Date.now();
  cache[key] = entry;
  await setCache(cache);
  return entry;
}

export async function writeCache(query, data) {
  const key = normalizeCacheKey(query);
  if (!key) return;
  const cache = await getCache();
  cache[key] = {
    ts: Date.now(),
    lastAccess: Date.now(),
    concepts: data.concepts || [],
    yearlyWorks: data.yearlyWorks || {}
  };
  await setCache(cache);
  await evictCache();
}

export async function evictCache() {
  const cache = await getCache();
  const keys = Object.keys(cache);
  if (keys.length <= CACHE_MAX) return;
  const sorted = keys.sort((a, b) => (cache[a].lastAccess || 0) - (cache[b].lastAccess || 0));
  const toRemove = sorted.slice(0, keys.length - CACHE_MAX);
  for (const k of toRemove) delete cache[k];
  await setCache(cache);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/content/trend-tracker.js tests/trend-tracker.test.mjs
git commit -m "feat: add LRU cache with 7-day TTL for trend tracker"
```

---

### Task 4: Add OpenAlex API integration to `trend-tracker.js`

**Files:**
- Modify: `src/content/trend-tracker.js`
- Modify: `tests/trend-tracker.test.mjs`

- [ ] **Step 1: Write failing tests for API helpers**

Append to `tests/trend-tracker.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: FAIL — `fetchConceptTrends` not exported.

- [ ] **Step 3: Add `fetchConceptTrends` to `trend-tracker.js`**

Append to `src/content/trend-tracker.js`:

```javascript
export const OPENALEX_RATE_LIMIT_MS = 1000;
let lastApiCallTs = 0;

async function rateLimitedFetch(url) {
  const now = Date.now();
  const wait = OPENALEX_RATE_LIMIT_MS - (now - lastApiCallTs);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastApiCallTs = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    return res;
  } catch (e) {
    clearTimeout(timeout);
    throw e;
  }
}

export async function fetchConceptTrends(query) {
  try {
    const encoded = encodeURIComponent(String(query || "").trim());
    if (!encoded) return [];
    const url = `https://api.openalex.org/concepts?search=${encoded}&per_page=8&mailto=scholar-extension@local`;
    const res = await rateLimitedFetch(url);
    if (!res.ok) return [];
    const data = await res.json();
    const results = data?.results || [];
    const currentYear = new Date().getFullYear();
    return results.slice(0, 8).map(concept => {
      const name = concept.display_name || "Unknown";
      const countsByYear = concept.counts_by_year || [];
      const last5 = [];
      for (let y = currentYear - 4; y <= currentYear; y++) {
        const entry = countsByYear.find(c => c.year === y);
        last5.push(entry ? entry.works_count : 0);
      }
      const totalWorks = last5.reduce((a, b) => a + b, 0);
      return { name, trend: computeTrend(last5), worksCount: totalWorks };
    });
  } catch {
    return [];
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/content/trend-tracker.js tests/trend-tracker.test.mjs
git commit -m "feat: add OpenAlex concept trends API with rate limiting"
```

---

### Task 5: Add panel DOM and UI to `trend-tracker.js`

**Files:**
- Modify: `src/content/trend-tracker.js`
- Modify: `src/content/content.css`

This task adds the DOM construction functions. These are not unit-tested (DOM rendering) — they will be tested manually in the browser.

- [ ] **Step 1: Add `initTrendPanel` and `destroyTrendPanel` to `trend-tracker.js`**

Append to `src/content/trend-tracker.js`:

```javascript
import { detectMethods } from "./trend-methods.js";

let panelEl = null;

function buildYearChart(yearCounts) {
  const years = Object.keys(yearCounts).sort();
  if (years.length === 0) return null;
  const maxCount = Math.max(...Object.values(yearCounts));
  const currentYear = new Date().getFullYear();
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-years";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Year Distribution";
  section.appendChild(heading);
  for (const year of years) {
    const count = yearCounts[year];
    const pct = maxCount > 0 ? Math.round((count / maxCount) * 100) : 0;
    const row = document.createElement("div");
    row.className = "su-trend-bar-row";
    const label = document.createElement("span");
    label.className = "su-trend-bar-label";
    label.textContent = year;
    const barWrap = document.createElement("div");
    barWrap.className = "su-trend-bar-wrap";
    const bar = document.createElement("div");
    bar.className = "su-trend-bar";
    const y = parseInt(year, 10);
    if (y >= currentYear - 1) bar.classList.add("su-trend-bar-recent");
    bar.style.width = pct + "%";
    barWrap.appendChild(bar);
    const countSpan = document.createElement("span");
    countSpan.className = "su-trend-bar-count";
    countSpan.textContent = count;
    row.appendChild(label);
    row.appendChild(barWrap);
    row.appendChild(countSpan);
    section.appendChild(row);
  }
  return section;
}

function buildConceptChips(concepts) {
  if (!concepts || concepts.length === 0) return null;
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-concepts";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Rising Concepts";
  section.appendChild(heading);
  const chipWrap = document.createElement("div");
  chipWrap.className = "su-trend-chip-wrap";
  for (const c of concepts) {
    const chip = document.createElement("button");
    chip.className = "su-trend-chip";
    chip.type = "button";
    const arrow = c.trend === "up" ? "↑" : c.trend === "down" ? "↓" : "→";
    const arrowClass = "su-trend-arrow-" + c.trend;
    chip.innerHTML = `<span class="su-trend-chip-name">${escapeHtml(c.name)}</span> <span class="${arrowClass}">${arrow}</span>`;
    chip.title = `${c.name} (${c.worksCount} works, trending ${c.trend})`;
    chip.addEventListener("click", () => {
      const searchBox = document.querySelector("#gs_hdr_tsi") || document.querySelector("input[name='q']");
      if (searchBox) {
        const current = searchBox.value.trim();
        if (!current.toLowerCase().includes(c.name.toLowerCase())) {
          searchBox.value = current + " " + c.name;
          searchBox.focus();
        }
      }
    });
    chipWrap.appendChild(chip);
  }
  section.appendChild(chipWrap);
  return section;
}

function buildMethodPills(methodCounts) {
  if (!methodCounts || methodCounts.size === 0) return null;
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-methods";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Method Signals";
  section.appendChild(heading);
  const pillWrap = document.createElement("div");
  pillWrap.className = "su-trend-pill-wrap";
  for (const [method, count] of methodCounts) {
    const pill = document.createElement("span");
    pill.className = "su-trend-pill";
    pill.textContent = `${method} (${count})`;
    pillWrap.appendChild(pill);
  }
  section.appendChild(pillWrap);
  return section;
}

function buildUnavailableMessage() {
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-unavailable";
  section.textContent = "Trend data unavailable";
  const retry = document.createElement("button");
  retry.className = "su-trend-retry";
  retry.type = "button";
  retry.textContent = "Retry";
  section.appendChild(retry);
  return section;
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

export async function initTrendPanel(resultsContainer, query, settings) {
  if (!resultsContainer || !query) return;
  destroyTrendPanel();

  const panel = document.createElement("div");
  panel.id = "su-trend-panel";
  const collapsed = await isPanelCollapsed();
  panel.classList.toggle("su-trend-collapsed", collapsed);

  const toggle = document.createElement("button");
  toggle.id = "su-trend-toggle";
  toggle.type = "button";
  toggle.className = "su-trend-toggle-btn";
  toggle.title = "Toggle trend tracker";
  toggle.innerHTML = '<span class="su-trend-toggle-icon">\u{1F4C8}</span>';
  toggle.addEventListener("click", async () => {
    const isCollapsed = panel.classList.toggle("su-trend-collapsed");
    await savePanelCollapsed(isCollapsed);
    if (!isCollapsed && !panel.dataset.loaded) {
      await loadPanelData(panel, resultsContainer, query);
    }
  });

  const content = document.createElement("div");
  content.className = "su-trend-content";

  const header = document.createElement("div");
  header.className = "su-trend-header";
  header.textContent = "Trend Tracker";

  content.appendChild(header);
  panel.appendChild(toggle);
  panel.appendChild(content);

  const gs_res = document.getElementById("gs_res_ccl") || document.getElementById("gs_res_ccl_mid") || resultsContainer;
  if (gs_res && gs_res.parentNode) {
    gs_res.parentNode.insertBefore(panel, gs_res.nextSibling);
  } else {
    document.body.appendChild(panel);
  }
  panelEl = panel;

  if (!collapsed) {
    await loadPanelData(panel, resultsContainer, query);
  }
}

async function loadPanelData(panel, resultsContainer, query) {
  const content = panel.querySelector(".su-trend-content");
  if (!content) return;
  panel.dataset.loaded = "1";

  const resultEls = Array.from(resultsContainer.querySelectorAll(".gs_r"));
  const yearCounts = extractYearsFromResults(resultEls);

  const yearChart = buildYearChart(yearCounts);
  if (yearChart) content.appendChild(yearChart);

  const methodCounts = detectMethods(resultEls);
  const methodPills = buildMethodPills(methodCounts);
  if (methodPills) content.appendChild(methodPills);

  let cached = await readCache(query);
  if (cached && cached.concepts) {
    const chips = buildConceptChips(cached.concepts);
    if (chips) content.appendChild(chips);
  } else {
    const concepts = await fetchConceptTrends(query);
    if (concepts.length > 0) {
      await writeCache(query, { concepts, yearlyWorks: yearCounts });
      const chips = buildConceptChips(concepts);
      if (chips) content.appendChild(chips);
    } else {
      const msg = buildUnavailableMessage();
      const retryBtn = msg.querySelector(".su-trend-retry");
      if (retryBtn) {
        retryBtn.addEventListener("click", async () => {
          msg.remove();
          const retryConcepts = await fetchConceptTrends(query);
          if (retryConcepts.length > 0) {
            await writeCache(query, { concepts: retryConcepts, yearlyWorks: yearCounts });
            const chips = buildConceptChips(retryConcepts);
            if (chips) content.appendChild(chips);
          } else {
            content.appendChild(buildUnavailableMessage());
          }
        });
      }
      content.appendChild(msg);
    }
  }
}

async function isPanelCollapsed() {
  try {
    const data = await chrome.storage.local.get({ trendPanelCollapsed: true });
    return !!data.trendPanelCollapsed;
  } catch {
    return true;
  }
}

async function savePanelCollapsed(collapsed) {
  try {
    await chrome.storage.local.set({ trendPanelCollapsed: !!collapsed });
  } catch {}
}

export function destroyTrendPanel() {
  if (panelEl) {
    panelEl.remove();
    panelEl = null;
  }
  const existing = document.getElementById("su-trend-panel");
  if (existing) existing.remove();
}
```

- [ ] **Step 2: Add CSS for the trend panel to `content.css`**

Append to `src/content/content.css`:

```css
/* ── Trend Tracker Panel ──────────────────────────────────────────── */
#su-trend-panel {
  position: fixed;
  top: 60px;
  right: 0;
  width: 280px;
  max-height: calc(100vh - 80px);
  overflow-y: auto;
  z-index: 10000;
  font-family: Arial, sans-serif;
  font-size: 13px;
  transition: transform 0.25s ease-out;
}
#su-trend-panel.su-trend-collapsed {
  transform: translateX(280px);
}
#su-trend-panel.su-trend-collapsed .su-trend-content {
  display: none;
}
.su-trend-toggle-btn {
  position: absolute;
  left: -32px;
  top: 8px;
  width: 30px;
  height: 30px;
  border: 1px solid #dadce0;
  border-right: none;
  border-radius: 4px 0 0 4px;
  background: #fff;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  box-shadow: -2px 1px 4px rgba(0,0,0,0.08);
  padding: 0;
}
.su-trend-toggle-btn:hover {
  background: #f1f3f4;
}
.su-trend-content {
  background: #fff;
  border: 1px solid #dadce0;
  border-radius: 8px 0 0 8px;
  padding: 12px;
  box-shadow: -2px 2px 8px rgba(0,0,0,0.08);
}
.su-trend-header {
  font-size: 14px;
  font-weight: 600;
  color: #202124;
  margin-bottom: 12px;
  padding-bottom: 8px;
  border-bottom: 1px solid #e8eaed;
}
.su-trend-section {
  margin-bottom: 14px;
}
.su-trend-section-title {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #5f6368;
  margin-bottom: 6px;
}
/* Year bars */
.su-trend-bar-row {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 3px;
}
.su-trend-bar-label {
  width: 36px;
  font-size: 11px;
  color: #5f6368;
  text-align: right;
  flex-shrink: 0;
}
.su-trend-bar-wrap {
  flex: 1;
  background: #f1f3f4;
  border-radius: 2px;
  height: 14px;
  overflow: hidden;
}
.su-trend-bar {
  height: 100%;
  background: #9aa0a6;
  border-radius: 2px;
  min-width: 2px;
  transition: width 0.3s ease;
}
.su-trend-bar.su-trend-bar-recent {
  background: #1a73e8;
}
.su-trend-bar-count {
  width: 20px;
  font-size: 11px;
  color: #5f6368;
  flex-shrink: 0;
}
/* Concept chips */
.su-trend-chip-wrap {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}
.su-trend-chip {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 3px 8px;
  border: 1px solid #dadce0;
  border-radius: 12px;
  background: #f8f9fa;
  font-size: 11px;
  cursor: pointer;
  color: #202124;
  line-height: 1.3;
}
.su-trend-chip:hover {
  background: #e8f0fe;
  border-color: #1a73e8;
}
.su-trend-chip-name {
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.su-trend-arrow-up { color: #0d904f; }
.su-trend-arrow-down { color: #c5221f; }
.su-trend-arrow-flat { color: #9aa0a6; }
/* Method pills */
.su-trend-pill-wrap {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}
.su-trend-pill {
  padding: 2px 8px;
  border-radius: 10px;
  background: #e8eaed;
  font-size: 11px;
  color: #3c4043;
}
/* Unavailable state */
.su-trend-unavailable {
  color: #5f6368;
  font-size: 12px;
}
.su-trend-retry {
  margin-left: 8px;
  padding: 2px 10px;
  border: 1px solid #dadce0;
  border-radius: 4px;
  background: #fff;
  cursor: pointer;
  font-size: 11px;
  color: #1a73e8;
}
.su-trend-retry:hover {
  background: #e8f0fe;
}
/* Dark mode */
body[data-su-theme="dark"] .su-trend-toggle-btn {
  background: #303134;
  border-color: #5f6368;
  color: #e8eaed;
}
body[data-su-theme="dark"] .su-trend-toggle-btn:hover {
  background: #3c4043;
}
body[data-su-theme="dark"] .su-trend-content {
  background: #303134;
  border-color: #5f6368;
}
body[data-su-theme="dark"] .su-trend-header {
  color: #e8eaed;
  border-bottom-color: #5f6368;
}
body[data-su-theme="dark"] .su-trend-section-title {
  color: #9aa0a6;
}
body[data-su-theme="dark"] .su-trend-bar-label,
body[data-su-theme="dark"] .su-trend-bar-count {
  color: #9aa0a6;
}
body[data-su-theme="dark"] .su-trend-bar-wrap {
  background: #3c4043;
}
body[data-su-theme="dark"] .su-trend-bar {
  background: #5f6368;
}
body[data-su-theme="dark"] .su-trend-bar.su-trend-bar-recent {
  background: #8ab4f8;
}
body[data-su-theme="dark"] .su-trend-chip {
  background: #3c4043;
  border-color: #5f6368;
  color: #e8eaed;
}
body[data-su-theme="dark"] .su-trend-chip:hover {
  background: #394457;
  border-color: #8ab4f8;
}
body[data-su-theme="dark"] .su-trend-pill {
  background: #3c4043;
  color: #bdc1c6;
}
body[data-su-theme="dark"] .su-trend-unavailable {
  color: #9aa0a6;
}
body[data-su-theme="dark"] .su-trend-retry {
  background: #303134;
  border-color: #5f6368;
  color: #8ab4f8;
}
body[data-su-theme="dark"] .su-trend-retry:hover {
  background: #394457;
}
```

- [ ] **Step 3: Commit**

```bash
git add src/content/trend-tracker.js src/content/content.css
git commit -m "feat: add trend tracker panel DOM, year chart, concept chips, method pills, and CSS"
```

---

### Task 6: Wire up settings, build, and manifest

**Files:**
- Modify: `src/common/storage.js` (line ~91, in `DEFAULT_SETTINGS`)
- Modify: `src/options/options.html` (after line 255, in the Features section)
- Modify: `src/options/options.js` (load ~line 219, save ~line 644)
- Modify: `manifest.template.json` (web_accessible_resources)
- Modify: `build.js` (TARGETS array)

- [ ] **Step 1: Add `showTrendTracker` to `DEFAULT_SETTINGS` in `storage.js`**

In `src/common/storage.js`, after the line `showAdvancedFilters: true,` (around line 90), add:

```javascript
  showTrendTracker: true,
```

- [ ] **Step 2: Add toggle to `options.html`**

In `src/options/options.html`, after the `showAdvancedFilters` toggle row (after line 256, before `</div></section>`), add:

```html
        <label class="toggle-row">
          <span>Show <strong>Trend Tracker</strong> panel on search results pages with year distribution, rising concepts from OpenAlex, and method signals.</span>
          <span class="toggle-switch"><input type="checkbox" id="showTrendTracker" /><span class="slider"></span></span>
        </label>
```

- [ ] **Step 3: Add load/save for toggle in `options.js`**

In the settings loading section (near line 219, after `showAdvancedFilters`), add:

```javascript
  el("showTrendTracker").checked = s.showTrendTracker !== false;
```

In the settings saving section (near line 644, after `showAdvancedFilters`), add:

```javascript
    showTrendTracker: el("showTrendTracker").checked,
```

- [ ] **Step 4: Add build targets to `build.js`**

In the `TARGETS` array in `build.js` (after the `content/data-loader.js` entry, around line 80), add:

```javascript
  {
    label: 'content/trend-tracker.js',
    args: ['src/content/trend-tracker.js', '--bundle=true', '--format=esm', '--minify', '--outfile=dist/content/trend-tracker.js', '--log-level=warning'],
  },
  {
    label: 'content/trend-methods.js',
    args: ['src/content/trend-methods.js', '--bundle=false', '--minify', '--outfile=dist/content/trend-methods.js', '--log-level=warning'],
  },
```

- [ ] **Step 5: Add to `web_accessible_resources` in `manifest.template.json`**

In `manifest.template.json`, in the `resources` array (line 48), add `"dist/content/trend-tracker.js"` and `"dist/content/trend-methods.js"` to the list:

Change the resources line from:
```json
"resources": ["dist/common/*.js", "dist/content/dom-cache.js", "dist/content/worker.js", "dist/content/content-author.js", "dist/content/data-loader.js", "src/data/*.txt", "src/data/*.csv", "src/data/*.json", "src/data/*.gz", "src/data/*.bin"],
```
to:
```json
"resources": ["dist/common/*.js", "dist/content/dom-cache.js", "dist/content/worker.js", "dist/content/content-author.js", "dist/content/data-loader.js", "dist/content/trend-tracker.js", "dist/content/trend-methods.js", "src/data/*.txt", "src/data/*.csv", "src/data/*.json", "src/data/*.gz", "src/data/*.bin"],
```

- [ ] **Step 6: Commit**

```bash
git add src/common/storage.js src/options/options.html src/options/options.js build.js manifest.template.json
git commit -m "feat: wire up trend tracker settings toggle, build targets, and manifest"
```

---

### Task 7: Integrate trend tracker into `content.js`

**Files:**
- Modify: `src/content/content.js`

- [ ] **Step 1: Add variable declarations for trend-tracker imports**

At the top of `content.js`, after the `checkRetractionStatus` declaration (line 58), add:

```javascript
  let trendTracker_initTrendPanel;
  let trendTracker_destroyTrendPanel;
```

- [ ] **Step 2: Add trend-tracker module loading in `ensureModulesLoaded`**

This is optional — we load lazily in `processAll` instead, following the pattern of loading only when the feature is enabled. No change needed to `ensureModulesLoaded`.

- [ ] **Step 3: Add trend panel initialization in `processAll`**

In the `processAll` function, after the filter bar setup for non-author pages (after line 16848 `applyResultFilters(results, state);`), add the trend tracker initialization:

```javascript
      if (!isAuthorProfile && state.settings.showTrendTracker) {
        const q = getScholarSearchQuery();
        if (q) {
          try {
            if (!trendTracker_initTrendPanel) {
              const trendMod = await importModuleWithRetry("dist/content/trend-tracker.js");
              trendTracker_initTrendPanel = trendMod.initTrendPanel;
              trendTracker_destroyTrendPanel = trendMod.destroyTrendPanel;
            }
            const container = document.getElementById("gs_res_ccl_mid") || document.getElementById("gs_res_ccl") || document.querySelector("#gs_bdy");
            if (container) {
              trendTracker_initTrendPanel(container, q, state.settings);
            }
          } catch (e) {
            console.warn("[SU] trend tracker init failed", e);
          }
        }
      } else if (!isAuthorProfile && !state.settings.showTrendTracker && trendTracker_destroyTrendPanel) {
        trendTracker_destroyTrendPanel();
      }
```

- [ ] **Step 4: Commit**

```bash
git add src/content/content.js
git commit -m "feat: integrate trend tracker lazy loading into content.js main flow"
```

---

### Task 8: Build, test, and verify

**Files:**
- No new files

- [ ] **Step 1: Run the test suite**

Run: `node --import ./tests/register-loader.mjs --test tests/trend-tracker.test.mjs`
Expected: All trend-tracker tests PASS.

Run: `node --import ./tests/register-loader.mjs --test tests/*.test.mjs`
Expected: All tests across all suites PASS (quality, storage, trend-tracker).

- [ ] **Step 2: Run the build**

Run: `node build.js`
Expected: All targets succeed including the two new ones (`content/trend-tracker.js`, `content/trend-methods.js`). Output shows `dist/ ready`.

- [ ] **Step 3: Verify manifest generation**

Run: `cat manifest.json | grep -c "trend-tracker"`
Expected: Output `1` (the trend-tracker.js entry in web_accessible_resources).

- [ ] **Step 4: Manually test in browser**

1. Load the extension at `chrome://extensions` (reload if already loaded).
2. Go to Google Scholar and search for any query (e.g., "machine learning").
3. Verify: a small chart icon tab appears on the right edge.
4. Click the tab — the panel should expand showing:
   - Year distribution bar chart from page results.
   - Method signal pills (if any methodology keywords are in results).
   - Rising concepts chips (loaded from OpenAlex after a moment).
5. Click a concept chip — it should append the concept to the search box.
6. Click the toggle again — panel collapses.
7. Reload the page — panel should stay collapsed (remembered state).
8. Go to Settings > Features section — verify "Trend Tracker" toggle exists.
9. Uncheck it, save, reload Scholar — panel toggle should not appear.
10. Test dark mode — panel should use dark colors.

- [ ] **Step 5: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "fix: address trend tracker integration issues from manual testing"
```
