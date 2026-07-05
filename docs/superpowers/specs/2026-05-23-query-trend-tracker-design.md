# Query Trend Tracker - Design Spec

**Date**: 2026-05-23
**Status**: Approved
**Scope**: New feature for Scholar Utility Belt v0.4.0

## Overview

The Query Trend Tracker adds a collapsible sidebar panel to Google Scholar search results that surfaces publication trends for the current query. It answers the question: "Is this topic growing, peaking, or declining?" using data already on the page plus free OpenAlex API calls.

Target user: active researchers deciding where to invest reading time or whether a topic is worth pursuing.

## 1. UI Placement & Layout

### Panel structure

A collapsible right-sidebar panel, injected after the Scholar results column. Collapsed by default on first use; the extension remembers the last state in `chrome.storage.local`.

When expanded, the panel contains three sections stacked vertically:

**Year Distribution Bar Chart**
- Horizontal bar chart showing publication counts per year, built from the years visible in the current Scholar results page (~10 results).
- Pure CSS bars (no canvas/SVG library). Each bar is a `<div>` with `width` set as a percentage of the max year count.
- Color-coded: bars for the most recent 2 years use the accent color; older bars use a muted shade.
- Shows years on the left axis, counts on the right of each bar.

**Rising Concepts**
- Chips/tags showing concepts associated with the query, sourced from the OpenAlex Concepts API.
- Each chip shows the concept name and a trend arrow (up/down/flat) based on the `works_count` trajectory over the last 5 years from OpenAlex. Trend is computed as the slope of a simple linear regression on the 5 yearly counts: positive slope > 10% of mean = up, negative slope < -10% of mean = down, otherwise flat.
- Clicking a chip appends the concept as a keyword to the Scholar search box (does not auto-submit).
- Limited to the top 8 concepts to avoid clutter.

**Method Signals**
- A small section detecting common methodology keywords in the titles/snippets on the current page.
- Scans for predefined keyword lists: "meta-analysis", "systematic review", "randomized controlled trial", "survey", "case study", "qualitative", "longitudinal", "cross-sectional", "machine learning", "deep learning", "NLP", "LLM".
- Displays matched methods as small pills with a count of how many results mention them.
- Pure string matching against title + snippet text already in the DOM. No API calls.

### Toggle control

A small tab/button on the right edge of the results area labeled with a trend icon. Clicking it toggles the panel open/closed. The panel slides in/out with a CSS transition.

## 2. Data Architecture

### Data sources

| Source | Cost | What it provides |
|--------|------|-----------------|
| Page scraping | Zero | Year counts, method keyword matches, result titles/snippets |
| OpenAlex `/concepts` | Free, rate-limited | Concept names, works_count history for trend arrows |
| OpenAlex `/works` | Free, rate-limited | Works count by year for the query (optional enrichment) |

### Caching strategy

- **Cache key**: normalized query string (lowercase, trimmed, sorted words).
- **Storage**: `chrome.storage.local` under key `trendCache`.
- **TTL**: 7 days per entry.
- **Eviction**: LRU with a 200-query cap. On each write, if the cache exceeds 200 entries, evict the oldest-accessed entry.
- **Cache structure**:
  ```
  trendCache: {
    [normalizedQuery]: {
      ts: <timestamp>,
      lastAccess: <timestamp>,
      concepts: [{ name, trend, worksCount }],
      yearlyWorks: { "2021": n, "2022": n, ... }
    }
  }
  ```

### API interaction

- Queries fire only when the panel is expanded and the query has no valid cache entry.
- Rate limiting: max 1 request per second to OpenAlex, enforced by a simple timestamp gate.
- Timeout: 8 seconds per request; on failure, the panel shows cached data if available or a "data unavailable" message.
- Email polite pool: requests include `mailto=scholar-extension@local` parameter per OpenAlex guidelines.

## 3. Module Structure

### New files

**`src/content/trend-tracker.js`** (~400-500 lines)
- Main module, lazy-loaded via `importModuleWithRetry("dist/content/trend-tracker.js")`.
- Exports:
  - `initTrendPanel(resultsContainer, query, settings)` — creates/updates the panel DOM.
  - `destroyTrendPanel()` — removes the panel and cleans up listeners.
- Internal responsibilities:
  - DOM construction for the sidebar panel.
  - Year extraction from Scholar result elements (regex on the green metadata line).
  - OpenAlex API calls with caching via storage helpers.
  - Concept chip rendering with trend arrows.
  - Panel expand/collapse state management.
  - LRU cache read/write/evict logic.

**`src/content/trend-methods.js`** (~100 lines)
- Exports:
  - `detectMethods(elements)` — scans an array of DOM elements for method keywords.
  - `METHOD_KEYWORDS` — the keyword list constant.
- Returns `Map<string, number>` of method name to occurrence count.

### Build integration

Both files added to `build.js` TARGETS as unbundled minified modules (same pattern as `dom-cache.js` and `content-author.js`):
```javascript
{ label: 'content/trend-tracker.js', args: ['src/content/trend-tracker.js', '--bundle=false', '--minify', '--outfile=dist/content/trend-tracker.js'] },
{ label: 'content/trend-methods.js', args: ['src/content/trend-methods.js', '--bundle=false', '--minify', '--outfile=dist/content/trend-methods.js'] },
```

Both added to `web_accessible_resources` in `manifest.template.json`.

### Integration with content.js

In the main IIFE's initialization path (after settings load and quality index resolution):

```javascript
if (settings.showTrendTracker && isSearchResultsPage()) {
  const trendMod = await importModuleWithRetry("dist/content/trend-tracker.js");
  trendTracker_initTrendPanel = trendMod.initTrendPanel;
  trendTracker_initTrendPanel(resultsContainer, currentQuery, settings);
}
```

This follows the existing lazy-load pattern used for `data-loader.js` and `content-author.js`.

## 4. Settings & Testing

### Settings

A single boolean toggle added to the options page:

- **Key**: `showTrendTracker`
- **Default**: `true`
- **Label**: "Show trend tracker panel on search results"
- **Location**: In the existing settings UI, under the quality badges toggle.

Added to the `DEFAULTS` object in `storage.js` and read in `content.js` alongside other settings.

### Testing

**`tests/trend-tracker.test.mjs`**
- Unit tests for:
  - Year extraction regex against various Scholar result formats.
  - Cache key normalization (word sorting, lowercasing, trimming).
  - LRU eviction logic (200-entry cap, oldest-access eviction).
  - Concept trend arrow calculation (up/down/flat thresholds).
  - Method keyword detection (exact match, case-insensitive, counts).
- Mocks: `chrome.storage.local`, `fetch` (for OpenAlex responses).
- Target: 20-30 tests covering the core logic, not DOM rendering.

### Graceful degradation

- If OpenAlex is unreachable: the year chart and method signals still render (page-only data). The concepts section shows "Trend data unavailable" with a retry link.
- If the page has no parseable years: the year chart section is hidden; concepts and methods still attempt to load.
- If the user disables the feature: no module is loaded, no DOM is injected, no API calls are made.

## Non-goals

- No historical tracking across sessions (no "your search history" feature).
- No cross-query comparison (e.g., "compare ML vs. DL trends").
- No author-level trend analysis (the author page feature is separate scope).
- No custom keyword lists for method detection (hardcoded list is sufficient for v1).
