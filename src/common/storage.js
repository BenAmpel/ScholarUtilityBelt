// Centralized storage helpers.
// Design goal: no network calls; everything is derived from the currently loaded Scholar DOM.

/** True when the extension was reloaded/updated and chrome.* APIs are no longer valid. */
function isContextInvalidated(e) {
  if (!e) return false;
  const msg = String(e?.message ?? e?.toString?.() ?? "").toLowerCase();
  return (
    msg.includes("context invalidated") ||
    msg.includes("extension context invalidated") ||
    msg.includes("access to storage is not allowed") ||
    msg.includes("cannot read properties of undefined (reading 'local')") ||
    msg.includes("cannot read properties of undefined (reading 'storage')") ||
    msg.includes("chrome is undefined") ||
    msg.includes("chrome.storage is undefined")
  );
}

function sameJsonValue(a, b) {
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}

function sameStringArray(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

export const DEFAULT_SETTINGS = {
  highlightSaved: true,
  autoTagFromQuery: false,
  defaultTagsCsv: "",
  keywordHighlightsCsv: "",
  hideTitleRegex: "",
  hideAuthorsRegex: "",
  compactResultButtons: true,

  // Appearance
  theme: "auto", // "auto" | "light" | "dark"
  badgePalette: "soft", // "soft" | "bold"
  viewMode: "detailed", // "minimal" | "detailed"
  qualityBadgeKinds: {
    quartile: true,
    abdc: true,
    vhb: true,
    fnege: true,
    jcr: true,
    if: true,
    ft50: true,
    utd24: true,
    core: true,
    ccf: true,
    era: true,
    norwegian: true,
    preprint: true,
    h5: true
  },

  // Quality index: user-pasted lists. Newline separated for lists, CSV for ranks.
  showQualityBadges: true,
  qualityFt50List: "",
  qualityUtd24List: "",
  qualityAbdcRanks: "",
  qualityVhbRanks: "",
  qualityFnegeRanks: "",
  qualityQuartiles: "",
  qualityCoreRanks: "",
  qualityCcfRanks: "",
  showSemanticExpansion: true,
  showFundingTag: true,
  showReadingGuide: true,
  showRetractionWatch: true,
  showHoverSummary: true,
  showArtifactBadge: true,
  showCSS: true,
  showAgeBiasHeatmap: true,
  showAuthorshipHeatmap: true,
  showRelevancySparkline: true,
  showSnippetCueEmphasis: true,
  showSkimmabilityStrip: true,
  showReadingLoadEstimator: true,
  showResearchIntel: true,
  showAdvancedFilters: true,
  groupVersions: true,
  versionOrder: "journal-first", // "journal-first" | "conference-first" | "preprint-first"

  // Power user
  showNewSinceLastVisit: true,
  showCitationSpike: true,
  citationSpikeThresholdPct: 50,
  citationSpikeMonths: 6,
  showEmergingScore: true,
  emergingScoreDataSource: "hybrid", // "local" | "openalex" | "hybrid"
  emergingScoreMinCohort: 5,

  // Author profile stats row visibility (top bar)
  authorStatsVisible: {
    filterQ1: true,
    filterAbdc: true,
    filterVhb: true,
    filterFt50: true,
    filterUtd24: true,
    filterAbs4star: true,
    filterClear: true,
    sortToggle: true,
    positionFilters: true,
    papers: true,
    citations: true,
    firstAuthor: true,
    firstAuthorCites: true,
    solo: true,
    soloCites: true,
    coauthors: true,
    collabShape: true,
    drift: true
  }
};

const DEFAULT_LIBRARY_STATE = {
  collections: [],
  savedSearches: [],
  watchedFolders: [],
  activeCollectionId: "",
  activeSavedSearchId: "",
  showDuplicates: false
};

const TRAJECTORY_VENUE_EXPECTED_KEY = "trajectoryVenueExpected";
const LOCAL_COHORT_CACHE_KEY = "localCohortCacheV1";
const LOCAL_COHORT_CACHE_MAX_KEYS = 4000;
const LOCAL_COHORT_CACHE_MAX_SAMPLES = 24;

/**
 * Batch get multiple storage keys in a single call.
 * @param {Object} defaults - Object with keys and default values
 * @returns {Promise<Object>} - Object with all requested keys
 */
export async function batchGetStorage(defaults) {
  try {
    return await chrome.storage.local.get(defaults || {});
  } catch (e) {
    if (isContextInvalidated(e)) return defaults || {};
    throw e;
  }
}

export async function getSettings() {
  try {
    const { settings } = await chrome.storage.local.get({ settings: DEFAULT_SETTINGS });
    return { ...DEFAULT_SETTINGS, ...(settings || {}) };
  } catch (e) {
    if (isContextInvalidated(e)) return { ...DEFAULT_SETTINGS };
    throw e;
  }
}

export async function setSettings(next) {
  try {
    const current = await getSettings();
    const merged = { ...current, ...next };
    if (sameJsonValue(current, merged)) return;
    await chrome.storage.local.set({ settings: merged });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function getSavedPapers() {
  try {
    const { savedPapers } = await chrome.storage.local.get({ savedPapers: {} });
    return savedPapers || {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function getLibraryState() {
  try {
    const { libraryState } = await chrome.storage.local.get({ libraryState: DEFAULT_LIBRARY_STATE });
    return { ...DEFAULT_LIBRARY_STATE, ...(libraryState || {}) };
  } catch (e) {
    if (isContextInvalidated(e)) return { ...DEFAULT_LIBRARY_STATE };
    throw e;
  }
}

export async function setLibraryState(next) {
  try {
    const current = await getLibraryState();
    const merged = { ...current, ...(next || {}) };
    if (sameJsonValue(current, merged)) return;
    await chrome.storage.local.set({ libraryState: merged });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function upsertPaper(paper) {
  if (!paper || !paper.key) throw new Error("paper.key is required");
  try {
    const savedPapers = await getSavedPapers();
    const existing = savedPapers[paper.key];
    const merged = {
      ...existing,
      ...paper,
      key: paper.key,
      savedAt: existing?.savedAt || paper.savedAt || new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    savedPapers[paper.key] = merged;
    await chrome.storage.local.set({ savedPapers });
    return merged;
  } catch (e) {
    if (isContextInvalidated(e)) return { ...paper, key: paper.key };
    throw e;
  }
}

export async function removePaper(key) {
  try {
    const savedPapers = await getSavedPapers();
    if (!savedPapers[key]) return false;
    delete savedPapers[key];
    await chrome.storage.local.set({ savedPapers });
    return true;
  } catch (e) {
    if (isContextInvalidated(e)) return false;
    throw e;
  }
}

export async function getQualityQuartilesIndex() {
  try {
    const { qualityQuartilesIndex, qualityQuartilesMeta } = await chrome.storage.local.get({
      qualityQuartilesIndex: {},
      qualityQuartilesMeta: null
    });
    return { index: qualityQuartilesIndex || {}, meta: qualityQuartilesMeta || null };
  } catch (e) {
    if (isContextInvalidated(e)) return { index: {}, meta: null };
    throw e;
  }
}

export async function setQualityQuartilesIndex(index, meta) {
  try {
    await chrome.storage.local.set({
      qualityQuartilesIndex: index || {},
      qualityQuartilesMeta: meta || null
    });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function clearQualityQuartilesIndex() {
  try {
    await chrome.storage.local.remove(["qualityQuartilesIndex", "qualityQuartilesMeta"]);
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function getQualityJcrIndex() {
  try {
    const { qualityJcrIndex, qualityJcrMeta } = await chrome.storage.local.get({
      qualityJcrIndex: {},
      qualityJcrMeta: null
    });
    return { index: qualityJcrIndex || {}, meta: qualityJcrMeta || null };
  } catch (e) {
    if (isContextInvalidated(e)) return { index: {}, meta: null };
    throw e;
  }
}

export async function setQualityJcrIndex(index, meta) {
  try {
    await chrome.storage.local.set({
      qualityJcrIndex: index || {},
      qualityJcrMeta: meta || null
    });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function clearQualityJcrIndex() {
  try {
    await chrome.storage.local.remove(["qualityJcrIndex", "qualityJcrMeta"]);
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

const READING_QUEUE_KEY = "readingQueue";

/** @typedef {{ id: string, title: string, link: string, searchQuery: string }} ReadingQueueItem */

export async function getReadingQueue() {
  try {
    if (!chrome?.storage?.local?.get) return [];
    const { [READING_QUEUE_KEY]: queue } = await chrome.storage.local.get({ [READING_QUEUE_KEY]: [] });
    return Array.isArray(queue) ? queue : [];
  } catch (e) {
    if (isContextInvalidated(e)) return [];
    throw e;
  }
}

export async function addToReadingQueue(item) {
  if (!item || typeof item.title !== "string" || typeof item.link !== "string") return null;
  try {
    const queue = await getReadingQueue();
    const entry = {
      id: item.id || `q:${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      title: String(item.title).trim() || "Untitled",
      link: String(item.link).trim(),
      searchQuery: String(item.searchQuery || "").trim()
    };
    queue.push(entry);
    await chrome.storage.local.set({ [READING_QUEUE_KEY]: queue });
    return entry;
  } catch (e) {
    if (isContextInvalidated(e)) return null;
    throw e;
  }
}

export async function clearReadingQueue() {
  try {
    await chrome.storage.local.set({ [READING_QUEUE_KEY]: [] });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

const READING_LOAD_PAGES_KEY = "readingLoadPageCounts";

export async function getReadingLoadPageCounts() {
  try {
    const { [READING_LOAD_PAGES_KEY]: obj } = await chrome.storage.local.get({
      [READING_LOAD_PAGES_KEY]: {}
    });
    return obj && typeof obj === "object" ? obj : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setReadingLoadPageCount(paperKey, pages) {
  if (!paperKey || typeof pages !== "number" || pages < 1) return;
  try {
    const counts = await getReadingLoadPageCounts();
    if (counts[paperKey] === pages) return;
    counts[paperKey] = pages;
    await chrome.storage.local.set({ [READING_LOAD_PAGES_KEY]: counts });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

const HIDDEN_PAPERS_KEY = "hiddenPapers";
const HIDDEN_VENUES_KEY = "hiddenVenues";
const HIDDEN_AUTHORS_KEY = "hiddenAuthors";

export async function getHiddenPapers() {
  try {
    const { [HIDDEN_PAPERS_KEY]: list } = await chrome.storage.local.get({ [HIDDEN_PAPERS_KEY]: [] });
    return Array.isArray(list) ? list : [];
  } catch (e) {
    if (isContextInvalidated(e)) return [];
    throw e;
  }
}

export async function getHiddenVenues() {
  try {
    const { [HIDDEN_VENUES_KEY]: list } = await chrome.storage.local.get({ [HIDDEN_VENUES_KEY]: [] });
    return Array.isArray(list) ? list : [];
  } catch (e) {
    if (isContextInvalidated(e)) return [];
    throw e;
  }
}

export async function getHiddenAuthors() {
  try {
    const { [HIDDEN_AUTHORS_KEY]: list } = await chrome.storage.local.get({ [HIDDEN_AUTHORS_KEY]: [] });
    return Array.isArray(list) ? list : [];
  } catch (e) {
    if (isContextInvalidated(e)) return [];
    throw e;
  }
}

export async function addHiddenPaper(key) {
  if (!key) return;
  try {
    const list = await getHiddenPapers();
    if (list.includes(key)) return;
    list.push(key);
    await chrome.storage.local.set({ [HIDDEN_PAPERS_KEY]: list });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function addHiddenVenue(venueNormalized) {
  if (!venueNormalized) return;
  try {
    const list = await getHiddenVenues();
    if (list.includes(venueNormalized)) return;
    list.push(venueNormalized);
    await chrome.storage.local.set({ [HIDDEN_VENUES_KEY]: list });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function addHiddenAuthor(authorNormalized) {
  if (!authorNormalized) return;
  try {
    const list = await getHiddenAuthors();
    if (list.includes(authorNormalized)) return;
    list.push(authorNormalized);
    await chrome.storage.local.set({ [HIDDEN_AUTHORS_KEY]: list });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function removeHiddenPaper(key) {
  try {
    const list = await getHiddenPapers();
    const next = list.filter((k) => k !== key);
    if (next.length === list.length) return false;
    await chrome.storage.local.set({ [HIDDEN_PAPERS_KEY]: next });
    return true;
  } catch (e) {
    if (isContextInvalidated(e)) return false;
    throw e;
  }
}

export async function removeHiddenVenue(venueNormalized) {
  try {
    const list = await getHiddenVenues();
    const next = list.filter((v) => v !== venueNormalized);
    if (next.length === list.length) return false;
    await chrome.storage.local.set({ [HIDDEN_VENUES_KEY]: next });
    return true;
  } catch (e) {
    if (isContextInvalidated(e)) return false;
    throw e;
  }
}

export async function removeHiddenAuthor(authorNormalized) {
  try {
    const list = await getHiddenAuthors();
    const next = list.filter((a) => a !== authorNormalized);
    if (next.length === list.length) return false;
    await chrome.storage.local.set({ [HIDDEN_AUTHORS_KEY]: next });
    return true;
  } catch (e) {
    if (isContextInvalidated(e)) return false;
    throw e;
  }
}

export async function clearHiddenPapers() {
  try {
    await chrome.storage.local.set({ [HIDDEN_PAPERS_KEY]: [] });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function clearHiddenVenues() {
  try {
    await chrome.storage.local.set({ [HIDDEN_VENUES_KEY]: [] });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export async function clearHiddenAuthors() {
  try {
    await chrome.storage.local.set({ [HIDDEN_AUTHORS_KEY]: [] });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

const PAGE_VISIT_CACHE_KEY = "pageVisitCache";
const CITATION_SNAPSHOTS_KEY = "citationSnapshots";

/** Get cache of last visit per page: { [pageKey]: { seenKeys: string[], timestamp: string } } */
export async function getPageVisitCache() {
  try {
    const { [PAGE_VISIT_CACHE_KEY]: cache } = await chrome.storage.local.get({ [PAGE_VISIT_CACHE_KEY]: {} });
    return cache && typeof cache === "object" ? cache : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setPageVisitCacheEntry(pageKey, seenKeys, timestamp) {
  try {
    const cache = await getPageVisitCache();
    const nextEntry = {
      seenKeys: Array.isArray(seenKeys) ? seenKeys : [],
      timestamp: timestamp || new Date().toISOString()
    };
    const existing = cache[pageKey];
    if (existing && existing.timestamp === nextEntry.timestamp && sameStringArray(existing.seenKeys, nextEntry.seenKeys)) {
      return;
    }
    cache[pageKey] = nextEntry;
    await chrome.storage.local.set({ [PAGE_VISIT_CACHE_KEY]: cache });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

/** Get citation snapshots for spike detection: { [clusterId]: { citations: number, date: string } } */
export async function getCitationSnapshots() {
  try {
    const { [CITATION_SNAPSHOTS_KEY]: snap } = await chrome.storage.local.get({ [CITATION_SNAPSHOTS_KEY]: {} });
    return snap && typeof snap === "object" ? snap : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setCitationSnapshot(clusterId, citations, date) {
  try {
    const snap = await getCitationSnapshots();
    const entry = snap[clusterId] && typeof snap[clusterId] === "object" ? snap[clusterId] : {};
    const history = Array.isArray(entry.history) ? entry.history.slice() : [];
    if (entry.citations != null && entry.date && history.length === 0) {
      history.push({ citations: Number(entry.citations), date: entry.date });
    }
    const next = { citations: Number(citations), date: date || new Date().toISOString() };
    const last = history[history.length - 1];
    if (!last || last.citations !== next.citations || last.date !== next.date) {
      history.push(next);
    }
    const trimmed = history.slice(-3);
    const nextEntry = { citations: next.citations, date: next.date, history: trimmed };
    if (sameJsonValue(entry, nextEntry)) return;
    snap[clusterId] = nextEntry;
    await chrome.storage.local.set({ [CITATION_SNAPSHOTS_KEY]: snap });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

/** Get cached expected citation benchmarks by venue/year. */
export async function getTrajectoryVenueExpectedCache() {
  try {
    const { [TRAJECTORY_VENUE_EXPECTED_KEY]: cache } = await chrome.storage.local.get({
      [TRAJECTORY_VENUE_EXPECTED_KEY]: {}
    });
    return cache && typeof cache === "object" ? cache : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setTrajectoryVenueExpectedCache(cache) {
  try {
    const payload = cache && typeof cache === "object" ? cache : {};
    await chrome.storage.local.set({ [TRAJECTORY_VENUE_EXPECTED_KEY]: payload });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

function normalizeLocalCohortSample(sample) {
  if (!sample || typeof sample !== "object") return null;
  const id = String(sample.id || "").trim();
  const citations = Number(sample.citations);
  const citesPerYear = Number(sample.citesPerYear);
  if (!id || !Number.isFinite(citations) || citations < 0) return null;
  return {
    id,
    citations,
    citesPerYear: Number.isFinite(citesPerYear) && citesPerYear >= 0 ? citesPerYear : null,
    observedAt: Number(sample.observedAt) || 0
  };
}

function pruneLocalCohortCache(cache) {
  if (!cache || typeof cache !== "object") return {};
  const entries = Object.entries(cache).map(([key, entry]) => {
    const rawSamples = Array.isArray(entry?.samples) ? entry.samples : [];
    const sampleMap = new Map();
    for (const rawSample of rawSamples) {
      const sample = normalizeLocalCohortSample(rawSample);
      if (!sample) continue;
      const existing = sampleMap.get(sample.id);
      if (!existing || sample.observedAt >= existing.observedAt) {
        sampleMap.set(sample.id, sample);
      }
    }
    const samples = Array.from(sampleMap.values())
      .sort((a, b) => (b.observedAt || 0) - (a.observedAt || 0))
      .slice(0, LOCAL_COHORT_CACHE_MAX_SAMPLES);
    const updatedAt = Number(entry?.updatedAt) || samples.reduce((acc, sample) => Math.max(acc, sample.observedAt || 0), 0);
    return {
      key,
      updatedAt,
      entry: {
        samples,
        updatedAt
      }
    };
  }).filter((entry) => entry.entry.samples.length > 0);
  if (entries.length <= LOCAL_COHORT_CACHE_MAX_KEYS) {
    return Object.fromEntries(entries.map((entry) => [entry.key, entry.entry]));
  }
  entries.sort((a, b) => b.updatedAt - a.updatedAt);
  return Object.fromEntries(entries.slice(0, LOCAL_COHORT_CACHE_MAX_KEYS).map((entry) => [entry.key, entry.entry]));
}

export async function getLocalCohortCache() {
  try {
    const { [LOCAL_COHORT_CACHE_KEY]: cache } = await chrome.storage.local.get({
      [LOCAL_COHORT_CACHE_KEY]: {}
    });
    return cache && typeof cache === "object" ? cache : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setLocalCohortCache(cache) {
  try {
    const pruned = pruneLocalCohortCache(cache || {});
    await chrome.storage.local.set({ [LOCAL_COHORT_CACHE_KEY]: pruned });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

const AUTHOR_HINDEX_SNAPSHOTS_KEY = "authorHIndexSnapshots";
const HINDEX_SNAPSHOT_MAX_AGE_MONTHS = 24;

/** Get h-index history per author profile: { [profileUrl]: [ { date: string, hIndex: number }, ... ] } */
export async function getAuthorHIndexSnapshots() {
  try {
    const { [AUTHOR_HINDEX_SNAPSHOTS_KEY]: data } = await chrome.storage.local.get({
      [AUTHOR_HINDEX_SNAPSHOTS_KEY]: {}
    });
    return data && typeof data === "object" ? data : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

/** Append one snapshot and prune to last N months */
export async function setAuthorHIndexSnapshot(profileUrl, hIndex) {
  if (!profileUrl || typeof hIndex !== "number") return;
  try {
    const data = await getAuthorHIndexSnapshots();
    const list = Array.isArray(data[profileUrl]) ? data[profileUrl] : [];
    const now = new Date();
    const cutoff = new Date(now.getFullYear(), now.getMonth() - HINDEX_SNAPSHOT_MAX_AGE_MONTHS, 1).toISOString();
    list.push({ date: now.toISOString(), hIndex });
    const pruned = list.filter((e) => e.date >= cutoff).slice(-50);
    data[profileUrl] = pruned;
    await chrome.storage.local.set({ [AUTHOR_HINDEX_SNAPSHOTS_KEY]: data });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

// External signals (Crossref/OpenCitations/DataCite) cache
const EXTERNAL_SIGNAL_CACHE_KEY = "externalSignalCacheV1";
const EXTERNAL_SIGNAL_CACHE_MAX = 3000;

function pruneExternalSignalCache(cache) {
  if (!cache || typeof cache !== "object") return {};
  const entries = Object.entries(cache).map(([doi, data]) => {
    const providers = data && typeof data === "object" ? Object.values(data) : [];
    const latest = providers.reduce((acc, item) => Math.max(acc, Number(item?.ts) || 0), 0);
    return { doi, latest };
  });
  if (entries.length <= EXTERNAL_SIGNAL_CACHE_MAX) return cache;
  entries.sort((a, b) => b.latest - a.latest);
  const keep = new Set(entries.slice(0, EXTERNAL_SIGNAL_CACHE_MAX).map((e) => e.doi));
  const out = {};
  for (const [doi, data] of Object.entries(cache)) {
    if (keep.has(doi)) out[doi] = data;
  }
  return out;
}

export async function getExternalSignalCache() {
  try {
    const { [EXTERNAL_SIGNAL_CACHE_KEY]: data } = await chrome.storage.local.get({
      [EXTERNAL_SIGNAL_CACHE_KEY]: {}
    });
    return data && typeof data === "object" ? data : {};
  } catch (e) {
    if (isContextInvalidated(e)) return {};
    throw e;
  }
}

export async function setExternalSignalCache(cache) {
  try {
    const pruned = pruneExternalSignalCache(cache || {});
    await chrome.storage.local.set({ [EXTERNAL_SIGNAL_CACHE_KEY]: pruned });
  } catch (e) {
    if (isContextInvalidated(e)) return;
    throw e;
  }
}

export function csvToTags(csv) {
  return String(csv || "")
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

export function uniqTags(tags) {
  const seen = new Set();
  const out = [];
  for (const t of tags || []) {
    const k = String(t || "").trim();
    if (!k) continue;
    const lk = k.toLowerCase();
    if (seen.has(lk)) continue;
    seen.add(lk);
    out.push(k);
  }
  return out;
}
