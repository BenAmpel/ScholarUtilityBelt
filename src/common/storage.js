// Centralized storage helpers.
// Design goal: no network calls; everything is derived from the currently loaded Scholar DOM.

/** True when the extension was reloaded/updated and chrome.* APIs are no longer valid. */
function isContextInvalidated(e) {
  if (!e) return false;
  const msg = String(e?.message ?? e?.toString?.() ?? "").toLowerCase();
  return msg.includes("context invalidated") || msg.includes("extension context invalidated");
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
  viewMode: "detailed", // "minimal" | "detailed"
  qualityBadgeKinds: {
    quartile: true,
    abdc: true,
    jcr: true,
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
  groupVersions: true,
  versionOrder: "journal-first", // "journal-first" | "conference-first" | "preprint-first"

  // Power user
  showNewSinceLastVisit: true,
  showCitationSpike: true,
  citationSpikeThresholdPct: 50,
  citationSpikeMonths: 6
};

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
    await chrome.storage.local.set({ settings: { ...current, ...next } });
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
    cache[pageKey] = { seenKeys: Array.isArray(seenKeys) ? seenKeys : [], timestamp: timestamp || new Date().toISOString() };
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
    snap[clusterId] = { citations: Number(citations), date: date || new Date().toISOString() };
    await chrome.storage.local.set({ [CITATION_SNAPSHOTS_KEY]: snap });
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
