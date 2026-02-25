(async () => {
  // Content scripts are loaded as classic scripts in many Chrome setups.
  // Use dynamic import() with chrome.runtime.getURL so we can share modules.
  const storage = await import(chrome.runtime.getURL("src/common/storage.js"));
  const quality = await import(chrome.runtime.getURL("src/common/quality.js"));
  const domCache = await import(chrome.runtime.getURL("src/content/dom-cache.js"));
  const { getCachedElement, getCachedElements } = domCache;

  const {
    addHiddenAuthor,
    addHiddenPaper,
    addHiddenVenue,
    addToReadingQueue,
    batchGetStorage,
    clearReadingQueue,
    csvToTags,
    getAuthorHIndexSnapshots,
    getCitationSnapshots,
    getExternalSignalCache,
    getHiddenAuthors,
    getHiddenPapers,
    getHiddenVenues,
    getPageVisitCache,
    getQualityJcrIndex,
    getQualityQuartilesIndex,
    getReadingLoadPageCounts,
    getReadingQueue,
    getSavedPapers,
    getSettings,
    removePaper,
    setAuthorHIndexSnapshot,
    setExternalSignalCache,
    setSettings,
    setCitationSnapshot,
    setPageVisitCacheEntry,
    setReadingLoadPageCount,
    uniqTags,
    upsertPaper
  } = storage;

  const {
    compileQualityIndex,
    extractVenueFromAuthorsVenue,
    isPreprintVenue,
    normalizeVenueName,
    normalizeVhbRank,
    qualityBadgesForVenue,
    venueWeightForVenue
  } = quality;
  
  // Import DEFAULT_SETTINGS for refreshState
  const { DEFAULT_SETTINGS } = storage;

  function parseCsvLine(line, delim = ";") {
    const s = String(line || "");
    const out = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < s.length; i++) {
      const ch = s[i];
      if (inQ) {
        if (ch === '"') {
          if (s[i + 1] === '"') { cur += '"'; i++; }
          else inQ = false;
        } else cur += ch;
      } else {
        if (ch === '"') inQ = true;
        else if (ch === delim) { out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim()); cur = ""; }
        else cur += ch;
      }
    }
    out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim());
    return out;
  }

  const SELF_CITE_CACHE_MS = 30 * 24 * 60 * 60 * 1000; // Used by ensureSelfCitationEstimate

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  function normalizeText(str) {
    return String(str || "")
      .toLowerCase()
      .replace(/[\.,\/#!$%\^&\*;:{}=\_`~?"“”()\[\]]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function jaroWinkler(a, b) {
    if (!a || !b) return 0;
    if (a === b) return 1;
    const s1 = String(a);
    const s2 = String(b);
    const maxDist = Math.floor(Math.max(s1.length, s2.length) / 2) - 1;
    const match1 = new Array(s1.length);
    const match2 = new Array(s2.length);
    let matches = 0;
    for (let i = 0; i < s1.length; i++) {
      const start = Math.max(0, i - maxDist);
      const end = Math.min(i + maxDist + 1, s2.length);
      for (let j = start; j < end; j++) {
        if (!match2[j] && s1[i] === s2[j]) {
          match1[i] = match2[j] = true;
          matches++;
          break;
        }
      }
    }
    if (!matches) return 0;
    let t = 0;
    let k = 0;
    for (let i = 0; i < s1.length; i++) {
      if (match1[i]) {
        while (!match2[k]) k++;
        if (s1[i] !== s2[k]) t++;
        k++;
      }
    }
    const m = matches;
    const jaro = (m / s1.length + m / s2.length + (m - t / 2) / m) / 3;
    let l = 0;
    while (l < 4 && s1[l] === s2[l]) l++;
    return jaro + l * 0.1 * (1 - jaro);
  }

  function getScholarAuthorName() {
    const el = document.getElementById("gsc_prf_in");
    return el ? el.textContent.trim() : "";
  }

  function getScholarSampleTitles(limit = 10) {
    return Array.from(document.querySelectorAll(".gsc_a_at")).slice(0, limit).map((el) => el.textContent.trim());
  }

  // Lazy reference to the dynamically-imported author module. Populated on first
  // call to ensureSelfCitationEstimate() on an author profile page.
  let _authorModule = null;
  async function getAuthorModule() {
    if (_authorModule) return _authorModule;
    _authorModule = await import(chrome.runtime.getURL("src/content/content-author.js"));
    return _authorModule;
  }

  async function ensureSelfCitationEstimate() {
    const scholarId = new URL(window.location.href).searchParams.get("user");
    if (!scholarId) return;
    const state = window.__suSelfCiteState || { key: null, loading: false, data: null };
    if (state.key === scholarId && (state.loading || state.data)) return;
    window.__suSelfCiteState = { key: scholarId, loading: true, data: null };

    const cacheKey    = `selfcite_${scholarId}`;
    const SESSION_KEY = `suSC_${scholarId}`;

    // L1: chrome.storage.session — same browser session, fastest lookup.
    if (chrome.storage?.session?.get) {
      try {
        const sess = await chrome.storage.session.get({ [SESSION_KEY]: null });
        if (sess[SESSION_KEY]) {
          window.__suSelfCiteState = { key: scholarId, loading: false, data: sess[SESSION_KEY] };
          await renderAuthorStatsWithGrowth(window.suFullAuthorStats);
          return;
        }
      } catch (_) {}
    }

    // L2: chrome.storage.local — 30-day persistent cache.
    let cached = null;
    try {
      cached = await chrome.storage.local.get({ [cacheKey]: null });
    } catch (_) {
      window.__suSelfCiteState = { key: scholarId, loading: false, data: { status: "error", message: "Extension context invalidated." } };
      return;
    }
    const entry  = cached[cacheKey];
    if (entry && entry.timestamp && Date.now() - entry.timestamp < SELF_CITE_CACHE_MS) {
      window.__suSelfCiteState = { key: scholarId, loading: false, data: entry.data };
      // Populate session cache so subsequent page navigations skip local storage.
      if (chrome.storage?.session?.set) {
        try { chrome.storage.session.set({ [SESSION_KEY]: entry.data }); } catch (_) {}
      }
      await renderAuthorStatsWithGrowth(window.suFullAuthorStats);
      return;
    }

    // Cache miss: fetch from OpenAlex via the lazily-imported author module.
    const authorName = getScholarAuthorName();
    const titles     = getScholarSampleTitles(10);
    if (!authorName || !titles.length) {
      window.__suSelfCiteState = { key: scholarId, loading: false, data: { status: "error", message: "Missing author data." } };
      await renderAuthorStatsWithGrowth(window.suFullAuthorStats);
      return;
    }

    let result;
    try {
      const mod = await getAuthorModule();
      result = await mod.computeSelfCitationRate(authorName, titles);
    } catch (_) {
      result = { status: "error", message: "Author module unavailable." };
    }

    window.__suSelfCiteState = { key: scholarId, loading: false, data: result };
    if (result.status === "success") {
      // Write to both caches.
      chrome.storage.local.set({ [cacheKey]: { data: result, timestamp: Date.now() } }).catch(() => {});
      if (chrome.storage?.session?.set) {
        try { chrome.storage.session.set({ [SESSION_KEY]: result }); } catch (_) {}
      }
    }
    await renderAuthorStatsWithGrowth(window.suFullAuthorStats);
  }

  const AUTHOR_FEATURE_TOGGLES_KEY = "authorFeatureToggles";
  const AUTHOR_CITEDBY_COLOR_KEY = "authorCitedByColorScheme";
  const AUTHOR_GRAPH_COLLECTIONS_KEY = "authorGraphCollections";
  const AUTHOR_GRAPH_STATE_KEY = "authorGraphStates";
  const AUTHOR_GRAPH_ALERTS_KEY = "authorGraphAlerts";
  const POP_PEER_CACHE_KEY = "popPeerCache";
  const POP_PEER_TTL_MS = 7 * 24 * 60 * 60 * 1000;
  const POP_CONCEPT_TTL_MS = 30 * 24 * 60 * 60 * 1000;
  const REVIEW_PROJECTS_KEY = "reviewProjects";
  const REVIEW_DEFAULT_QUALITY_CHECKLIST = [
    { key: "randomization", label: "Randomization" },
    { key: "blinding", label: "Blinding" },
    { key: "allocation", label: "Allocation concealment" },
    { key: "attrition", label: "Attrition handling" },
    { key: "reporting", label: "Selective reporting" }
  ];
  const REVIEW_DEFAULT_REVIEWERS = [{ id: "you", name: "You" }];
  const DEFAULT_AUTHOR_FEATURE_TOGGLES = {
    citedBy: true,
    coauthors: true,
    velocityBadge: true,
    venues: true,
    citationBands: true,
    topics: true,
    filterBadges: true,
    graph: true,
    researchIntel: true
  };
  const DEFAULT_CITEDBY_COLOR_SCHEME = "red-blue";

  async function getAuthorFeatureToggles(scholarId) {
    if (!scholarId || !chrome?.storage?.local?.get) {
      return { ...DEFAULT_AUTHOR_FEATURE_TOGGLES };
    }
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_FEATURE_TOGGLES_KEY]: {} });
      const map = stored[AUTHOR_FEATURE_TOGGLES_KEY] || {};
      const entry = map[scholarId] || {};
      return { ...DEFAULT_AUTHOR_FEATURE_TOGGLES, ...entry };
    } catch {
      return { ...DEFAULT_AUTHOR_FEATURE_TOGGLES };
    }
  }

  async function setAuthorFeatureToggles(scholarId, next) {
    if (!scholarId || !chrome?.storage?.local?.get || !chrome?.storage?.local?.set) return;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_FEATURE_TOGGLES_KEY]: {} });
      const map = stored[AUTHOR_FEATURE_TOGGLES_KEY] || {};
      map[scholarId] = { ...DEFAULT_AUTHOR_FEATURE_TOGGLES, ...next };
      await chrome.storage.local.set({ [AUTHOR_FEATURE_TOGGLES_KEY]: map });
    } catch {
      // ignore
    }
  }

  async function getAuthorCitedByColorScheme(scholarId) {
    if (!scholarId || !chrome?.storage?.local?.get) {
      return DEFAULT_CITEDBY_COLOR_SCHEME;
    }
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_CITEDBY_COLOR_KEY]: {} });
      const map = stored[AUTHOR_CITEDBY_COLOR_KEY] || {};
      return map[scholarId] || DEFAULT_CITEDBY_COLOR_SCHEME;
    } catch {
      return DEFAULT_CITEDBY_COLOR_SCHEME;
    }
  }

  async function setAuthorCitedByColorScheme(scholarId, scheme) {
    if (!scholarId || !chrome?.storage?.local?.get || !chrome?.storage?.local?.set) return;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_CITEDBY_COLOR_KEY]: {} });
      const map = stored[AUTHOR_CITEDBY_COLOR_KEY] || {};
      map[scholarId] = scheme || DEFAULT_CITEDBY_COLOR_SCHEME;
      await chrome.storage.local.set({ [AUTHOR_CITEDBY_COLOR_KEY]: map });
    } catch {
      // ignore
    }
  }

  let popPeerCache = null;
  let popPeerCacheSaveTimer = null;
  let popPeerStorageBlocked = false;
  const POP_CONCEPT_PRESETS = {
    "Information Systems": ["Information Systems", "Management information systems", "Computer information systems", "Information science", "Information technology"],
    "Computer Science": ["Computer science", "Artificial intelligence", "Software engineering"],
    "Biology": ["Biology", "Molecular biology"],
    "Economics": ["Economics"],
    "Business": ["Business"],
    "Psychology": ["Psychology"],
    "Medicine": ["Medicine"],
    "Engineering": ["Engineering"],
    "Physics": ["Physics"],
    "Chemistry": ["Chemistry"],
    "Mathematics": ["Mathematics"],
    "Education": ["Education"],
    "Sociology": ["Sociology"],
    "Custom…": []
  };

  function getPopPeerConfig() {
    const defaults = { preset: "Information Systems", yearWindow: 2, customConcepts: "" };
    const cfg = window.suPopPeerConfig && typeof window.suPopPeerConfig === "object" ? window.suPopPeerConfig : {};
    const preset = Object.prototype.hasOwnProperty.call(POP_CONCEPT_PRESETS, cfg.preset) ? cfg.preset : defaults.preset;
    const yearWindow = Number.isFinite(cfg.yearWindow) ? Math.max(0, Math.min(2, cfg.yearWindow)) : defaults.yearWindow;
    const customConcepts = typeof cfg.customConcepts === "string" ? cfg.customConcepts : defaults.customConcepts;
    return { preset, yearWindow, customConcepts };
  }

  function setPopPeerConfig(next) {
    const cfg = getPopPeerConfig();
    window.suPopPeerConfig = { ...cfg, ...next };
    return window.suPopPeerConfig;
  }

  function getPopPeerConceptQueries(cfg) {
    if (!cfg) return [];
    if (cfg.preset === "Custom…") {
      return String(cfg.customConcepts || "")
        .split(/[;,]/g)
        .map((s) => s.trim())
        .filter(Boolean);
    }
    const presetList = POP_CONCEPT_PRESETS[cfg.preset];
    if (Array.isArray(presetList) && presetList.length) return presetList;
    return cfg.preset ? [cfg.preset] : [];
  }

  function isStorageBlockedError(err) {
    const msg = String(err?.message || err || "").toLowerCase();
    return msg.includes("access to storage is not allowed") || msg.includes("context invalidated");
  }

  async function getPopPeerCache() {
    if (popPeerCache) return popPeerCache;
    if (popPeerStorageBlocked || !chrome?.storage?.local?.get) {
      popPeerCache = { peers: {}, concepts: {} };
      return popPeerCache;
    }
    try {
      const stored = await chrome.storage.local.get({ [POP_PEER_CACHE_KEY]: {} });
      const cache = stored[POP_PEER_CACHE_KEY] || {};
      popPeerCache = {
        peers: cache.peers || {},
        concepts: cache.concepts || {}
      };
      return popPeerCache;
    } catch {
      popPeerStorageBlocked = true;
      popPeerCache = { peers: {}, concepts: {} };
      return popPeerCache;
    }
  }

  function schedulePopPeerCacheSave() {
    if (popPeerCacheSaveTimer) return;
    popPeerCacheSaveTimer = setTimeout(() => {
      popPeerCacheSaveTimer = null;
      if (!popPeerCache || popPeerStorageBlocked || !chrome?.storage?.local?.set) return;
      try {
        chrome.storage.local.set({ [POP_PEER_CACHE_KEY]: popPeerCache }).catch((err) => {
          if (isStorageBlockedError(err)) popPeerStorageBlocked = true;
        });
      } catch (err) {
        if (isStorageBlockedError(err)) popPeerStorageBlocked = true;
      }
    }, 1000);
  }

  async function getAuthorGraphCollections(scholarId) {
    if (!scholarId || !chrome?.storage?.local?.get) return [];
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_COLLECTIONS_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_COLLECTIONS_KEY] || {};
      const list = map[scholarId];
      return Array.isArray(list) ? list : [];
    } catch {
      return [];
    }
  }

  async function setAuthorGraphCollections(scholarId, list) {
    if (!scholarId || !chrome?.storage?.local?.get || !chrome?.storage?.local?.set) return;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_COLLECTIONS_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_COLLECTIONS_KEY] || {};
      map[scholarId] = Array.isArray(list) ? list : [];
      await chrome.storage.local.set({ [AUTHOR_GRAPH_COLLECTIONS_KEY]: map });
    } catch {
      // ignore
    }
  }

  async function getAuthorGraphState(scholarId) {
    if (!scholarId || !chrome?.storage?.local?.get) return null;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_STATE_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_STATE_KEY] || {};
      return map[scholarId] || null;
    } catch {
      return null;
    }
  }

  async function setAuthorGraphState(scholarId, graphState) {
    if (!scholarId || !chrome?.storage?.local?.get || !chrome?.storage?.local?.set) return;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_STATE_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_STATE_KEY] || {};
      map[scholarId] = graphState || null;
      await chrome.storage.local.set({ [AUTHOR_GRAPH_STATE_KEY]: map });
    } catch {
      // ignore
    }
  }

  async function getAuthorGraphAlerts(scholarId) {
    if (!scholarId || !chrome?.storage?.local?.get) return null;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_ALERTS_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_ALERTS_KEY] || {};
      return map[scholarId] || null;
    } catch {
      return null;
    }
  }

  async function setAuthorGraphAlerts(scholarId, data) {
    if (!scholarId || !chrome?.storage?.local?.get || !chrome?.storage?.local?.set) return;
    try {
      const stored = await chrome.storage.local.get({ [AUTHOR_GRAPH_ALERTS_KEY]: {} });
      const map = stored[AUTHOR_GRAPH_ALERTS_KEY] || {};
      map[scholarId] = data || null;
      await chrome.storage.local.set({ [AUTHOR_GRAPH_ALERTS_KEY]: map });
    } catch {
      // ignore
    }
  }

  async function getReviewProjects() {
    if (!chrome?.storage?.local?.get) return {};
    try {
      const stored = await chrome.storage.local.get({ [REVIEW_PROJECTS_KEY]: {} });
      return stored[REVIEW_PROJECTS_KEY] || {};
    } catch {
      return {};
    }
  }

  async function setReviewProjects(map) {
    if (!chrome?.storage?.local?.set) return;
    try {
      await chrome.storage.local.set({ [REVIEW_PROJECTS_KEY]: map || {} });
    } catch {
      // ignore
    }
  }

  function normalizeReviewProject(project) {
    if (!project || typeof project !== "object") return null;
    project.papers = project.papers || {};
    project.fingerprints = project.fingerprints || {};
    project.decisions = project.decisions || {};
    project.pico = project.pico || {};
    project.tags = project.tags || {};
    project.notes = project.notes || {};
    project.extractionFields = Array.isArray(project.extractionFields) && project.extractionFields.length
      ? project.extractionFields
      : ["Design", "Sample", "Outcome"];
    project.extraction = project.extraction || {};
    project.dedupeCount = Number(project.dedupeCount) || 0;
    project.duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
    project.highlights = project.highlights || {};
    project.quality = project.quality || {};
    project.qualityChecklist = Array.isArray(project.qualityChecklist) && project.qualityChecklist.length
      ? project.qualityChecklist
      : REVIEW_DEFAULT_QUALITY_CHECKLIST.map((c) => ({ ...c }));
    project.reviewers = Array.isArray(project.reviewers) && project.reviewers.length
      ? project.reviewers
      : REVIEW_DEFAULT_REVIEWERS.map((r) => ({ ...r }));
    if (!project.activeReviewerId || !project.reviewers.some((r) => r.id === project.activeReviewerId)) {
      project.activeReviewerId = project.reviewers[0]?.id || "you";
    }
    project.blindMode = !!project.blindMode;
    project.updates = Array.isArray(project.updates) ? project.updates : [];
    project.lastUpdateCheck = project.lastUpdateCheck || null;
    return project;
  }

  function createReviewProject(name, query) {
    const id = `rev_${Date.now().toString(36)}`;
    return {
      id,
      name: name || "New review",
      query: query || "",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      papers: {},
      fingerprints: {},
      decisions: {},
      pico: {},
      tags: {},
      notes: {},
      extractionFields: ["Design", "Sample", "Outcome"],
      extraction: {},
      dedupeCount: 0,
      duplicates: [],
      highlights: {},
      quality: {},
      qualityChecklist: REVIEW_DEFAULT_QUALITY_CHECKLIST.map((c) => ({ ...c })),
      reviewers: REVIEW_DEFAULT_REVIEWERS.map((r) => ({ ...r })),
      activeReviewerId: "you",
      blindMode: false,
      lastUpdateCheck: null,
      updates: []
    };
  }

  async function exportAuthorCsv(state) {
    const { results, isAuthorProfile } = scanResults();
    if (!isAuthorProfile) return;
    const settings = state?.settings || {};
    const rows = [
      ["Title", "Authors", "Venue", "Year", "Citations", "Citations/Year", "URL", "Cited By URL", "Cluster ID", "Quality Badges"]
    ];
    for (const r of results) {
      const paper = getCachedAuthorPaper(r);
      if (!paper || !paper.title) continue;
      const authorsPart = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0]?.trim() || "";
      const citations = getCachedAuthorCitationCount(r);
      const velocity = computeVelocity(citations, paper.year) || "";
      const badges = settings.showQualityBadges
        ? (qualityBadgesForVenue(paper.venue, state.qIndex) || []).map((b) => b.text).join("; ")
        : "";
      rows.push([
        paper.title || "",
        authorsPart,
        paper.venue || "",
        paper.year || "",
        citations || 0,
        velocity,
        paper.url || "",
        paper.citedByUrl || "",
        paper.clusterId || "",
        badges
      ]);
    }
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    const authorName = getScholarAuthorName().replace(/[^a-z0-9]+/gi, "-").replace(/^-+|-+$/g, "").toLowerCase() || "author";
    a.href = url;
    a.download = `${authorName}-papers.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }
  // Per-row parse cache to avoid repeated DOM parsing.
  const rowParseCache = new WeakMap();
  function getRowCache(row) {
    let cache = rowParseCache.get(row);
    if (!cache) {
      cache = { fast: null, full: null, author: null, authorCitations: null };
      rowParseCache.set(row, cache);
    }
    return cache;
  }
  function invalidateRowCache(row) {
    if (!row) return;
    const cache = rowParseCache.get(row);
    if (cache) {
      cache.fast = null;
      cache.full = null;
      cache.author = null;
      cache.authorCitations = null;
    }
    if (row.dataset) delete row.dataset.suRetractChecked;
    row.__suDirty = true;
  }
  function getCachedPaperFast(row) {
    if (!row) return null;
    const cache = getRowCache(row);
    if (!row.__suDirty && cache.fast) return cache.fast;
    const paper = extractPaperFromResultFast(row);
    cache.fast = paper;
    if (row.__suDirty) cache.full = null;
    row.__suDirty = false;
    return paper;
  }
  function getCachedPaperFull(row) {
    if (!row) return null;
    const cache = getRowCache(row);
    if (!row.__suDirty && cache.full) return cache.full;
    const paper = extractPaperFromResult(row, { allowCrossResult: false, deepScan: true });
    cache.full = paper;
    if (!cache.fast) cache.fast = paper;
    row.__suDirty = false;
    return paper;
  }

  function getCachedAuthorPaper(row) {
    if (!row) return null;
    const cache = getRowCache(row);
    if (cache.author) return cache.author;
    const paper = extractPaperFromAuthorProfile(row);
    cache.author = paper;
    return paper;
  }

  function getCachedAuthorCitationCount(row) {
    if (!row) return 0;
    const cache = getRowCache(row);
    if (cache.authorCitations != null) return cache.authorCitations;
    const citations = extractCitationCount(row);
    cache.authorCitations = citations;
    return citations;
  }

  function attachFloatingTooltip(el, text) {
    if (!el || !text) return;
    if (el.__suFloatingTooltip) {
      el.__suFloatingTooltip.textContent = text;
      return;
    }
    if (el.hasAttribute("title")) el.removeAttribute("title");
    const tip = document.createElement("div");
    tip.className = "su-floating-tooltip";
    tip.textContent = text;
    tip.style.display = "none";
    document.body.appendChild(tip);
    el.__suFloatingTooltip = tip;
    tip.__suOwner = el;

    if (!window.__suFloatingTooltipObserver) {
      window.__suFloatingTooltipObserver = new MutationObserver(() => {
        const tips = document.querySelectorAll(".su-floating-tooltip");
        for (const t of tips) {
          const owner = t.__suOwner;
          if (owner && !owner.isConnected) {
            t.remove();
          }
        }
      });
      window.__suFloatingTooltipObserver.observe(document.body, { childList: true, subtree: true });
    }

    const hide = () => {
      tip.classList.remove("su-floating-tooltip-visible");
      tip.style.display = "none";
      if (tip.__suOwner && !tip.__suOwner.isConnected) {
        tip.remove();
      }
    };

    const position = (e) => {
      if (!tip.classList.contains("su-floating-tooltip-visible")) return;
      if (!el.isConnected) {
        hide();
        return;
      }
      const pad = 8;
      const gap = 12;
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const tipRect = tip.getBoundingClientRect();
      let left = (e?.clientX ?? 0) + gap;
      let top = (e?.clientY ?? 0) + gap;
      if (left + tipRect.width > vw - pad) left = vw - tipRect.width - pad;
      if (left < pad) left = pad;
      if (top + tipRect.height > vh - pad) top = vh - tipRect.height - pad;
      if (top < pad) top = pad;
      tip.style.left = left + "px";
      tip.style.top = top + "px";
    };

    const show = (e) => {
      const openTips = document.querySelectorAll(".su-floating-tooltip-visible");
      for (const t of openTips) {
        if (t !== tip) {
          t.classList.remove("su-floating-tooltip-visible");
          t.style.display = "none";
        }
      }
      tip.classList.add("su-floating-tooltip-visible");
      tip.style.display = "block";
      position(e);
    };
    el.addEventListener("pointerenter", show);
    el.addEventListener("pointermove", position);
    el.addEventListener("pointerleave", hide);
    document.addEventListener("pointerdown", hide, true);
    window.addEventListener("scroll", hide, true);
    window.addEventListener("blur", hide);
  }

  /** Keep a fixed-position tooltip inside the viewport. Call after making it visible. */
  function clampFixedToViewport(el, pad = 8) {
    if (!el || !el.isConnected) return;
    const rect = el.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    let left = rect.left;
    let top = rect.top;
    if (rect.right > vw - pad) left = vw - rect.width - pad;
    if (rect.left < pad) left = pad;
    if (rect.bottom > vh - pad) top = vh - rect.height - pad;
    if (rect.top < pad) top = pad;
    el.style.left = left + "px";
    el.style.top = top + "px";
  }

  async function loadEraAndNorwegian() {
    if (window.__suEraNorwegian) return window.__suEraNorwegian;
    let eraSet = new Set();
    let norwegianMap = new Map();
    let absIndex = new Map();
    try {
      const base = chrome.runtime.getURL("src/data/");
      const [eraText, absText] = await Promise.all([
        fetch(base + "era2023.txt").then((r) => (r.ok ? r.text() : "")).catch(() => ""),
        fetch(base + "abs2024.csv").then((r) => (r.ok ? r.text() : "")).catch(() => "")
      ]);

      for (const line of (eraText || "").split(/\r?\n/)) {
        const t = line.trim();
        if (!t || t.startsWith("#")) continue;
        const n = normalizeVenueName(t);
        if (n) eraSet.add(n);
      }

      // Norwegian register: prefer the pre-processed compact JSON (1 MB) over the raw
      // 15 MB CSV.  Fall back to the CSV only if the compact file is missing (e.g., in a
      // dev environment before running scripts/build_norwegian_compact.js).
      let norwegianLoaded = false;
      try {
        const norJson = await fetch(base + "norwegian_compact.json").then((r) => r.ok ? r.json() : null).catch(() => null);
        if (norJson && typeof norJson === "object") {
          for (const [key, level] of Object.entries(norJson)) {
            if (level === "1" || level === "2") norwegianMap.set(key, level);
          }
          norwegianLoaded = true;
        }
      } catch (_) {}

      if (!norwegianLoaded) {
        // Fallback: parse the full CSV (slow but always available).
        const norwegianText = await fetch(base + "norwegian_register.csv").then((r) => (r.ok ? r.text() : "")).catch(() => "");
        const norLines = (norwegianText || "").split(/\r?\n/).filter((l) => l.trim());
        if (norLines.length > 0) {
          const header = parseCsvLine(norLines[0], ";");
          const titleIdx = header.findIndex((h) => /International Title|Original Title/i.test(String(h)));
          const levelIdx = header.findIndex((h) => /^Level 20\d{2}$/.test(String(h).trim()));
          const useTitleIdx = titleIdx >= 0 ? titleIdx : 2;
          const useLevelIdx = levelIdx >= 0 ? levelIdx : 9;
          for (let i = 1; i < norLines.length; i++) {
            const cells = parseCsvLine(norLines[i], ";");
            const name = (cells[useTitleIdx] || "").trim();
            const level = String(cells[useLevelIdx] || "").replace(/\D/g, "").slice(0, 1);
            if (name && (level === "1" || level === "2")) {
              const n = normalizeVenueName(name);
              if (n) norwegianMap.set(n, level);
            }
          }
        }
      }

      for (const line of (absText || "").split(/\r?\n/)) {
        const idx = line.lastIndexOf(",");
        if (idx <= 0 || idx >= line.length - 1) continue;
        const name = line.slice(0, idx).trim().replace(/^"|"$/g, "");
        const rank = line.slice(idx + 1).trim();
        if (!name || !rank) continue;
        const n = normalizeVenueName(name);
        if (n && /^4\*?$|^[1234]$/i.test(rank)) absIndex.set(n, rank);
      }
    } catch (_) {
      eraSet = new Set();
      norwegianMap = new Map();
      absIndex = new Map();
    }
    window.__suEraNorwegian = { eraSet, norwegianMap, absIndex };
    return window.__suEraNorwegian;
  }

  async function loadH5Index() {
    if (window.__suH5Index) return window.__suH5Index;
    try {
      const url = chrome.runtime.getURL("src/data/venue_h5_index.json");
      const r = await fetch(url);
      const data = (r.ok ? await r.json() : null) || {};
      window.__suH5Index = data;
      return data;
    } catch (_) {
      window.__suH5Index = {};
      return {};
    }
  }

  async function loadVhbIndex() {
    if (window.__suVhbIndex) return window.__suVhbIndex;
    const map = new Map();
    try {
      const url = chrome.runtime.getURL("src/data/vhb2024.csv");
      const r = await fetch(url);
      const text = r.ok ? await r.text() : "";
      for (const line of (text || "").split(/\r?\n/)) {
        const idx = line.lastIndexOf(",");
        if (idx <= 0 || idx >= line.length - 1) continue;
        const name = line.slice(0, idx).trim();
        const rank = normalizeVhbRank(line.slice(idx + 1).trim());
        if (!name || !rank) continue;
        for (const syn of String(name).split("|")) {
          const n = normalizeVenueName(syn);
          if (n) map.set(n, rank);
        }
      }
    } catch (_) {
      // Ignore; fall back to user list.
    }
    window.__suVhbIndex = map;
    return map;
  }

  async function loadImpactIndex() {
    if (window.__suImpactIndex) return window.__suImpactIndex;
    const map = new Map();
    try {
      const url = chrome.runtime.getURL("src/data/journal_impact_2024.csv");
      const r = await fetch(url);
      const text = r.ok ? await r.text() : "";
      for (const line of (text || "").split(/\r?\n/)) {
        if (!line || /^\\s*Journal\\s*Name\\s*,/i.test(line)) continue;
        const idx = line.lastIndexOf(",");
        if (idx <= 0 || idx >= line.length - 1) continue;
        const name = line.slice(0, idx).trim().replace(/^\"|\"$/g, "");
        const raw = line.slice(idx + 1).trim();
        const val = parseFloat(String(raw || "").replace(/[^0-9.]/g, ""));
        if (!name || !Number.isFinite(val) || val <= 0) continue;
        const n = normalizeVenueName(name);
        if (n) map.set(n, val);
      }
    } catch (_) {
      // Ignore; IF badge will be unavailable if load fails.
    }
    window.__suImpactIndex = map;
    return map;
  }

  // ——— Retraction Watch (local Bloom filter + optional Crossref check) ———
  const retractionCheckCache = new Map();

  function fnv1a32(str) {
    let h = 2166136261;
    const prime = 16777619;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, prime);
    }
    return h >>> 0;
  }

  function bloomIndices(doi, m, k) {
    const s = String(doi).toLowerCase().trim();
    const h1 = fnv1a32(s);
    const h2 = (fnv1a32(s + "\u0001salt") | 1) >>> 0;
    const indices = [];
    for (let i = 0; i < k; i++) {
      indices.push(((h1 + i * h2) >>> 0) % m);
    }
    return indices;
  }

  function decodeBloomBits(bitsBase64) {
    const bin = atob(String(bitsBase64 || ""));
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }

  async function loadRetractionBloom() {
    if (window.__suRetractionBloom) return window.__suRetractionBloom;
    try {
      const url = chrome.runtime.getURL("src/data/retraction_bloom.json");
      const r = await fetch(url);
      const data = r.ok ? await r.json() : null;
      if (!data || !data.bits || !data.m || !data.k) throw new Error("Invalid bloom data");
      const bits = decodeBloomBits(data.bits);
      window.__suRetractionBloom = { m: data.m, k: data.k, bits, source: data.source, built: data.built, count: data.count };
      return window.__suRetractionBloom;
    } catch (_) {
      window.__suRetractionBloom = null;
      return null;
    }
  }

  function bloomHasDoi(doi, bloom) {
    if (!doi || !bloom) return false;
    const { m, k, bits } = bloom;
    if (!m || !k || !bits) return false;
    for (const idx of bloomIndices(doi, m, k)) {
      const byteIdx = idx >> 3;
      const mask = 1 << (idx & 7);
      if ((bits[byteIdx] & mask) === 0) return false;
    }
    return true;
  }

  async function checkRetractionStatus(doi) {
    const key = String(doi).toLowerCase().trim();
    if (!key || key.length < 10) return false;
    if (retractionCheckCache.has(key)) return retractionCheckCache.get(key);
    try {
      const url = `https://api.crossref.org/works/${encodeURIComponent(key)}?mailto=scholar-extension@local`;
      const r = await fetch(url);
      if (!r.ok) {
        retractionCheckCache.set(key, false);
        return false;
      }
      const data = await r.json();
      const updatedBy = data?.message?.["updated-by"];
      const isRetracted = Array.isArray(updatedBy) && updatedBy.length > 0;
      retractionCheckCache.set(key, isRetracted);
      return isRetracted;
    } catch {
      retractionCheckCache.set(key, false);
      return false;
    }
  }

  const DOI_REGEX = /10\.\d{4,}\/[^\s"'<>]+/gi;
  function extractDOIFromResult(container) {
    const seen = new Set();
    const candidates = [];
    const add = (raw) => {
      const normalized = raw.toLowerCase().trim().replace(/#.*$/, "").split("?")[0];
      if (normalized.length < 10 || seen.has(normalized)) return;
      seen.add(normalized);
      candidates.push(normalized);
    };
    const scanText = (str) => {
      if (!str) return;
      DOI_REGEX.lastIndex = 0;
      let match;
      while ((match = DOI_REGEX.exec(str)) !== null) add(match[0]);
    };
    const snippetEl = container.querySelector(".gs_rs");
    if (snippetEl) scanText(text(snippetEl));
    const authorVenueEl = container.querySelector(".gs_a");
    if (authorVenueEl) scanText(text(authorVenueEl));
    const links = container.querySelectorAll("a[href]");
    for (const a of links) {
      const href = a.getAttribute("href") || "";
      const m = href.match(/10\.\d{4,}\/[^\s"'<>?#]+/i);
      if (m) add(m[0]);
    }
    if (candidates.length === 0) scanText(text(container));
    return candidates[0] || null;
  }

  function text(el) {
    return (el?.innerText || "").replace(/\s+/g, " ").trim();
  }

  function escapeHtml(s) {
    const t = String(s ?? "");
    return t
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  /** Abstract/snippet cue phrases (regex): bold for visual salience, no LLM. */
  const SNIPPET_CUE_PATTERNS = [
    /\bWe propose\b/gi,
    /\bWe evaluate\b/gi,
    /\bWe find that\b/gi,
    /\bUsing\s+.{1,40}?\s+dataset\b/gi,
    /\bWe show that\b/gi,
    /\bWe demonstrate\b/gi,
    /\bOur results?\s+(?:show|suggest|indicate)\b/gi,
    /\bIn this paper\b/gi
  ];

  function applySnippetCueEmphasis(snippetText) {
    if (!snippetText || typeof snippetText !== "string") return "";
    let out = escapeHtml(snippetText);
    for (const re of SNIPPET_CUE_PATTERNS) {
      out = out.replace(re, (match) => `<span class="su-snippet-cue">${match}</span>`);
    }
    return out;
  }

  /**
   * Detect code/data artifact mentions in snippet and links (GitHub, Zenodo, OSF, etc.).
   * Returns { code: boolean, data: boolean }.
   */
  function detectArtifacts(container) {
    const codePatterns = [
      /github\.com/i, /gitlab\.com/i, /bitbucket\.org/i,
      /\bcode\s+repository\b/i, /\breplication\s+package\b/i,
      /\bcode\s+available\b/i, /\bsource\s+code\b/i, /\bsoftware\s+availability\b/i
    ];
    const dataPatterns = [
      /zenodo\.org/i, /osf\.io/i, /figshare\.com/i, /\bopen\s+science\s+framework\b/i,
      /\bdataset\b/i, /\bsupplementary\s+(?:material|data|information)?\b/i,
      /\bdata\s+availability\b/i, /\bdata\s+available\b/i, /\bdata\s+and\s+code\b/i,
      /\breplication\s+data\b/i
    ];
    const haystack = [];
    const snippetEl = container.querySelector(".gs_rs");
    if (snippetEl) haystack.push(text(snippetEl));
    for (const a of container.querySelectorAll("a[href]")) {
      haystack.push(a.getAttribute("href") || "");
      haystack.push(text(a));
    }
    haystack.push(text(container));
    const joined = haystack.join(" ");
    const code = codePatterns.some((p) => p.test(joined));
    const data = dataPatterns.some((p) => p.test(joined));
    return { code, data };
  }

  // --- External signals (no-key APIs) ---
  const EXTERNAL_SIGNAL_TTLS = {
    crossref: 30 * 24 * 60 * 60 * 1000,
    opencitations: 30 * 24 * 60 * 60 * 1000,
    opencitations_coci: 30 * 24 * 60 * 60 * 1000,
    datacite: 90 * 24 * 60 * 60 * 1000,
    unpaywall: 30 * 24 * 60 * 60 * 1000,
    openalex: 30 * 24 * 60 * 60 * 1000,
    arxiv: 60 * 24 * 60 * 60 * 1000,
    epmc: 30 * 24 * 60 * 60 * 1000,
    ncbi: 30 * 24 * 60 * 60 * 1000,
    ror: 180 * 24 * 60 * 60 * 1000,
    dblp: 90 * 24 * 60 * 60 * 1000,
    dblp_sparql: 7 * 24 * 60 * 60 * 1000,
    s2: 30 * 24 * 60 * 60 * 1000
  };
  let externalSignalCache = null;
  let externalSignalSaveTimer = null;
  const externalSignalInFlight = new Map();
  const externalSignalQueue = [];
  let externalSignalActive = 0;
  const EXTERNAL_SIGNAL_CONCURRENCY = 2;
  const EXTERNAL_SIGNAL_DELAY_MS = 500;
  const EXTERNAL_SIGNAL_PROVIDER_LIMITS = {
    arxiv: { minDelay: 3100 },
    ncbi: { minDelay: 400 },
    epmc: { minDelay: 500 },
    unpaywall: { minDelay: 600 },
    ror: { minDelay: 800 },
    dblp: { minDelay: 700 },
    dblp_sparql: { minDelay: 1600 },
    s2: { minDelay: 900 },
    crossref: { minDelay: 400 },
    opencitations: { minDelay: 600 },
    opencitations_coci: { minDelay: 700 },
    datacite: { minDelay: 600 },
    openalex: { minDelay: 700 }
  };
  const externalSignalProviderNext = new Map();
  let externalSignalReapplyTimer = null;

  function normalizeDoi(doi) {
    if (!doi) return null;
    let d = String(doi).trim().toLowerCase();
    d = d.replace(/^https?:\/\/(dx\.)?doi\.org\//, "");
    d = d.replace(/^doi:\s*/i, "");
    return d || null;
  }

  function makeExternalKey(prefix, raw) {
    const v = String(raw || "").trim();
    if (!v) return null;
    const clean = v.toLowerCase();
    return prefix ? `${prefix}:${clean}` : clean;
  }

  async function fetchExternalJson(url, { timeoutMs = 10000, headers = null, preferBackground = true } = {}) {
    if (!url) return null;
    try {
      if (!preferBackground) {
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), timeoutMs);
        const res = await fetch(url, { credentials: "omit", headers: headers || {}, signal: ctrl.signal });
        clearTimeout(t);
        if (res.ok) return await res.json();
      }
    } catch (_) {}
    try {
      const res = await chrome.runtime.sendMessage({
        action: "fetchExternal",
        url,
        responseType: "json",
        timeoutMs,
        headers: headers || {}
      });
      if (res?.ok) return res.body;
    } catch (_) {}
    return null;
  }

  async function fetchExternalText(url, { timeoutMs = 10000, headers = null, preferBackground = true } = {}) {
    if (!url) return null;
    try {
      if (!preferBackground) {
        const ctrl = new AbortController();
        const t = setTimeout(() => ctrl.abort(), timeoutMs);
        const res = await fetch(url, { credentials: "omit", headers: headers || {}, signal: ctrl.signal });
        clearTimeout(t);
        if (res.ok) return await res.text();
      }
    } catch (_) {}
    try {
      const res = await chrome.runtime.sendMessage({
        action: "fetchExternal",
        url,
        responseType: "text",
        timeoutMs,
        headers: headers || {}
      });
      if (res?.ok) return res.body;
    } catch (_) {}
    return null;
  }

  async function ensureExternalSignalCacheLoaded() {
    if (externalSignalCache) return externalSignalCache;
    externalSignalCache = await getExternalSignalCache();
    return externalSignalCache;
  }

  function getExternalSignalEntry(key, provider) {
    if (!key || !provider || !externalSignalCache) return null;
    const entry = externalSignalCache[key]?.[provider];
    if (!entry || typeof entry !== "object") return null;
    const ttl = EXTERNAL_SIGNAL_TTLS[provider] || EXTERNAL_SIGNAL_TTLS.crossref;
    const ts = Number(entry.ts) || 0;
    if (ts && Date.now() - ts > ttl) return null;
    return entry;
  }

  function scheduleExternalSignalSave() {
    if (externalSignalSaveTimer) return;
    externalSignalSaveTimer = setTimeout(() => {
      externalSignalSaveTimer = null;
      if (externalSignalCache) {
        setExternalSignalCache(externalSignalCache).catch(() => {});
      }
    }, 1200);
  }

  function setExternalSignalEntry(key, provider, data) {
    if (!key || !provider) return;
    if (!externalSignalCache) externalSignalCache = {};
    if (!externalSignalCache[key]) externalSignalCache[key] = {};
    externalSignalCache[key][provider] = { ts: Date.now(), data: data == null ? null : data };
    scheduleExternalSignalSave();
  }

  function scheduleExternalSignalReapply() {
    if (externalSignalReapplyTimer) return;
    externalSignalReapplyTimer = setTimeout(() => {
      externalSignalReapplyTimer = null;
      try {
        const { results, isAuthorProfile } = scanResults();
        if (!isAuthorProfile) applyResultFilters(results, window.suState);
      } catch (_) {}
    }, 250);
  }

  async function fetchCrossrefSignal(doi) {
    const url = `https://api.crossref.org/works/${encodeURIComponent(doi)}?mailto=scholar-extension@local`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const msg = data?.message || {};
    const hasLicense = Array.isArray(msg.license) && msg.license.length > 0;
    const hasFullText = Array.isArray(msg.link) && msg.link.length > 0;
    const hasFunder = Array.isArray(msg.funder) && msg.funder.length > 0;
    const hasUpdate = Array.isArray(msg["update-to"]) && msg["update-to"].length > 0;
    const funderNames = Array.isArray(msg.funder)
      ? msg.funder.map((f) => f?.name || "").filter(Boolean)
      : [];
    const awardNumbers = Array.isArray(msg.funder)
      ? msg.funder.flatMap((f) => Array.isArray(f?.award) ? f.award : []).map((a) => String(a || "")).filter(Boolean)
      : [];
    return { hasLicense, hasFullText, hasFunder, hasUpdate, funderNames, awardNumbers };
  }

  async function fetchOpenCitationsSignal(doi) {
    const url = `https://opencitations.net/index/coci/api/v1/citations/${encodeURIComponent(doi)}`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const count = Array.isArray(data) ? data.length : 0;
    return { count };
  }

  async function fetchDataCiteSignal(doi) {
    const url = `https://api.datacite.org/dois/${encodeURIComponent(doi)}`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const rel = data?.data?.attributes?.relatedIdentifiers || [];
    const hasDataset = rel.some((r) => String(r?.resourceTypeGeneral || "").toLowerCase() === "dataset");
    const hasSoftware = rel.some((r) => String(r?.resourceTypeGeneral || "").toLowerCase() === "software");
    return { hasDataset, hasSoftware };
  }

  async function fetchUnpaywallSignal(doi) {
    const url = `https://api.unpaywall.org/v2/${encodeURIComponent(doi)}?email=scholar-extension@local`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const best = data?.best_oa_location || {};
    const isOa = !!data?.is_oa;
    const oaStatus = String(data?.oa_status || "").toLowerCase();
    const hostType = String(best?.host_type || "");
    const version = String(best?.version || "");
    const license = String(best?.license || "");
    const bestOaUrl = best?.url || data?.oa_locations?.[0]?.url || "";
    const bestOaUrlPdf = best?.url_for_pdf || data?.oa_locations?.[0]?.url_for_pdf || "";
    return {
      isOa,
      oaStatus,
      hostType,
      version,
      license,
      bestOaUrl,
      bestOaUrlPdf
    };
  }

  async function fetchArxivSignal(lookup) {
    const raw = typeof lookup === "string" ? lookup : (lookup?.id || "");
    const id = String(raw || "").replace(/^arxiv:/i, "").trim();
    if (!id) return null;
    const url = `https://export.arxiv.org/api/query?id_list=${encodeURIComponent(id)}`;
    const xml = await fetchExternalText(url, { timeoutMs: 15000 });
    if (!xml) return null;
    const doc = new DOMParser().parseFromString(xml, "text/xml");
    const entry = doc.querySelector("entry");
    if (!entry) return null;
    const idText = entry.querySelector("id")?.textContent || "";
    const idMatch = idText.match(/arxiv\.org\/abs\/([^v]+)(v\\d+)?/i);
    const version = idMatch?.[2] || "";
    const published = entry.querySelector("published")?.textContent || "";
    const updated = entry.querySelector("updated")?.textContent || "";
    const primaryCategory = entry.querySelector("arxiv\\:primary_category")?.getAttribute("term")
      || entry.getElementsByTagName("arxiv:primary_category")?.[0]?.getAttribute("term")
      || "";
    const categories = Array.from(entry.querySelectorAll("category"))
      .map((c) => c.getAttribute("term"))
      .filter(Boolean);
    return { id, version, primaryCategory, categories, published, updated };
  }

  async function fetchEpmcSignal(lookup) {
    let doi = null;
    let pmid = null;
    let pmcid = null;
    if (lookup && typeof lookup === "object") {
      doi = lookup.doi || null;
      pmid = lookup.pmid || null;
      pmcid = lookup.pmcid || null;
    } else if (typeof lookup === "string") {
      const raw = lookup.trim();
      if (raw.startsWith("pmid:")) pmid = raw.slice(5);
      else if (raw.startsWith("pmcid:")) pmcid = raw.slice(6);
      else if (raw.startsWith("doi:")) doi = raw.slice(4);
      else doi = raw;
    }
    let query = "";
    if (pmid) query = `EXT_ID:${pmid} AND SRC:MED`;
    else if (pmcid) query = `PMCID:${pmcid}`;
    else if (doi) query = `DOI:${doi}`;
    if (!query) return null;
    const url = `https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=${encodeURIComponent(query)}&format=json`;
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    if (!data) return null;
    const result = data?.resultList?.result?.[0] || null;
    if (!result) return null;
    const isOpenAccess = String(result?.isOpenAccess || "").toLowerCase() === "y";
    const pmidOut = result?.pmid || pmid || "";
    const pmcidOut = result?.pmcid || pmcid || "";
    const citationCount = Number(result?.citedByCount) || 0;
    const referenceCount = Number(result?.referenceCount) || 0;
    const fullTextUrls = Array.isArray(result?.fullTextUrlList?.fullTextUrl)
      ? result.fullTextUrlList.fullTextUrl.map((u) => u?.url).filter(Boolean)
      : [];
    return {
      pmid: pmidOut,
      pmcid: pmcidOut,
      isOpenAccess,
      citationCount,
      referenceCount,
      fullTextUrls
    };
  }

  async function fetchNcbiSignal(lookup) {
    let pmid = null;
    if (lookup && typeof lookup === "object") {
      pmid = lookup.pmid || null;
    } else if (typeof lookup === "string") {
      const raw = lookup.trim();
      pmid = raw.startsWith("pmid:") ? raw.slice(5) : raw;
    }
    if (!pmid) return null;
    const url = `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${encodeURIComponent(pmid)}&retmode=xml`;
    const xml = await fetchExternalText(url, { timeoutMs: 15000 });
    if (!xml) return null;
    const doc = new DOMParser().parseFromString(xml, "text/xml");
    const meshTerms = Array.from(doc.querySelectorAll("MeshHeading DescriptorName"))
      .map((el) => (el.textContent || "").trim())
      .filter(Boolean);
    const publicationTypes = Array.from(doc.querySelectorAll("PublicationTypeList PublicationType"))
      .map((el) => (el.textContent || "").trim())
      .filter(Boolean);
    return { pmid, meshTerms, publicationTypes };
  }

  async function fetchRorSignal(lookup) {
    const q = String(lookup || "").trim();
    if (!q) return null;
    const url = `https://api.ror.org/organizations?query=${encodeURIComponent(q)}`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const item = Array.isArray(data?.items) ? data.items[0] : null;
    if (!item || !item.organization) return null;
    const score = Number(item.score) || 0;
    if (score && score < 0.8) return null;
    return {
      id: item.organization.id || "",
      name: item.organization.name || "",
      country: item.organization.country?.country_name || "",
      score
    };
  }

  async function fetchDblpAuthorSignal(lookup) {
    const name = String(lookup || "").trim();
    if (!name) return null;
    const url = `https://dblp.org/search/author/api?q=${encodeURIComponent(name)}&format=json`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    const hit = data?.result?.hits?.hit?.[0]?.info || null;
    if (!hit) return null;
    return {
      url: hit.url || "",
      name: hit.author || name
    };
  }

  async function fetchDblpSparqlSignal(lookup) {
    let pid = "";
    if (typeof lookup === "string") {
      const m = lookup.match(/dblp\.org\/pid\/([^?#]+)/i);
      if (m) pid = m[1];
      else pid = lookup;
    } else if (lookup && typeof lookup === "object") {
      pid = lookup.pid || "";
    }
    pid = String(pid || "").replace(/\.html?$/i, "").replace(/\/$/, "");
    if (!pid) return null;
    const authorUri = `https://dblp.org/pid/${pid}`;
    const query = `SELECT (COUNT(?pub) AS ?count) WHERE { ?pub <https://dblp.org/rdf/schema#authoredBy> <${authorUri}> . }`;
    const url = `https://sparql.dblp.org/sparql?query=${encodeURIComponent(query)}&format=application/sparql-results+json`;
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const countRaw = data?.results?.bindings?.[0]?.count?.value;
    const count = Number(countRaw) || 0;
    return { pid, count };
  }

  async function fetchSemanticScholarSignal(doi) {
    if (!doi) return null;
    const url = `https://api.semanticscholar.org/graph/v1/paper/DOI:${encodeURIComponent(doi)}?fields=paperId,fieldsOfStudy,influentialCitationCount,abstract`;
    const data = await fetchExternalJson(url, { timeoutMs: 12000 });
    if (!data) return null;
    const fields = Array.isArray(data?.fieldsOfStudy) ? data.fieldsOfStudy : [];
    const influential = Number(data?.influentialCitationCount) || 0;
    return {
      paperId: data?.paperId || "",
      fields,
      influential
    };
  }

  function normalizeOpenAlexId(id) {
    if (!id) return null;
    const raw = String(id).trim();
    const match = raw.match(/W\\d+/i);
    if (match) return match[0].toUpperCase();
    return raw;
  }

  function normalizeOpenAlexAuthorId(id) {
    if (!id) return null;
    const raw = String(id).trim();
    const match = raw.match(/A\\d+/i);
    if (match) return match[0].toUpperCase();
    return raw;
  }


  function formatOpenAlexUrl(url) {
    try {
      const u = new URL(url);
      if (!u.searchParams.has("mailto")) u.searchParams.set("mailto", "scholar-extension@local");
      return u.toString();
    } catch {
      return url;
    }
  }

  function compactOpenAlexWork(work) {
    if (!work || typeof work !== "object") return null;
    const authorships = Array.isArray(work.authorships) ? work.authorships : [];
    const authors = authorships.map((a) => ({
      id: a?.author?.id || "",
      name: a?.author?.display_name || "",
      institutions: Array.isArray(a?.institutions)
        ? a.institutions.map((i) => i?.display_name || "").filter(Boolean)
        : []
    })).filter((a) => a.name);
    const grants = Array.isArray(work.grants)
      ? work.grants.map((g) => ({
          funder: g?.funder_display_name || g?.funder || "",
          award: g?.award_id || g?.award || ""
        })).filter((g) => g.funder || g.award)
      : [];
    const concepts = Array.isArray(work.concepts)
      ? work.concepts
          .filter((c) => c && c.display_name)
          .map((c) => ({ id: c.id || "", name: c.display_name, score: Number(c.score) || 0 }))
      : [];
    return {
      id: work.id || "",
      openalexId: normalizeOpenAlexId(work.id || ""),
      title: work.display_name || work.title || "",
      year: work.publication_year || null,
      doi: normalizeDoi(work.doi || ""),
      url: work.id || work?.primary_location?.landing_page_url || "",
      citedByCount: Number(work.cited_by_count) || 0,
      referencedWorks: Array.isArray(work.referenced_works) ? work.referenced_works : [],
      relatedWorks: Array.isArray(work.related_works) ? work.related_works : [],
      citedByApi: work.cited_by_api_url || "",
      hostVenue: work?.host_venue?.display_name || "",
      authors,
      grants,
      concepts
    };
  }

  function normalizeTitleForMatch(title) {
    return String(title || "")
      .toLowerCase()
      .replace(/[^a-z0-9\\s]/g, " ")
      .replace(/\\s+/g, " ")
      .trim();
  }

  function looksLikeDoi(raw) {
    const d = normalizeDoi(raw);
    return !!(d && /^10\.\d{4,9}\/\S+$/i.test(d));
  }

  function titleSimilarity(a, b) {
    const aTokens = new Set(normalizeTitleForMatch(a).split(" ").filter(Boolean));
    const bTokens = new Set(normalizeTitleForMatch(b).split(" ").filter(Boolean));
    if (!aTokens.size || !bTokens.size) return 0;
    let inter = 0;
    for (const t of aTokens) if (bTokens.has(t)) inter += 1;
    const union = new Set([...aTokens, ...bTokens]).size;
    return union ? inter / union : 0;
  }

  function scoreOpenAlexCandidate(work, title, year, authorName) {
    if (!work) return 0;
    let score = titleSimilarity(title, work.display_name || work.title || "");
    const wYear = Number(work.publication_year) || null;
    if (year && wYear && Math.abs(wYear - year) <= 1) score += 0.15;
    if (authorName && Array.isArray(work.authorships)) {
      const found = work.authorships.some((a) =>
        String(a?.author?.display_name || "").toLowerCase().includes(authorName.toLowerCase())
      );
      if (found) score += 0.15;
    }
    return score;
  }

  async function fetchOpenAlexWorkById(id) {
    const norm = normalizeOpenAlexId(id);
    if (!norm) return null;
    const cacheKey = makeExternalKey("openalex", norm);
    const cached = getExternalSignalEntry(cacheKey, "openalex");
    if (cached) return cached.data || null;
    const url = formatOpenAlexUrl(`https://api.openalex.org/works/${encodeURIComponent(norm)}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const compact = compactOpenAlexWork(data);
    if (compact) setExternalSignalEntry(cacheKey, "openalex", compact);
    return compact || null;
  }

  async function fetchOpenAlexWorkByDoi(doi) {
    const norm = normalizeDoi(doi);
    if (!norm) return null;
    const cacheKey = makeExternalKey("openalex", norm);
    const cached = getExternalSignalEntry(cacheKey, "openalex");
    if (cached) return cached.data || null;
    const url = formatOpenAlexUrl(`https://api.openalex.org/works/https://doi.org/${encodeURIComponent(norm)}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const compact = compactOpenAlexWork(data);
    if (compact) setExternalSignalEntry(cacheKey, "openalex", compact);
    return compact || null;
  }

  async function fetchOpenAlexAuthorById(id) {
    const norm = normalizeOpenAlexAuthorId(id);
    if (!norm) return null;
    const url = formatOpenAlexUrl(`https://api.openalex.org/authors/${encodeURIComponent(norm)}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    return data || null;
  }


  async function fetchOpenAlexAuthorFirstYear(id) {
    const norm = normalizeOpenAlexAuthorId(id);
    if (!norm) return null;
    const params = new URLSearchParams();
    params.set("filter", `author.id:${norm}`);
    params.set("sort", "publication_year:asc");
    params.set("per_page", "1");
    params.set("select", "publication_year");
    const url = formatOpenAlexUrl(`https://api.openalex.org/works?${params.toString()}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const year = Number(data?.results?.[0]?.publication_year) || null;
    return year;
  }

  async function searchOpenAlexWorkByTitle(title, year, authorName) {
    const q = String(title || "").trim();
    if (!q) return null;
    const params = new URLSearchParams();
    params.set("search", q);
    params.set("per_page", "5");
    if (year) {
      const y = Number(year);
      if (Number.isFinite(y) && y > 1500) {
        params.set("filter", `from_publication_date:${y}-01-01,to_publication_date:${y}-12-31`);
      }
    }
    const url = formatOpenAlexUrl(`https://api.openalex.org/works?${params.toString()}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const results = Array.isArray(data?.results) ? data.results : [];
    if (!results.length) return null;
    let best = results[0];
    let bestScore = scoreOpenAlexCandidate(best, q, year, authorName);
    for (const cand of results.slice(1)) {
      const score = scoreOpenAlexCandidate(cand, q, year, authorName);
      if (score > bestScore) {
        bestScore = score;
        best = cand;
      }
    }
    if (bestScore < 0.2) return null;
    return compactOpenAlexWork(best);
  }

  async function fetchOpenAlexWorkForPaper(paper, authorName) {
    if (!paper) return null;
    const doiCandidate = paper.doi || paper.url || "";
    const doi = normalizeDoi(doiCandidate);
    if (looksLikeDoi(doi)) {
      const byDoi = await fetchOpenAlexWorkByDoi(doi);
      if (byDoi) return byDoi;
    }
    const title = paper.title || "";
    const year = Number(paper.year) || null;
    return await searchOpenAlexWorkByTitle(title, year, authorName);
  }

  function pickFirstAuthorName(paper) {
    if (!paper) return "";
    const authors = parseAuthors((paper.authorsVenue || "").split(" - ")[0] || paper.authorsVenue || "");
    return authors && authors.length ? authors[0] : "";
  }

  function buildOpenAlexKeyForPaper(paper) {
    if (!paper) return null;
    const doi = normalizeDoi(paper.doi || paper.url || "");
    if (doi) return makeExternalKey("openalex", doi);
    const title = normalizeTitleForMatch(paper.title || "");
    if (!title) return null;
    const year = Number(paper.year) || "";
    const hash = hashString(`${title}|${year}`);
    return makeExternalKey("openalex", `title:${hash}`);
  }

  async function fetchOpenAlexSignalForPaper(lookup) {
    const paper = lookup?.paper || null;
    const authorName = lookup?.authorName || "";
    if (!paper) return null;
    return await fetchOpenAlexWorkForPaper(paper, authorName);
  }

  function scoreOpenAlexConcept(concept, name) {
    if (!concept || !name) return 0;
    const display = String(concept.display_name || concept.displayName || "").trim();
    if (!display) return 0;
    const target = String(name || "").trim().toLowerCase();
    const displayLower = display.toLowerCase();
    if (displayLower === target) return 1;
    if (displayLower.replace(/s$/i, "") === target.replace(/s$/i, "")) return 0.95;
    return titleSimilarity(display, name);
  }

  async function fetchOpenAlexConceptByName(name) {
    const q = String(name || "").trim();
    if (!q) return null;
    const params = new URLSearchParams();
    params.set("search", q);
    params.set("per_page", "5");
    const url = formatOpenAlexUrl(`https://api.openalex.org/concepts?${params.toString()}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    const results = Array.isArray(data?.results) ? data.results : [];
    if (!results.length) return null;
    let best = results[0];
    let bestScore = scoreOpenAlexConcept(best, q);
    for (const cand of results.slice(1)) {
      const score = scoreOpenAlexConcept(cand, q);
      if (score > bestScore) {
        bestScore = score;
        best = cand;
      }
    }
    if (bestScore < 0.2) return null;
    return {
      id: best.id || "",
      displayName: best.display_name || best.displayName || q
    };
  }

  async function getOpenAlexConceptInfo(name) {
    const cache = await getPopPeerCache();
    const key = String(name || "").trim().toLowerCase();
    if (!key) return null;
    const existing = cache.concepts?.[key];
    if (existing && existing.id && existing.ts && (Date.now() - existing.ts < POP_CONCEPT_TTL_MS)) {
      return existing;
    }
    const fetched = await fetchOpenAlexConceptByName(name);
    if (!fetched) return null;
    const entry = { id: fetched.id, displayName: fetched.displayName, ts: Date.now() };
    cache.concepts[key] = entry;
    schedulePopPeerCacheSave();
    return entry;
  }

  function extractOpenAlexAuthorYearBounds(author) {
    const years = Array.isArray(author?.counts_by_year)
      ? author.counts_by_year.map((c) => Number(c?.year)).filter((y) => Number.isFinite(y))
      : [];
    if (!years.length) return { firstYear: null, lastYear: null };
    return {
      firstYear: Math.min(...years),
      lastYear: Math.max(...years)
    };
  }

  function compactOpenAlexAuthor(author) {
    if (!author || typeof author !== "object") return null;
    const { firstYear, lastYear } = extractOpenAlexAuthorYearBounds(author);
    const stats = author?.summary_stats || {};
    return {
      id: author.id || "",
      name: author.display_name || "",
      worksCount: Number(author.works_count) || 0,
      citedByCount: Number(author.cited_by_count) || 0,
      hIndex: Number(stats.h_index) || null,
      i10Index: Number(stats.i10_index) || null,
      institution: author.last_known_institution?.display_name || "",
      firstYear,
      lastYear,
      orcid: author.orcid || ""
    };
  }

  async function fetchOpenAlexAuthorsPage(conceptId, cursor, perPage = 200) {
    if (!conceptId) return { results: [], nextCursor: null };
    const params = new URLSearchParams();
    params.set("filter", `concepts.id:${conceptId}`);
    params.set("per_page", String(perPage));
    params.set("cursor", cursor || "*");
    const url = formatOpenAlexUrl(`https://api.openalex.org/authors?${params.toString()}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    if (!data) return { results: [], nextCursor: null, error: "OpenAlex request failed" };
    const results = Array.isArray(data?.results) ? data.results : [];
    const nextCursor = data?.meta?.next_cursor || null;
    return { results, nextCursor, error: null };
  }

  async function fetchOpenAlexWorksPage({ conceptId, year, perPage = 200, page = 1 }) {
    if (!conceptId || !year) return { results: [], error: "Missing concept or year" };
    const params = new URLSearchParams();
    params.set("filter", `concepts.id:${conceptId},publication_year:${year}`);
    params.set("per_page", String(perPage));
    params.set("page", String(page));
    params.set("select", "authorships,publication_year");
    const url = formatOpenAlexUrl(`https://api.openalex.org/works?${params.toString()}`);
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    if (!data) return { results: [], error: "OpenAlex request failed" };
    const results = Array.isArray(data?.results) ? data.results : [];
    return { results, error: null };
  }

  function collectCandidateAuthorsFromWorks(works, map) {
    for (const work of works || []) {
      const authorships = Array.isArray(work?.authorships) ? work.authorships : [];
      for (const auth of authorships) {
        const author = auth?.author || {};
        const rawId = author.id || "";
        if (!rawId) continue;
        const id = normalizeOpenAlexAuthorId(rawId);
        if (!id) continue;
        const name = author.display_name || author.name || "";
        const existing = map.get(id);
        if (existing) {
          existing.count += 1;
          if (name && name.length > (existing.name || "").length) existing.name = name;
        } else {
          map.set(id, { id, name, count: 1 });
        }
      }
    }
  }

  async function fetchCandidateAuthorsFromWorks(concepts, years, opts = {}) {
    const perPage = Number(opts.perPage) || 200;
    const pagesPerYear = Number(opts.pagesPerYear) || 1;
    const authorMap = new Map();
    let scannedWorks = 0;
    let requests = 0;
    for (const concept of concepts || []) {
      if (!concept?.id) continue;
      for (const year of years || []) {
        for (let page = 1; page <= pagesPerYear; page += 1) {
          const res = await fetchOpenAlexWorksPage({ conceptId: concept.id, year, perPage, page });
          requests += 1;
          if (res.error) return { authors: [], scannedWorks, requests, error: res.error };
          const works = Array.isArray(res.results) ? res.results : [];
          scannedWorks += works.length;
          collectCandidateAuthorsFromWorks(works, authorMap);
        }
      }
    }
    const authors = Array.from(authorMap.values());
    authors.sort((a, b) => b.count - a.count);
    return { authors, scannedWorks, requests, error: null };
  }

  async function fetchPopPeersByStartYear(conceptId, year, opts = {}) {
    const perPage = Number(opts.perPage) || 200;
    const maxPages = Number(opts.maxPages) || 2;
    const yearWindow = Number.isFinite(opts.yearWindow) ? Number(opts.yearWindow) : 0;
    const minYear = Number.isFinite(year) ? year - yearWindow : null;
    const maxYear = Number.isFinite(year) ? year + yearWindow : null;
    const peers = [];
    let scanned = 0;
    let pages = 0;
    let error = null;
    let cursor = "*";
    for (let i = 0; i < maxPages; i++) {
      const page = await fetchOpenAlexAuthorsPage(conceptId, cursor, perPage);
      if (page.error) {
        error = page.error;
        break;
      }
      pages += 1;
      for (const raw of page.results || []) {
        scanned += 1;
        const compact = compactOpenAlexAuthor(raw);
        if (!compact || !compact.firstYear) continue;
        if (minYear != null && maxYear != null) {
          if (compact.firstYear >= minYear && compact.firstYear <= maxYear) peers.push(compact);
        } else if (compact.firstYear === year) {
          peers.push(compact);
        }
      }
      if (!page.nextCursor) break;
      cursor = page.nextCursor;
    }
    peers.sort((a, b) => (b.citedByCount || 0) - (a.citedByCount || 0));
    return { peers, scanned, pages, error, yearWindow };
  }

  async function fetchPopPeersByStartYearMulti(concepts, year, opts = {}) {
    const perPage = Number(opts.perPage) || 200;
    const maxPages = Number(opts.maxPages) || 2;
    const yearWindow = Number.isFinite(opts.yearWindow) ? Number(opts.yearWindow) : 0;
    const minYear = Number.isFinite(year) ? year - yearWindow : null;
    const maxYear = Number.isFinite(year) ? year + yearWindow : null;
    const peers = [];
    let scanned = 0;
    let pages = 0;
    let error = null;
    const seen = new Set();
    const conceptNames = [];
    for (const concept of concepts || []) {
      if (!concept?.id) continue;
      conceptNames.push(concept.displayName || concept.name || "");
      let cursor = "*";
      for (let i = 0; i < maxPages; i++) {
        const page = await fetchOpenAlexAuthorsPage(concept.id, cursor, perPage);
        if (page.error) {
          error = page.error;
          break;
        }
        pages += 1;
        for (const raw of page.results || []) {
          const id = String(raw?.id || "");
          if (id && seen.has(id)) continue;
          if (id) seen.add(id);
          scanned += 1;
          const compact = compactOpenAlexAuthor(raw);
          if (!compact || !compact.firstYear) continue;
          if (minYear != null && maxYear != null) {
            if (compact.firstYear >= minYear && compact.firstYear <= maxYear) peers.push(compact);
          } else if (compact.firstYear === year) {
            peers.push(compact);
          }
        }
        if (!page.nextCursor) break;
        cursor = page.nextCursor;
      }
      if (error) break;
    }
    peers.sort((a, b) => (b.citedByCount || 0) - (a.citedByCount || 0));
    return { peers, scanned, pages, error, yearWindow, concepts: conceptNames.filter(Boolean) };
  }

  async function fetchOpenCitationsCociList(doi) {
    const norm = normalizeDoi(doi);
    if (!norm) return null;
    const cacheKey = makeExternalKey("coci", norm);
    const cached = getExternalSignalEntry(cacheKey, "opencitations_coci");
    if (cached) return cached.data || null;
    const url = `https://opencitations.net/index/coci/api/v1/citations/${encodeURIComponent(norm)}`;
    const data = await fetchExternalJson(url, { timeoutMs: 15000 });
    if (!data) return null;
    const citing = Array.isArray(data) ? data.map((d) => normalizeDoi(d?.citing || "")).filter(Boolean) : [];
    setExternalSignalEntry(cacheKey, "opencitations_coci", citing);
    return citing;
  }

  function enqueueExternalSignalFetch(key, provider, fetcher, lookup) {
    if (!key || !provider || typeof fetcher !== "function") return;
    const inflightKey = `${provider}|${key}`;
    if (externalSignalInFlight.has(inflightKey)) return;
    externalSignalInFlight.set(inflightKey, true);
    externalSignalQueue.push({ key, provider, fetcher, inflightKey, lookup: lookup || key });
    pumpExternalSignalQueue();
  }

  function providerReadyAt(provider) {
    const next = externalSignalProviderNext.get(provider);
    return Number.isFinite(next) ? next : 0;
  }

  function markProviderUsed(provider) {
    const minDelay = EXTERNAL_SIGNAL_PROVIDER_LIMITS[provider]?.minDelay || EXTERNAL_SIGNAL_DELAY_MS;
    externalSignalProviderNext.set(provider, Date.now() + minDelay);
  }

  function pickNextExternalJob() {
    if (!externalSignalQueue.length) return null;
    const now = Date.now();
    let soonest = null;
    for (let i = 0; i < externalSignalQueue.length; i++) {
      const job = externalSignalQueue[i];
      const readyAt = providerReadyAt(job.provider);
      if (now >= readyAt) {
        externalSignalQueue.splice(i, 1);
        return job;
      }
      if (soonest == null || readyAt < soonest) soonest = readyAt;
    }
    if (soonest != null) {
      const delay = Math.max(50, soonest - now);
      setTimeout(pumpExternalSignalQueue, delay);
    }
    return null;
  }

  function pumpExternalSignalQueue() {
    if (externalSignalActive >= EXTERNAL_SIGNAL_CONCURRENCY) return;
    const job = pickNextExternalJob();
    if (!job) return;
    externalSignalActive += 1;
    markProviderUsed(job.provider);
    (async () => {
      try {
        const data = await job.fetcher(job.lookup);
        setExternalSignalEntry(job.key, job.provider, data);
      } catch {
        setExternalSignalEntry(job.key, job.provider, null);
      } finally {
        externalSignalActive -= 1;
        externalSignalInFlight.delete(job.inflightKey);
        scheduleExternalSignalReapply();
        setTimeout(pumpExternalSignalQueue, EXTERNAL_SIGNAL_DELAY_MS);
        pumpExternalSignalQueue();
      }
    })();
  }

  const CODE_LINK_CACHE = new Map(); // key -> { status: "pending"|"found"|"none", url: string|null }

  function extractArxivIdFromUrl(url) {
    const u = String(url || "");
    const m = u.match(/arxiv\.org\/(?:abs|pdf)\/([0-9.]+)(?:v\\d+)?/i);
    if (m) return m[1];
    const m2 = u.match(/arxiv-vanity\.com\/papers\/([0-9.]+)(?:v\\d+)?/i);
    if (m2) return m2[1];
    return null;
  }

  function extractArxivIdFromResult(container) {
    if (!container) return null;
    const links = Array.from(container.querySelectorAll("a[href]"));
    for (const a of links) {
      const href = a.getAttribute("href") || "";
      const found = extractArxivIdFromUrl(href);
      if (found) return found;
    }
    const doi = extractDOIFromResult(container);
    if (doi) {
      const d = String(doi).toLowerCase();
      const m = d.match(/10\.48550\/arxiv\.([0-9.]+)/i);
      if (m) return m[1];
    }
    const snippet = getSnippetText(container);
    const sm = snippet.match(/arxiv[:\s]*([0-9.]+)(v\d+)?/i);
    if (sm) return sm[1];
    return null;
  }

  function extractPubmedIdFromResult(container) {
    if (!container) return null;
    const links = Array.from(container.querySelectorAll("a[href]"));
    for (const a of links) {
      const href = a.getAttribute("href") || "";
      const m = href.match(/pubmed\.ncbi\.nlm\.nih\.gov\/(\d+)/i) || href.match(/ncbi\.nlm\.nih\.gov\/pubmed\/(\d+)/i);
      if (m) return m[1];
    }
    return null;
  }

  function extractPmcIdFromResult(container) {
    if (!container) return null;
    const links = Array.from(container.querySelectorAll("a[href]"));
    for (const a of links) {
      const href = a.getAttribute("href") || "";
      const m = href.match(/ncbi\.nlm\.nih\.gov\/pmc\/articles\/(PMC\d+)/i);
      if (m) return m[1];
    }
    return null;
  }

  async function fetchCatalyzeXCodeLink({ title, url }) {
    const arxivId = extractArxivIdFromUrl(url);
    let api = "https://www.catalyzex.com/api/code?extension=true";
    if (arxivId) api += `&paper_arxiv_id=${encodeURIComponent(arxivId)}`;
    if (title) api += `&paper_title=${encodeURIComponent(title)}`;
    if (url) api += `&paper_url=${encodeURIComponent(url)}`;
    const r = await fetch(api);
    if (!r.ok) return null;
    const data = await r.json();
    const link = data?.code_url || data?.unshortened_url || data?.cx_url || null;
    return link || null;
  }

  async function renderCodeLinkBadge(container, paper) {
    const existing = container.querySelector(".su-code-badge");
    if (existing) existing.remove();
    const titleEl = container.querySelector(".gs_rt") || container.querySelector(".gsc_a_at");
    if (!titleEl) return;
    const key = paper?.clusterId || paper?.url || paper?.title;
    if (!key) return;
    const cached = CODE_LINK_CACHE.get(key);
    if (cached?.status === "found" && cached.url) {
      const a = document.createElement("a");
      a.className = "su-code-badge";
      a.href = cached.url;
      a.target = "_blank";
      a.rel = "noopener";
      a.textContent = "Code";
      a.title = "Open associated code repository (CatalyzeX)";
      titleEl.appendChild(a);
      return;
    }
    if (cached?.status === "pending" || cached?.status === "none") return;
    CODE_LINK_CACHE.set(key, { status: "pending", url: null });
    try {
      const link = await fetchCatalyzeXCodeLink({ title: paper?.title, url: paper?.url });
      if (link) {
        CODE_LINK_CACHE.set(key, { status: "found", url: link });
        const a = document.createElement("a");
        a.className = "su-code-badge";
        a.href = link;
        a.target = "_blank";
        a.rel = "noopener";
        a.textContent = "Code";
        a.title = "Open associated code repository (CatalyzeX)";
        titleEl.appendChild(a);
      } else {
        CODE_LINK_CACHE.set(key, { status: "none", url: null });
      }
    } catch {
      CODE_LINK_CACHE.set(key, { status: "none", url: null });
    }
  }

  /**
   * Detect funding mentions in snippet/abstract text (e.g. from .gs_rs).
   * Returns { label, type: 'government'|'industry'|'unknown', excerpt } or null.
   */
  function detectFunding(snippetText) {
    const t = String(snippetText || "").trim();
    if (!t) return null;

    const excerptLen = 120;
    const takeExcerpt = (str) =>
      str.length <= excerptLen ? str : str.slice(0, excerptLen).trim() + "…";

    // Government / public funding patterns (case-insensitive)
    const govPatterns = [
      /\b(?:supported|funded)\s+by\s+(?:the\s+)?(?:NIH|NSF|NIH\/NIAID|NIEHS|NCI)\b/i,
      /\b(?:NIH|NSF)\s+grant\s*#?\s*\w+/i,
      /\bGrant\s*#?\s*\w+.*?(?:NIH|NSF|DOE|FDA|CDC)\b/i,
      /\b(?:European\s+Research\s+Council|ERC)\s+(?:Grant|funding)\b/i,
      /\bHorizon\s+(?:2020|Europe)\b/i,
      /\bWellcome\s+(?:Trust|Foundation)\b/i,
      /\b(?:UKRI|MRC|BBSRC|EPSRC)\b/i,
      /\b(?:supported|funded)\s+in\s+part\s+by\s+(?:the\s+)?(?:NIH|NSF|government)\b/i,
      /\bgovernment\s+funding\b/i,
      /\b(?:NIH|NSF|DOE|FDA)\s+(?:award|grant)\b/i
    ];

    // Industry / corporate funding patterns
    const industryNames = [
      "Google", "Microsoft", "Meta", "Facebook", "Amazon", "Apple", "IBM",
      "Pfizer", "Johnson\\s*&\\s*Johnson", "Merck", "Novartis", "Roche",
      "AstraZeneca", "Bayer", "Sanofi", "GSK", "GlaxoSmithKline", "Eli\\s+Lilly",
      "Bristol-Myers", "BMS", "AbbVie", "Amgen", "Gilead", "Moderna",
      "OpenAI", "Anthropic", "NVIDIA", "Intel", "Qualcomm"
    ];
    const industryPattern = new RegExp(
      "\\b(?:supported|funded|sponsored|grant from)\\s+(?:by\\s+)?(?:the\\s+)?(" +
      industryNames.join("|") + ")\\b",
      "i"
    );
    const industryOnly = new RegExp("\\b(" + industryNames.join("|") + ")\\b", "i");

    // Generic funding phrases (type unknown unless we match gov/industry)
    const genericPatterns = [
      /\b(?:supported|funded)\s+by\s+.{5,80}/gi,
      /\bsponsored\s+by\s+.{5,80}/gi,
      /\bGrant\s*#?\s*[\w-]+/gi,
      /\bGrant\s+[Nn]o\.?\s*[\w-]+/gi,
      /\b(?:Acknowledgments?|Acknowledgements?)\s*[.:]\s*.{10,200}/gi,
      /\b(?:Funding|Funder)s?\s*[.:]\s*.{10,200}/gi
    ];

    let type = "unknown";
    let excerpt = "";

    for (const p of govPatterns) {
      const m = t.match(p);
      if (m) {
        type = "government";
        excerpt = takeExcerpt(m[0]);
        break;
      }
    }
    if (!excerpt && industryPattern.test(t)) {
      type = "industry";
      const m = t.match(industryPattern);
      excerpt = m ? takeExcerpt(m[0]) : takeExcerpt(t.match(industryOnly)?.[0] || t.slice(0, excerptLen));
    }
    if (!excerpt) {
      for (const p of genericPatterns) {
        const m = t.match(p);
        if (m) {
          excerpt = takeExcerpt(m[0]);
          break;
        }
      }
    }

    if (!excerpt) return null;

    const label =
      type === "government"
        ? "Government funding"
        : type === "industry"
          ? "Industry funding"
          : "Funding mentioned";
    return { label, type, excerpt };
  }

  function tryGetHref(el) {
    const href = el?.getAttribute?.("href") || "";
    if (!href) return null;
    try {
      return new URL(href, window.location.href).toString();
    } catch {
      return null;
    }
  }

  function isHttpUrl(url) {
    if (!url) return false;
    try {
      const u = new URL(url, window.location.href);
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  }

  function parseYear(s) {
    const m = String(s || "").match(/\b(19\d{2}|20\d{2})\b/);
    return m ? Number(m[1]) : null;
  }

  function findFlLinks(gsR) {
    const fl = gsR.querySelector(".gs_fl");
    const links = Array.from(fl?.querySelectorAll("a") || []);
    const out = { citedByUrl: null, versionsUrl: null, relatedUrl: null };

    for (const a of links) {
      const t = text(a);
      const u = tryGetHref(a);
      if (!u) continue;
      if (/^Cited by\b/i.test(t)) out.citedByUrl = u;
      else if (/^All \d+ versions\b/i.test(t)) out.versionsUrl = u;
      else if (/^Related articles\b/i.test(t)) out.relatedUrl = u;
    }

    return out;
  }

  /**
   * Best PDF URL for this result: publisher PDF > arXiv/SSRN/author .pdf > null (then use title+pdf search).
   * Returns { url, label } or null.
   */
  function getBestPdfUrl(container) {
    const links = Array.from(container.querySelectorAll("a[href]"));
    let publisherPdf = null;
    let anyPdf = null;
    let arxivAbs = null;
    let ssrnId = null;

    const unwrapScholarUrl = (href) => {
      if (!href) return "";
      const m = href.match(/[?&]q=([^&]+)/i);
      if (m) {
        try {
          return decodeURIComponent(m[1]);
        } catch {
          return m[1];
        }
      }
      return href;
    };
    const normalizeHref = (href) => {
      const unwrapped = unwrapScholarUrl(href);
      if (!unwrapped) return "";
      try {
        const u = new URL(unwrapped, window.location.href);
        if (u.protocol !== "http:" && u.protocol !== "https:") return "";
        return u.toString();
      } catch {
        return "";
      }
    };

    for (const a of links) {
      const rawHref = (a.getAttribute("href") || "").trim();
      const href = normalizeHref(rawHref);
      if (!href || href.startsWith("#")) continue;
      const linkText = text(a).trim().replace(/\s+/g, " ");
      const isPdfLink = /\.pdf(\?|#|$)/i.test(href);
      const isPdfLabel = /^\[?\s*PDF\s*\]?$/i.test(linkText);
      const inPdfBox = !!a.closest(".gs_or_ggsm, .gs_ggs");

      if (isPdfLabel || inPdfBox || (isPdfLink && a.closest(".gs_rt"))) {
        publisherPdf = href;
        break;
      }
      if (isPdfLink) anyPdf = anyPdf || href;
      const arxivM = href.match(/arxiv\.org\/abs\/([^/?#]+)/i);
      if (arxivM) arxivAbs = arxivM[1];
      const ssrnM = href.match(/ssrn\.com\/abstract=(\d+)/i) || href.match(/papers\.ssrn\.com\/sol3\/papers\.cfm\?abstract_id=(\d+)/i);
      if (ssrnM) ssrnId = ssrnId || ssrnM[1];
    }

    if (publisherPdf) return { url: publisherPdf, label: "Publisher PDF" };
    if (anyPdf) return { url: anyPdf, label: "PDF" };

    const doi = normalizeDoi(extractDOIFromResult(container));
    if (doi) {
      const unpay = getExternalSignalEntry(doi, "unpaywall")?.data || {};
      const oaPdf = unpay.bestOaUrlPdf || "";
      const oaUrl = unpay.bestOaUrl || "";
      if (oaPdf && isHttpUrl(oaPdf)) return { url: oaPdf, label: "OA PDF" };
      if (oaUrl && isHttpUrl(oaUrl)) return { url: oaUrl, label: "OA" };
    }
    if (arxivAbs) return { url: `https://arxiv.org/pdf/${arxivAbs}.pdf`, label: "arXiv PDF" };
    if (ssrnId) return { url: `https://papers.ssrn.com/sol3/Delivery.cfm?abstractid=${ssrnId}&type=2`, label: "SSRN" };
    return null;
  }

  const STOPWORDS = new Set(["the", "a", "an", "and", "or", "but", "of", "in", "on", "at", "to", "for", "with", "by", "from", "as", "is", "was", "are", "were", "been", "be", "have", "has", "had", "do", "does", "did", "this", "that", "these", "those", "it", "its"]);
  const TOPIC_TOKEN_STOPWORDS = new Set([
    ...STOPWORDS,
    "using", "based", "approach", "approaches", "analysis", "model", "models", "framework",
    "study", "studies", "toward", "towards", "via", "new", "results", "evidence",
    "case", "cases", "effect", "effects", "method", "methods", "system", "systems",
    "data", "dataset", "datasets", "paper", "papers", "survey", "review"
  ]);

  function addTitleTokens(map, title) {
    if (!map || !title) return;
    const rawTokens = String(title).match(/[A-Za-z0-9]+/g);
    if (!rawTokens) return;
    for (const tok of rawTokens) {
      const lower = tok.toLowerCase();
      if (lower.length < 2) continue;
      if (TOPIC_TOKEN_STOPWORDS.has(lower)) continue;
      if (/^\\d+$/.test(lower)) continue;
      const existing = map.get(lower);
      if (existing) {
        existing.count++;
        const hasUpper = /[A-Z]/.test(tok);
        if (hasUpper && !/[A-Z]/.test(existing.display || "")) {
          existing.display = tok;
        } else if (tok.length > (existing.display || "").length) {
          existing.display = tok;
        }
      } else {
        map.set(lower, { count: 1, display: tok });
      }
    }
  }

  function normalizeTitleForGrouping(title) {
    if (!title || typeof title !== "string") return "";
    const t = title
      .toLowerCase()
      .replace(/[^\w\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return t
      .split(/\s+/)
      .filter((w) => w.length > 0 && !STOPWORDS.has(w))
      .join(" ");
  }

  function extractDOIFromText(textValue) {
    if (!textValue) return null;
    DOI_REGEX.lastIndex = 0;
    const m = DOI_REGEX.exec(String(textValue));
    return m ? m[0] : null;
  }

  function buildMinimalBibTeX(item) {
    const keyBase = normalizeTitleForGrouping(item.title || "paper").split(" ").slice(0, 4).join("");
    const key = (keyBase || "paper") + String(Date.now()).slice(-4);
    const title = (item.title || "Untitled").replace(/[{}]/g, "");
    const url = item.link || "";
    const note = item.searchQuery ? `Search query: ${item.searchQuery}` : "";
    return `@misc{${key},
  title = {${title}},
  howpublished = {\\url{${url}}},
  note = {${note}}
}`;
  }

  async function getBibTeXForQueueItem(item) {
    const doi = extractDOIFromText(item.link) || extractDOIFromText(item.title);
    if (doi) {
      const enc = encodeURIComponent(doi);
      const url = `https://api.crossref.org/works/${enc}/transform/application/x-bibtex`;
      try {
        const res = await chrome.runtime.sendMessage({ action: "fetchBib", url });
        if (res?.ok && res.body) return res.body;
      } catch {}
    }
    return buildMinimalBibTeX(item);
  }

  function csvEscape(value) {
    const s = String(value ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  /** Venue tier for version ordering: 3 = journal, 2 = conference, 1 = preprint, 0 = other. */
  function getVenueTier(venue) {
    const v = String(venue || "").toLowerCase();
    if (isPreprintVenue(venue)) return 1;
    if (/\bjournal\b|transactions|quarterly|review\b|science\b/.test(v)) return 3;
    if (/\bconference\b|proceedings|workshop\b|symposium\b/.test(v)) return 2;
    return 0;
  }

  /** Venue type for reading load: "journal" | "conference" | "preprint" | "other". */
  function getVenueType(venue) {
    const tier = getVenueTier(venue);
    if (tier === 3) return "journal";
    if (tier === 2) return "conference";
    if (tier === 1) return "preprint";
    return "other";
  }

  /**
   * Reading load label: skim (≤5 pp or preprint), read (6–15 or conference), deep read (16+ or journal).
   * Uses stored page count from PDF metadata when available (after user clicks PDF).
   */
  function computeReadingLoadLabel(paper, pageCount) {
    if (pageCount != null && typeof pageCount === "number" && pageCount >= 1) {
      if (pageCount <= 5) return "skim";
      if (pageCount <= 15) return "read";
      return "deep read";
    }
    const venueType = getVenueType(paper.venue);
    if (venueType === "preprint") return "skim";
    if (venueType === "conference") return "read";
    if (venueType === "journal") return "deep read";
    return "read";
  }

  /** Sort key for "best" version by user preference (higher = show first). */
  function versionSortKey(tier, order) {
    if (order === "preprint-first") return tier === 1 ? 4 : tier === 2 ? 3 : tier === 3 ? 2 : 1;
    if (order === "conference-first") return tier === 2 ? 4 : tier === 3 ? 3 : tier === 1 ? 2 : 1;
    return tier; // journal-first: 3 > 2 > 1 > 0
  }

  /** DOM-only: show/hide result rows by state.filters (year range, venue, has PDF, min citations, author, artifacts, quality). */
  function applyResultFiltersToRows(rows, state) {
    const f = state.filters || {};
    const showAdvancedFilters = state?.settings?.showAdvancedFilters !== false;
    const yearMin = f.yearMin ? parseInt(f.yearMin, 10) : null;
    const yearMax = f.yearMax ? parseInt(f.yearMax, 10) : null;
    const venueKw = (f.venueKeyword || "").trim().toLowerCase();
    const hasPdf = !!f.hasPdf;
    const hasCode = !!f.hasCode;
    const qualityFilter = (f.qualityFilter || "").trim();
    const hasFullText = !!f.hasFullText;
    const hasFunding = !!f.hasFunding;
    const hasOpenCitations = !!f.hasOpenCitations;
    const hasDatasetDoi = !!f.hasDatasetDoi;
    const hasSoftwareDoi = !!f.hasSoftwareDoi;
    const hasUpdates = !!f.hasUpdates;
    const hasOa = !!f.hasOa;
    const hasPreprint = !!f.hasPreprint;
    const hasPubmed = !!f.hasPubmed;
    const hasPmc = !!f.hasPmc;
    const hasMesh = !!f.hasMesh;
    const minCite = f.minCitations ? parseInt(f.minCitations, 10) : null;
    const authorKw = (f.authorMatch || "").trim().toLowerCase();
    const maxCite = showAdvancedFilters && f.maxCitations ? parseInt(f.maxCitations, 10) : null;
    const minCitesPerYear = showAdvancedFilters && f.minCitesPerYear ? parseFloat(f.minCitesPerYear) : null;
    const inflMin = showAdvancedFilters && f.minInfluential ? parseInt(f.minInfluential, 10) : null;
    const inflMax = showAdvancedFilters && f.maxInfluential ? parseInt(f.maxInfluential, 10) : null;
    const affiliationKw = showAdvancedFilters ? (f.affiliationContains || "").trim().toLowerCase() : "";
    const funderKw = showAdvancedFilters ? (f.funderContains || "").trim().toLowerCase() : "";
    const anyActive =
      yearMin != null ||
      yearMax != null ||
      venueKw.length > 0 ||
      hasPdf ||
      hasCode ||
      qualityFilter.length > 0 ||
      hasFullText ||
      hasFunding ||
      hasOpenCitations ||
      hasDatasetDoi ||
      hasSoftwareDoi ||
      hasUpdates ||
      hasOa ||
      hasPreprint ||
      hasPubmed ||
      hasPmc ||
      hasMesh ||
      (minCite != null && !isNaN(minCite)) ||
      authorKw.length > 0 ||
      (showAdvancedFilters && ((maxCite != null && !isNaN(maxCite)) ||
        (minCitesPerYear != null && !isNaN(minCitesPerYear)) ||
        (inflMin != null && !isNaN(inflMin)) ||
        (inflMax != null && !isNaN(inflMax)) ||
        affiliationKw.length > 0 ||
        funderKw.length > 0));

    if (!anyActive) {
      if (state._hadActiveFilters) {
        for (const r of rows) r.classList.remove("su-filtered-out");
        state._hadActiveFilters = false;
      }
      return;
    }
    state._hadActiveFilters = true;

    for (const r of rows) {
      if (r.style.display === "none") continue; // e.g. author-page filter
      let paper;
      try {
        paper = getCachedPaperFast(r);
        const needsDeepScan = (!paper.authorsVenue || paper.authorsVenue.includes("…")) && (venueKw.length > 0 || authorKw.length > 0);
        if (needsDeepScan) {
          paper = getCachedPaperFull(r);
        }
        if (state?.venueCache && paper?.clusterId) {
          const cachedVenue = state.venueCache.get(paper.clusterId);
          if (cachedVenue && cachedVenue.length > (paper.venue?.length || 0)) {
            paper = { ...paper, venue: cachedVenue };
          }
        }
      } catch {
        r.classList.add("su-filtered-out");
        continue;
      }
      const year = paper.year != null ? parseYear(String(paper.year)) : null;
      if (yearMin != null && !isNaN(yearMin) && (year == null || year < yearMin)) {
        r.classList.add("su-filtered-out");
        continue;
      }
      if (yearMax != null && !isNaN(yearMax) && (year == null || year > yearMax)) {
        r.classList.add("su-filtered-out");
        continue;
      }
      if (venueKw.length > 0) {
        const venue = (paper.venue || "").toLowerCase();
        if (!venue.includes(venueKw)) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (hasPdf && !getBestPdfUrl(r)) {
        r.classList.add("su-filtered-out");
        continue;
      }
      if (hasCode) {
        const cachedCode = r.dataset.suArtifactCode;
        let code = cachedCode === "1";
        if (cachedCode == null) {
          const artifacts = detectArtifacts(r);
          code = !!artifacts.code;
          r.dataset.suArtifactCode = code ? "1" : "0";
        }
        if (hasCode && !code) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (qualityFilter) {
        if (!paperMatchesFilter(paper, qualityFilter, state)) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (hasFullText || hasFunding || hasOpenCitations || hasDatasetDoi || hasSoftwareDoi || hasUpdates || hasOa || funderKw.length > 0 || (showAdvancedFilters && ((inflMin != null && !isNaN(inflMin)) || (inflMax != null && !isNaN(inflMax))))) {
        let doi = paper.doi || extractDOIFromResult(r);
        doi = normalizeDoi(doi);
        if (!doi) {
          r.classList.add("su-filtered-out");
          continue;
        }
        const crossrefEntry = getExternalSignalEntry(doi, "crossref");
        const ocEntry = getExternalSignalEntry(doi, "opencitations");
        const dcEntry = getExternalSignalEntry(doi, "datacite");
        const unpayEntry = getExternalSignalEntry(doi, "unpaywall");
        const needsInfl = showAdvancedFilters && ((inflMin != null && !isNaN(inflMin)) || (inflMax != null && !isNaN(inflMax)));
        const s2Entry = needsInfl ? getExternalSignalEntry(doi, "s2") : null;
        if (!crossrefEntry) enqueueExternalSignalFetch(doi, "crossref", fetchCrossrefSignal, doi);
        if (!ocEntry) enqueueExternalSignalFetch(doi, "opencitations", fetchOpenCitationsSignal, doi);
        if (!dcEntry) enqueueExternalSignalFetch(doi, "datacite", fetchDataCiteSignal, doi);
        if (!unpayEntry) enqueueExternalSignalFetch(doi, "unpaywall", fetchUnpaywallSignal, doi);
        if (needsInfl && !s2Entry) enqueueExternalSignalFetch(doi, "s2", fetchSemanticScholarSignal, doi);
        const crossref = crossrefEntry?.data || {};
        const openCites = ocEntry?.data || {};
        const datacite = dcEntry?.data || {};
        const unpay = unpayEntry?.data || {};
        const s2 = s2Entry?.data || {};
        if (hasFullText && !crossref.hasFullText) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasFunding && !crossref.hasFunder) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasUpdates && !crossref.hasUpdate) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasOpenCitations && !(openCites.count > 0)) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasDatasetDoi && !datacite.hasDataset) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasSoftwareDoi && !datacite.hasSoftware) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasOa && !unpay.isOa) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (funderKw.length > 0) {
          const funders = Array.isArray(crossref.funderNames) ? crossref.funderNames : [];
          const awards = Array.isArray(crossref.awardNumbers) ? crossref.awardNumbers : [];
          const hay = `${funders.join(" ")} ${awards.join(" ")}`.toLowerCase();
          if (!hay || !hay.includes(funderKw)) {
            r.classList.add("su-filtered-out");
            continue;
          }
        }
        if (needsInfl) {
          const infl = Number(s2.influential) || 0;
          if ((inflMin != null && !isNaN(inflMin) && infl < inflMin) || (inflMax != null && !isNaN(inflMax) && infl > inflMax)) {
            r.classList.add("su-filtered-out");
            continue;
          }
        }
      }
      if (hasPreprint) {
        const arxivId = extractArxivIdFromResult(r);
        if (!arxivId) {
          r.classList.add("su-filtered-out");
          continue;
        }
        const arxivKey = makeExternalKey("arxiv", arxivId);
        const arxivEntry = arxivKey ? getExternalSignalEntry(arxivKey, "arxiv") : null;
        if (!arxivEntry) enqueueExternalSignalFetch(arxivKey, "arxiv", fetchArxivSignal, arxivId);
      }
      if (hasPubmed || hasPmc || hasMesh) {
        const pmid = extractPubmedIdFromResult(r);
        const pmcid = extractPmcIdFromResult(r);
        const doi = normalizeDoi(paper.doi || extractDOIFromResult(r));
        const epmcKey = doi || (pmid ? makeExternalKey("pmid", pmid) : (pmcid ? makeExternalKey("pmcid", pmcid) : null));
        if (!epmcKey) {
          r.classList.add("su-filtered-out");
          continue;
        }
        const epmcEntry = getExternalSignalEntry(epmcKey, "epmc");
        if (!epmcEntry) enqueueExternalSignalFetch(epmcKey, "epmc", fetchEpmcSignal, { doi, pmid, pmcid });
        const epmc = epmcEntry?.data || {};
        const pmidVal = pmid || epmc.pmid;
        const pmcidVal = pmcid || epmc.pmcid;
        if (hasPubmed && !pmidVal) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasPmc && !(pmcidVal || epmc.isOpenAccess)) {
          r.classList.add("su-filtered-out");
          continue;
        }
        if (hasMesh) {
          const ncbiEntry = getExternalSignalEntry(epmcKey, "ncbi");
          if (pmidVal && !ncbiEntry) enqueueExternalSignalFetch(epmcKey, "ncbi", fetchNcbiSignal, { pmid: pmidVal });
          const mesh = ncbiEntry?.data?.meshTerms || [];
          if (!mesh.length) {
            r.classList.add("su-filtered-out");
            continue;
          }
        }
      }
      if (minCite != null && !isNaN(minCite)) {
        const cite = getCitationCountFromResult(r);
        if (cite == null || cite < minCite) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (maxCite != null && !isNaN(maxCite)) {
        const cite = getCitationCountFromResult(r);
        if (cite == null || cite > maxCite) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (minCitesPerYear != null && !isNaN(minCitesPerYear)) {
        const cite = getCitationCountFromResult(r);
        const vel = computeVelocityValue(cite, year);
        const velValue = vel?.velocity;
        if (velValue == null || velValue < minCitesPerYear) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (authorKw.length > 0) {
        const authors = (paper.authorsVenue || "").toLowerCase();
        if (!authors.includes(authorKw)) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      if (affiliationKw.length > 0) {
        const openalexKey = buildOpenAlexKeyForPaper(paper);
        if (!openalexKey) {
          r.classList.add("su-filtered-out");
          continue;
        }
        const oaEntry = getExternalSignalEntry(openalexKey, "openalex");
        if (!oaEntry) enqueueExternalSignalFetch(openalexKey, "openalex", fetchOpenAlexSignalForPaper, { paper, authorName: pickFirstAuthorName(paper) });
        const oa = oaEntry?.data || {};
        const affils = Array.isArray(oa.authors)
          ? oa.authors.flatMap((a) => Array.isArray(a.institutions) ? a.institutions : []).join(" ").toLowerCase()
          : "";
        if (!affils || !affils.includes(affiliationKw)) {
          r.classList.add("su-filtered-out");
          continue;
        }
      }
      r.classList.remove("su-filtered-out");
    }
  }

  function applyResultFilters(results, state) {
    applyResultFiltersToRows(results, state);
  }

  function applyVersionGrouping(results, state) {
    if (!results.length) return;
    const groupVersions = !!state.settings?.groupVersions;
    const order = state.settings?.versionOrder || "journal-first";

    const clearGrouping = () => {
      for (const r of results) {
        r.classList.remove("su-version-grouped-hidden");
        r.removeAttribute("data-su-group-id");
        r.removeAttribute("data-su-group-best");
        const chev = r.querySelector(".su-version-chevron");
        if (chev) chev.remove();
      }
    };

    if (!groupVersions) {
      clearGrouping();
      return;
    }

    const groups = new Map(); // normalizedTitle -> [{ el, paper, tier, sortKey }]
    for (const r of results) {
      if (r.style.display === "none") continue;
      let paper;
      try {
        paper = getCachedPaperFast(r);
        if (!paper.venue || paper.venue.includes("…")) {
          paper = getCachedPaperFull(r);
        }
        if (state?.venueCache && paper?.clusterId) {
          const cachedVenue = state.venueCache.get(paper.clusterId);
          if (cachedVenue && cachedVenue.length > (paper.venue?.length || 0)) {
            paper = { ...paper, venue: cachedVenue };
          }
        }
      } catch {
        continue;
      }
      const norm = normalizeTitleForGrouping(paper.title);
      if (!norm || norm.length < 4) continue;
      const tier = getVenueTier(paper.venue);
      const sortKey = versionSortKey(tier, order);
      if (!groups.has(norm)) groups.set(norm, []);
      groups.get(norm).push({ el: r, paper, tier, sortKey });
    }

    clearGrouping();

    for (const [, entries] of groups) {
      if (entries.length <= 1) continue;
      entries.sort((a, b) => b.sortKey - a.sortKey);
      const best = entries[0];
      const others = entries.slice(1);
      const groupId = "g-" + Math.random().toString(36).slice(2, 10);
      best.el.setAttribute("data-su-group-id", groupId);
      best.el.setAttribute("data-su-group-best", "1");
      for (const { el } of others) {
        el.setAttribute("data-su-group-id", groupId);
        el.classList.add("su-version-grouped-hidden");
      }
      const chevron = document.createElement("button");
      chevron.type = "button";
      chevron.className = "su-version-chevron";
      chevron.setAttribute("aria-label", `Show ${entries.length} versions`);
      chevron.title = `${entries.length} versions (click to expand)`;
      chevron.textContent = "▶";
      chevron.dataset.groupId = groupId;
      chevron.dataset.expanded = "0";
      const titleArea = best.el.querySelector(".gs_rt") || best.el.querySelector(".gs_ri") || best.el;
      if (titleArea) {
        titleArea.style.position = "relative";
        titleArea.insertBefore(chevron, titleArea.firstChild);
      }
      chevron.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        const expanded = chevron.dataset.expanded === "1";
        chevron.dataset.expanded = expanded ? "0" : "1";
        chevron.textContent = expanded ? "▶" : "▼";
        const id = chevron.dataset.groupId;
        for (const r of results) {
          if (r.getAttribute("data-su-group-id") === id && !r.hasAttribute("data-su-group-best")) {
            r.classList.toggle("su-version-grouped-hidden", expanded);
          }
        }
      });
    }
  }

  /** Get citation count from a search result (.gs_r) by parsing "Cited by N" in .gs_fl. */
  function getCitationCountFromResult(gsR) {
    const fl = gsR?.querySelector(".gs_fl");
    if (!fl) return null;
    for (const a of fl.querySelectorAll("a")) {
      const t = text(a).trim();
      if (!/^Cited by\b/i.test(t)) continue;
      const m = t.match(/Cited by\s+([\d,]+)/i);
      if (!m) return null;
      const num = parseInt(m[1].replace(/,/g, ""), 10);
      return isNaN(num) ? null : num;
    }
    // Fallback: scan any links in the row
    for (const a of gsR.querySelectorAll("a")) {
      const t = text(a).trim();
      if (!/^Cited by\b/i.test(t)) continue;
      const m = t.match(/Cited by\s+([\d,]+)/i);
      if (!m) continue;
      const num = parseInt(m[1].replace(/,/g, ""), 10);
      return isNaN(num) ? null : num;
    }
    // Fallback: scan row text
    const rowText = text(gsR);
    const m = rowText.match(/Cited by\s+([\d,]+)/i);
    if (!m) return null;
    const num = parseInt(m[1].replace(/,/g, ""), 10);
    return isNaN(num) ? null : num;
  }

  /**
   * Velocity = citations per year since publication.
   * Returns formatted string (e.g. "25/yr") or null if not computable.
   */
  function computeVelocity(citations, year) {
    if (citations == null || citations < 0) return null;
    const y = year != null ? parseYear(String(year)) : null;
    if (y == null) return null;
    const currentYear = new Date().getFullYear();
    const yearsAgo = Math.max(1, currentYear - y);
    const velocity = citations / yearsAgo;
    if (!Number.isFinite(velocity)) return null;
    const display = velocity >= 10 ? Math.round(velocity) : velocity.toFixed(1);
    return `${display}/yr`;
  }

  function computeVelocityValue(citations, year) {
    if (citations == null || citations < 0) return null;
    const y = year != null ? parseYear(String(year)) : null;
    if (y == null) return null;
    const currentYear = new Date().getFullYear();
    const yearsAgo = Math.max(1, currentYear - y);
    const velocity = citations / yearsAgo;
    if (!Number.isFinite(velocity)) return null;
    return { velocity, yearsAgo };
  }

  function getVelocityBucket(yearsAgo) {
    if (yearsAgo <= 3) return "early";
    if (yearsAgo <= 7) return "mid";
    return "late";
  }

  function getTrajectoryForVelocity(velocity, yearsAgo, bucketAverages) {
    if (!velocity || !yearsAgo || !bucketAverages) return null;
    const bucket = getVelocityBucket(yearsAgo);
    const avg = bucketAverages[bucket];
    if (!avg || !Number.isFinite(avg) || avg <= 0) return null;
    const ratio = velocity / avg;
    if (ratio >= 1.25) return { arrow: "↑", label: "accelerating" };
    if (ratio <= 0.75) return { arrow: "↓", label: "decaying" };
    return { arrow: "→", label: "stable" };
  }

  function getAccelerationArrow(velocity, yearsAgo, bucketAverages) {
    if (!velocity || !yearsAgo || !bucketAverages) return null;
    const bucket = getVelocityBucket(yearsAgo);
    const avg = bucketAverages[bucket];
    if (!avg || !Number.isFinite(avg) || avg <= 0) return null;
    const ratio = velocity / avg;
    if (ratio >= 1.1) return { arrow: "↑", label: "accelerating", level: "up" };
    if (ratio <= 0.9) return { arrow: "↓", label: "decelerating", level: "down" };
    return null;
  }

  /**
   * Color for age-bias heatmap: bright blue (≤2 years) → muted gray (≥10 years).
   * Returns CSS hex color. year is numeric; yearsAgo = currentYear - year.
   */
  function ageBiasColor(yearsAgo) {
    if (yearsAgo == null || yearsAgo < 0) return "#9aa0a6"; // unknown → gray
    const brightBlue = { r: 26, g: 115, b: 232 };
    const mutedGray = { r: 154, g: 160, b: 166 };
    const t = yearsAgo <= 2 ? 0 : yearsAgo >= 10 ? 1 : (yearsAgo - 2) / 8;
    const r = Math.round(brightBlue.r + t * (mutedGray.r - brightBlue.r));
    const g = Math.round(brightBlue.g + t * (mutedGray.g - brightBlue.g));
    const b = Math.round(brightBlue.b + t * (mutedGray.b - brightBlue.b));
    return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${b.toString(16).padStart(2, "0")}`;
  }

  function extractClusterIdFromUrl(u) {
    if (!u) return null;
    try {
      const url = new URL(u);
      const cites = url.searchParams.get("cites");
      if (cites) return cites;
      const q = url.searchParams.get("q");
      if (q && /^info:([^:]+):scholar\.google\.com/.test(q)) return q.match(/^info:([^:]+):scholar\.google\.com/)[1];
      return null;
    } catch {
      return null;
    }
  }

  /** Get the Scholar cite popup URL for a result (for lazy venue lookup). */
  function getCiteUrlForResult(container, paper) {
    const clusterId = paper?.clusterId || (container && extractClusterIdFromUrl(findFlLinks(container).citedByUrl)) || container?.getAttribute("data-cid") || null;
    if (!clusterId) return null;
    return `${window.location.origin}/scholar?q=info:${encodeURIComponent(clusterId)}:scholar.google.com/&output=cite&scirp=0&hl=en`;
  }

  /** Parse venue name from Scholar citation popup HTML (#gs_citt italics). */
  function parseVenueFromCiteHtml(html) {
    if (!html || typeof html !== "string") return null;
    try {
      const parser = new DOMParser();
      const doc = parser.parseFromString(html, "text/html");
      const citt = doc.querySelector("#gs_citt");
      if (!citt) return null;
      const italics = citt.querySelectorAll("i");
      for (const el of italics) {
        const t = (el.textContent || "").trim();
        if (t.length > 3) return t;
      }
      return null;
    } catch {
      return null;
    }
  }

  /** Escape BibTeX special characters (braces, backslash). */
  function escapeBibTeX(s) {
    return String(s || "")
      .replace(/\\/g, "\\\\")
      .replace(/[{}]/g, (c) => (c === "{" ? "\\{" : "\\}"))
      .trim();
  }

  /** Build a minimal BibTeX entry from paper when Scholar export is unavailable. */
  function buildMinimalBibTeX(paper) {
    const author = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0].trim() || "Unknown";
    const year = paper.year || new Date().getFullYear();
    const key = `${author.replace(/\W+/g, "").slice(0, 20)}${year}`;
    const title = escapeBibTeX(paper.title) || "Unknown";
    const url = paper.url ? `\\url{${paper.url}}` : "";
    const lines = [
      `@misc{${key},`,
      `  author = {${escapeBibTeX(author)}},`,
      `  title = {${title}},`,
      `  year = {${year}}`
    ];
    if (url) lines.push(`  note = {${url}}`);
    lines.push("}");
    return lines.join("\n");
  }

  function extractAuthorsList(authorsVenue) {
    const raw = String(authorsVenue || "").split(/\s*[-–—]\s*/)[0] || "";
    const cleaned = raw.replace(/\u2026/g, "").trim();
    if (!cleaned) return [];
    let parts = cleaned.split(/\s*,\s*/).map((p) => p.trim()).filter(Boolean);
    if (parts.length <= 1 && /\s+and\s+/i.test(cleaned)) {
      parts = cleaned.split(/\s+and\s+/i).map((p) => p.trim()).filter(Boolean);
    }
    return parts.length ? parts : [cleaned];
  }

  function formatAuthorNameApa(name) {
    const clean = String(name || "").replace(/\s+/g, " ").trim();
    if (!clean) return "";
    if (/et al\.?$/i.test(clean)) return clean.replace(/\s+et al\.?$/i, " et al.");
    const parts = clean.split(" ").filter(Boolean);
    if (parts.length === 1) return clean;
    const last = parts.pop();
    const initials = parts.map((p) => p[0]?.toUpperCase() + ".").join(" ").trim();
    return initials ? `${last}, ${initials}` : last;
  }

  function formatAuthorsApa(list) {
    if (!list || !list.length) return "Unknown";
    const formatted = list.map(formatAuthorNameApa).filter(Boolean);
    if (formatted.length === 1) return formatted[0];
    if (formatted.length === 2) return `${formatted[0]} & ${formatted[1]}`;
    return `${formatted.slice(0, -1).join(", ")}, & ${formatted[formatted.length - 1]}`;
  }

  function formatAuthorNameMla(name, isFirst) {
    const clean = String(name || "").replace(/\s+/g, " ").trim();
    if (!clean) return "";
    if (/et al\.?$/i.test(clean)) return clean.replace(/\s+et al\.?$/i, " et al.");
    if (!isFirst) return clean;
    const parts = clean.split(" ").filter(Boolean);
    if (parts.length === 1) return clean;
    const last = parts.pop();
    const first = parts.join(" ");
    return `${last}, ${first}`.trim();
  }

  function formatAuthorsMla(list) {
    if (!list || !list.length) return "Unknown";
    const formatted = list.map((n, i) => formatAuthorNameMla(n, i === 0)).filter(Boolean);
    if (formatted.length === 1) return formatted[0];
    if (formatted.length === 2) return `${formatted[0]} and ${formatted[1]}`;
    return `${formatted[0]}, et al.`;
  }

  function formatInlineCitationText(paper) {
    if (!paper) return "";
    const title = (paper.title || "Untitled").trim();
    const yearMatch = String(paper.year || "").match(/\d{4}/);
    const year = yearMatch ? yearMatch[0] : "n.d.";
    const venue = (paper.venue || extractVenueFromAuthorsVenue(paper.authorsVenue) || "").trim();
    const authors = extractAuthorsList(paper.authorsVenue);

    const apaAuthors = formatAuthorsApa(authors);
    const apaParts = [
      `${apaAuthors} (${year}).`,
      title ? `${title}.` : "",
      venue ? `${venue}.` : ""
    ].filter(Boolean);
    const apa = apaParts.join(" ").replace(/\.\./g, ".");

    const mlaAuthors = formatAuthorsMla(authors);
    const mlaParts = [
      mlaAuthors ? `${mlaAuthors}.` : "",
      title ? `"${title}."` : "",
      venue ? `${venue},` : "",
      year ? `${year}.` : ""
    ].filter(Boolean);
    const mla = mlaParts.join(" ").replace(/\.\./g, ".");

    const bib = buildMinimalBibTeX(paper);
    return `APA: ${apa}\nMLA: ${mla}\nBibTeX:\n${bib}`.trim();
  }

  function attachInlineCitationTooltip(el, paper) {
    if (!el || !paper) return;
    const text = formatInlineCitationText(paper);
    if (!text) return;
    attachFloatingTooltip(el, text);
    if (el.__suFloatingTooltip) el.__suFloatingTooltip.classList.add("su-cite-tooltip");
  }

  /** Fetch a URL via the background script (works for scholar.googleusercontent.com .bib URLs). */
  async function fetchBibViaBackground(url) {
    try {
      const res = await chrome.runtime.sendMessage({ action: "fetchBib", url });
      if (res?.ok && res.body && /@\s*\w+\s*\{/.test(res.body)) return res.body.trim();
    } catch {
      // ignore
    }
    return null;
  }

  /** Extract one full BibTeX entry (handles nested braces) from text starting at @. */
  function extractOneBibTeXEntry(text) {
    const start = text.search(/@\s*\w+\s*\{/);
    if (start < 0) return null;
    let depth = 0;
    let i = text.indexOf("{", start);
    if (i < 0) return null;
    for (; i < text.length; i++) {
      const c = text[i];
      if (c === "{" && (i === 0 || text[i - 1] !== "\\")) depth++;
      else if (c === "}" && (i === 0 || text[i - 1] !== "\\")) {
        depth--;
        if (depth === 0) return text.slice(start, i + 1).trim();
      }
    }
    return null;
  }

  /**
   * Get BibTeX for a result: try Crossref by DOI (full metadata), then Scholar link/cite page,
   * else minimal entry. Uses background script for .bib URLs; Crossref works on author pages when DOI is present.
   */
  async function getBibTeXForPaper(paper, container) {
    const resolveUrl = (href, base) => {
      if (!href) return null;
      try {
        return new URL(href, base || window.location.href).href;
      } catch {
        return href;
      }
    };

    let doi = container ? extractDOIFromResult(container) : null;
    if (!doi && paper.url) {
      const m = String(paper.url).match(/10\.\d{4,}\/[^\s"'<>?#]+/i);
      if (m) doi = m[0].toLowerCase().trim();
    }

    // 0) Crossref API by DOI (full BibTeX with pages, full names) – works on author page when DOI is present
    if (doi) {
      try {
        const enc = encodeURIComponent(doi);
        const r = await fetch(`https://api.crossref.org/works/${enc}/transform/application/x-bibtex`, {
          headers: { Accept: "application/x-bibtex" }
        });
        if (r.ok) {
          const body = await r.text();
          if (body && /@\s*\w+\s*\{/.test(body)) return body.trim();
        }
      } catch {
        // fall through
      }
    }

    // Resolve cluster ID: from paper first, then from "Cited by" link in container (author profile rows use same cites= param)
    let clusterId = paper.clusterId;
    if (!clusterId && container) {
      const citeLink = container.querySelector('a[href*="cites="]');
      if (citeLink) clusterId = extractClusterIdFromUrl(tryGetHref(citeLink));
    }

    // 1) Look for existing BibTeX link in the result row (when user has "Show links to import into BibTeX")
    if (container) {
      const bibLink = container.querySelector('a[href*="scholar.bib"], a[href*="bibtex"]');
      const byText = Array.from(container.querySelectorAll("a")).find(
        (a) => /import into bibtex|bibtex/i.test(text(a))
      );
      const href = (bibLink || byText)?.getAttribute?.("href");
      if (href) {
        const absUrl = resolveUrl(href);
        const body = await fetchBibViaBackground(absUrl);
        if (body) return body;
        try {
          const r = await fetch(absUrl);
          if (r.ok) {
            const body2 = await r.text();
            if (/@\s*\w+\s*\{/.test(body2)) return body2.trim();
          }
        } catch {
          // fall through
        }
      }
    }

    // 2) Fetch cite page and parse for BibTeX link or raw text
    if (clusterId) {
      const citeUrl = `https://scholar.google.com/scholar?q=info:${clusterId}:scholar.google.com/&output=cite`;
      try {
        const r = await fetch(citeUrl);
        if (!r.ok) throw new Error("Cite page failed");
        const html = await r.text();
        const doc = new DOMParser().parseFromString(html, "text/html");

        const bibAnchor = doc.querySelector('a[href*="scholar.bib"]') ||
          Array.from(doc.querySelectorAll("a")).find((a) => /import into bibtex|bibtex/i.test(a.textContent || ""));
        const bibHref = bibAnchor?.getAttribute?.("href");
        if (bibHref) {
          const bibUrl = resolveUrl(bibHref, citeUrl);
          const body = await fetchBibViaBackground(bibUrl);
          if (body) return body;
          try {
            const bibRes = await fetch(bibUrl);
            if (bibRes.ok) {
              const body2 = await bibRes.text();
              if (/@\s*\w+\s*\{/.test(body2)) return body2.trim();
            }
          } catch {
            // fall through
          }
        }

        const textarea = doc.querySelector("textarea");
        if (textarea) {
          const raw = (textarea.value || textarea.textContent || "").trim();
          const entry = extractOneBibTeXEntry(raw);
          if (entry) return entry;
        }
        const pre = doc.querySelector("pre");
        if (pre) {
          const raw = (pre.textContent || "").trim();
          const entry = extractOneBibTeXEntry(raw);
          if (entry) return entry;
        }
        const bodyHtml = doc.body?.innerHTML || "";
        const bodyText = doc.body?.innerText || "";
        for (const raw of [bodyHtml, bodyText]) {
          const entry = extractOneBibTeXEntry(raw);
          if (entry) return entry;
        }

        // 3) Try direct .bib URL from cluster ID (Scholar sometimes injects the link via JS)
        const directBibUrl = `https://scholar.googleusercontent.com/scholar.bib?q=info:${clusterId}:scholar.google.com/&output=citation`;
        const body = await fetchBibViaBackground(directBibUrl);
        if (body) return body;
      } catch {
        // fall through to fallback
      }
    }

    return buildMinimalBibTeX(paper);
  }

  function computeKey({ clusterId, url, title, authors, year }) {
    if (clusterId) return `cid:${clusterId}`;
    if (url) return `url:${url}`;
    const norm = [title, authors, year]
      .map((x) =>
        String(x || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, " ")
          .trim()
      )
      .filter(Boolean)
      .join("|");
    return `fp:${norm.slice(0, 200)}`;
  }

  function extractPaperFromResult(gsR, opts = {}) {
    const allowCrossResult = opts.allowCrossResult !== false;
    const deepScan = opts.deepScan !== false;
    const titleEl = gsR.querySelector(".gs_rt");
    const titleLink = titleEl?.querySelector("a");

    const title = text(titleEl).replace(/^\[[^\]]+\]\s*/g, "").trim();
    const url = tryGetHref(titleLink);

    // Try to find authors/venue - might be in .gs_a or other locations for multi-version papers
    // For multi-version papers, the structure can be different - try multiple strategies
    let authorsVenueEl = gsR.querySelector(".gs_a");
    let authorsVenue = "";
    
    // Strategy 1: Try .gs_a directly in the result
    if (authorsVenueEl) {
      authorsVenue = text(authorsVenueEl);
    }
    
    // Strategy 2: For multi-version papers, look in .gs_ri nested structure
    if (!authorsVenue) {
      const gsRi = gsR.querySelector(".gs_ri");
      if (gsRi) {
        authorsVenueEl = gsRi.querySelector(".gs_a");
        if (authorsVenueEl) {
          authorsVenue = text(authorsVenueEl);
        }
      }
    }
    
    // Strategy 3: If venue is truncated or missing, try to find it from other *versions of the same paper* only.
    // (Do not borrow from a different paper, or stitched rows would all get the first row's badge.)
    const flLinks = findFlLinks(gsR);
    const gsRClusterId = extractClusterIdFromUrl(flLinks.citedByUrl) || gsR.getAttribute("data-cid") || null;
    const gsRDataCid = gsR.getAttribute("data-cid");
    if (allowCrossResult && (!authorsVenue || authorsVenue.includes("…") || authorsVenue.length < 20)) {
      const parent = gsR.closest("#gs_res_ccl_mid, #gs_res_ccl");
      if (parent) {
        const allResults = Array.from(parent.querySelectorAll(".gs_r"));
        for (const otherResult of allResults) {
          if (otherResult === gsR) continue;
          const otherCid = otherResult.getAttribute("data-cid");
          const otherCited = findFlLinks(otherResult).citedByUrl;
          const otherClusterId = extractClusterIdFromUrl(otherCited) || otherCid || null;
          const samePaper = (gsRClusterId && gsRClusterId === otherClusterId) ||
            (gsRDataCid && gsRDataCid === otherCid);
          if (!samePaper) continue;
          const otherVenueEl = otherResult.querySelector(".gs_a") ||
            otherResult.querySelector(".gs_ri .gs_a");
          if (otherVenueEl) {
            const otherVenue = text(otherVenueEl);
            if (otherVenue && !otherVenue.includes("…") &&
                otherVenue.length > (authorsVenue?.length || 0) &&
                (otherVenue.includes("Journal") || otherVenue.includes("Conference") ||
                 otherVenue.match(/,\s*(19|20)\d{2}/))) {
              authorsVenue = otherVenue;
              authorsVenueEl = otherVenueEl;
              break;
            }
          }
        }
      }
    }
    
    // Strategy 4: Look for text patterns that suggest author/venue info
    if (deepScan && (!authorsVenue || authorsVenue.includes("…"))) {
      // Try to find text that looks like "Author - Journal, Year" or "Journal, Year"
      const allElements = Array.from(gsR.querySelectorAll("div, span, p"));
      for (const el of allElements) {
        const txt = text(el);
        // Look for patterns: contains " - " or contains ", 20XX" or ", 19XX"
        if (txt && (txt.includes(" - ") || /,\s*(19|20)\d{2}/.test(txt))) {
          // Check if it's a reasonable length and contains journal-like info
          if (txt.length > 15 && txt.length < 300 && !txt.includes("Cited by") && !txt.includes("…")) {
            authorsVenue = txt;
            authorsVenueEl = el;
            break;
          }
        }
      }
    }
    
    // Strategy 5: Extract from the entire result text if nothing else works
    if (deepScan && (!authorsVenue || authorsVenue.includes("…"))) {
      const fullText = text(gsR);
      // Try to extract author/venue line from full text
      const lines = fullText.split("\n").map(l => l.trim()).filter(l => l.length > 0);
      for (const line of lines) {
        if ((line.includes(" - ") || /,\s*(19|20)\d{2}/.test(line)) && 
            !line.includes("…") && !line.includes("Cited by")) {
          if (line.length > 15 && line.length < 300) {
            authorsVenue = line;
            break;
          }
        }
      }
    }
    
    const snippet = text(gsR.querySelector(".gs_rs"));

    const pdfLink = gsR.querySelector(".gs_or_ggsm a, .gs_ggs a");
    const pdfUrl = tryGetHref(pdfLink);

    const { citedByUrl, versionsUrl, relatedUrl } = flLinks;
    const clusterId =
      extractClusterIdFromUrl(citedByUrl) || gsR.getAttribute("data-cid") || null;

    const year = parseYear(authorsVenue);
    const venue = extractVenueFromAuthorsVenue(authorsVenue);

    const key = computeKey({ clusterId, url, title, authors: authorsVenue, year });

    return {
      key,
      title,
      url,
      authorsVenue,
      venue,
      year,
      snippet,
      pdfUrl,
      citedByUrl,
      versionsUrl,
      relatedUrl,
      clusterId,
      sourcePageUrl: window.location.href,
      _authorsVenueEl: authorsVenueEl
    };
  }

  function extractPaperFromResultFast(gsR) {
    return extractPaperFromResult(gsR, { allowCrossResult: false, deepScan: false });
  }

  function extractPaperFromAuthorProfile(gscTr) {
    // Author profile pages use .gsc_a_tr table rows
    const titleEl = gscTr.querySelector(".gsc_a_at");
    const titleLink = titleEl;

    const title = text(titleEl).trim();
    const url = tryGetHref(titleLink);

    // Author profile pages structure:
    // - Title is in .gsc_a_at (within td.gsc_a_t)
    // - Authors are in div.gs_gray (first one, within td.gsc_a_t)
    // - Venue/journal info is in div.gs_gray (second one, within td.gsc_a_t)
    // - Cited by is in td.gsc_a_c
    // - Year is in td.gsc_a_y
    
    const titleCell = gscTr.querySelector(".gsc_a_t");
    const grayDivs = titleCell ? Array.from(titleCell.querySelectorAll("div.gs_gray")) : [];
    
    let authorsVenue = "";
    let authors = "";
    let venueText = "";
    
    if (grayDivs.length >= 1) {
      authors = text(grayDivs[0]).trim();
    }
    if (grayDivs.length >= 2) {
      venueText = text(grayDivs[1]).trim();
      // Combine authors and venue for compatibility
      authorsVenue = authors ? `${authors} - ${venueText}` : venueText;
    } else if (grayDivs.length === 1) {
      // Fallback: if only one div, try to extract from it
      authorsVenue = text(grayDivs[0]);
    }

    // Year is in .gsc_a_y
    const yearEl = gscTr.querySelector(".gsc_a_y");
    const yearText = text(yearEl);
    const year = parseYear(yearText || venueText || authorsVenue);

    // Extract venue - prefer the venueText if we found it separately
    let venue = "";
    if (venueText) {
      // Try extracting from the venue text directly
      venue = extractVenueFromAuthorsVenue(venueText);
      // If that doesn't work, try with the full authorsVenue
      if (!venue) {
        venue = extractVenueFromAuthorsVenue(authorsVenue);
      }
    } else {
      venue = extractVenueFromAuthorsVenue(authorsVenue);
    }

    // Cited by link is in .gsc_a_c
    const citedByCell = gscTr.querySelector(".gsc_a_c");
    const citedByLink = citedByCell?.querySelector("a.gsc_a_ac");
    const citedByUrl = tryGetHref(citedByLink);
    const clusterId = extractClusterIdFromUrl(citedByUrl);

    const key = computeKey({ clusterId, url, title, authors: authorsVenue, year });

    return {
      key,
      title,
      url,
      authorsVenue,
      venue,
      year,
      snippet: "",
      pdfUrl: null,
      citedByUrl,
      versionsUrl: null,
      relatedUrl: null,
      clusterId,
      sourcePageUrl: window.location.href,
      _venueDiv: grayDivs.length >= 2 ? grayDivs[1] : null // Store venue div for badge insertion
    };
  }

  function normalizeAuthorForHide(s) {
    return String(s || "").toLowerCase().replace(/\s+/g, " ").trim();
  }

  function shouldHide(paper, settings, qIndex, state) {
    const titleReStr = String(settings.hideTitleRegex || "").trim();
    const authorsReStr = String(settings.hideAuthorsRegex || "").trim();

    let titleRe = null;
    let authorsRe = null;

    try {
      if (titleReStr) titleRe = new RegExp(titleReStr, "i");
    } catch {}
    try {
      if (authorsReStr) authorsRe = new RegExp(authorsReStr, "i");
    } catch {}

    if (titleRe && titleRe.test(paper.title || "")) return true;
    if (authorsRe && authorsRe.test(paper.authorsVenue || "")) return true;

    if (state?.hiddenPapers?.has(paper.key)) return true;
    const venueNorm = normalizeVenueName(paper.venue);
    if (venueNorm && state?.hiddenVenues?.has(venueNorm)) return true;
    const authorsPart = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0] || "";
    const authors = authorsPart.split(/\s*,\s*|\s+and\s+/i).map((a) => a.trim()).filter(Boolean);
    for (const a of authors) {
      const an = normalizeAuthorForHide(a);
      if (an && state?.hiddenAuthors?.has(an)) return true;
    }

    return false;
  }

  function highlightKeywords(container, state, isAuthorProfile = false) {
    const settings = state?.settings || {};
    const kws = state?.keywordHighlights || [];
    const regexes = state?.keywordHighlightRegexes || [];
    const hlKey = state?.keywordHighlightKey || "";
    const applyCue = !!settings.showSnippetCueEmphasis;
    const targetEls = isAuthorProfile
      ? [container.querySelector(".gsc_a_at")].filter(Boolean)
      : [container.querySelector(".gs_rt"), container.querySelector(".gs_rs")].filter(Boolean);

    for (const el of targetEls) {
      if (!kws.length && !applyCue) {
        if (el.__suOrigHTMLRaw !== undefined && el.dataset.suHlKey) {
          el.innerHTML = el.__suOrigHTMLRaw;
          delete el.dataset.suHlKey;
        }
        continue;
      }

      if (el.dataset.suHlKey === hlKey) continue;

      if (el.__suOrigHTMLRaw === undefined) el.__suOrigHTMLRaw = el.innerHTML;
      const isSnippet = el.classList && el.classList.contains("gs_rs");
      const hasDynamicSnippet = isSnippet && !!el.querySelector("[class*='gs_fma']");
      if (hasDynamicSnippet) {
        if (el.__suOrigHTMLRaw === undefined) el.__suOrigHTMLRaw = el.innerHTML;
        if (el.innerHTML !== el.__suOrigHTMLRaw) el.innerHTML = el.__suOrigHTMLRaw;
        el.dataset.suHlKey = hlKey;
        continue;
      }
      const baseHtml = isSnippet && applyCue
        ? applySnippetCueEmphasis(el.textContent || "")
        : el.__suOrigHTMLRaw;

      let next = baseHtml;
      if (regexes.length) {
        for (const re of regexes) {
          next = next.replace(re, '<span class="su-hl">$1</span>');
        }
      }
      el.innerHTML = next;
      el.dataset.suHlKey = hlKey;
    }
  }

  function createButton(label, { danger = false, act } = {}) {
    const b = document.createElement("button");
    b.type = "button";
    b.className = `su-btn${danger ? " su-danger" : ""}`;
    b.textContent = label;
    if (act) b.dataset.act = act;
    return b;
  }

  function createBadgeTooltip(badge) {
    if (!badge.metadata) return null;
    
    const tooltip = document.createElement("div");
    tooltip.className = "su-badge-tooltip";
    
    const meta = badge.metadata;
    const parts = [];
    
    // Add system name
    if (meta.system) {
      parts.push(`<strong>${meta.system}</strong>`);
    }
    
    // Add rank/quartile information
    if (meta.rank) {
      parts.push(`Rank: <strong>${meta.rank}</strong>`);
    }
    if (meta.quartile) {
      parts.push(`Quartile: <strong>${meta.quartile.toUpperCase()}</strong>`);
    }
    
    // Add JCR-specific metrics
    if (badge.kind === "jcr" && meta.jcrData) {
      const jcr = meta.jcrData;
      if (jcr.jif !== undefined && jcr.jif !== null) {
        parts.push(`JIF: ${parseFloat(jcr.jif).toFixed(3)}`);
      }
      if (jcr.jci !== undefined && jcr.jci !== null) {
        parts.push(`JCI: ${parseFloat(jcr.jci).toFixed(3)}`);
      }
      if (jcr.ais !== undefined && jcr.ais !== null) {
        parts.push(`AIS: ${parseFloat(jcr.ais).toFixed(3)}`);
      }
      if (jcr.fiveYJif !== undefined && jcr.fiveYJif !== null) {
        parts.push(`5-Year JIF: ${parseFloat(jcr.fiveYJif).toFixed(3)}`);
      }
      if (jcr.category) {
        parts.push(`Category: ${jcr.category}`);
      }
    }
    
    // Add specific metric values if available
    if (meta.jif !== undefined && meta.jif !== null) {
      parts.push(`JIF: ${parseFloat(meta.jif).toFixed(3)}`);
    }
    if (meta.jci !== undefined && meta.jci !== null) {
      parts.push(`JCI: ${parseFloat(meta.jci).toFixed(3)}`);
    }
    if (meta.ais !== undefined && meta.ais !== null) {
      parts.push(`AIS: ${parseFloat(meta.ais).toFixed(3)}`);
    }
    if (meta.fiveYJif !== undefined && meta.fiveYJif !== null) {
      parts.push(`5-Year JIF: ${parseFloat(meta.fiveYJif).toFixed(3)}`);
    }
    if (badge.kind === "h5" && meta.h5 != null) {
      parts.push(`h5-index: <strong>${meta.h5}</strong>`);
    }
    if (badge.kind === "if" && meta.impact != null) {
      const val = Number(meta.impact);
      if (Number.isFinite(val)) parts.push(`Impact Factor: <strong>${val.toFixed(1)}</strong>`);
    }
    
    tooltip.innerHTML = parts.join("<br>");
    return tooltip;
  }

  function buildMarkdownFromRow(row, isAuthorProfile) {
    const p = isAuthorProfile ? getCachedAuthorPaper(row) : extractPaperFromResult(row);
    const snippetText = getSnippetText(row);
    const authorsPart = (p.authorsVenue || "").split(/\s*[-–—]\s*/)[0]?.trim() || "";
    const year = p.year != null ? String(p.year).replace(/\D/g, "").slice(0, 4) : "";
    const link = (p.url || "").trim() || "#";
    const title = (p.title || "Untitled").trim();
    const abstract = (snippetText || "").trim() || "";
    const lines = [
      `**Title:** [${title}](${link})`,
      `**Authors:** ${authorsPart || "—"}`,
      `**Year:** ${year || "—"}`,
      abstract ? `**Abstract:** ${abstract}` : "",
      "#tags: [[Topic]] [[Scholar]]"
    ].filter(Boolean);
    return lines.join("\n");
  }

  function ensureAbstractVisible(container) {
    if (!container) return;
    const fma = container.querySelector(".gs_fma");
    if (!fma) return;
    const raw = (fma.textContent || "").trim();
    const visible = (fma.innerText || "").trim();
    if (raw.length < 40) return;
    if (visible.length >= 20 && !/^abstract$/i.test(visible)) return;
    const toggle = container.querySelector(".gs_fma_sml_a");
    if (toggle && /show more/i.test(toggle.textContent || "")) {
      try { toggle.click(); } catch {}
    }
    setTimeout(() => {
      const v2 = (fma.innerText || "").trim();
      if (v2.length >= 20 && !/^abstract$/i.test(v2)) return;
      let fallback = container.querySelector(".su-abstract-fallback");
      if (!fallback) {
        fallback = document.createElement("div");
        fallback.className = "su-abstract-fallback";
        fma.insertAdjacentElement("afterend", fallback);
      }
      fallback.textContent = raw.replace(/^Abstract\s*/i, "Abstract: ");
    }, 50);
  }

  function renderQuality(container, paper, state, isAuthorProfile = false) {
    const venueKey = paper?.venue || "";
    let retracted = false;
    // Find the info element - try multiple strategies for robustness
    let info = null;
    if (isAuthorProfile) {
      const titleCell = getCachedElement(container, ".gsc_a_t");
      // For author profile pages, try to find the venue div first (must belong to this row).
      if (paper._venueDiv && container.contains(paper._venueDiv)) {
        info = paper._venueDiv;
      } else if (titleCell) {
        // Fallback: look for the venue div in the title cell
        const grayDivs = getCachedElements(titleCell, "div.gs_gray");
        if (grayDivs.length >= 2) {
          info = grayDivs[1]; // Second div contains venue info
        } else if (grayDivs.length === 1) {
          info = grayDivs[0];
        } else {
          info = titleCell; // Last resort: append inside title cell
        }
      }
    } else {
      // Strategy 1: Use the stored element from extraction only if it belongs to this container
      if (paper._authorsVenueEl && container.contains(paper._authorsVenueEl)) {
        info = paper._authorsVenueEl;
      }
      
      // Strategy 2: Try .gs_a first (standard location)
      if (!info) {
        info = getCachedElement(container, ".gs_a");
      }
      
      // Strategy 3: Try finding it in nested structures (for multi-version papers)
      if (!info) {
        const gsRi = getCachedElement(container, ".gs_ri");
        if (gsRi) {
          info = getCachedElement(gsRi, ".gs_a");
        }
      }
      
      // Strategy 4: Try finding by looking for elements with author/venue-like patterns
      if (!info && paper.authorsVenue) {
        // Find element that contains the venue text we extracted
        const venueText = paper.authorsVenue;
        const candidates = Array.from(container.querySelectorAll("div, span, p")).filter(el => {
          const txt = text(el);
          // Match if text is exactly the same or contains a significant portion
          return txt === venueText || 
                 (venueText.length > 20 && txt.length > 10 && 
                  (txt.includes(venueText.slice(0, Math.min(30, venueText.length))) ||
                   venueText.includes(txt.slice(0, Math.min(30, txt.length)))));
        });
        if (candidates.length > 0) {
          // Prefer elements with gs_ class names
          info = candidates.find(el => {
            const cls = el.className || "";
            return cls.includes("gs_");
          }) || candidates[0];
        }
      }
      
      // Strategy 5: Try finding by text pattern matching
      if (!info) {
        const candidates = Array.from(container.querySelectorAll("div, span")).filter(el => {
          const txt = text(el);
          // Look for patterns that suggest author/venue info
          return (txt.includes(" - ") || /,\s*(19|20)\d{2}/.test(txt)) && 
                 txt.length > 10 && txt.length < 300 &&
                 !txt.includes("Cited by") && !txt.includes("All") && !txt.includes("versions");
        });
        if (candidates.length > 0) {
          // Prefer elements that are likely to be the author/venue line
          info = candidates.find(el => {
            const cls = el.className || "";
            return cls.includes("gs_");
          }) || candidates[0];
        }
      }
      
      // Strategy 6: Last resort - try finding any .gs_a in the container (might be deeply nested)
      if (!info) {
        info = getCachedElement(container, ".gs_a");
      }
    }
    
    // If still no info element found, try to insert after snippet or other elements
    if (!info) {
      // Try common fallback locations - prefer elements that come after the title
      const fallback = getCachedElement(container, ".gs_rs") || getCachedElement(container, ".gs_ri") || getCachedElement(container, ".gs_fl");
      if (fallback) {
        info = fallback;
      } else {
        // Last resort: insert after title
        const titleEl = getCachedElement(container, ".gs_rt");
        if (titleEl) {
          info = titleEl;
        } else {
          // Can't find insertion point, skip rendering
          return false;
        }
      }
    }

    // Remove ALL existing quality badges to prevent duplicates
    const existingBadges = getCachedElements(container, ".su-quality");
    existingBadges.forEach(badge => badge.remove());

    if (state.settings.showRetractionWatch && state.retractionBloom) {
      const doi = extractDOIFromResult(container);
      if (doi && bloomHasDoi(doi, state.retractionBloom)) retracted = true;
    }
    
    // Get quality badges (only if enabled), using a per-epoch cache to avoid
    // recomputing the same venue's badges for every result on the page.
    let rawBadges = [];
    if (state.settings.showQualityBadges) {
      if (state.venueQualityCache && state.venueQualityCache.has(venueKey)) {
        rawBadges = state.venueQualityCache.get(venueKey);
      } else {
        rawBadges = qualityBadgesForVenue(venueKey, state.qIndex) || [];
        if (state.venueQualityCache) state.venueQualityCache.set(venueKey, rawBadges);
      }
    }
    const allowedKinds = state.settings.qualityBadgeKinds;
    const badges = allowedKinds && typeof allowedKinds === "object"
      ? rawBadges.filter((b) => allowedKinds[b.kind] !== false)
      : rawBadges;
    const citeUrl = !isAuthorProfile && paper ? getCiteUrlForResult(container, paper) : null;
    const showLookupButton = state.settings.showQualityBadges && !badges.length && citeUrl;
    
    if (retracted) {
      const root = document.createElement("div");
      root.className = "su-quality";
      root.setAttribute("data-su-quality", "true");
      const span = document.createElement("span");
      span.className = "su-badge su-retraction-neutral";
      span.textContent = "Potential update/retraction";
      span.title = "Potential update/retraction (from Retraction Watch list). Please verify via DOI or publisher.";
      root.appendChild(span);
      if (isAuthorProfile) {
        let inserted = false;
        try {
          const titleCell = getCachedElement(container, ".gsc_a_t");
          if (info) {
            if (info.tagName === "TD") {
              info.appendChild(root);
              inserted = true;
            } else if (titleCell && titleCell.contains(info)) {
              info.insertAdjacentElement("afterend", root);
              inserted = true;
            }
          }
          if (!inserted && titleCell) {
            titleCell.appendChild(root);
            inserted = true;
          }
        } catch (_) {}
        if (!inserted) container.appendChild(root);
      } else {
        if (info) {
          try {
            info.insertAdjacentElement("afterend", root);
          } catch (e) {
            const parent = info.parentElement || container;
            if (parent) {
              parent.appendChild(root);
            }
          }
        } else {
          const snippet = container.querySelector(".gs_rs");
          if (snippet) {
            snippet.insertAdjacentElement("afterend", root);
          } else {
            container.appendChild(root);
          }
        }
      }
      return true;
    }

    if (!badges.length && !showLookupButton) {
      return false;
    }
    
    // Create new badge container
    const root = document.createElement("div");
    root.className = "su-quality";
    root.setAttribute("data-su-quality", "true"); // Mark as quality badge
    const venueNorm = normalizeVenueName(venueKey || "");
    if (venueKey) root.setAttribute("data-su-venue", venueKey);
    if (venueNorm) root.setAttribute("data-su-venue-norm", venueNorm);
    const vhbMapRank = venueNorm ? state?.qIndex?.vhb?.get?.(venueNorm) : null;
    if (vhbMapRank) root.setAttribute("data-su-vhb-map", String(vhbMapRank));
    const vhbBadge = rawBadges.find((b) => b.kind === "vhb");
    if (vhbBadge?.text) root.setAttribute("data-su-vhb-badge", String(vhbBadge.text));

    // Short native tooltip for each badge kind (shown on hover)
    const badgeKindTitles = {
      quartile: "SCImago Journal Rank quartile: Q1 = top 25%, Q2 = next 25%, etc.",
      abdc: "ABDC Journal Quality List rank (A*, A, B, C).",
      vhb: "VHB JOURQUAL 2024 journal ranking (A+, A, B, C, D, E).",
      jcr: "Clarivate JCR (Journal Citation Reports) impact quartile or indicator.",
      if: "Journal Impact Factor (2024).",
      ft50: "Financial Times 50 list of top journals used in business school research.",
      utd24: "UT Dallas 24: top journals from the UTD Top 100 Business School Research Rankings.",
      core: "CORE/ICORE conference ranking (A*, A, B, C).",
      ccf: "China Computer Federation conference ranking.",
      era: "Excellence in Research for Australia 2023 listed venue.",
      norwegian: "Norwegian Register: Level 1 or 2 journal.",
      preprint: "Pre-print server (e.g. arXiv, SSRN); not peer-reviewed journal.",
      h5: "Google Scholar 5-year h-index for this venue (from Scholar Metrics)."
    };
    const isValidBadgeText = (b) => {
      const t = String(b.text || "").trim();
      if (!t || t.length > 50) return false;
      switch (b.kind) {
        case "quartile": return /^Q[1-4]$/i.test(t);
        case "abdc": return /^ABDC\s+(A\*?|[BCD])$/i.test(t);
        case "vhb": return /^VHB\s+(A\+?|[BCDE])$/i.test(t);
        case "jcr": return /^(JIF|JCI|AIS|5Y)\s+Q[1-4]$/i.test(t);
        case "if": return /^IF\s+\d+(?:\.\d+)?$/i.test(t);
        case "core": return /^CORE\s+(A\*?|[ABC])$/i.test(t);
        case "ccf": return /^CCF\s+[ABC]$/i.test(t);
        case "norwegian": return /^Level\s+[12]$/i.test(t);
        case "ft50": return t === "FT50";
        case "utd24": return t === "UTD24";
        case "era": return t === "ERA 2023";
        case "preprint": return /^(SSRN|arXiv|bioRxiv|medRxiv|Research Square|SocArXiv|PsyArXiv|ChemRxiv|EdArXiv|OSF Preprints|Preprints\.org|Zenodo)$/i.test(t);
        case "h5": return /^h5:\s*\d+$/i.test(t);
        default: return true;
      }
    };
    if (state.settings.showQualityBadges) {
      for (const b of badges) {
        if (!isValidBadgeText(b)) continue;
        const span = document.createElement("span");
        const extraClass = b.kind === "if" ? " su-jcr" : "";
        span.className = `su-badge su-${b.kind}${extraClass}`;
        span.textContent = b.text;
        span.title = badgeKindTitles[b.kind] || (b.metadata?.system ? `${b.text}: ${b.metadata.system}` : b.text);

        // Add tooltip with metrics if metadata is available
        if (b.metadata) {
          const tooltip = createBadgeTooltip(b);
          if (tooltip) {
            span.appendChild(tooltip);
            span.style.position = "relative";
            span.style.cursor = "help";
            
            span.addEventListener("mouseenter", (e) => {
              tooltip.style.display = "block";
              const rect = span.getBoundingClientRect();
              const tooltipRect = tooltip.getBoundingClientRect();
              if (rect.top < tooltipRect.height + 20) {
                tooltip.style.bottom = "auto";
                tooltip.style.top = "100%";
                tooltip.style.marginTop = "8px";
                tooltip.style.marginBottom = "0";
                tooltip.style.transform = "translateX(-50%)";
                tooltip.classList.add("su-tooltip-below");
              } else {
                tooltip.style.bottom = "100%";
                tooltip.style.top = "auto";
                tooltip.style.marginTop = "0";
                tooltip.style.marginBottom = "8px";
                tooltip.style.transform = "translateX(-50%)";
                tooltip.classList.remove("su-tooltip-below");
              }
              requestAnimationFrame(() => {
                const tr = tooltip.getBoundingClientRect();
                const vw = window.innerWidth;
                const pad = 8;
                const spanRect = span.getBoundingClientRect();
                if (tr.right > vw - pad) {
                  tooltip.style.left = ((vw - pad - spanRect.left) - tr.width) + "px";
                  tooltip.style.transform = "none";
                } else if (tr.left < pad) {
                  tooltip.style.left = (pad - spanRect.left) + "px";
                  tooltip.style.transform = "none";
                }
              });
            });
            
            span.addEventListener("mouseleave", () => {
              tooltip.style.display = "none";
            });
          }
        }
        
        root.appendChild(span);
      }
    }

    if (showLookupButton) {
      const lookupBtn = document.createElement("button");
      lookupBtn.type = "button";
      lookupBtn.className = "su-venue-lookup-btn su-badge";
      lookupBtn.textContent = "?";
      lookupBtn.title = "Look up venue ranking (fetch full venue name from citation)";
      lookupBtn.setAttribute("aria-label", "Look up venue ranking");
      lookupBtn.style.cssText = "margin-left:4px;padding:0 5px;font-weight:bold;cursor:pointer;min-width:22px;";
      lookupBtn.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (lookupBtn.disabled) return;
        lookupBtn.disabled = true;
        lookupBtn.textContent = "…";
        try {
          const res = await chrome.runtime.sendMessage({ action: "fetchSnippet", url: citeUrl });
          const venue = res?.ok && res?.html ? parseVenueFromCiteHtml(res.html) : null;
          if (venue && paper.clusterId && state.venueCache) {
            state.venueCache.set(paper.clusterId, venue);
          }
          delete container.dataset.suProcessed;
          delete container.dataset.suFastKey;
          await ensureResultUI(container, state, isAuthorProfile);
        } catch {
          lookupBtn.disabled = false;
          lookupBtn.textContent = "?";
        }
      });
      root.appendChild(lookupBtn);
    }

    // For author profile pages, insert after the .gsc_a_c element
    // For search results, insert after .gs_a or fallback element
    if (isAuthorProfile) {
      // Author profile rows are table-based. Prefer inserting within the title cell
      // to avoid invalid table structures.
      let inserted = false;
      try {
        const titleCell = getCachedElement(container, ".gsc_a_t");
        if (info) {
          if (info.tagName === "TD") {
            info.appendChild(root);
            inserted = true;
          } else if (titleCell && titleCell.contains(info)) {
            // Insert right after the venue/author line when possible.
            info.insertAdjacentElement("afterend", root);
            inserted = true;
          }
        }
        if (!inserted && titleCell) {
          titleCell.appendChild(root);
          inserted = true;
        }
      } catch (_) {}
      if (!inserted) container.appendChild(root);
    } else {
      // For search results, insert after the info element
      if (info) {
        try {
          info.insertAdjacentElement("afterend", root);
        } catch (e) {
          // If insertion fails, try appending to container or parent
          const parent = info.parentElement || container;
          if (parent) {
            parent.appendChild(root);
          }
        }
      } else {
        // No info element found - try to insert in a reasonable location
        // Try after snippet, or after title, or just append to container
        const snippet = container.querySelector(".gs_rs");
        if (snippet) {
          snippet.insertAdjacentElement("afterend", root);
        } else {
          const title = container.querySelector(".gs_rt");
          if (title) {
            title.insertAdjacentElement("afterend", root);
          } else {
            container.appendChild(root);
          }
        }
      }
    }

    // Final safety: ensure only one quality badge row per result
    const allBadges = container.querySelectorAll(".su-quality");
    if (allBadges.length > 1) {
      allBadges.forEach((b, idx) => { if (idx > 0) b.remove(); });
    }
    return false;
  }

  function applyAuthorSort() {
    const tbody = document.getElementById("gsc_a_b");
    if (!tbody) return;
    const rows = Array.from(tbody.querySelectorAll("tr.gsc_a_tr"));
    if (rows.length === 0) return;

    if (window.suAuthorSortByVelocity) {
      if (!window.suAuthorRowsOriginalOrder) {
        window.suAuthorRowsOriginalOrder = rows.slice();
      }
      const currentYear = new Date().getFullYear();
      const withVelocity = rows.map((tr) => {
        const paper = getCachedAuthorPaper(tr);
        const citations = getCachedAuthorCitationCount(tr);
        const yearsAgo = Math.max(1, currentYear - (paper?.year || currentYear));
        const velocity = citations != null ? citations / yearsAgo : -1;
        return { tr, velocity };
      });
      withVelocity.sort((a, b) => b.velocity - a.velocity);
      withVelocity.forEach(({ tr }) => tbody.appendChild(tr));
    } else {
      if (window.suAuthorRowsOriginalOrder && window.suAuthorRowsOriginalOrder.length > 0) {
        const savedSet = new Set(window.suAuthorRowsOriginalOrder);
        window.suAuthorRowsOriginalOrder.forEach((tr) => tbody.appendChild(tr));
        rows.forEach((tr) => { if (!savedSet.has(tr)) tbody.appendChild(tr); });
      }
    }
  }

  function renderVelocity(container, paper, isAuthorProfile) {
    if (isAuthorProfile && window.suState?.authorFeatureToggles?.velocityBadge === false) {
      const existing = getCachedElement(container, ".su-velocity");
      if (existing) {
        const prev = existing.previousSibling;
        if (prev && prev.nodeType === Node.TEXT_NODE && prev.textContent === " ") prev.remove();
        existing.remove();
      }
      return;
    }
    const citations = isAuthorProfile
      ? getCachedAuthorCitationCount(container)
      : getCitationCountFromResult(container);
    const velocityStr = computeVelocity(citations, paper.year);
    if (!velocityStr) return;
    const velocityData = computeVelocityValue(citations, paper.year);
    const accel = velocityData
      ? getAccelerationArrow(velocityData.velocity, velocityData.yearsAgo, window.suState?.velocityBucketAvg)
      : null;

    const anchor = isAuthorProfile
      ? getCachedElement(container, ".gsc_a_c")
      : getCachedElement(container, ".gs_fl");
    if (!anchor) return;

    const existing = getCachedElement(container, ".su-velocity");
    if (existing) {
      const prev = existing.previousSibling;
      if (prev && prev.nodeType === Node.TEXT_NODE && prev.textContent === " ") prev.remove();
      existing.remove();
    }
    const vel = document.createElement("span");
    vel.className = "su-velocity";
    vel.textContent = velocityStr;
    if (accel) {
      const accelSpan = document.createElement("span");
      accelSpan.className = `su-velocity-accel su-velocity-accel-${accel.level}`;
      accelSpan.textContent = ` ${accel.arrow}`;
      vel.appendChild(accelSpan);
    }
    vel.title = "Citations per year: total citations ÷ years since publication. Not field-normalized.";
    if (accel) {
      vel.title += ` Citation acceleration: ${accel.label} (cites/yr vs similar-age papers).`;
    }
    const bar = getCachedElement(container, ".su-btnbar");
    if (bar) {
      bar.appendChild(vel);
    } else {
      anchor.appendChild(document.createTextNode(" "));
      anchor.appendChild(vel);
    }
  }

  function renderCitationSpikeBadge(container, paper, state, citations, isAuthorProfile) {
    if (!state.settings.showCitationSpike || !paper?.clusterId || citations == null || citations < 0) return;
    const thresholdPct = Math.max(0, Number(state.settings.citationSpikeThresholdPct) || 50);
    const months = Math.max(1, Math.min(24, Number(state.settings.citationSpikeMonths) || 6));
    const snap = state.citationSnapshots?.[paper.clusterId];
    if (!snap || typeof snap.citations !== "number" || snap.citations < 1) {
      setCitationSnapshot(paper.clusterId, citations).catch(() => {});
      if (state.citationSnapshots) state.citationSnapshots[paper.clusterId] = { citations, date: new Date().toISOString() };
      return;
    }
    const prevDate = new Date(snap.date);
    const now = new Date();
    const monthsAgo = (now.getFullYear() - prevDate.getFullYear()) * 12 + (now.getMonth() - prevDate.getMonth());
    if (monthsAgo < Math.max(1, months - 1)) {
      setCitationSnapshot(paper.clusterId, citations).catch(() => {});
      if (state.citationSnapshots) state.citationSnapshots[paper.clusterId] = { citations, date: new Date().toISOString() };
      return;
    }
    const minCurrent = snap.citations * (1 + thresholdPct / 100);
    if (citations <= minCurrent) {
      setCitationSnapshot(paper.clusterId, citations).catch(() => {});
      if (state.citationSnapshots) state.citationSnapshots[paper.clusterId] = { citations, date: new Date().toISOString() };
      return;
    }
    const pct = Math.round(((citations - snap.citations) / snap.citations) * 100);
    const existing = container.querySelector(".su-citation-spike-badge");
    if (existing) existing.remove();
    const badge = document.createElement("span");
    badge.className = "su-citation-spike-badge su-btn";
    badge.textContent = `↑${pct}% in ${monthsAgo}mo`;
    badge.title = `Citations increased by ${pct}% since ${monthsAgo} months ago (${snap.citations} → ${citations}).`;
    const anchor = isAuthorProfile ? container.querySelector(".gsc_a_c") : container.querySelector(".gs_fl");
    if (anchor) {
      anchor.appendChild(document.createTextNode(" "));
      anchor.appendChild(badge);
    }
    setCitationSnapshot(paper.clusterId, citations).catch(() => {});
    if (state.citationSnapshots) state.citationSnapshots[paper.clusterId] = { citations, date: new Date().toISOString() };
  }

  /**
   * Authorship heatmap: First Author = bold underline (primary); Last Author = dotted border (PI).
   * Preserves existing author links; wraps first/last in styled spans so names stay clickable.
   */
  function renderAuthorshipHeatmap(container, state, isAuthorProfile) {
    if (!state.settings.showAuthorshipHeatmap) {
      const authorEl = container.querySelector(".gs_a, .gsc_a_t div.gs_gray");
      if (authorEl?.querySelector(".su-author")) {
        authorEl.textContent = authorEl.textContent;
      }
      return;
    }
    let authorEl = null;
    if (isAuthorProfile) {
      const titleCell = container.querySelector(".gsc_a_t");
      const grayDivs = titleCell ? Array.from(titleCell.querySelectorAll("div.gs_gray")) : [];
      authorEl = grayDivs[0] || null;
    } else {
      authorEl = container.querySelector(".gs_ri .gs_a") || container.querySelector(".gs_a");
    }
    if (!authorEl) return;
    if (authorEl.querySelector(".su-author-first")) return; // already applied

    // Preserve clickable author links: wrap each link's content in a styled span instead of replacing DOM
    const authorLinks = authorEl.querySelectorAll('a[href*="citations"]');
    if (authorLinks.length > 0) {
      authorLinks.forEach((a, i) => {
        const span = document.createElement("span");
        span.className = "su-author";
        // Check if link text contains "*" marker (corresponding author)
        const linkText = a.textContent || "";
        const hasAsterisk = linkText.includes("*");
        if (i === 0 || hasAsterisk) span.classList.add("su-author-first");
        while (a.firstChild) span.appendChild(a.firstChild);
        a.appendChild(span);
      });
      return;
    }

    const fullText = text(authorEl);
    const dashSplit = fullText.split(/\s*[-–—]\s*/);
    const authorsPart = (dashSplit[0] || "").trim();
    const rest = dashSplit.slice(1).join(" - ").trim();
    const authors = authorsPart.split(/\s*,\s*|\s+and\s+/i).map((s) => s.trim()).filter(Boolean);
    if (authors.length === 0) return;

    const fragment = document.createDocumentFragment();
    for (let i = 0; i < authors.length; i++) {
      const span = document.createElement("span");
      span.textContent = authors[i];
      // Mark as first author if position 0 OR has "*" marker (corresponding author)
      const hasAsterisk = authors[i].includes("*");
      span.className = (i === 0 || hasAsterisk) ? "su-author su-author-first" : "su-author";
      fragment.appendChild(span);
      if (i < authors.length - 1) fragment.appendChild(document.createTextNode(", "));
    }
    if (rest) {
      fragment.appendChild(document.createTextNode(" - " + rest));
    }
    authorEl.textContent = "";
    authorEl.appendChild(fragment);
  }

  /**
   * Skimmability heat: green = PDF + abstract + citations; yellow = missing abstract or venue; red = no PDF, no citations, odd venue.
   */
  function computeSkimmability(container, paper, isAuthorProfile) {
    const hasPdf = !!getBestPdfUrl(container);
    const snippetText = getSnippetText(container);
    const hasAbstract = (snippetText || "").trim().length >= 25;
    const cite = isAuthorProfile ? getCachedAuthorCitationCount(container) : getCitationCountFromResult(container);
    const hasCitations = cite != null && cite >= 0;
    const venue = (paper.venue || "").trim();
    const hasVenue = venue.length >= 3;
    const oddVenue = !hasVenue || isPreprintVenue(venue);
    if (hasPdf && hasAbstract && hasCitations) return "green";
    if (!hasPdf && !hasCitations && oddVenue) return "red";
    return "yellow";
  }

  function renderSkimmabilityStrip(container, paper, state, isAuthorProfile) {
    const existing = container.querySelector(".su-skimmability-strip");
    if (existing) existing.remove();
    container.classList.remove("su-has-skimmability");
    if (!state.settings.showSkimmabilityStrip) return;
    const color = computeSkimmability(container, paper, isAuthorProfile);
    const strip = document.createElement("div");
    strip.className = "su-skimmability-strip su-skimmability-" + color;
    strip.setAttribute("aria-label", `Skimmability: ${color}`);
    strip.title =
      color === "green"
        ? "Skimmability: this result has a PDF link, abstract/snippet, and citation count—easier to assess quickly."
        : color === "red"
          ? "Skimmability: missing PDF, no citations, and odd or missing venue—harder to assess from the snippet alone."
          : "Skimmability: abstract or venue info is missing—partial signal for quick assessment.";
    container.classList.add("su-has-skimmability");
    if (!container.style.position || container.style.position === "static") {
      container.style.position = "relative";
    }
    container.insertBefore(strip, container.firstChild);
  }

  function renderArtifactBadges(container, state) {
    const existing = container.querySelector(".su-artifact-badge");
    if (existing) existing.remove();
    if (!state.settings.showArtifactBadge) return;

    const titleEl = container.querySelector(".gs_rt") || container.querySelector(".gsc_a_at");
    if (!titleEl) return;

    const { code } = detectArtifacts(container);
    if (!code) return;

    const wrap = document.createElement("span");
    wrap.className = "su-artifact-badge";
    wrap.setAttribute("aria-label", "Has code/link");
    wrap.textContent = " 💻";
    wrap.title = "Code/repository (e.g. GitHub) detected";
    titleEl.appendChild(wrap);
  }

  function renderExternalSignalBadges(container, state, isAuthorProfile, opts = {}) {
    const existing = container.querySelectorAll(".su-external");
    existing.forEach((el) => el.remove());
    const doiRaw = extractDOIFromResult(container);
    const doi = normalizeDoi(doiRaw);
    const arxivId = extractArxivIdFromResult(container);
    const pmid = extractPubmedIdFromResult(container);
    const pmcid = extractPmcIdFromResult(container);
    const doiKey = doi || null;
    const arxivKey = arxivId ? makeExternalKey("arxiv", arxivId) : null;
    const epmcKey = doiKey || (pmid ? makeExternalKey("pmid", pmid) : (pmcid ? makeExternalKey("pmcid", pmcid) : null));

    const crossrefEntry = doiKey ? getExternalSignalEntry(doiKey, "crossref") : null;
    const ocEntry = doiKey ? getExternalSignalEntry(doiKey, "opencitations") : null;
    const dcEntry = doiKey ? getExternalSignalEntry(doiKey, "datacite") : null;
    const unpayEntry = doiKey ? getExternalSignalEntry(doiKey, "unpaywall") : null;
    const s2Entry = doiKey ? getExternalSignalEntry(doiKey, "s2") : null;
    const arxivEntry = arxivKey ? getExternalSignalEntry(arxivKey, "arxiv") : null;
    const epmcEntry = epmcKey ? getExternalSignalEntry(epmcKey, "epmc") : null;
    const ncbiEntry = epmcKey ? getExternalSignalEntry(epmcKey, "ncbi") : null;
    if (opts.inViewport !== false) {
      if (doiKey) {
        if (!crossrefEntry) enqueueExternalSignalFetch(doiKey, "crossref", fetchCrossrefSignal, doi);
        if (!ocEntry) enqueueExternalSignalFetch(doiKey, "opencitations", fetchOpenCitationsSignal, doi);
        if (!dcEntry) enqueueExternalSignalFetch(doiKey, "datacite", fetchDataCiteSignal, doi);
        if (!unpayEntry) enqueueExternalSignalFetch(doiKey, "unpaywall", fetchUnpaywallSignal, doi);
        if (!s2Entry) enqueueExternalSignalFetch(doiKey, "s2", fetchSemanticScholarSignal, doi);
      }
      if (arxivKey && !arxivEntry) enqueueExternalSignalFetch(arxivKey, "arxiv", fetchArxivSignal, arxivId);
      if (epmcKey && !epmcEntry) enqueueExternalSignalFetch(epmcKey, "epmc", fetchEpmcSignal, { doi, pmid, pmcid });
      const pmidForNcbi = pmid || epmcEntry?.data?.pmid;
      if (epmcKey && pmidForNcbi && !ncbiEntry) enqueueExternalSignalFetch(epmcKey, "ncbi", fetchNcbiSignal, { pmid: pmidForNcbi });
    }

    const crossref = crossrefEntry?.data || {};
    const openCites = ocEntry?.data || {};
    const datacite = dcEntry?.data || {};
    const unpay = unpayEntry?.data || {};
    const arxiv = arxivEntry?.data || {};
    const epmc = epmcEntry?.data || {};
    const ncbi = ncbiEntry?.data || {};
    const s2 = s2Entry?.data || {};

    const badges = [];
    if (unpay.isOa) {
      const status = unpay.oaStatus ? ` (${String(unpay.oaStatus).toUpperCase()})` : "";
      const host = unpay.hostType ? ` · ${unpay.hostType}` : "";
      badges.push({ text: "OA", cls: "su-external-oa", title: `Open access${status}${host}` });
    }
    if (crossref.hasFullText) badges.push({ text: "Full text", cls: "su-external-fulltext", title: "Full-text link detected (Crossref)" });
    if (crossref.hasFunder) badges.push({ text: "Funded", cls: "su-external-funded", title: "Funder metadata present (Crossref)" });
    if (crossref.hasUpdate) badges.push({ text: "Updated", cls: "su-external-updated", title: "Corrections/updates detected (Crossref)" });
    if (Number(openCites.count) > 0) badges.push({ text: `Open cites ${openCites.count}`, cls: "su-external-opencites", title: "Open citations from OpenCitations COCI" });
    if (datacite.hasDataset) badges.push({ text: "Dataset DOI", cls: "su-external-dataset", title: "Related dataset DOI (DataCite)" });
    if (datacite.hasSoftware) badges.push({ text: "Software DOI", cls: "su-external-software", title: "Related software DOI (DataCite)" });
    if (arxiv.id || arxivId) {
      const ver = arxiv.version ? ` ${arxiv.version}` : "";
      const cat = arxiv.primaryCategory ? ` · ${arxiv.primaryCategory}` : "";
      badges.push({ text: `arXiv${ver}`, cls: "su-external-arxiv", title: `Preprint${cat}` });
    }
    if (epmc.pmid || pmid) {
      const pm = epmc.pmid || pmid;
      const cite = epmc.citationCount ? ` · cites ${epmc.citationCount}` : "";
      badges.push({ text: "PubMed", cls: "su-external-pubmed", title: `PubMed ${pm}${cite}` });
    }
    if (epmc.pmcid || pmcid) {
      const pmc = epmc.pmcid || pmcid;
      const label = epmc.isOpenAccess ? "PMC OA" : "PMC";
      badges.push({ text: label, cls: "su-external-pmc", title: `PubMed Central ${pmc}` });
    }
    if (Array.isArray(ncbi.meshTerms) && ncbi.meshTerms.length) {
      const top = ncbi.meshTerms.slice(0, 4).join(", ");
      badges.push({ text: "MeSH", cls: "su-external-mesh", title: `MeSH terms: ${top}` });
    }
    if (s2.paperId || (Array.isArray(s2.fields) && s2.fields.length)) {
      const fields = Array.isArray(s2.fields) && s2.fields.length ? `Fields: ${s2.fields.slice(0, 3).join(", ")}` : "";
      const infl = s2.influential ? `Influential cites: ${s2.influential}` : "";
      const tip = [fields, infl].filter(Boolean).join(" · ") || "Semantic Scholar";
      badges.push({ text: "S2", cls: "su-external-s2", title: tip });
    }

    if (!badges.length) return;

    const root = document.createElement("div");
    root.className = "su-external";
    for (const b of badges) {
      const span = document.createElement("span");
      span.className = `su-badge su-external-badge ${b.cls}`;
      span.textContent = b.text;
      if (b.title) {
        span.setAttribute("aria-label", b.title);
        span.removeAttribute("title");
        const tip = document.createElement("span");
        tip.className = "su-badge-tooltip su-external-tooltip";
        tip.textContent = b.title;
        span.appendChild(tip);
      }
      root.appendChild(span);
    }

    const qualityRow = container.querySelector(".su-quality");
    if (qualityRow) {
      qualityRow.insertAdjacentElement("afterend", root);
      return;
    }
    if (isAuthorProfile) {
      const info = container.querySelector(".gsc_a_c") || container.querySelector(".gsc_a_y");
      if (info) info.insertAdjacentElement("afterend", root);
      else container.appendChild(root);
      return;
    }
    const info = container.querySelector(".gs_a") || container.querySelector(".gs_rs") || container.querySelector(".gs_rt");
    if (info) {
      try { info.insertAdjacentElement("afterend", root); } catch { container.appendChild(root); }
    } else {
      container.appendChild(root);
    }
  }

  /**
   * Contribution Signal Score (CSS): composite 0–100 estimate of intellectual contribution.
   * Formula: CSS = 40%·V + 40%·W + 20%·N (reference entropy E omitted—no data).
   * V = citation velocity (log-normalized, cap 20/yr → 1). W = venue tier weight [0,1]. N = artifact novelty (0, 0.1, or 0.2).
   */
  function computeCSS(container, paper, state, isAuthorProfile) {
    const citations = isAuthorProfile
      ? getCachedAuthorCitationCount(container)
      : getCitationCountFromResult(container);
    const year = paper.year != null ? parseYear(String(paper.year)) : null;
    if (year == null) return null;
    const currentYear = new Date().getFullYear();
    const yearsAgo = Math.max(1, currentYear - year);
    const velocity = citations != null && citations >= 0 ? citations / yearsAgo : 0;
    const V_norm = Math.min(1, Math.log(1 + velocity) / Math.log(1 + 20));
    const W = venueWeightForVenue(paper.venue, state.qIndex);
    const { code, data } = detectArtifacts(container);
    const N = (code && data) ? 0.2 : (code || data) ? 0.1 : 0;
    const score = 0.4 * V_norm + 0.4 * W + 0.2 * N;
    const velocityStr = velocity >= 10 ? Math.round(velocity) : velocity.toFixed(1);
    return {
      score: Math.round(score * 100),
      V_norm: Math.round(V_norm * 100) / 100,
      W: Math.round(W * 100) / 100,
      N,
      velocityStr,
      formula: `CSS = 0.4×V + 0.4×W + 0.2×N (E omitted)`
    };
  }

  function renderCSSBadge(container, paper, state, isAuthorProfile) {
    const existing = container.querySelector(".su-css-badge");
    if (existing) existing.remove();
    if (!state.settings.showCSS) return;

    const css = computeCSS(container, paper, state, isAuthorProfile);
    if (!css) return;

    const anchor = isAuthorProfile
      ? container.querySelector(".gsc_a_c")
      : container.querySelector(".gs_fl");
    if (!anchor) return;

    const badge = document.createElement("span");
    badge.className = "su-css-badge";
    badge.setAttribute("aria-label", `Contribution Signal Score: ${css.score}`);
    badge.textContent = `CSS ${css.score}`;
    const tooltip = document.createElement("span");
    tooltip.className = "su-css-tooltip";
    tooltip.innerHTML = [
      "Contribution Signal Score (0–100). Not raw popularity.",
      "",
      `Formula: ${css.formula}`,
      `V (velocity): ${css.V_norm} (≈ ${css.velocityStr} cites/yr, log‑normalized; not field‑normalized)`,
      `W (venue): ${css.W}`,
      `N (artifacts): ${css.N}`,
      "",
      "Reference entropy E omitted (no data)."
    ].join("<br>");
    badge.appendChild(tooltip);
    badge.addEventListener("mouseenter", () => { tooltip.classList.add("su-css-tooltip-visible"); });
    badge.addEventListener("mouseleave", () => { tooltip.classList.remove("su-css-tooltip-visible"); });
    anchor.appendChild(document.createTextNode(" "));
    anchor.appendChild(badge);
  }

  function renderReadingLoadBadge(container, paper, state, isAuthorProfile) {
    const existing = container.querySelector(".su-reading-load-badge");
    if (existing) existing.remove();
    // Feature disabled (removed per request)
    return;
  }

  function getCurrentSearchQuery() {
    try {
      const u = new URL(window.location.href);
      const q = u.searchParams.get("q");
      if (q) return q.trim();
    } catch {}
    const input = document.querySelector('#gs_hdr_ts_in, input[name="q"]');
    return (input?.value ?? "").trim();
  }

  async function ensureReadingQueueSidebar() {
    const queue = await getReadingQueue();
    let root = document.getElementById("su-reading-queue-sidebar");
    if (!root) {
      root = document.createElement("div");
      root.id = "su-reading-queue-sidebar";
      root.className = "su-reading-queue-sidebar";
      document.body.appendChild(root);
    }

    if (queue.length === 0) {
      root.classList.remove("su-reading-queue-visible");
      root.innerHTML = "";
      return;
    }

    root.classList.add("su-reading-queue-visible");
    const heading = document.createElement("div");
    heading.className = "su-reading-queue-heading";
    heading.textContent = `Reading Queue (${queue.length})`;
    heading.title = "Papers you added with the + button. Click a title to open; clear all here or in the extension options.";
    const list = document.createElement("div");
    list.className = "su-reading-queue-list";
    for (const item of queue) {
      const entry = document.createElement("div");
      entry.className = "su-reading-queue-item";
      const link = document.createElement("a");
      link.href = item.link;
      link.target = "_blank";
      link.rel = "noopener";
      link.textContent = item.title.length > 60 ? item.title.slice(0, 57) + "…" : item.title;
      link.title = item.title;
      entry.appendChild(link);
      if (item.searchQuery) {
        const query = document.createElement("div");
        query.className = "su-reading-queue-query";
        query.textContent = `“${item.searchQuery.length > 40 ? item.searchQuery.slice(0, 37) + "…" : item.searchQuery}”`;
        entry.appendChild(query);
      }
      list.appendChild(entry);
    }
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "su-reading-queue-clear";
    clearBtn.textContent = "Clear All";
    clearBtn.title = "Remove all items from the reading queue. You can also manage the queue in the extension options.";
    clearBtn.addEventListener("click", async () => {
      await clearReadingQueue();
      await ensureReadingQueueSidebar();
    });

    const actions = document.createElement("div");
    actions.className = "su-reading-queue-actions";

    const copyAllBibBtn = document.createElement("button");
    copyAllBibBtn.type = "button";
    copyAllBibBtn.className = "su-reading-queue-action";
    copyAllBibBtn.textContent = "Copy All BibTeX";
    copyAllBibBtn.title = "Copy BibTeX for all queued papers into the clipboard";

    const exportCsvBtn = document.createElement("button");
    exportCsvBtn.type = "button";
    exportCsvBtn.className = "su-reading-queue-action";
    exportCsvBtn.textContent = "Export CSV";
    exportCsvBtn.title = "Export the queue as a CSV spreadsheet";

    const openAllBtn = document.createElement("button");
    openAllBtn.type = "button";
    openAllBtn.className = "su-reading-queue-action";
    openAllBtn.textContent = "Open All";
    openAllBtn.title = "Open all queued papers in new tabs";

    copyAllBibBtn.addEventListener("click", async () => {
      if (queue.length === 0) return;
      const prev = copyAllBibBtn.textContent;
      copyAllBibBtn.textContent = "Copying…";
      copyAllBibBtn.disabled = true;
      try {
        const bibs = [];
        for (const item of queue) {
          const bib = await getBibTeXForQueueItem(item);
          if (bib) bibs.push(bib.trim());
        }
        const all = bibs.join("\n\n");
        await navigator.clipboard.writeText(all);
        copyAllBibBtn.textContent = "Copied!";
      } catch {
        copyAllBibBtn.textContent = "Failed";
      }
      setTimeout(() => {
        copyAllBibBtn.textContent = prev;
        copyAllBibBtn.disabled = false;
      }, 1500);
    });

    exportCsvBtn.addEventListener("click", async () => {
      if (queue.length === 0) return;
      const rows = [
        ["Title", "URL", "Search Query"],
        ...queue.map((item) => [item.title || "", item.link || "", item.searchQuery || ""])
      ];
      const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
      try {
        await navigator.clipboard.writeText(csv);
      } catch {}
      const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "reading-queue.csv";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    });

    openAllBtn.addEventListener("click", () => {
      if (queue.length === 0) return;
      if (queue.length > 10) {
        const ok = window.confirm(`Open ${queue.length} tabs?`);
        if (!ok) return;
      }
      for (const item of queue) {
        if (item.link) window.open(item.link, "_blank", "noopener");
      }
    });

    actions.appendChild(copyAllBibBtn);
    actions.appendChild(exportCsvBtn);
    actions.appendChild(openAllBtn);

    root.innerHTML = "";
    root.appendChild(heading);
    root.appendChild(list);
    root.appendChild(clearBtn);
    root.appendChild(actions);
  }

  function renderRetractionBadge(container, state) {
    const existing = container.querySelector(".su-retraction-badge");
    if (existing) existing.remove();
    const existingLink = container.querySelector(".su-retraction-badge-link");
    if (existingLink) existingLink.remove();
    if (!state.settings.showRetractionWatch) return;

    const doi = extractDOIFromResult(container);
    const gsRi = container.querySelector(".gs_ri");
    const insertTarget = gsRi || container.querySelector(".gs_rs")?.parentElement || container;
    if (!insertTarget) return;

    const tooltip = document.createElement("span");
    tooltip.className = "su-retraction-tooltip";
    tooltip.innerHTML = [
      "This work has an update (e.g. retraction) in Crossref. Click to open the DOI page for this paper.",
      "",
      "Source: Crossref (includes Retraction Watch)",
      '<a href="https://retractionwatch.com/retraction-watch-database-user-guide/" target="_blank" rel="noopener">Verify</a>',
      " · ",
      '<a href="https://retractionwatch.com/contact/" target="_blank" rel="noopener">Report false positive</a>'
    ].join("<br>");

    let badge;
    if (doi) {
      badge = document.createElement("a");
      badge.href = "https://doi.org/" + encodeURIComponent(doi.trim());
      badge.target = "_blank";
      badge.rel = "noopener";
      badge.className = "su-retraction-badge su-retraction-badge-link";
      badge.setAttribute("aria-label", "Open DOI page for this paper (Crossref)");
      badge.textContent = "Update/retraction in Crossref";
      badge.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        window.open(badge.href, "_blank", "noopener");
      });
    } else {
      badge = document.createElement("span");
      badge.className = "su-retraction-badge";
      badge.setAttribute("aria-label", "Has retraction or update in Crossref");
      badge.textContent = "Update/retraction in Crossref";
    }
    badge.appendChild(tooltip);

    badge.addEventListener("mouseenter", () => { tooltip.classList.add("su-retraction-tooltip-visible"); });
    badge.addEventListener("mouseleave", () => { tooltip.classList.remove("su-retraction-tooltip-visible"); });

    const nodeToInsert = badge;

    if (gsRi) {
      gsRi.insertAdjacentElement("afterbegin", badge);
    } else {
      insertTarget.insertAdjacentElement("afterbegin", badge);
    }
  }

  // --- Quality index session cache helpers ---
  // Serializes Maps/Sets to plain arrays so they can be stored in chrome.storage.session.
  function serializeQualityIndex(qIndex) {
    return {
      ft50:      [...qIndex.ft50],
      utd24:     [...qIndex.utd24],
      era:       [...qIndex.era],
      abdc:      [...qIndex.abdc.entries()],
      vhb:       [...qIndex.vhb.entries()],
      quartiles: [...qIndex.quartiles.entries()],
      core:      [...qIndex.core.entries()],
      ccf:       [...qIndex.ccf.entries()],
      jcr:       [...qIndex.jcr.entries()],
      impact:    [...qIndex.impact.entries()],
      norwegian: [...qIndex.norwegian.entries()],
      abs:       [...qIndex.abs.entries()],
      h5:        [...qIndex.h5.entries()]
    };
  }

  function deserializeQualityIndex(s) {
    return {
      ft50:      new Set(s.ft50      || []),
      utd24:     new Set(s.utd24     || []),
      era:       new Set(s.era       || []),
      abdc:      new Map(s.abdc      || []),
      vhb:       new Map(s.vhb       || []),
      quartiles: new Map(s.quartiles || []),
      core:      new Map(s.core      || []),
      ccf:       new Map(s.ccf       || []),
      jcr:       new Map(s.jcr       || []),
      impact:    new Map(s.impact    || []),
      norwegian: new Map(s.norwegian || []),
      abs:       new Map(s.abs       || []),
      h5:        new Map(s.h5        || [])
    };
  }
  // --- End quality index session cache helpers ---

  async function refreshState(state) {
    // Batch all storage reads into a single call
    const storageData = await batchGetStorage({
      savedPapers: {},
      settings: DEFAULT_SETTINGS,
      qualityQuartilesIndex: {},
      qualityQuartilesMeta: null,
      qualityJcrIndex: {},
      qualityJcrMeta: null,
      hiddenPapers: [],
      hiddenVenues: [],
      hiddenAuthors: []
    });
    
    const saved = storageData.savedPapers || {};
    const settings = { ...DEFAULT_SETTINGS, ...(storageData.settings || {}) };
    const quartiles = {
      index: storageData.qualityQuartilesIndex || {},
      meta: storageData.qualityQuartilesMeta || null
    };
    const jcr = {
      index: storageData.qualityJcrIndex || {},
      meta: storageData.qualityJcrMeta || null
    };
    const hiddenPapers = Array.isArray(storageData.hiddenPapers) ? storageData.hiddenPapers : [];
    const hiddenVenues = Array.isArray(storageData.hiddenVenues) ? storageData.hiddenVenues : [];
    const hiddenAuthors = Array.isArray(storageData.hiddenAuthors) ? storageData.hiddenAuthors : [];

    state.saved = saved;
    state.settings = settings;
    state.quartilesIndex = quartiles.index;
    state.quartilesMeta = quartiles.meta;
    state.jcrIndex = jcr.index;
    state.jcrMeta = jcr.meta;

    // Hash uses only in-memory data so cache can be checked before any file I/O.
    // VHB/Impact index sizes are omitted; settings changes (rank filters) act as
    // a sufficient proxy for detecting when those files' effective contents change.
    const QUALITY_INDEX_VERSION = 2;
    const settingsHash = JSON.stringify({
      qualityFt50List: settings.qualityFt50List || "",
      qualityUtd24List: settings.qualityUtd24List || "",
      qualityAbdcRanks: settings.qualityAbdcRanks || "",
      qualityVhbRanks: settings.qualityVhbRanks || "",
      qualityQuartiles: settings.qualityQuartiles || "",
      qualityCoreRanks: settings.qualityCoreRanks || "",
      qualityCcfRanks: settings.qualityCcfRanks || "",
      quartilesIndexKeys: Object.keys(quartiles.index || {}).length,
      jcrIndexKeys: Object.keys(jcr.index || {}).length,
      qualityIndexVersion: QUALITY_INDEX_VERSION
    });
    
    // Only recompile the quality index when settings changed.
    // L1: in-memory cache (survives re-renders within a page).
    // L2: chrome.storage.session (survives page navigations within a browser session).
    const SESSION_QINDEX_KEY = "suQualityIndexCache";
    let qIndexResolved = false;

    if (state.qIndexCache && state.qIndexCache.settingsHash === settingsHash) {
      state.qIndex = state.qIndexCache.qIndex;
      qIndexResolved = true;
    }

    if (!qIndexResolved && chrome.storage?.session?.get) {
      try {
        const sessData = await chrome.storage.session.get({ [SESSION_QINDEX_KEY]: null });
        const sessCache = sessData[SESSION_QINDEX_KEY];
        if (sessCache && sessCache.hash === settingsHash && sessCache.compiled) {
          state.qIndex = deserializeQualityIndex(sessCache.compiled);
          state.qIndexCache = { qIndex: state.qIndex, settingsHash };
          qIndexResolved = true;
        }
      } catch (_) {}
    }

    if (!qIndexResolved) {
      // On cache miss, load all data sources in parallel.
      // (All four loaders cache at window level, so repeated calls within a session are instant.)
      const [eraNorwegian, h5Index, vhbIndex, impactIndex] = await Promise.all([
        loadEraAndNorwegian(), loadH5Index(), loadVhbIndex(), loadImpactIndex()
      ]);
      state.qIndex = compileQualityIndex(state.settings, {
        quartilesIndex: state.quartilesIndex,
        jcrIndex: state.jcrIndex,
        vhbIndex,
        impactIndex,
        eraSet: eraNorwegian.eraSet,
        absIndex: eraNorwegian.absIndex,
        norwegianMap: eraNorwegian.norwegianMap,
        h5Index
      });
      state.qIndexCache = { qIndex: state.qIndex, settingsHash };
      // Persist to session storage for future page loads in this session
      if (chrome.storage?.session?.set) {
        try {
          chrome.storage.session.set({
            [SESSION_QINDEX_KEY]: { hash: settingsHash, compiled: serializeQualityIndex(state.qIndex) }
          });
        } catch (_) {}
      }
    }
    state.hiddenPapers = new Set(hiddenPapers);
    state.hiddenVenues = new Set(hiddenVenues);
    state.hiddenAuthors = new Set(hiddenAuthors);
    if (settings.showRetractionWatch) {
      state.retractionBloom = await loadRetractionBloom();
    } else {
      state.retractionBloom = null;
    }
    applyTheme(settings.theme);
    applyBadgePalette(settings.badgePalette);
    const themeBtn = document.getElementById("su-theme-toggle");
    if (themeBtn) themeBtn.textContent = (settings.theme === "dark" ? "Dark" : settings.theme === "light" ? "Light" : "Auto");
    state.keywordHighlights = csvToTags(settings.keywordHighlightsCsv);
    state.keywordHighlightRegexes = (state.keywordHighlights || []).map((kw) => {
      const escaped = String(kw || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return new RegExp(`(${escaped})`, "gi");
    });
    state.keywordHighlightKey = `${settings.keywordHighlightsCsv || ""}::${settings.showSnippetCueEmphasis ? "1" : "0"}`;
    state.renderEpoch = (state.renderEpoch || 0) + 1;
    state.venueQualityCache = new Map();
  }

  function applyTheme(theme) {
    const resolved = theme === "dark" ? "dark" : theme === "light" ? "light" : (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    if (document.body) document.body.setAttribute("data-su-theme", resolved);
    if (document.documentElement) document.documentElement.setAttribute("data-su-theme", resolved);
  }

  function applyBadgePalette(palette) {
    const value = ["soft", "bold", "colorblind", "mono", "material", "fluent", "nord", "solarized", "monoAccent", "highContrast", "inverted", "warm"].includes(palette)
      ? palette
      : "soft";
    if (document.body) document.body.setAttribute("data-su-badge-palette", value);
    if (document.documentElement) document.documentElement.setAttribute("data-su-badge-palette", value);
  }

  async function ensureResultUI(container, state, isAuthorProfile = false, opts = {}) {
    const inViewport = opts.inViewport !== false;
    const renderEpoch = String(state.renderEpoch || 0);
    const isDirty = container.dataset.suDirty === "1";
    let fastPaper = isAuthorProfile
      ? getCachedAuthorPaper(container)
      : getCachedPaperFast(container);

    const fastSig = `${fastPaper.key}::${(fastPaper.snippet || "").slice(0, 120)}`;

    // Skip heavy work if we've already rendered this row for the current settings epoch.
    if (!isDirty && container.dataset.suFastKey === fastSig && container.dataset.suRenderEpoch === renderEpoch) {
      return;
    }

    let paper = fastPaper;
    if (!isAuthorProfile) {
      const needsDeepScan = !paper.authorsVenue || paper.authorsVenue.includes("…") || paper.authorsVenue.length < 20;
      if (needsDeepScan) {
        paper = getCachedPaperFull(container);
      }
    }
    
    // For multi-version papers, use the best venue from cache if available
    if (!isAuthorProfile && state.venueCache && paper.clusterId) {
      const cachedVenue = state.venueCache.get(paper.clusterId);
      if (cachedVenue && cachedVenue.length > (paper.venue?.length || 0)) {
        // Use the cached venue if it's better
        paper = { ...paper, venue: cachedVenue };
      }
    }

    // Hide/unhide based on current settings.
    if (shouldHide(paper, state.settings, state.qIndex, state)) container.classList.add("su-hidden");
    else container.classList.remove("su-hidden");

    // Skip heavy re-render if this row's content hasn't changed (reduces latency on DOM churn).
    if (!isDirty && container.dataset.suFastKey === fastSig && container.dataset.suRenderEpoch === renderEpoch) return;
    container.dataset.suProcessed = paper.key;
    container.dataset.suFastKey = fastSig;
    container.dataset.suRenderEpoch = renderEpoch;
    delete container.dataset.suDirty;

    container.classList.remove("su-new-since-visit");
    const newBadge = getCachedElement(container, ".su-new-badge");
    if (newBadge) newBadge.remove();

    highlightKeywords(container, state, isAuthorProfile);
    if (!isAuthorProfile) {
      const fma = container.querySelector(".gs_fma");
      if (fma) container.classList.add("su-has-fma");
      else container.classList.remove("su-has-fma");
      ensureAbstractVisible(container);
    }

    const isSaved = !!state.saved[paper.key];
    if (isSaved && state.settings.highlightSaved) container.classList.add("su-saved");
    else container.classList.remove("su-saved");

    const isRetracted = renderQuality(container, paper, state, isAuthorProfile);
    const isDetailed = state.settings.viewMode !== "minimal";
    if (isRetracted) {
      container
        .querySelectorAll(".su-velocity, .su-skimmability-strip, .su-artifact-badge, .su-reading-load-badge, .su-external, .su-citation-spike-badge, .su-css-badge, .su-code-badge")
        .forEach((el) => el.remove());
    } else if (isDetailed) {
      renderExternalSignalBadges(container, state, isAuthorProfile, { inViewport });
      // renderSkimmabilityStrip(container, paper, state, isAuthorProfile);
      renderArtifactBadges(container, state);
      renderCodeLinkBadge(container, paper);
      renderVelocity(container, paper, isAuthorProfile);
    } else {
      container.querySelectorAll(".su-velocity, .su-skimmability-strip, .su-artifact-badge, .su-reading-load-badge, .su-external").forEach((el) => el.remove());
    }
    renderAuthorshipHeatmap(container, state, isAuthorProfile);

    // Citation spike and age-bias heatmap are cosmetic and non-blocking. When the
    // result is off-screen, defer them to idle time so they don't compete with the
    // critical render path (badges, buttons, keyword highlights).
    const applyCitationAndAge = () => {
      if (!container.isConnected) return;
      if (state.settings.showCitationSpike) {
        const citations = isAuthorProfile ? getCachedAuthorCitationCount(container) : getCitationCountFromResult(container);
        renderCitationSpikeBadge(container, paper, state, citations, isAuthorProfile);
      }
      const currentYear = new Date().getFullYear();
      const pubYear = paper.year != null ? parseYear(String(paper.year)) : null;
      const yearsAgo = pubYear != null ? currentYear - pubYear : null;
      if (state.settings.showAgeBiasHeatmap) {
        container.classList.add("su-age-bias");
        container.style.setProperty("--su-age-color", ageBiasColor(yearsAgo));
      } else {
        container.classList.remove("su-age-bias");
        container.style.removeProperty("--su-age-color");
      }
    };

    if (inViewport) {
      applyCitationAndAge();
    } else if (typeof requestIdleCallback === "function") {
      requestIdleCallback(applyCitationAndAge, { timeout: 3000 });
    } else {
      setTimeout(applyCitationAndAge, 500);
    }

    if (!isRetracted && !isAuthorProfile && state.settings.showRetractionWatch && !state.retractionBloom) {
      const doi = extractDOIFromResult(container);
      if (doi && inViewport && container.dataset.suRetractChecked !== "1") {
        container.dataset.suRetractChecked = "1";
        const runRetractionCheck = () => {
          checkRetractionStatus(doi).then((isRetracted) => {
            if (isRetracted && container.isConnected) renderRetractionBadge(container, state);
          });
        };
        if (typeof requestIdleCallback === "function") {
          requestIdleCallback(runRetractionCheck, { timeout: 2000 });
        } else {
          runRetractionCheck();
        }
      }
    }

    // Inline citation tooltip (APA/MLA/BibTeX) on hover.
    const titleLink = isAuthorProfile
      ? getCachedElement(container, ".gsc_a_at")
      : getCachedElement(container, ".gs_rt a");
    const fl = isAuthorProfile
      ? getCachedElement(container, ".gsc_a_c")
      : getCachedElement(container, ".gs_fl");
    let citeTarget = null;
    if (!isAuthorProfile && fl) {
      const flLinks = Array.from(fl.querySelectorAll("a"));
      citeTarget =
        flLinks.find((a) => /^Cite$/i.test((text(a) || "").trim())) ||
        flLinks.find((a) => /\bcite\b/i.test((text(a) || "").trim())) ||
        null;
    }
    if (!citeTarget) citeTarget = titleLink;
    if (citeTarget) attachInlineCitationTooltip(citeTarget, paper);

    // Buttons (create once, update state each pass).
    if (!fl) return;

    let bar = getCachedElement(container, ".su-btnbar");
    if (!bar) {
      bar = document.createElement("span");
      bar.className = "su-btnbar";

      const removeBtn = createButton("Remove", { danger: true, act: "remove" });
      removeBtn.title = "Remove this paper from your saved list. Only visible if you have saved papers.";
      if (!isAuthorProfile) {
        const hideBtn = createButton("Hide", { act: "hide" });
        hideBtn.title = "Hide this result: choose to hide this paper only, the whole venue/journal, or this author. Manage hidden items in the extension options.";
        bar.append(hideBtn);
      }
      if (!isAuthorProfile) {
        const queueBtn = document.createElement("button");
        queueBtn.type = "button";
        queueBtn.className = "su-btn su-queue-btn";
        queueBtn.dataset.act = "addqueue";
        queueBtn.textContent = "+";
        queueBtn.title = "Add this paper to your reading queue. Open the queue from the extension icon or the list that appears when the queue has items.";
        queueBtn.setAttribute("aria-label", "Add to reading queue");
        bar.append(queueBtn);
        const pdfBtn = createButton("PDF", { act: "openpdf" });
        const pdfTip = "Open the best available PDF: tries publisher link first, then arXiv/SSRN if present, then a search. If you open the PDF, the extension can store its page count to improve the reading-load estimate.";
        pdfBtn.title = pdfTip;
        attachFloatingTooltip(pdfBtn, pdfTip);
        bar.append(pdfBtn);
        const copyBibBtn = createButton("Copy Bib", { act: "copybib" });
        copyBibBtn.title = "Copy a BibTeX entry for this paper to the clipboard for use in LaTeX or reference managers.";
        bar.append(copyBibBtn);
        const mdBtn = createButton("M", { act: "copymd" });
        const mdTip = "Copy to clipboard as Markdown: title (linked), authors, year, abstract, and #tags. Paste into Obsidian or Notion for a formatted note.";
        mdBtn.title = mdTip;
        mdBtn.classList.add("su-btn-md");
        attachFloatingTooltip(mdBtn, mdTip);
        bar.append(mdBtn);
      }
      bar.append(removeBtn);
      fl.appendChild(bar);

      bar.addEventListener("click", async (e) => {
        const btn = e.target?.closest?.("button[data-act]");
        if (!btn) return;

        const act = btn.dataset.act;
        if (act === "remove") {
          const p = isAuthorProfile
            ? getCachedAuthorPaper(container)
            : extractPaperFromResult(container);
          await removePaper(p.key);
          await refreshState(state);
          await ensureResultUI(container, state, isAuthorProfile);
        } else if (act === "addqueue") {
          const p = extractPaperFromResult(container);
          const searchQuery = getCurrentSearchQuery();
          await addToReadingQueue({ title: p.title, link: p.url || "#", searchQuery });
          await ensureReadingQueueSidebar();
          const prevText = btn.textContent;
          btn.textContent = "✓";
          btn.disabled = true;
          setTimeout(() => { btn.textContent = prevText; btn.disabled = false; }, 1200);
        } else if (act === "openpdf") {
          const p = isAuthorProfile ? getCachedAuthorPaper(container) : extractPaperFromResult(container);
          const best = getBestPdfUrl(container);
          if (best && isHttpUrl(best.url)) {
            window.open(best.url, "_blank", "noopener");
            chrome.runtime.sendMessage(
              { action: "fetchPdfPageCount", url: best.url, paperKey: p.key },
              (res) => {
                if (res?.ok && typeof res.pages === "number" && res.pages >= 1) {
                  setReadingLoadPageCount(p.key, res.pages).then(() => {
                    refreshState(state).then(() => {
                      if (window.suProcessAll) window.suProcessAll();
                    });
                  });
                }
              }
            );
          } else {
            const q = encodeURIComponent((p.title || "paper") + " pdf");
            window.open(`https://www.google.com/search?q=${q}`, "_blank", "noopener");
          }
        } else if (act === "copybib") {
          const p = isAuthorProfile
            ? getCachedAuthorPaper(container)
            : extractPaperFromResult(container);
          const label = btn.textContent;
          try {
            btn.textContent = "…";
            btn.disabled = true;
            const bib = await getBibTeXForPaper(p, container);
            await navigator.clipboard.writeText(bib);
            btn.textContent = "Copied!";
          } catch {
            btn.textContent = "Failed";
          }
          setTimeout(() => {
            btn.textContent = label;
            btn.disabled = false;
          }, 1500);
        } else if (act === "copymd") {
          const p = isAuthorProfile
            ? getCachedAuthorPaper(container)
            : extractPaperFromResult(container);
          const snippetText = getSnippetText(container);
          const authorsPart = (p.authorsVenue || "").split(/\s*[-–—]\s*/)[0]?.trim() || "";
          const year = p.year != null ? String(p.year).replace(/\D/g, "").slice(0, 4) : "";
          const link = (p.url || "").trim() || "#";
          const title = (p.title || "Untitled").trim();
          const abstract = (snippetText || "").trim() || "";
          const lines = [
            `**Title:** [${title}](${link})`,
            `**Authors:** ${authorsPart || "—"}`,
            `**Year:** ${year || "—"}`,
            abstract ? `**Abstract:** ${abstract}` : "",
            "#tags: [[Topic]] [[Scholar]]"
          ].filter(Boolean);
          const markdown = lines.join("\n");
          const prevText = btn.textContent;
          try {
            await navigator.clipboard.writeText(markdown);
            btn.textContent = "✓";
          } catch {
            btn.textContent = "Failed";
          }
          setTimeout(() => { btn.textContent = prevText; }, 1500);
        } else if (act === "hide") {
          const p = isAuthorProfile
            ? getCachedAuthorPaper(container)
            : extractPaperFromResult(container);
          const menu = document.createElement("div");
          menu.className = "su-hide-menu";
          menu.innerHTML = [
            '<button type="button" data-hide="paper">Hide this paper</button>',
            '<button type="button" data-hide="venue">Hide this venue</button>',
            '<button type="button" data-hide="author">Hide this author</button>'
          ].join("");
          const close = () => { menu.remove(); document.removeEventListener("click", close); };
          document.body.appendChild(menu);
          const rect = btn.getBoundingClientRect();
          menu.style.left = rect.left + "px";
          menu.style.top = rect.bottom + 4 + "px";
          requestAnimationFrame(() => document.addEventListener("click", close));
          menu.addEventListener("click", async (e) => {
            const which = e.target?.dataset?.hide;
            if (!which) return;
            e.stopPropagation();
            if (which === "paper") {
              await addHiddenPaper(p.key);
            } else if (which === "venue") {
              const vn = normalizeVenueName(p.venue);
              if (vn) await addHiddenVenue(vn);
            } else if (which === "author") {
              const authorsPart = (p.authorsVenue || "").split(/\s*[-–—]\s*/)[0] || "";
              const first = authorsPart.split(/\s*,\s*|\s+and\s+/i).map((a) => a.trim()).filter(Boolean)[0];
              if (first) await addHiddenAuthor(normalizeAuthorForHide(first));
            }
            close();
            await refreshState(state);
            if (window.suProcessAll) await window.suProcessAll();
          });
        }
      });
    }

    const removeBtn = bar.querySelector('button[data-act="remove"]');
    if (removeBtn) removeBtn.style.display = isSaved ? "" : "none";
  }

  function scanResults() {
    // Check if we're on an author profile page
    const isAuthorProfile =
      !!document.querySelector("#gsc_prf_in, #gsc_prf") ||
      /^\/citations\b/.test(window.location.pathname) ||
      (document.querySelector(".gsc_a_tr") !== null && document.querySelector("#gsc_prf_in") !== null);
    
    if (isAuthorProfile) {
      return {
        results: Array.from(document.querySelectorAll(".gsc_a_tr")),
        isAuthorProfile: true
      };
    } else {
      // For search results, get all .gs_r elements
      // This includes both main results and version results (gs_r gs_or gs_scl)
      const allResults = Array.from(document.querySelectorAll(".gs_r"));
      return {
        results: allResults,
        isAuthorProfile: false
      };
    }
  }

  function extractAuthorName() {
    // Try to find the author name from the profile page
    const nameEl = document.querySelector("#gsc_prf_in");
    if (nameEl) {
      // Get text content, removing any edit buttons or extra elements
      const nameText = nameEl.cloneNode(true);
      // Remove edit button if present
      const editBtn = nameText.querySelector("a[aria-label='Edit profile'], .gsc_prf_btn");
      if (editBtn) editBtn.remove();
      return nameText.textContent.trim();
    }
    return null;
  }

  function isScholarHostname(hostname) {
    const h = String(hostname || "").toLowerCase();
    return h === "scholar.google.com" || h.startsWith("scholar.google.");
  }

  function generateAuthorNameVariations(fullName) {
    // Generate variations like "BM Ampel", "B Ampel", "Benjamin M. Ampel", etc.
    const baseName = stripNameCredentials(fullName);
    const baseNoSuffix = stripTrailingSuffixTokens(baseName);
    const variations = new Set([fullName, baseName]);
    if (baseNoSuffix && baseNoSuffix !== baseName) variations.add(baseNoSuffix);
    
    // Split name into parts
    const parts = baseName.split(/\s+/).filter(p => p.length > 0);
    if (parts.length >= 2) {
      const lastName = parts[parts.length - 1];
      const firstNames = parts.slice(0, -1);
      
      // Full name
      variations.add(fullName);
      
      // Last name only
      variations.add(lastName);
      
      // First initial + Last name
      if (firstNames.length > 0) {
        const firstInitial = firstNames[0][0];
        variations.add(`${firstInitial} ${lastName}`);
        variations.add(`${firstInitial}${lastName}`);
      }
      
      // All initials + Last name
      if (firstNames.length > 0) {
        const initials = firstNames.map(n => n[0]).join("");
        variations.add(`${initials} ${lastName}`);
        variations.add(`${initials}${lastName}`);
      }
      
      // First name + Last name (if middle names present)
      if (firstNames.length >= 1) {
        variations.add(`${firstNames[0]} ${lastName}`);
      }
      
      // Common abbreviations
      if (firstNames.some(n => n.toLowerCase() === "benjamin")) {
        variations.add(`B ${lastName}`);
        variations.add(`B${lastName}`);
        variations.add(`BM ${lastName}`);
        variations.add(`BM${lastName}`);
      }
    }
    
    return Array.from(variations).filter(v => v.length > 0);
  }

  function highlightAuthorName(container, authorVariations) {
    const authorDivs = container.querySelectorAll("div.gs_gray");
    for (const authorDiv of authorDivs) {
      if (authorDiv.dataset.suAuthorHighlighted === "true") continue;

      const authorSpans = authorDiv.querySelectorAll(".su-author");
      if (authorSpans.length > 0) {
        // Authorship heatmap already wrapped names in .su-author spans; add highlight to the profile owner
        for (const span of authorSpans) {
          const name = (span.textContent || "").trim();
          if (isAuthorVariation(name, authorVariations)) {
            span.classList.add("su-author-highlight");
          }
        }
        authorDiv.dataset.suAuthorHighlighted = "true";
        continue;
      }

      const authorText = authorDiv.textContent || "";
      let foundVariation = null;
      for (const variation of authorVariations) {
        const escaped = variation.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const pattern = new RegExp(`(^|,\\s*)(${escaped})(\\s*,|$)`, "i");
        if (pattern.test(authorText)) {
          foundVariation = variation;
          break;
        }
      }

      if (foundVariation) {
        authorDiv.dataset.suAuthorHighlighted = "true";
        if (authorDiv.__suOrigAuthorHTML === undefined) {
          authorDiv.__suOrigAuthorHTML = authorDiv.innerHTML;
        }
        authorDiv.innerHTML = authorDiv.__suOrigAuthorHTML;
        const escaped = foundVariation.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const pattern = new RegExp(`(^|,\\s*)(${escaped})(\\s*,|$)`, "gi");
        authorDiv.innerHTML = authorDiv.innerHTML.replace(
          pattern,
          (match, before, name, after) => `${before}<span class="su-author-highlight">${name}</span>${after}`
        );
      }
    }
  }

  async function loadAllPublications() {
    // Find the "Show more" button
    const showMoreBtn = document.getElementById("gsc_bpf_more");
    if (!showMoreBtn || showMoreBtn.disabled) {
      return false; // No more to load or button doesn't exist
    }
    
    // Click the button
    showMoreBtn.click();
    
    // Wait for new publications to load
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Check if button is still enabled (more to load)
    const stillMore = document.getElementById("gsc_bpf_more") && !document.getElementById("gsc_bpf_more").disabled;
    return stillMore;
  }

  async function loadAllPublicationsRecursively(opts = {}) {
    const cfg = typeof opts === "number" ? { maxAttempts: opts } : (opts || {});
    const maxAttempts = Number.isFinite(cfg.maxAttempts) ? cfg.maxAttempts : 50;
    const maxPubsRaw = cfg.maxPubs == null ? Infinity : Number(cfg.maxPubs);
    const maxPubs = Number.isFinite(maxPubsRaw) && maxPubsRaw > 0 ? maxPubsRaw : Infinity;
    // Load publications by clicking "Show more" until it's disabled or we reach maxPubs
    for (let i = 0; i < maxAttempts; i++) {
      const currentCount = scanResults().results.length;
      if (currentCount >= maxPubs) {
        return { status: "partial", reason: "maxPubs", count: currentCount };
      }
      const hasMore = await loadAllPublications();
      const nextCount = scanResults().results.length;
      if (!hasMore) {
        return { status: "complete", count: nextCount };
      }
      // Small delay between clicks to avoid overwhelming the page
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    return { status: "partial", reason: "maxAttempts", count: scanResults().results.length };
  }

  function extractCitationCount(gscTr) {
    // Citation count is in .gsc_a_c > a.gsc_a_ac
    const citedByCell = gscTr.querySelector(".gsc_a_c");
    const citedByLink = citedByCell?.querySelector("a.gsc_a_ac");
    if (citedByLink) {
      const citationText = text(citedByLink).trim();
      // Parse number from text like "130" or "1,234"
      const num = parseInt(citationText.replace(/,/g, ""), 10);
      return isNaN(num) ? 0 : num;
    }
    return 0;
  }

  function parseAuthors(authorsText) {
    // Parse author list like "B Ampel, S Samtani, H Chen" or "BM Ampel, S Samtani, H Zhu, H Chen, JF Nunamaker Jr"
    if (!authorsText) return [];
    
    // Split by comma and clean up
    return authorsText
      .split(",")
      .map(a => stripNameCredentials(a.trim()))
      .filter(a => a.length > 0 && !isCredentialOnly(a))
      .filter(a => !isEllipsisAuthor(a));
  }

  function isEllipsisAuthor(name) {
    const cleaned = String(name || "").replace(/\s+/g, "").replace(/\.+/g, ".").trim();
    if (!cleaned) return true;
    if (cleaned === "." || cleaned === ".." || cleaned === "...") return true;
    if (cleaned === "…" || cleaned === "…." || cleaned === "…") return true;
    return false;
  }

  const SU_AUTHOR_SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "v"]);
  const SU_AUTHOR_PREFIXES = new Set([
    "dr", "prof", "professor", "mr", "mrs", "ms", "miss", "sir", "dame"
  ]);
  const SU_AUTHOR_CREDENTIALS = new Set([
    "phd", "dphil", "md", "mba", "jd", "esq", "dds", "dmd", "dvm", "pharmd", "do", "dnp", "dpt", "od",
    "ms", "ma", "msc", "mcs", "mse", "meng", "m.eng", "mph", "mpa", "mpp", "msw",
    "bs", "ba", "bsc", "beng", "b.eng",
    "cpa", "cfa", "cissp", "cism", "cisa", "csp", "pe", "peng", "p.eng",
    "rn", "lcsw", "lmft", "lpc", "np", "pa",
    "facp", "facc", "facs", "frcpc", "frcs", "frs"
  ]);

  function normalizeNameToken(token) {
    return String(token || "")
      .replace(/[^a-z0-9]/gi, "")
      .toLowerCase();
  }

  function isCredentialToken(token) {
    const t = normalizeNameToken(token);
    if (!t) return false;
    if (SU_AUTHOR_CREDENTIALS.has(t)) return true;
    // Handle "P.Eng" / "P.E." and similar dotted abbreviations.
    if (t === "peng" && SU_AUTHOR_CREDENTIALS.has("p.eng")) return true;
    return false;
  }

  function isCredentialOnly(name) {
    const tokens = String(name || "").split(/\s+/).map(normalizeNameToken).filter(Boolean);
    return tokens.length > 0 && tokens.every((t) => SU_AUTHOR_CREDENTIALS.has(t));
  }

  function stripNameCredentials(name) {
    if (!name) return "";
    const base = stripLeadingTitles(String(name).replace(/\*+$/, "").trim());
    if (!base) return "";
    const parts = base.split(/\s*[,，‚‛﹐﹑;]+\s*/).map(p => p.trim()).filter(Boolean);
    const primary = parts[0] || "";
    const kept = [];
    for (let i = 1; i < parts.length; i++) {
      const segment = parts[i];
      const tokens = segment.split(/\s+/).filter(Boolean);
      const normalized = tokens.map(normalizeNameToken).filter(Boolean);
      if (normalized.length === 1 && SU_AUTHOR_SUFFIXES.has(normalized[0])) {
        kept.push(segment);
        continue;
      }
      if (tokens.length > 0 && tokens.every(isCredentialToken)) {
        continue;
      }
      kept.push(segment);
    }
    const joined = kept.length ? `${primary}, ${kept.join(", ")}` : primary;
    return stripTrailingCredentialsNoComma(joined);
  }

  function stripTrailingSuffixTokens(name) {
    const tokens = String(name || "").trim().split(/\s+/).filter(Boolean);
    while (tokens.length > 1) {
      const last = normalizeNameToken(tokens[tokens.length - 1].replace(/[.,]+$/g, ""));
      if (last && SU_AUTHOR_SUFFIXES.has(last)) {
        tokens.pop();
        continue;
      }
      break;
    }
    return tokens.join(" ");
  }

  function stripLeadingTitles(name) {
    const tokens = String(name || "").trim().split(/\s+/).filter(Boolean);
    if (tokens.length === 0) return "";
    while (tokens.length > 1) {
      const raw = tokens[0].replace(/^[^a-z0-9]+/i, "").replace(/[.,]+$/g, "");
      const norm = normalizeNameToken(raw);
      if (norm && SU_AUTHOR_PREFIXES.has(norm)) {
        tokens.shift();
        continue;
      }
      break;
    }
    return tokens.join(" ");
  }
  function stripTrailingCredentialsNoComma(name) {
    const tokens = String(name || "").trim().split(/\s+/).filter(Boolean);
    if (tokens.length <= 1) return String(name || "").trim();
    while (tokens.length > 1) {
      const last = tokens[tokens.length - 1].replace(/[.,]+$/, "");
      if (isCredentialToken(last)) {
        tokens.pop();
        continue;
      }
      break;
    }
    return tokens.join(" ");
  }

  function runAuthorNameTests() {
    const cases = [
      {
        input: "Amrou Awaysheh, PhD, MBA",
        stripped: "Amrou Awaysheh",
        last: "awaysheh"
      },
      {
        input: "Paul Bliese, Ph.D., MBA, CISSP",
        stripped: "Paul Bliese",
        last: "bliese"
      },
      {
        input: "Paul Bliese, Ph.D.",
        stripped: "Paul Bliese",
        last: "bliese"
      },
      {
        input: "Dr. Paul T. Bartone",
        stripped: "Paul T. Bartone",
        last: "bartone"
      },
      {
        input: "J F Nunamaker, Jr.",
        stripped: "J F Nunamaker, Jr.",
        last: "nunamaker"
      },
      {
        input: "Jane Q Public, M.D., PhD, FACC",
        stripped: "Jane Q Public",
        last: "public"
      },
      {
        input: "John Doe, Jr., PhD",
        stripped: "John Doe, Jr.",
        last: "doe"
      },
      {
        input: "Mary Ann Smith Jr., MBA",
        stripped: "Mary Ann Smith Jr.",
        last: "smith"
      },
      {
        input: "Carlos M. Ruiz, II, MD, MPH",
        stripped: "Carlos M. Ruiz, II",
        last: "ruiz"
      },
      {
        input: "A. B. Chen, MEng, P.Eng",
        stripped: "A. B. Chen",
        last: "chen"
      }
    ];

    const failures = [];
    cases.forEach((t) => {
      const stripped = stripNameCredentials(t.input);
      const last = extractLastName(t.input);
      if (stripped !== t.stripped) {
        failures.push(`stripNameCredentials("${t.input}") => "${stripped}" (expected "${t.stripped}")`);
      }
      if (t.last && last !== t.last) {
        failures.push(`extractLastName("${t.input}") => "${last}" (expected "${t.last}")`);
      }
    });

    if (failures.length) {
      console.warn("[ScholarUtilityBelt] Author name tests failed:", failures);
    } else {
      console.log("[ScholarUtilityBelt] Author name tests passed.");
    }
  }

  // Optional dev harness: run in console with `window.__SU_RUN_NAME_TESTS = true; location.reload();`
  if (window.__SU_RUN_NAME_TESTS === true) {
    try {
      runAuthorNameTests();
    } catch (err) {
      console.warn("[ScholarUtilityBelt] Author name tests crashed:", err);
    }
  }

  function normalizeAuthorName(name) {
    // Normalize author name for comparison (handle variations like "B Ampel" vs "BM Ampel" vs "Benjamin M. Ampel")
    // Also handle cases like "CH Yang" vs "Chi-Heng Yang"
    // Strip "*" markers used by Google Scholar to mark corresponding authors
    const cleaned = stripNameCredentials(name);
    const noSuffix = stripTrailingSuffixTokens(cleaned);
    return noSuffix.replace(/\*+$/, "").toLowerCase().trim();
  }

  function paperHasCoauthor(paper, normalizedCoauthor) {
    if (!normalizedCoauthor) return true;
    const authorsRaw = String(paper?.authorsVenue || "");
    const authorsPart = authorsRaw.split(" - ")[0] || authorsRaw;
    const authors = parseAuthors(authorsPart);
    return authors.some((a) => normalizeAuthorName(a) === normalizedCoauthor);
  }

  function extractLastName(name) {
    // Extract last name (last word) from a name
    // Strip "*" markers that might be attached to the last name
    const cleaned = stripTrailingSuffixTokens(stripNameCredentials(name)).replace(/\*+$/, "").trim();
    const parts = cleaned.split(/\s+/).filter(Boolean);
    return parts.length > 0 ? parts[parts.length - 1].toLowerCase() : "";
  }

  function extractInitials(name) {
    // Extract initials from a name (e.g., "CH Yang" -> "ch", "Chi-Heng Yang" -> "ch")
    const cleaned = stripTrailingSuffixTokens(stripNameCredentials(name)).trim();
    const parts = cleaned.split(/\s+/);
    if (parts.length === 0) return "";
    
    // Get all parts except the last (which is usually the last name)
    const nameParts = parts.slice(0, -1);
    return nameParts.map(p => p[0]?.toLowerCase() || "").join("");
  }

  function isAuthorVariation(name, authorVariations) {
    if (!authorVariations || authorVariations.length === 0) return false;
    
    const normalized = normalizeAuthorName(name);
    const nameLastName = extractLastName(name);
    const nameInitials = extractInitials(name);
    
    // Check exact match first
    if (authorVariations.some(v => normalizeAuthorName(v) === normalized)) {
      return true;
    }
    
    // Check if last names match and initials match
    for (const variation of authorVariations) {
      const varLastName = extractLastName(variation);
      const varInitials = extractInitials(variation);
      
      // If last names match
      if (nameLastName && varLastName && nameLastName === varLastName) {
        // Check if initials match (e.g., "CH Yang" matches "Chi-Heng Yang" if both have "CH" initials)
        if (nameInitials && varInitials && nameInitials === varInitials) {
          return true;
        }
        // Also check if one is just initials and the other starts with those initials
        // e.g., "CH Yang" matches "Chi-Heng Yang" (CH initials match Chi-Heng)
        if (nameInitials && varInitials) {
          // Check if initials match or if one set of initials is a prefix of the other
          if (nameInitials === varInitials || 
              (nameInitials.length <= varInitials.length && varInitials.startsWith(nameInitials)) ||
              (varInitials.length <= nameInitials.length && nameInitials.startsWith(varInitials))) {
            return true;
          }
        }
        // Check if the variation starts with the initials (e.g., "CH" matches "Chi-Heng")
        const varFirstPart = variation.split(/\s+/)[0]?.toLowerCase() || "";
        if (nameInitials && varFirstPart.startsWith(nameInitials)) {
          return true;
        }
        const nameFirstPart = name.split(/\s+/)[0]?.toLowerCase() || "";
        if (varInitials && nameFirstPart.startsWith(varInitials)) {
          return true;
        }
      }
    }
    
    return false;
  }

  function computeAuthorStats(results, state, authorPositionFilter) {
    const positionFilter = authorPositionFilter || "all";
    if (positionFilter !== "all") {
      results = results.filter((r) => {
        try {
          const paper = getCachedAuthorPaper(r);
          return paper && paperMatchesPositionFilter(paper, positionFilter, state.authorVariations);
        } catch {
          return false;
        }
      });
    }
    const stats = {
      totalPublications: 0,
      totalCitations: 0,
      avgCitations: 0,
      mostCited: 0,
      firstAuthorCitations: 0,
      soloCitations: 0,
      years: [],
      venues: new Map(), // venueKey -> { count, display }
      qualityCounts: { q1: 0, q2: 0, q3: 0, q4: 0, a: 0, vhb: 0, utd24: 0, ft50: 0, abs4star: 0, core: { "A*": 0, "A": 0, "B": 0, "C": 0 }, era: 0 },
      recentActivity: { last1Year: 0, last3Years: 0, last5Years: 0 },
      coAuthors: new Map(), // co-author name -> count of collaborations
      soloAuthored: 0,
      coAuthored: 0,
      uniqueCoAuthors: 0,
      firstAuthor: 0,
      lastAuthor: 0,
      middleAuthor: 0,
      authorshipDrift: null,
      topTitleTokens: [],
      __citations: [], // for h-index / m-index / g-index / Gini
      __authorCounts: [], // authors per paper for avg team size
      __yearCitations: [], // { year, citations } for citation half-life
      __lSum: 0, // for L-index: sum of c_i / (a_i * y_i)
      __eigenfactorSum: 0, // Eigenfactor-style: sum of citations * venue weight (1 + log(1+h5))
      __mssiImpact: 0 // MSSI: authorship-weighted citation sum (w = position weight / N_authors)
    };
    
    const currentYear = new Date().getFullYear();
    const papers = [];
    const authorVariations = state.authorVariations || [];
    const wantsCoauthorInsights = state?.authorFeatureToggles?.coauthors !== false;
    const topicTokenCounts = new Map();
    
    for (const result of results) {
      // Extract paper info
      const paper = getCachedAuthorPaper(result);
      if (!paper) continue;
      addTitleTokens(topicTokenCounts, paper.title);
      
      stats.totalPublications++;
      
      // Extract citation count
      const citations = getCachedAuthorCitationCount(result);
      stats.totalCitations += citations;
      if (citations > stats.mostCited) {
        stats.mostCited = citations;
      }
      
      // Parse authors
      const authors = parseAuthors(paper.authorsVenue.split(" - ")[0] || paper.authorsVenue);
      const coAuthorSet = new Set();
      const coAuthors = [];
      const authorIndex = authors.findIndex((a) => isAuthorVariation(a, authorVariations));
      const authorIndexValid = authorIndex >= 0 ? authorIndex : null;
      const positionDenom = Math.max(1, authors.length - 1);
      const authorRole = getAuthorRole(paper, authorVariations);
      for (let i = 0; i < authors.length; i++) {
        const a = authors[i];
        if (isAuthorVariation(a, authorVariations)) continue;
        const normalized = normalizeAuthorName(a);
        if (!normalized || coAuthorSet.has(normalized)) continue;
        coAuthorSet.add(normalized);
        coAuthors.push({ name: a, normalized, index: i });
      }
      
      // Check if author is first author (position 0) OR has "*" marker (corresponding author)
      const isFirstAuthor = authors.length > 0 && (
        isAuthorVariation(authors[0], authorVariations) ||
        authors.some(a => a.includes("*") && isAuthorVariation(a, authorVariations))
      );
      const isLastAuthor = authors.length > 1 && isAuthorVariation(authors[authors.length - 1], authorVariations);
      if (isFirstAuthor) {
        stats.firstAuthor++;
        stats.firstAuthorCitations += citations;
      }
      if (isLastAuthor) {
        stats.lastAuthor++;
      }
      
      // Track co-authors
      const isSolo = coAuthors.length === 0;
      if (isSolo) {
        // Solo authored (only the profile author)
        stats.soloAuthored++;
        stats.soloCitations += citations;
      } else {
        stats.coAuthored++;
        const citeVal = Number.isFinite(citations) ? citations : (parseInt(citations, 10) || 0);
        const paperYear = Number.isFinite(paper.year) ? paper.year : null;
        coAuthors.forEach(({ name, normalized, index }) => {
          const existing = stats.coAuthors.get(normalized);
          const hasPos = authorIndexValid != null && Number.isFinite(index);
          const positionDelta = hasPos ? (index - authorIndexValid) / positionDenom : null;
          const coRole = index === 0 ? "first" : (index === authors.length - 1 ? "last" : "middle");
          if (existing) {
            existing.count++;
            existing.citations = (existing.citations || 0) + citeVal;
            if (paperYear != null) {
              if (existing.lastYear == null || paperYear > existing.lastYear) existing.lastYear = paperYear;
              if (existing.firstYear == null || paperYear < existing.firstYear) existing.firstYear = paperYear;
            }
            if (!Array.isArray(existing.citeList)) existing.citeList = [];
            existing.citeList.push(citeVal);
            if (!existing.roleCounts) existing.roleCounts = { solo: 0, first: 0, middle: 0, last: 0 };
            if (existing.roleCounts[coRole] != null) existing.roleCounts[coRole] += 1;
            if (positionDelta != null) {
              existing.positionDeltaSum = (existing.positionDeltaSum || 0) + positionDelta;
              existing.positionDeltaCount = (existing.positionDeltaCount || 0) + 1;
            }
            // Keep the longest/most complete version of the name
            const currentWords = (existing.name || "").split(/\s+/).length;
            const newWords = name.split(/\s+/).length;
            if (newWords > currentWords || (newWords === currentWords && name.length > (existing.name || "").length)) {
              existing.name = name;
            }
            if (!existing.variations) existing.variations = [existing.name];
            if (!existing.variations.includes(name)) {
              existing.variations.push(name);
            }
          } else {
            stats.coAuthors.set(normalized, {
              key: normalized,
              name: name,
              count: 1,
              variations: [name],
              citations: citeVal,
              citeList: [citeVal],
              roleCounts: { solo: 0, first: coRole === "first" ? 1 : 0, middle: coRole === "middle" ? 1 : 0, last: coRole === "last" ? 1 : 0 },
              positionDeltaSum: positionDelta != null ? positionDelta : 0,
              positionDeltaCount: positionDelta != null ? 1 : 0,
              firstYear: paperYear,
              lastYear: paperYear
            });
          }
        });
      }

      if (wantsCoauthorInsights && coAuthors.length > 0) {
        if (!stats.__coauthorPaperSets) stats.__coauthorPaperSets = [];
        stats.__coauthorPaperSets.push(coAuthors.map((c) => c.normalized));
      }
      
      // Track years
      if (paper.year) {
        stats.years.push(paper.year);
        
        // Recent activity
        const yearsAgo = currentYear - paper.year;
        if (yearsAgo <= 1) stats.recentActivity.last1Year++;
        if (yearsAgo <= 3) stats.recentActivity.last3Years++;
        if (yearsAgo <= 5) stats.recentActivity.last5Years++;
      }

      // Track role by year for authorship drift
      if (paper.year) {
        const role = isSolo ? "solo" : (isFirstAuthor ? "first" : (isLastAuthor ? "last" : "middle"));
        if (!stats.__roleByYear) stats.__roleByYear = [];
        stats.__roleByYear.push({ year: paper.year, role });
      }
      
      // Track venues
      if (paper.venue) {
        const venueDisplay = normalizeProceedingsVenue(paper.venue);
        const venueKey = normalizeVenueKey(venueDisplay);
        if (!venueKey) {
          // Skip unusable/empty venue keys
          continue;
        }
        const existingVenue = stats.venues.get(venueKey);
        if (existingVenue) {
          existingVenue.count += 1;
          const nextDisplay = pickVenueDisplay(existingVenue.display, venueDisplay);
          if (nextDisplay !== existingVenue.display) existingVenue.display = nextDisplay;
        } else {
          stats.venues.set(venueKey, { count: 1, display: venueDisplay });
        }
        
        // Get badges for this venue
        const badges = qualityBadgesForVenue(paper.venue, state.qIndex);
        
        // Count quality badges
        for (const badge of badges) {
          if (badge.kind === "quartile") {
            const q = badge.text.toUpperCase();
            if (q === "Q1") stats.qualityCounts.q1++;
            else if (q === "Q2") stats.qualityCounts.q2++;
            else if (q === "Q3") stats.qualityCounts.q3++;
            else if (q === "Q4") stats.qualityCounts.q4++;
          } else if (badge.kind === "abdc") {
            const rank = badge.text.replace(/^ABDC\s+/i, "").trim().toUpperCase();
            if (rank === "A*" || rank === "A") {
              stats.qualityCounts.a++;
            }
          } else if (badge.kind === "vhb") {
            stats.qualityCounts.vhb++;
          } else if (badge.kind === "utd24") {
            stats.qualityCounts.utd24++;
          } else if (badge.kind === "ft50") {
            stats.qualityCounts.ft50++;
          } else if (badge.kind === "abs" && badge.text === "ABS 4*") {
            stats.qualityCounts.abs4star++;
          } else if (badge.kind === "core" && badge.metadata?.rank) {
            const r = String(badge.metadata.rank).toUpperCase().replace(/\s/g, "");
            if (stats.qualityCounts.core[r] !== undefined) stats.qualityCounts.core[r]++;
          } else if (badge.kind === "era") {
            stats.qualityCounts.era++;
          }
        }
      }
      
      papers.push({
        title: paper.title || "",
        year: paper.year != null ? Number(paper.year) : null,
        citations: Number.isFinite(citations) ? citations : (parseInt(citations, 10) || 0),
        authorsCount: Math.max(1, authors.length || 1),
        authorsText: (authors || []).join(", "),
        doi: paper.doi || "",
        url: paper.url || "",
        clusterId: paper.clusterId || ""
      });
      stats.__citations.push(citations);
      stats.__authorCounts.push(Math.max(1, authors.length));
      if (paper.year != null && citations != null && citations >= 0) {
        stats.__yearCitations.push({ year: paper.year, citations });
      }
      if (citations > stats.mostCited) {
        stats.mostCited = citations;
        stats.mostCitedPaper = {
          title: paper.title || "",
          citations,
          year: paper.year || null,
          url: paper.url || ""
        };
      }
      // L-index: L = ln(Σ c_i/(a_i*y_i)) + 1; a_i = authors, y_i = years since publication
      const a_i = Math.max(1, authors.length);
      const y_i = paper.year != null ? Math.max(1, currentYear - paper.year) : 1;
      stats.__lSum += (citations || 0) / (a_i * y_i);
      // Eigenfactor-style (West et al.): additive venue-weighted citation sum. Weight = 1 + log(1+h5).
      const badges = paper.venue ? qualityBadgesForVenue(paper.venue, state.qIndex) : [];
      const h5Badge = badges.find((b) => b.kind === "h5");
      const h5 = h5Badge?.metadata?.h5 ?? 0;
      const venueWeight = 1 + Math.log(1 + (h5 || 0));
      stats.__eigenfactorSum += (citations || 0) * venueWeight;
      // MSSI impact component: I = Σ w_{p,i} * C_p. w = position weight / N_authors (first=0.4, last=0.35, middle=0.25, solo=1)
      const N = Math.max(1, authors.length);
      const posWeight = isSolo ? 1 : (isFirstAuthor ? 0.4 : (isLastAuthor ? 0.35 : 0.25));
      stats.__mssiImpact += (citations || 0) * (posWeight / N);
    }

    if (topicTokenCounts.size) {
      const tokens = Array.from(topicTokenCounts.entries())
        .map(([token, meta]) => ({ token, display: meta.display || token, count: meta.count }))
        .sort((a, b) => (b.count - a.count) || a.token.localeCompare(b.token));
      stats.topTitleTokens = tokens.slice(0, 10);
    } else {
      stats.topTitleTokens = [];
    }

    // Middle-author count: not solo, not first, not last
    if (stats.totalPublications > 0) {
      stats.middleAuthor = Math.max(0, stats.totalPublications - stats.soloAuthored - stats.firstAuthor - stats.lastAuthor);
    }

    // Authorship drift: compare early vs late role distribution
    if (stats.__roleByYear && stats.__roleByYear.length >= 6) {
      try {
        const rows = stats.__roleByYear
          .map((r) => ({ year: parseYear(String(r.year)), role: r.role }))
          .filter((r) => Number.isFinite(r.year))
          .sort((a, b) => a.year - b.year);
        if (rows.length >= 6) {
          const midIdx = Math.floor(rows.length / 2);
          const early = rows.slice(0, midIdx);
          const late = rows.slice(midIdx);
          const pct = (arr, role) => {
            if (!arr.length) return 0;
            return arr.filter((r) => r.role === role).length / arr.length;
          };
          const eFirst = pct(early, "first");
          const eMiddle = pct(early, "middle");
          const eLast = pct(early, "last");
          const eSolo = pct(early, "solo");
          const lFirst = pct(late, "first");
          const lMiddle = pct(late, "middle");
          const lLast = pct(late, "last");
          const lSolo = pct(late, "solo");
          const eCollab = eMiddle + eLast;
          const lCollab = lMiddle + lLast;
          const pickDominant = (first, middle, last, solo, collab) => {
            const entries = [
              ["first", first],
              ["middle", middle],
              ["last", last],
              ["solo", solo],
              ["collaborator", collab]
            ].sort((a, b) => b[1] - a[1]);
            return { role: entries[0][0], pct: entries[0][1], next: entries[1][1] };
          };
          const earlyDom = pickDominant(eFirst, eMiddle, eLast, eSolo, eCollab);
          const lateDom = pickDominant(lFirst, lMiddle, lLast, lSolo, lCollab);
          const th = 0.08; // 8% shift to count as drift
          let label = "stable";
          if (earlyDom.role !== lateDom.role) {
            label = `${earlyDom.role} → ${lateDom.role}`;
          } else {
            // Same dominant but check meaningful trend by share shifts (≥8%)
            if (eFirst - lFirst >= th && lLast - eLast >= th) label = "first → last";
            else if (eFirst - lFirst >= th && lCollab - eCollab >= th) label = "first → collaborator";
            else if (eLast - lLast >= th && lFirst - eFirst >= th) label = "last → first";
            else if (eLast - lLast >= th && lCollab - eCollab >= th) label = "last → collaborator";
            else if (eCollab - lCollab >= th && lFirst - eFirst >= th) label = "collaborator → first";
            else if (eCollab - lCollab >= th && lLast - eLast >= th) label = "collaborator → last";
          }
          stats.authorshipDrift = {
            label,
            detail: `Early: ${Math.round(eFirst * 100)}% first, ${Math.round(eMiddle * 100)}% middle, ${Math.round(eLast * 100)}% last, ${Math.round(eSolo * 100)}% solo; Late: ${Math.round(lFirst * 100)}% first, ${Math.round(lMiddle * 100)}% middle, ${Math.round(lLast * 100)}% last, ${Math.round(lSolo * 100)}% solo`
          };
        }
      } catch {
        // Ignore drift errors
      }
      delete stats.__roleByYear;
    }
    
    // Calculate averages
    if (stats.totalPublications > 0) {
      stats.avgCitations = Math.round((stats.totalCitations / stats.totalPublications) * 10) / 10;
    }
    // FLP Index: N × (F%)^2, N = total publications, F% = first-author proportion
    if (stats.totalPublications > 0) {
      const fRatio = (stats.firstAuthor || 0) / stats.totalPublications;
      stats.flpIndex = Math.round((stats.totalPublications * fRatio * fRatio) * 100) / 100;
      stats.firstAuthorPct = Math.round(fRatio * 1000) / 1000;
    } else {
      stats.flpIndex = null;
      stats.firstAuthorPct = null;
    }
    
    // Get top venues (top 3)
    const topVenues = Array.from(stats.venues.entries())
      .sort((a, b) => (b[1]?.count || 0) - (a[1]?.count || 0))
      .slice(0, 3)
      .map(([venue, meta]) => ({ venue: meta?.display || venue, count: meta?.count || 0, key: venue }));
    
    stats.topVenues = topVenues;
    stats.venueDiversity = stats.venues.size;
    
    // Build full co-author stats list (sorted later in render)
    const coAuthorStats = Array.from(stats.coAuthors.values()).map(coAuthor => {
      let bestName = coAuthor.name;
      if (coAuthor.variations && coAuthor.variations.length > 1) {
        const sortedVariations = coAuthor.variations.sort((a, b) => {
          const aWords = a.split(/\s+/).length;
          const bWords = b.split(/\s+/).length;
          if (aWords !== bWords) return bWords - aWords;
          return b.length - a.length;
        });
        bestName = sortedVariations[0];
        const lastName = bestName.split(/\s+/).pop();
        const fullNameMatch = coAuthor.variations.find(v => {
          const parts = v.split(/\s+/);
          return parts.length >= 2 &&
                 parts[parts.length - 1] === lastName &&
                 parts[0].length > 1 &&
                 !parts[0].match(/^[A-Z]\.?$/);
        });
        if (fullNameMatch) {
          bestName = fullNameMatch;
        }
      }
      const citeList = Array.isArray(coAuthor.citeList) ? coAuthor.citeList : [];
      const hIndex = computeHIndexFromCitations(citeList);
      const rc = coAuthor.roleCounts || { solo: 0, first: 0, middle: 0, last: 0 };
      const totalRole = Math.max(1, (rc.solo || 0) + (rc.first || 0) + (rc.middle || 0) + (rc.last || 0));
      const roleShare = {
        solo: (rc.solo || 0) / totalRole,
        first: (rc.first || 0) / totalRole,
        middle: (rc.middle || 0) / totalRole,
        last: (rc.last || 0) / totalRole
      };
      return {
        key: coAuthor.key || normalizeAuthorName(coAuthor.name || ""),
        name: bestName,
        count: coAuthor.count,
        citations: coAuthor.citations || 0,
        hIndex,
        positionDeltaSum: coAuthor.positionDeltaSum ?? 0,
        positionDeltaCount: coAuthor.positionDeltaCount ?? 0,
        roleShare,
        roleCounts: rc,
        firstYear: coAuthor.firstYear ?? null,
        lastYear: coAuthor.lastYear ?? null
      };
    });

    stats.coAuthorStats = coAuthorStats;
    stats.uniqueCoAuthors = stats.coAuthors.size;
    
    // Get year range
    if (stats.years.length > 0) {
      stats.years.sort((a, b) => a - b);
      stats.firstYear = stats.years[0];
      stats.lastYear = stats.years[stats.years.length - 1];
      stats.yearSpan = stats.lastYear - stats.firstYear + 1;
    }
    // Yearly publication series for mini trend visual
    if (stats.years.length > 0) {
      const yearCounts = new Map();
      for (const y of stats.years) {
        yearCounts.set(y, (yearCounts.get(y) || 0) + 1);
      }
      const yearsSorted = Array.from(yearCounts.keys()).sort((a, b) => a - b);
      const lastYears = yearsSorted.slice(-10);
      stats.yearSeries = lastYears.map((y) => ({ year: y, count: yearCounts.get(y) || 0 }));
    } else {
      stats.yearSeries = [];
    }

    // h-index: largest h such that h papers have >= h citations each
    const sortedCitations = (stats.__citations || []).slice().sort((a, b) => b - a);
    let hIndex = 0;
    for (let i = 0; i < sortedCitations.length; i++) {
      if (sortedCitations[i] >= i + 1) hIndex = i + 1;
      else break;
    }
    stats.hIndex = hIndex;

    // Citation distribution bands
    const citationBands = [
      { label: "0–9", min: 0, max: 9, count: 0 },
      { label: "10–49", min: 10, max: 49, count: 0 },
      { label: "50–99", min: 50, max: 99, count: 0 },
      { label: "100–499", min: 100, max: 499, count: 0 },
      { label: "500+", min: 500, max: Infinity, count: 0 }
    ];
    for (const c of sortedCitations) {
      const val = Number(c) || 0;
      const band = citationBands.find((b) => val >= b.min && val <= b.max);
      if (band) band.count += 1;
    }
    stats.citationBands = citationBands;

    // Top cited year (sum of citations by publication year)
    const ycLocal = stats.__yearCitations || [];
    if (ycLocal.length > 0) {
      const yearTotals = new Map();
      for (const row of ycLocal) {
        if (!Number.isFinite(row.year)) continue;
        const prev = yearTotals.get(row.year) || 0;
        yearTotals.set(row.year, prev + (Number(row.citations) || 0));
      }
      let topYear = null;
      let topYearCites = -1;
      for (const [year, total] of yearTotals.entries()) {
        if (total > topYearCites || (total === topYearCites && year > topYear)) {
          topYear = year;
          topYearCites = total;
        }
      }
      stats.topCitedYear = topYear != null ? { year: topYear, citations: topYearCites } : null;
    } else {
      stats.topCitedYear = null;
    }

    // Venue diversity (Shannon entropy, normalized 0–1)
    const venueCounts = Array.from(stats.venues.values()).map((v) => Number(v.count) || 0);
    const venueTotal = venueCounts.reduce((a, b) => a + b, 0);
    if (venueTotal > 0 && venueCounts.length > 1) {
      let entropy = 0;
      for (const count of venueCounts) {
        const p = count / venueTotal;
        if (p > 0) entropy -= p * Math.log(p);
      }
      const maxEntropy = Math.log(venueCounts.length);
      stats.venueEntropy = maxEntropy > 0 ? Math.round((entropy / maxEntropy) * 100) / 100 : null;
    } else {
      stats.venueEntropy = venueCounts.length === 1 ? 0 : null;
    }

    // g-index: largest g such that top g papers have ≥ g² citations (cumulative sum)
    let gIndex = 0;
    let cumSum = 0;
    for (let g = 1; g <= sortedCitations.length; g++) {
      cumSum += sortedCitations[g - 1];
      if (cumSum >= g * g) gIndex = g;
    }
    stats.gIndex = gIndex > 0 ? gIndex : null;

    const yearsSinceFirst = stats.firstYear != null ? Math.max(1, currentYear - stats.firstYear) : 1;
    stats.yearsSinceFirst = yearsSinceFirst;
    stats.careerAge = stats.firstYear != null ? currentYear - stats.firstYear : null;
    stats.mIndex = yearsSinceFirst >= 1 ? Math.round((hIndex / yearsSinceFirst) * 100) / 100 : null;

    // Average team size: mean authors per paper
    const authorCounts = stats.__authorCounts || [];
    stats.avgTeamSize = authorCounts.length > 0
      ? Math.round((authorCounts.reduce((a, b) => a + b, 0) / authorCounts.length) * 10) / 10
      : null;
    delete stats.__authorCounts;

    // Gini coefficient over citation counts (0 = equal, 1 = maximally unequal)
    const n = sortedCitations.length;
    const totalCite = sortedCitations.reduce((a, b) => a + b, 0);
    if (n >= 1 && totalCite > 0) {
      const asc = sortedCitations.slice().sort((a, b) => a - b);
      let B = 0;
      for (let i = 0; i < n; i++) B += (i + 1) * asc[i];
      stats.citationGini = Math.round(((2 * B) / (n * totalCite) - (n + 1) / n) * 100) / 100;
      stats.citationGini = Math.max(0, Math.min(1, stats.citationGini));
    } else {
      stats.citationGini = null;
    }

    // e-index: excess citations beyond h-core. e = √(Σ_{i=1..h} (c_i - h))
    if (hIndex >= 1 && sortedCitations.length >= hIndex) {
      let excessSum = 0;
      for (let i = 0; i < hIndex; i++) excessSum += Math.max(0, sortedCitations[i] - hIndex);
      stats.eIndex = excessSum > 0 ? Math.round(Math.sqrt(excessSum) * 100) / 100 : null;
    } else {
      stats.eIndex = null;
    }

    // h-core share: citations in top h papers / total citations
    if (hIndex >= 1 && totalCite > 0 && sortedCitations.length >= hIndex) {
      let hCoreCite = 0;
      for (let i = 0; i < hIndex; i++) hCoreCite += sortedCitations[i];
      stats.hCoreShare = Math.round((hCoreCite / totalCite) * 1000) / 1000;
    } else {
      stats.hCoreShare = null;
    }

    // Median citations per paper
    if (n >= 1) {
      const sorted = sortedCitations.slice().sort((a, b) => a - b);
      const mid = Math.floor(n / 2);
      stats.medianCitations = n % 2 === 1 ? sorted[mid] : Math.round((sorted[mid - 1] + sorted[mid]) / 2);
    } else {
      stats.medianCitations = null;
    }
    // Mean citations per paper (avgCitations is set later; set here for dropdown)
    stats.meanCitations = n >= 1 && totalCite >= 0 ? Math.round((totalCite / n) * 10) / 10 : null;

    // Consistency index: coefficient of variation σ/μ
    if (n >= 2 && totalCite > 0) {
      const mean = totalCite / n;
      let variance = 0;
      for (let i = 0; i < n; i++) variance += (sortedCitations[i] - mean) ** 2;
      variance /= n;
      const sigma = Math.sqrt(variance);
      stats.consistencyIndex = mean > 0 ? Math.round((sigma / mean) * 100) / 100 : null;
    } else {
      stats.consistencyIndex = null;
    }

    // Citation half-life: median publication year weighted by citations (year at 50% of cumulative citations)
    const yc = stats.__yearCitations || [];
    if (yc.length > 0) {
      const totalC = yc.reduce((s, p) => s + p.citations, 0);
      const byYear = yc.slice().sort((a, b) => a.year - b.year);
      let cum = 0;
      let halfLifeYear = byYear[byYear.length - 1]?.year ?? null;
      for (const p of byYear) {
        cum += p.citations;
        if (cum >= totalC / 2) {
          halfLifeYear = p.year;
          break;
        }
      }
      stats.citationHalfLifeYear = halfLifeYear;
    } else {
      stats.citationHalfLifeYear = null;
    }
    delete stats.__yearCitations;

    // Time to impact: median estimated years to reach 10 citations (linear assumption)
    if (yc.length > 0) {
      const estYears = [];
      for (const row of yc) {
        if (!Number.isFinite(row.year)) continue;
        const cite = Number(row.citations) || 0;
        if (cite >= 10) {
          const yearsSince = Math.max(1, currentYear - row.year + 1);
          const est = Math.max(0.5, (yearsSince * 10) / cite);
          estYears.push(est);
        }
      }
      if (estYears.length > 0) {
        estYears.sort((a, b) => a - b);
        const mid = Math.floor(estYears.length / 2);
        const median = estYears.length % 2 === 1 ? estYears[mid] : (estYears[mid - 1] + estYears[mid]) / 2;
        stats.timeToTenMedian = Math.round(median * 10) / 10;
      } else {
        stats.timeToTenMedian = null;
      }
    } else {
      stats.timeToTenMedian = null;
    }

    delete stats.__citations;

    // L-index: L = ln(Σ c_i/(a_i*y_i)) + 1 (independent of number of publications; typically 0–9.9)
    const lSum = stats.__lSum;
    delete stats.__lSum;
    if (lSum != null && lSum > 0) {
      const raw = Math.log(lSum) + 1;
      stats.lIndex = Math.max(0, Math.round(raw * 100) / 100); // clamp to 0 (typically 0–9.9)
    } else {
      stats.lIndex = null;
    }

    // Eigenfactor-style score (West et al.): additive venue-weighted citation sum
    const efSum = stats.__eigenfactorSum;
    delete stats.__eigenfactorSum;
    stats.eigenfactorStyle = efSum != null && efSum > 0 ? Math.round(efSum * 100) / 100 : null;

    // MSSI: composite = α·log(1+I/50) + β·log(1+V/5) + γ·log(1+L/10), scaled by 10. I = authorship-weighted citation sum, V = career velocity, L = longevity (years).
    const mssiI = stats.__mssiImpact || 0;
    delete stats.__mssiImpact;
    const mssiV = yearsSinceFirst >= 1 ? stats.totalCitations / yearsSinceFirst : 0;
    const mssiL = yearsSinceFirst || 1;
    const mssiRaw = Math.log(1 + mssiI / 50) + Math.log(1 + mssiV / 5) + Math.log(1 + mssiL / 10);
    stats.mssi = mssiRaw > 0 ? Math.round(mssiRaw * 10 * 100) / 100 : null;

    stats.papers = papers;
    
    return stats;
  }

  function computePoPMetrics(stats) {
    const papers = Array.isArray(stats?.papers) ? stats.papers : [];
    if (!papers.length) return null;
    const currentYear = new Date().getFullYear();
    const citations = papers.map((p) => Number(p.citations) || 0);
    const authorCounts = papers.map((p) => Math.max(1, Number(p.authorsCount) || 1));
    const totalPubs = papers.length;
    const totalCites = citations.reduce((a, b) => a + b, 0);
    const totalAuthors = authorCounts.reduce((a, b) => a + b, 0);
    const yearsSinceFirst = Math.max(1, stats?.yearsSinceFirst || (stats?.lastYear && stats?.firstYear ? (stats.lastYear - stats.firstYear + 1) : 1));

    const roundTo = (v, d = 2) => (v == null || !Number.isFinite(v)) ? null : Math.round(v * (10 ** d)) / (10 ** d);
    const mean = (list) => list.length ? list.reduce((a, b) => a + b, 0) / list.length : null;
    const median = (list) => {
      if (!list.length) return null;
      const sorted = list.slice().sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    };
    const mode = (list) => {
      if (!list.length) return null;
      const freq = new Map();
      for (const v of list) freq.set(v, (freq.get(v) || 0) + 1);
      let best = null;
      let bestCount = 0;
      for (const [val, count] of freq.entries()) {
        if (count > bestCount || (count === bestCount && (best == null || val > best))) {
          best = val;
          bestCount = count;
        }
      }
      return bestCount > 1 ? best : null;
    };
    const hIndexFromScores = (list) => {
      if (!list || !list.length) return 0;
      const sorted = list.map((v) => Number(v) || 0).sort((a, b) => b - a);
      let h = 0;
      for (let i = 0; i < sorted.length; i++) {
        if (sorted[i] >= i + 1) h = i + 1;
        else break;
      }
      return h;
    };

    const citesPerYear = totalCites / yearsSinceFirst;
    const citesPerPaperMean = mean(citations);
    const citesPerPaperMedian = median(citations);
    const citesPerPaperMode = mode(citations);
    const citesPerAuthor = totalAuthors ? totalCites / totalAuthors : null;
    const papersPerAuthor = totalAuthors ? totalPubs / totalAuthors : null;
    const authorsPerPaperMean = mean(authorCounts);
    const authorsPerPaperMedian = median(authorCounts);
    const authorsPerPaperMode = mode(authorCounts);

    const hIndex = stats?.hIndex ?? hIndexFromScores(citations);
    const gIndex = stats?.gIndex ?? null;
    const eIndex = stats?.eIndex ?? null;

    const normCitations = citations.map((c, i) => c / (authorCounts[i] || 1));
    const hINorm = hIndexFromScores(normCitations);
    const hIAnnual = yearsSinceFirst ? hINorm / yearsSinceFirst : null;

    let avgAuthorsHCore = null;
    if (hIndex > 0) {
      const sorted = papers.slice().sort((a, b) => (b.citations || 0) - (a.citations || 0)).slice(0, hIndex);
      if (sorted.length) {
        const sum = sorted.reduce((acc, p) => acc + (Number(p.authorsCount) || 1), 0);
        avgAuthorsHCore = sum / sorted.length;
      }
    }
    const hAIndex = avgAuthorsHCore ? (hIndex / avgAuthorsHCore) : null;

    let awcr = 0;
    for (const p of papers) {
      const year = Number(p.year) || null;
      const age = year ? Math.max(1, currentYear - year + 1) : 1;
      awcr += (Number(p.citations) || 0) / age;
    }
    const awIndex = awcr > 0 ? Math.sqrt(awcr) : null;
    const awcrpA = totalAuthors ? (awcr / totalAuthors) : null;

    const acc = {
      c1: citations.filter((c) => c >= 1).length,
      c2: citations.filter((c) => c >= 2).length,
      c5: citations.filter((c) => c >= 5).length,
      c10: citations.filter((c) => c >= 10).length,
      c20: citations.filter((c) => c >= 20).length
    };

    const contemporaryScores = papers.map((p) => {
      const year = Number(p.year) || null;
      const age = year ? Math.max(1, currentYear - year + 1) : 1;
      const cite = Number(p.citations) || 0;
      return (4 / age) * cite;
    });
    const hContemporary = hIndexFromScores(contemporaryScores);

    return {
      totalPubs,
      totalCites,
      yearsSinceFirst,
      citesPerYear: roundTo(citesPerYear, 2),
      citesPerPaperMean: roundTo(citesPerPaperMean, 2),
      citesPerPaperMedian: roundTo(citesPerPaperMedian, 2),
      citesPerPaperMode: citesPerPaperMode,
      citesPerAuthor: roundTo(citesPerAuthor, 3),
      papersPerAuthor: roundTo(papersPerAuthor, 4),
      authorsPerPaperMean: roundTo(authorsPerPaperMean, 2),
      authorsPerPaperMedian: roundTo(authorsPerPaperMedian, 2),
      authorsPerPaperMode: authorsPerPaperMode,
      hIndex,
      gIndex,
      hINorm,
      hIAnnual: roundTo(hIAnnual, 3),
      hAIndex: roundTo(hAIndex, 3),
      eIndex,
      awcr: roundTo(awcr, 2),
      awcrpA: roundTo(awcrpA, 3),
      awIndex: roundTo(awIndex, 2),
      acc,
      hContemporary
    };
  }

  function getPoPMetricsCached(stats) {
    if (!stats) return null;
    const authorId = new URL(window.location.href).searchParams.get("user") || "author";
    const key = `${authorId}|${stats.totalPublications || 0}|${stats.totalCitations || 0}|${stats.firstYear || 0}|${stats.lastYear || 0}`;
    if (!window.suPopMetricsCache) window.suPopMetricsCache = {};
    const cache = window.suPopMetricsCache;
    if (cache.key === key && cache.data) {
      stats.popMetrics = cache.data;
      return cache.data;
    }
    const data = computePoPMetrics(stats);
    cache.key = key;
    cache.data = data;
    stats.popMetrics = data;
    return data;
  }

  /** Returns Δh over last 12 months from cached snapshots; saves current h for future. */
  async function getAuthorHIndexGrowth(profileUrl, currentH) {
    if (!profileUrl || typeof currentH !== "number") return null;
    const data = await getAuthorHIndexSnapshots();
    const list = Array.isArray(data[profileUrl]) ? data[profileUrl] : [];
    const now = new Date();
    const targetFrom = new Date(now.getFullYear(), now.getMonth() - 14, 1).getTime();
    const targetTo = new Date(now.getFullYear(), now.getMonth() - 10, 1).getTime();
    const inWindow = list
      .map((e) => ({ ...e, ts: new Date(e.date).getTime() }))
      .filter((e) => e.ts >= targetFrom && e.ts <= targetTo)
      .sort((a, b) => b.ts - a.ts);
    const oldSnapshot = inWindow[0];
    await setAuthorHIndexSnapshot(profileUrl, currentH);
    if (!oldSnapshot || oldSnapshot.hIndex == null) return null;
    return currentH - oldSnapshot.hIndex;
  }

  function countPublicationBadges(results, state) {
    const stats = computeAuthorStats(results, state, window.suAuthorPositionFilter);
    // Return the quality counts for backward compatibility
    return {
      q1: stats.qualityCounts.q1,
      a: stats.qualityCounts.a,
      utd24: stats.qualityCounts.utd24,
      ft50: stats.qualityCounts.ft50
    };
  }

  function getAuthorStatTooltipHtml(statId, stats) {
    const years = stats?.yearsSinceFirst != null ? stats.yearsSinceFirst : "n";
    const tips = {
      hindex: {
        formula: "<i>h</i> = largest number such that you have <i>h</i> papers with at least <i>h</i> citations each.",
        description: "Measures productivity and citation impact.",
        good: "<i>h</i> ≥ 10 (mid-career), ≥ 20 (senior). Field-dependent."
      },
      mindex: {
        formula: `<i>m</i> = <i>h</i> / <i>y</i> (y = ${years} yr)`,
        description: "Normalizes <i>h</i>-index by career length.",
        good: "<i>m</i> ≥ 1 solid; ≥ 1.5 strong; ≥ 2 exceptional."
      },
      lindex: {
        formula: "<i>L</i> = ln(Σ <i>c</i><sub><i>i</i></sub> / (<i>a</i><sub><i>i</i></sub> × <i>y</i><sub><i>i</i></sub>)) + 1",
        description: "Combines citations, coauthors, and age.",
        good: "Range 0–9.9. <i>L</i> ≥ 2 good; ≥ 3 strong; ≥ 4 very high impact."
      },
      eigenfactor: {
        formula: "EF-style = Σ (<i>c</i><sub><i>i</i></sub> × <i>w</i><sub><i>i</i></sub>) where <i>w</i><sub><i>i</i></sub> = 1 + ln(1 + <i>h</i><sub>5</sub>)",
        description: "Venue-weighted citation sum.",
        good: "Hundreds = solid; thousands = strong."
      },
      gindex: {
        formula: "<i>g</i> = max {<i>k</i> : Σ<sub><i>i</i>=1</sub><sup><i>k</i></sup> <i>c</i><sub><i>i</i></sub> ≥ <i>k</i><sup>2</sup>}",
        description: "Extension of <i>h</i>-index rewarding sustained impact.",
        good: "<i>g</i> ≥ <i>h</i> always. Typically 1.2–1.5× <i>h</i>."
      },
      careerAge: {
        formula: "Career age = current year − first publication year.",
        description: "Years since first publication.",
        good: "Early &lt;10 yr; mid 10–20; senior 20+."
      },
      avgTeamSize: {
        formula: "Average team size = mean number of authors per paper.",
        description: "Collaboration pattern.",
        good: "&lt;2 = solo/pairs; 2–4 = typical; 5+ = team science."
      },
      gini: {
        formula: "Gini coefficient (0 = equal, 1 = one paper has all citations).",
        description: "Measures inequality in citation distribution.",
        good: "High (0.6+) = few blockbusters; low (0.3–) = consistent."
      },
      citationHalfLife: {
        formula: "Year when cumulative citations reach 50% of total.",
        description: "Half of citations are from this year or earlier.",
        good: "Recent = current impact; older = legacy/classic."
      },
      eindex: {
        formula: "<i>e</i> = √(Σ<sub><i>i</i>=1</sub><sup><i>h</i></sup> (<i>c</i><sub><i>i</i></sub> − <i>h</i>))",
        description: "Excess citations beyond <i>h</i>-index threshold.",
        good: "Higher = top papers cited well above <i>h</i>."
      },
      hCoreShare: {
        formula: "<i>h</i>-core share = Σ<sub><i>i</i>=1</sub><sup><i>h</i></sup> <i>c</i><sub><i>i</i></sub> / Σ <i>c</i><sub><i>i</i></sub>",
        description: "Fraction of citations in the <i>h</i>-core.",
        good: "High = concentrated; low = spread across many papers."
      },
      medianCitations: {
        formula: "Median citations per paper.",
        description: "Less sensitive to outliers than mean.",
        good: "Compare with mean: mean ≫ median = few highly cited papers."
      },
      meanCitations: {
        formula: "Mean = Σ <i>c</i><sub><i>i</i></sub> / <i>n</i>",
        description: "Average citation count per publication.",
        good: "Field-dependent. Compare with median to see skew."
      },
      consistencyIndex: {
        formula: "Consistency index = <i>σ</i> / <i>μ</i>",
        description: "Coefficient of variation. Lower = more consistent.",
        good: "Low (&lt;0.8) = even spread; high (&gt;1.5) = mix of low/high impact."
      },
      flpIndex: {
        formula: "FLP = <i>N</i> × (<i>F</i>%)<sup>2</sup>",
        description: "First‑author leadership publication index.",
        good: "Higher = more first‑author leadership given output volume."
      },
      hIndexGrowth: {
        formula: "Δ<i>h</i> = current <i>h</i> − <i>h</i> from ~12 months ago.",
        description: "Change in <i>h</i>-index over the last year.",
        good: "Positive = increased; 0/— = no snapshot or no change."
      }
    };
    const t = tips[statId];
    if (!t) return "";
    return `${t.formula}<br>${t.description}<br>${t.good}`;
  }

  async function renderAuthorStatsWithGrowth(fullStats) {
    const growth = await getAuthorHIndexGrowth(window.location.href, fullStats.hIndex);
    if (growth != null) fullStats.hIndexGrowth = growth;
    renderAuthorStats(fullStats);
  }

  function renderAuthorStats(stats, isLoading = false) {
    // Find or create the stats container
    let statsContainer = document.getElementById("su-author-stats");
    
    if (!statsContainer) {
      // Find where to insert it - after the last .gsc_prf_il or after #gsc_prf_inw
      const profileInfo = document.querySelector("#gsc_prf_i");
      if (!profileInfo) return;
      
      statsContainer = document.createElement("div");
      statsContainer.id = "su-author-stats";
      statsContainer.className = "su-author-stats";
      
      // Insert after the last .gsc_prf_il or after #gsc_prf_inw
      const lastInfoLine = profileInfo.querySelector(".gsc_prf_il:last-of-type");
      if (lastInfoLine) {
        lastInfoLine.insertAdjacentElement("afterend", statsContainer);
      } else {
        const nameWrapper = document.querySelector("#gsc_prf_inw");
        if (nameWrapper) {
          nameWrapper.insertAdjacentElement("afterend", statsContainer);
        } else {
          profileInfo.appendChild(statsContainer);
        }
      }
      // Single delegated pointer handler (survives innerHTML updates)
      statsContainer.addEventListener("pointerdown", async (e) => {
        if (!e.target.closest(".su-metrics-dropdown")) {
          statsContainer.querySelectorAll(".su-metrics-dropdown.su-metrics-open").forEach((d) => {
            d.classList.remove("su-metrics-open");
            d.querySelector(".su-metrics-trigger")?.setAttribute("aria-expanded", "false");
          });
        }
        const sortToggle = e.target.closest("[data-sort-toggle]");
        if (sortToggle) {
          e.stopPropagation();
          e.preventDefault();
          window.suAuthorSortByVelocity = !window.suAuthorSortByVelocity;
          applyAuthorSort();
          const el = document.getElementById("su-sort-toggle");
          if (el) el.textContent = window.suAuthorSortByVelocity ? "Sort: citations/yr ✓" : "Sort by citations/yr";
          return;
        }
        const metricsTrigger = e.target.closest(".su-metrics-trigger");
        if (metricsTrigger) {
          e.stopPropagation();
          e.preventDefault();
          const dropdown = metricsTrigger.closest(".su-metrics-dropdown");
          if (dropdown) {
            const open = dropdown.classList.toggle("su-metrics-open");
            metricsTrigger.setAttribute("aria-expanded", String(open));
            if (open) {
              // Position the panel with fixed coords so it escapes any ancestor
              // overflow:hidden on Scholar's own DOM (which clips absolute children).
              const panel = dropdown.querySelector(".su-metrics-panel");
              if (panel) {
                const rect = metricsTrigger.getBoundingClientRect();
                panel.style.left = rect.left + "px";
                panel.style.top  = (rect.bottom + 4) + "px";
                // After first paint, clamp to viewport edges.
                requestAnimationFrame(() => {
                  const pr = panel.getBoundingClientRect();
                  if (pr.right > window.innerWidth - 8) {
                    panel.style.left = Math.max(8, window.innerWidth - pr.width - 8) + "px";
                  }
                  if (pr.bottom > window.innerHeight - 8) {
                    // Flip above the trigger if it would go off the bottom.
                    panel.style.top = Math.max(8, rect.top - pr.height - 4) + "px";
                  }
                });
              }
            }
          }
          return;
        }
        const positionBtn = e.target.closest("[data-position-filter]");
        if (positionBtn) {
          e.stopPropagation();
          e.preventDefault();
          window.suAuthorPositionFilter = positionBtn.dataset.positionFilter || "all";
          if (window.suProcessAll && window.suState) {
            await window.suProcessAll();
            const { results, isAuthorProfile } = scanResults();
            if (isAuthorProfile) {
              const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
              const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            }
          }
          return;
        }
        const loadAllBtn = e.target.closest("[data-load-all-publications]");
        if (loadAllBtn) {
          e.stopPropagation();
          e.preventDefault();
          if (window.suState) window.suState.authorAutoLoadDisabled = false;
          renderAuthorStats({ qualityCounts: { q1: 0, a: 0, vhb: 0, utd24: 0, ft50: 0, abs4star: 0 }, totalPublications: 0, totalCitations: 0, avgCitations: 0, recentActivity: { last5Years: 0 } }, true);
          try {
            const res = await loadAllPublicationsRecursively({ maxPubs: Infinity });
            const fullyLoaded = res && res.status === "complete";
            if (window.suState) {
              window.suState.allPublicationsLoaded = fullyLoaded;
              window.suState.authorStatsPartial = !fullyLoaded;
              window.suState.authorAutoLoadDisabled = !fullyLoaded;
            }
            if (window.suProcessAll) await window.suProcessAll();
          } catch {
            if (window.suState) {
              window.suState.authorStatsPartial = true;
              window.suState.authorAutoLoadDisabled = true;
            }
            if (window.suProcessAll) await window.suProcessAll();
          }
          return;
        }
        const exportBtn = e.target.closest("[data-author-export]");
        if (exportBtn) {
          e.stopPropagation();
          e.preventDefault();
          const ok = window.confirm("Export all publications to CSV? This will first load all publications.");
          if (!ok) return;
          await loadAllPublicationsRecursively();
          await exportAuthorCsv(window.suState);
          return;
        }
        const popBtn = e.target.closest("[data-pop-report]");
        if (popBtn) {
          e.stopPropagation();
          e.preventDefault();
          openPoPOverlay();
          return;
        }
        const compareBtn = e.target.closest(".su-compare-authors-btn");
        if (compareBtn) {
          e.stopPropagation();
          e.preventDefault();
          openAuthorCompareOverlay(window.suState);
          return;
        }
        const venueTag = e.target.closest("[data-venue-filter]");
        if (venueTag) {
          e.stopPropagation();
          e.preventDefault();
          const venue = String(venueTag.dataset.venueFilter || "").trim().toLowerCase();
          if (venue && window.suAuthorVenueFilter === venue) {
            window.suAuthorVenueFilter = "";
          } else {
            window.suAuthorVenueFilter = venue;
          }
          if (window.suProcessAll && window.suState) {
            await window.suProcessAll();
            const { results, isAuthorProfile } = scanResults();
            if (isAuthorProfile) {
              const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
              const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            }
          }
          return;
        }
        const coSort = e.target.closest("[data-coauthor-sort]");
        if (coSort) {
          e.stopPropagation();
          e.preventDefault();
          const key = String(coSort.dataset.coauthorSort || "").trim();
          if (!key) return;
          const current = window.suCoauthorSort || { key: "count", dir: "desc" };
          if (current.key === key) {
            current.dir = current.dir === "asc" ? "desc" : "asc";
          } else {
            current.key = key;
            current.dir = "desc";
          }
          window.suCoauthorSort = current;
          updateCoauthorTable(window.suFullAuthorStats);
          return;
        }
        const topicClear = e.target.closest("[data-topic-clear]");
        if (topicClear) {
          e.stopPropagation();
          e.preventDefault();
          window.suAuthorTitleFilters = [];
          if (window.suProcessAll && window.suState) {
            await window.suProcessAll();
            const { results, isAuthorProfile } = scanResults();
            if (isAuthorProfile) {
              const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
              const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            }
          }
          return;
        }
        const topicTag = e.target.closest("[data-topic-filter]");
        if (topicTag) {
          e.stopPropagation();
          e.preventDefault();
          const token = String(topicTag.dataset.topicToken || "").toLowerCase().trim();
          const active = new Set(normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter));
          if (token) {
            if (active.has(token)) active.delete(token);
            else active.add(token);
          }
          window.suAuthorTitleFilters = Array.from(active);
          if (window.suProcessAll && window.suState) {
            await window.suProcessAll();
            const { results, isAuthorProfile } = scanResults();
            if (isAuthorProfile) {
              const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
              const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            }
          }
          return;
        }
        const selfciteBtn = e.target.closest("[data-selfcite-refresh]");
        if (selfciteBtn) {
          e.stopPropagation();
          e.preventDefault();
          const scholarId = new URL(window.location.href).searchParams.get("user");
          if (scholarId) {
            await chrome.storage.local.remove(`selfcite_${scholarId}`);
          }
          window.__suSelfCiteState = { key: null, loading: false, data: null };
          await ensureSelfCitationEstimate();
          return;
        }
        const badge = e.target.closest("[data-filter]");
        if (!badge) return;
        e.stopPropagation();
        e.preventDefault();
        const filter = badge.dataset.filter;
        if (filter === "clear") {
          window.suActiveFilter = null;
        } else {
          window.suActiveFilter = window.suActiveFilter === filter ? null : filter;
        }
        if (window.suProcessAll && window.suState) {
          await window.suProcessAll();
          const { results, isAuthorProfile } = scanResults();
          if (isAuthorProfile) {
            const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
            const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
            await renderAuthorStatsWithGrowth(fullStats);
          }
        }
      });
      statsContainer.addEventListener("input", (e) => {
        const input = e.target.closest?.("#su-coauthor-search");
        if (!input) return;
        window.suCoauthorQuery = input.value || "";
        updateCoauthorTable(window.suFullAuthorStats);
      });
      statsContainer.addEventListener("change", async (e) => {
        const toggle = e.target.closest?.("[data-panel-toggle]");
        if (!toggle) return;
        const key = String(toggle.dataset.panelToggle || "");
        if (!key) return;
        if (toggle.closest(".su-view-settings-dropdown")) {
          window.suKeepViewSettingsOpen = true;
        }
        const scholarId = new URL(window.location.href).searchParams.get("user");
        if (!scholarId) return;
        const current = window.suState?.authorFeatureToggles || { ...DEFAULT_AUTHOR_FEATURE_TOGGLES };
        const next = { ...current, [key]: !!toggle.checked };
        if (window.suState) window.suState.authorFeatureToggles = next;
        await setAuthorFeatureToggles(scholarId, next);
        if (key === "coauthors" && toggle.checked && window.suProcessAll) {
          await window.suProcessAll();
          return;
        }
        if (key === "graph") {
          if (!toggle.checked) closeGraphOverlay();
          renderAuthorStats(window.suFullAuthorStats);
          return;
        }
        if (key === "velocityBadge") {
          if (!toggle.checked) window.suAuthorSortByVelocity = false;
          await window.suProcessAll();
          return;
        }
        if (key === "venues") {
          if (!toggle.checked) window.suAuthorVenueFilter = "";
          if (window.suProcessAll) {
            await window.suProcessAll();
            return;
          }
        }
        if (key === "citationBands") {
          if (!toggle.checked) window.suAuthorCitationBand = null;
          if (window.suProcessAll) {
            await window.suProcessAll();
            return;
          }
        }
        if (key === "topics") {
          if (!toggle.checked) {
            window.suAuthorTitleFilters = [];
            window.suAuthorTitleFilter = "";
          }
          if (window.suProcessAll) {
            await window.suProcessAll();
            return;
          }
        }
        if (key === "filterBadges") {
          if (!toggle.checked) {
            window.suActiveFilter = null;
          }
          if (window.suProcessAll) {
            await window.suProcessAll();
            return;
          }
        }
        if (key === "researchIntel") {
          if (!toggle.checked) closePoPOverlay();
          renderAuthorStats(window.suFullAuthorStats);
          return;
        }
        renderAuthorStats(window.suFullAuthorStats);
      });
      if (!window.suRightPanelFilterHandlerAdded) {
        window.suRightPanelFilterHandlerAdded = true;
        document.addEventListener("pointerdown", async (e) => {
          const venueTag = e.target.closest("[data-venue-filter]");
          if (venueTag) {
            e.stopPropagation();
            e.preventDefault();
            const venue = String(venueTag.dataset.venueFilter || "").trim().toLowerCase();
            if (venue && window.suAuthorVenueFilter === venue) {
              window.suAuthorVenueFilter = "";
            } else {
              window.suAuthorVenueFilter = venue;
            }
            if (window.suProcessAll && window.suState) {
              await window.suProcessAll();
              const { results, isAuthorProfile } = scanResults();
              if (isAuthorProfile) {
                const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
                const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
                await renderAuthorStatsWithGrowth(fullStats);
              }
            }
            return;
          }
          const coauthorTag = e.target.closest("[data-coauthor-filter]");
          if (coauthorTag) {
            if (e.target.closest(".su-coauthor-link")) return;
            e.stopPropagation();
            e.preventDefault();
            const coauthor = String(coauthorTag.dataset.coauthorFilter || "").trim();
            if (coauthor && window.suAuthorCoauthorFilter === coauthor) {
              window.suAuthorCoauthorFilter = "";
            } else {
              window.suAuthorCoauthorFilter = coauthor;
            }
            updateCoauthorTable(window.suFullAuthorStats);
            if (window.suProcessAll && window.suState) {
              await window.suProcessAll();
            }
            return;
          }
          const bandTag = e.target.closest("[data-cite-band-min]");
          if (bandTag) {
            e.stopPropagation();
            e.preventDefault();
            const min = Number(bandTag.dataset.citeBandMin || "");
            const maxRaw = bandTag.dataset.citeBandMax;
            const max = maxRaw === "" ? Infinity : Number(maxRaw);
            const current = window.suAuthorCitationBand || null;
            if (current && current.min === min && current.max === max) {
              window.suAuthorCitationBand = null;
            } else {
              window.suAuthorCitationBand = { min, max };
            }
            if (window.suProcessAll && window.suState) {
              await window.suProcessAll();
              const { results, isAuthorProfile } = scanResults();
              if (isAuthorProfile) {
                const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
                const fullStats = computeAuthorStats(visible, window.suState, window.suAuthorPositionFilter);
                await renderAuthorStatsWithGrowth(fullStats);
              }
            }
            return;
          }
          const coSort = e.target.closest("[data-coauthor-sort]");
          if (coSort) {
            e.stopPropagation();
            e.preventDefault();
            const key = String(coSort.dataset.coauthorSort || "").trim();
            if (!key) return;
            const current = window.suCoauthorSort || { key: "count", dir: "desc" };
            if (current.key === key) {
              current.dir = current.dir === "asc" ? "desc" : "asc";
            } else {
              current.key = key;
              current.dir = "desc";
            }
            window.suCoauthorSort = current;
            updateCoauthorTable(window.suFullAuthorStats);
            return;
          }
        });
        document.addEventListener("input", (e) => {
          const input = e.target?.closest?.("#su-coauthor-search");
          if (!input) return;
          window.suCoauthorQuery = input.value || "";
          updateCoauthorTable(window.suFullAuthorStats);
        });
      }
      if (!window.suGraphHandlersAdded) {
        window.suGraphHandlersAdded = true;
        document.addEventListener("click", async (e) => {
          if (e.target.closest("#su-graph-overlay")) return;
          const openBtn = e.target.closest("[data-graph-open]");
          if (openBtn) {
            e.preventDefault();
            await openGraphOverlay();
            return;
          }
          const buildBtn = e.target.closest("[data-graph-build]");
          if (buildBtn) {
            e.preventDefault();
            const count = Math.min(50, Math.max(3, Number(buildBtn.dataset.graphBuildCount) || 10));
            await openGraphOverlay();
            await buildGraphFromSeeds(count);
          }
        });
      }
      if (!window.suLineageHandlersAdded) {
        window.suLineageHandlersAdded = true;
        document.addEventListener("click", async (e) => {
          const closeBtn = e.target.closest("[data-lineage-close]");
          if (closeBtn) {
            e.preventDefault();
            closeLineageOverlay();
            return;
          }
          if (e.target.closest("#su-lineage-overlay")) return;
          const openBtn = e.target.closest("[data-lineage-open]");
          if (openBtn) {
            e.preventDefault();
            await openLineageOverlay();
          }
        });
      }
      // Rich tooltips for h-index, m-index, L-index, EF-style (show on hover)
      function ensureFloatingTooltip() {
        let el = document.getElementById("su-metrics-floating-tooltip");
        if (!el) {
          el = document.createElement("div");
          el.id = "su-metrics-floating-tooltip";
          document.body.appendChild(el);
        }
        return el;
      }
      function ensureCoauthorTooltip() {
        let el = document.getElementById("su-coauthor-tooltip");
        if (!el) {
          el = document.createElement("div");
          el.id = "su-coauthor-tooltip";
          document.body.appendChild(el);
        }
        return el;
      }
      statsContainer.addEventListener("mouseenter", (e) => {
        const item = e.target.closest(".su-stat-item-with-tooltip[data-stat-tooltip]");
        if (!item) return;
        const tip = item.querySelector(".su-author-stat-tooltip");
        if (!tip) return;
        const inMetricsPanel = item.closest(".su-metrics-panel");
        if (inMetricsPanel) {
          statsContainer.querySelectorAll(".su-author-stat-tooltip-visible").forEach((t) => t.classList.remove("su-author-stat-tooltip-visible"));
          const floating = ensureFloatingTooltip();
          floating.innerHTML = tip.innerHTML;
          floating.style.cssText = "";
          floating.style.position = "fixed";
          floating.style.left = "-9999px";
          floating.style.top = "0px";
          floating.style.width = "480px";
          floating.style.maxWidth = "calc(100vw - 20px)";
          floating.style.zIndex = "999999";
          floating.classList.add("su-metrics-floating-tooltip-visible");
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              const tooltipRect = floating.getBoundingClientRect();
              const rowRect = item.getBoundingClientRect();
              const panel = item.closest(".su-metrics-panel");
              const panelRect = panel ? panel.getBoundingClientRect() : rowRect;
              const tooltipWidth = tooltipRect.width;
              const tooltipHeight = tooltipRect.height;
              const margin = 10;
              let left = panelRect.right + margin;
              let top = rowRect.top;
              if (left + tooltipWidth + margin > window.innerWidth) {
                left = Math.max(margin, panelRect.left - tooltipWidth - margin);
              }
              if (left < margin) left = margin;
              if (top + tooltipHeight + margin > window.innerHeight) {
                top = Math.max(margin, window.innerHeight - tooltipHeight - margin);
              }
              if (top < margin) top = margin;
              floating.style.left = left + "px";
              floating.style.top = top + "px";
            });
          });
        } else {
          statsContainer.querySelectorAll(".su-author-stat-tooltip-visible").forEach((t) => t.classList.remove("su-author-stat-tooltip-visible"));
          document.getElementById("su-metrics-floating-tooltip")?.classList.remove("su-metrics-floating-tooltip-visible");
          tip.classList.add("su-author-stat-tooltip-visible");
        }
      }, true);
      statsContainer.addEventListener("mouseleave", (e) => {
        const item = e.target.closest(".su-stat-item-with-tooltip[data-stat-tooltip]");
        if (item) {
          const tip = item.querySelector(".su-author-stat-tooltip");
          if (tip) tip.classList.remove("su-author-stat-tooltip-visible");
          if (item.closest(".su-metrics-panel") && !e.relatedTarget?.closest?.(".su-stat-item-with-tooltip") && !e.relatedTarget?.closest?.("#su-metrics-floating-tooltip")) {
            document.getElementById("su-metrics-floating-tooltip")?.classList.remove("su-metrics-floating-tooltip-visible");
          }
        }
        if (e.target.closest?.(".su-metrics-panel") && !e.relatedTarget?.closest?.(".su-metrics-panel") && !e.relatedTarget?.closest?.("#su-metrics-floating-tooltip")) {
          document.getElementById("su-metrics-floating-tooltip")?.classList.remove("su-metrics-floating-tooltip-visible");
        }
      }, true);

      // Co-author hover tooltip (table)
      statsContainer.addEventListener("mouseenter", (e) => {
        const row = e.target.closest?.(".su-coauthor-row");
        if (!row) return;
        const raw = row.dataset.coauthorTooltip || row.getAttribute("title") || "";
        if (!raw) return;
        const text = String(raw).replace(/&#10;/g, "\n");
        const lines = text.split(/\n/).filter(Boolean);
        if (!lines.length) return;
        const tip = ensureCoauthorTooltip();
        tip.innerHTML = lines.map((line) => {
          if (line.startsWith("__EQ__")) {
            const eq = line.slice("__EQ__".length);
            return `<div class="su-coauthor-tooltip-equation">${escapeHtml(eq)}</div>`;
          }
          return `<div>${escapeHtml(line)}</div>`;
        }).join("");
        tip.classList.add("su-coauthor-tooltip-visible");
        tip.style.left = "-9999px";
        tip.style.top = "0px";
        requestAnimationFrame(() => {
          const rect = row.getBoundingClientRect();
          const tipRect = tip.getBoundingClientRect();
          const margin = 10;
          let left = rect.right + margin;
          let top = rect.top;
          if (left + tipRect.width + margin > window.innerWidth) {
            left = Math.max(margin, rect.left - tipRect.width - margin);
          }
          if (left < margin) left = margin;
          if (top + tipRect.height + margin > window.innerHeight) {
            top = Math.max(margin, window.innerHeight - tipRect.height - margin);
          }
          if (top < margin) top = margin;
          tip.style.left = `${left}px`;
          tip.style.top = `${top}px`;
        });
      }, true);
      statsContainer.addEventListener("mouseleave", (e) => {
        if (!e.target.closest?.(".su-coauthor-row")) return;
        if (e.relatedTarget?.closest?.("#su-coauthor-tooltip")) return;
        document.getElementById("su-coauthor-tooltip")?.classList.remove("su-coauthor-tooltip-visible");
      }, true);

      // Venue chip hover scroll
      statsContainer.addEventListener("mouseenter", (e) => {
        const chip = e.target.closest(".su-top-venue-chip");
        if (!chip) return;
        const textEl = chip.querySelector(".su-top-venue-text");
        if (!textEl) return;
        const max = textEl.scrollWidth - chip.clientWidth;
        if (max > 2) {
          textEl.style.transition = "transform 2.4s linear";
          textEl.style.transform = `translateX(-${max}px)`;
        }
      }, true);
      statsContainer.addEventListener("mouseleave", (e) => {
        const chip = e.target.closest(".su-top-venue-chip");
        if (!chip) return;
        const textEl = chip.querySelector(".su-top-venue-text");
        if (!textEl) return;
        textEl.style.transition = "transform 0.25s ease";
        textEl.style.transform = "translateX(0px)";
      }, true);
    }

    if (isLoading) {
      statsContainer.style.display = "block";
      statsContainer.innerHTML = '<span class="su-stat-loading">Loading publications...</span>';
      return;
    }

    if (!stats || !stats.qualityCounts) return;
    window.suFullAuthorStats = stats;
    if (!isLoading) {
      ensureSelfCitationEstimate();
    }

    try {
    // Build stats HTML with badge-style boxes
    const parts = [];
    const qc = stats.qualityCounts;
    const vis = window.suState?.settings?.authorStatsVisible || {};
    const featureToggles = window.suState?.authorFeatureToggles || DEFAULT_AUTHOR_FEATURE_TOGGLES;
    const settings = window.suState?.settings || {};
    const showResearchIntel = settings.showResearchIntel !== false && featureToggles.researchIntel !== false;
    const show = (key) => vis[key] !== false;

    const groups = [];
    const partialParts = [];
    if (window.suState?.authorStatsPartial) {
      const loadedCount = stats.totalPublications || 0;
      const note = loadedCount > 0
        ? `Stats based on ${loadedCount} loaded papers.`
        : "Stats based on loaded papers.";
      partialParts.push(`<span class="su-stat-item su-author-partial-note">${note}</span>`);
      partialParts.push(`<span class="su-stat-badge su-badge su-author-partial-action" data-load-all-publications="1" title="Load all publications to compute full stats">Load all for full stats</span>`);
      groups.push(partialParts);
    }
    const filterParts = [];
    let filterBadgeCount = 0;
    const activeFilter = window.suActiveFilter || null;
    const allowFilterBadges = featureToggles.filterBadges !== false;
    if (allowFilterBadges) {
      if (show("filterQ1") && (qc.q1 || 0) > 0) {
        const isActive = activeFilter === "q1";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-quartile ${isActive ? 'su-filter-active' : ''}" data-filter="q1" style="cursor: pointer;" title="Click to filter by Q1 papers"><span class="su-stat-label">Q1:</span> <strong>${qc.q1}</strong></span>`);
      }
      if (show("filterAbdc") && (qc.a || 0) > 0) {
        const isActive = activeFilter === "a";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-abdc ${isActive ? 'su-filter-active' : ''}" data-filter="a" style="cursor: pointer;" title="Click to filter by A-ranked papers"><span class="su-stat-label">ABDC:</span> <strong>${qc.a}</strong></span>`);
      }
      if (show("filterVhb") && (qc.vhb || 0) > 0) {
        const isActive = activeFilter === "vhb";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-vhb ${isActive ? 'su-filter-active' : ''}" data-filter="vhb" style="cursor: pointer;" title="Click to filter by VHB-ranked papers"><span class="su-stat-label">VHB:</span> <strong>${qc.vhb}</strong></span>`);
      }
      if (show("filterFt50") && (qc.ft50 || 0) > 0) {
        const isActive = activeFilter === "ft50";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-ft50 ${isActive ? 'su-filter-active' : ''}" data-filter="ft50" style="cursor: pointer;" title="Click to filter by FT50 papers"><span class="su-stat-label">FT50:</span> <strong>${qc.ft50}</strong></span>`);
      }
      if (show("filterUtd24") && (qc.utd24 || 0) > 0) {
        const isActive = activeFilter === "utd24";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-utd24 ${isActive ? 'su-filter-active' : ''}" data-filter="utd24" style="cursor: pointer;" title="Click to filter by UTD24 papers"><span class="su-stat-label">UTD24:</span> <strong>${qc.utd24}</strong></span>`);
      }
      if (show("filterAbs4star") && (qc.abs4star || 0) > 0) {
        const isActive = activeFilter === "abs4star";
        filterBadgeCount += 1;
        filterParts.push(`<span class="su-stat-badge su-badge su-abs4star ${isActive ? 'su-filter-active' : ''}" data-filter="abs4star" style="cursor: pointer;" title="Click to filter by ABS 4* papers"><span class="su-stat-label">ABS 4*:</span> <strong>${qc.abs4star}</strong></span>`);
      }
      if (activeFilter && show("filterClear")) {
        filterParts.push(`<span class="su-stat-badge su-badge" style="cursor: pointer; opacity: 0.7;" data-filter="clear" title="Click to clear filter">Clear filter</span>`);
      }
      if (filterParts.length) groups.push(filterParts);
    }

    const sortParts = [];
    if (show("sortToggle") && (window.suState?.authorFeatureToggles?.velocityBadge !== false)) {
      const sortByVelocity = !!window.suAuthorSortByVelocity;
      sortParts.push(`<span id="su-sort-toggle" class="su-stat-badge su-badge" data-sort-toggle="1" style="cursor: pointer;" title="Toggle sort by citations per year">${sortByVelocity ? "Sort: citations/yr ✓" : "Sort by citations/yr"}</span>`);
    }
    if (sortParts.length) groups.push(sortParts);

    const viewParts = [];
    if (show("positionFilters")) {
      const posFilter = window.suAuthorPositionFilter || "all";
      const posOptions = [
        { value: "all", label: "All" },
        { value: "first", label: "1st only" },
        { value: "last", label: "Last only" },
        { value: "middle", label: "Middle only" },
        { value: "first+last", label: "1st+Last" },
        { value: "first+middle+last", label: "1st+Mid+Last" }
      ];
      viewParts.push('<span class="su-stat-item"><span class="su-stat-label">View:</span></span>');
      for (const opt of posOptions) {
        const active = posFilter === opt.value ? " su-filter-active" : "";
        viewParts.push(`<span class="su-stat-badge su-badge su-position-filter${active}" data-position-filter="${opt.value}" style="cursor: pointer;" title="Show stats for ${opt.label}">${opt.label}</span>`);
      }
    }
    if (viewParts.length) groups.push(viewParts);

    const statsParts = [];
    if (show("papers") && (stats.totalPublications || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">Papers:</span> <strong>${stats.totalPublications}</strong></span>`);
    }
    if (show("citations") && (stats.totalCitations || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">Citations:</span> <strong>${stats.totalCitations}</strong></span>`);
    }
    if (show("firstAuthor") && (stats.firstAuthor || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">1st Author:</span> <strong>${stats.firstAuthor}</strong></span>`);
    }
    if (show("firstAuthorCites") && (stats.firstAuthorCitations || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">1st Author Cites:</span> <strong>${stats.firstAuthorCitations}</strong></span>`);
    }
    if (show("solo") && (stats.soloAuthored || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">Solo:</span> <strong>${stats.soloAuthored}</strong></span>`);
    }
    if (show("soloCites") && (stats.soloCitations || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">Solo Cites:</span> <strong>${stats.soloCitations}</strong></span>`);
    }
    const metricsItems = [];
    if ((stats.hIndex || 0) > 0) {
      const tip = getAuthorStatTooltipHtml("hindex", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="hindex"><span class="su-stat-label">h-index:</span> <strong>${stats.hIndex}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.mIndex != null && stats.mIndex > 0) {
      const mDisplay = stats.mIndex >= 10 ? Math.round(stats.mIndex) : (stats.mIndex % 1 === 0 ? stats.mIndex : stats.mIndex.toFixed(2));
      const tip = getAuthorStatTooltipHtml("mindex", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="mindex"><span class="su-stat-label">m-index:</span> <strong>${mDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.lIndex != null && stats.lIndex >= 0) {
      const lDisplay = stats.lIndex >= 10 ? Math.round(stats.lIndex) : (stats.lIndex % 1 === 0 ? stats.lIndex : stats.lIndex.toFixed(2));
      const tip = getAuthorStatTooltipHtml("lindex", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="lindex"><span class="su-stat-label">L-index:</span> <strong>${lDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.eigenfactorStyle != null && stats.eigenfactorStyle > 0) {
      const efDisplay = stats.eigenfactorStyle >= 1000 ? Math.round(stats.eigenfactorStyle) : (stats.eigenfactorStyle % 1 === 0 ? stats.eigenfactorStyle : stats.eigenfactorStyle.toFixed(2));
      const tip = getAuthorStatTooltipHtml("eigenfactor", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="eigenfactor"><span class="su-stat-label">EF-style:</span> <strong>${efDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.gIndex != null && stats.gIndex > 0) {
      const tip = getAuthorStatTooltipHtml("gindex", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="gindex"><span class="su-stat-label">g-index:</span> <strong>${stats.gIndex}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.careerAge != null && stats.careerAge >= 0) {
      const tip = getAuthorStatTooltipHtml("careerAge", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="careerAge"><span class="su-stat-label">Career age:</span> <strong>${stats.careerAge}</strong> yr<span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.avgTeamSize != null && stats.avgTeamSize > 0) {
      const tip = getAuthorStatTooltipHtml("avgTeamSize", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="avgTeamSize"><span class="su-stat-label">Avg team size:</span> <strong>${stats.avgTeamSize}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.citationGini != null && stats.citationGini >= 0) {
      const tip = getAuthorStatTooltipHtml("gini", stats);
      const giniDisplay = stats.citationGini % 1 === 0 ? stats.citationGini : stats.citationGini.toFixed(2);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="gini"><span class="su-stat-label">Citation Gini:</span> <strong>${giniDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.citationHalfLifeYear != null) {
      const tip = getAuthorStatTooltipHtml("citationHalfLife", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="citationHalfLife"><span class="su-stat-label">Citation half-life:</span> <strong>${stats.citationHalfLifeYear}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.eIndex != null && stats.eIndex > 0) {
      const tip = getAuthorStatTooltipHtml("eindex", stats);
      const eDisplay = stats.eIndex >= 10 ? Math.round(stats.eIndex) : (stats.eIndex % 1 === 0 ? stats.eIndex : stats.eIndex.toFixed(2));
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="eindex"><span class="su-stat-label">e-index:</span> <strong>${eDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.hCoreShare != null && stats.hCoreShare >= 0) {
      const tip = getAuthorStatTooltipHtml("hCoreShare", stats);
      const pct = Math.round(stats.hCoreShare * 1000) / 10;
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="hCoreShare"><span class="su-stat-label">h-core share:</span> <strong>${pct}%</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    const selfCiteState = window.__suSelfCiteState || { key: null, loading: false, data: null };
    const selfData = selfCiteState.data;
    if (selfCiteState.loading) {
      const tip = "Estimating self-citation rate using OpenAlex for the top cited works.";
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="selfcite"><span class="su-stat-label">Self-cite (est.):</span> <strong>…</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
      metricsItems.push(`<span class="su-stat-item"><button type="button" class="su-metrics-mini-btn" data-selfcite-refresh="1">Refresh self-cite</button></span>`);
    } else if (selfData && selfData.status === "success") {
      const pct = Math.round(selfData.rate * 1000) / 10;
      const tip = `Definition: a self-citation is when a citing work shares any author with the cited work.<br><br>Method: match the author to OpenAlex, take the top ${selfData.sampleWorks} most-cited works, then compute self-citations among citing works.<br><br>Data source: ${selfData.source}.`;
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="selfcite"><span class="su-stat-label">Self-cite (est.):</span> <strong>${pct}%</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
      metricsItems.push(`<span class="su-stat-item"><button type="button" class="su-metrics-mini-btn" data-selfcite-refresh="1">Refresh self-cite</button></span>`);
    } else if (selfData && selfData.status === "error") {
      const tip = escapeHtml(selfData.message || "Unavailable");
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="selfcite"><span class="su-stat-label">Self-cite (est.):</span> <strong>—</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
      metricsItems.push(`<span class="su-stat-item"><button type="button" class="su-metrics-mini-btn" data-selfcite-refresh="1">Retry self-cite</button></span>`);
    }
    if (stats.medianCitations != null && stats.medianCitations >= 0) {
      const tip = getAuthorStatTooltipHtml("medianCitations", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="medianCitations"><span class="su-stat-label">Median cites/paper:</span> <strong>${stats.medianCitations}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.meanCitations != null && stats.meanCitations >= 0) {
      const tip = getAuthorStatTooltipHtml("meanCitations", stats);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="meanCitations"><span class="su-stat-label">Mean cites/paper:</span> <strong>${stats.meanCitations}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.consistencyIndex != null && stats.consistencyIndex >= 0) {
      const tip = getAuthorStatTooltipHtml("consistencyIndex", stats);
      const cvDisplay = stats.consistencyIndex % 1 === 0 ? stats.consistencyIndex : stats.consistencyIndex.toFixed(2);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="consistencyIndex"><span class="su-stat-label">Consistency (CV):</span> <strong>${cvDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.flpIndex != null && stats.flpIndex >= 0) {
      const fPct = stats.firstAuthorPct != null ? stats.firstAuthorPct : 0;
      const tip = `FLP = N × (F%)²<br>N = ${stats.totalPublications || 0}<br>F% = ${fPct.toFixed(2)}`;
      const flpDisplay = stats.flpIndex % 1 === 0 ? stats.flpIndex : stats.flpIndex.toFixed(2);
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="flpIndex"><span class="su-stat-label">FLP Index:</span> <strong>${flpDisplay}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    if (stats.hIndexGrowth != null) {
      const tip = getAuthorStatTooltipHtml("hIndexGrowth", stats);
      const sign = stats.hIndexGrowth >= 0 ? "+" : "";
      metricsItems.push(`<span class="su-stat-item su-stat-item-with-tooltip su-metrics-row" data-stat-tooltip="hIndexGrowth"><span class="su-stat-label">Δh (12 mo):</span> <strong>${sign}${stats.hIndexGrowth}</strong><span class="su-author-stat-tooltip">${tip}</span></span>`);
    }
    const metricsDropdownHtml =
      metricsItems.length > 0
        ? `<span class="su-stat-item su-metrics-dropdown">
        <button type="button" class="su-metrics-trigger" aria-expanded="false" aria-haspopup="true">Metrics (${metricsItems.length}) ▼</button>
        <div class="su-metrics-panel" role="menu"><div class="su-metrics-panel-scroll">${metricsItems.join("")}</div></div>
      </span>`
        : "";
    const panelsDropdownHtml = `<span class="su-stat-item su-metrics-dropdown su-panels-dropdown su-view-settings-dropdown">
        <button type="button" class="su-metrics-trigger su-view-settings-trigger" aria-expanded="false" aria-haspopup="true">View Settings ▼</button>
        <div class="su-metrics-panel" role="menu">
          <div class="su-metrics-panel-scroll">
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="citedBy" ${featureToggles.citedBy !== false ? "checked" : ""} />
              Enhanced cited‑by table
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="coauthors" ${featureToggles.coauthors !== false ? "checked" : ""} />
              Co‑author insights
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="graph" ${featureToggles.graph !== false ? "checked" : ""} />
              Citation Atlas
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="velocityBadge" ${featureToggles.velocityBadge !== false ? "checked" : ""} />
              Citations/yr badges
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="venues" ${featureToggles.venues !== false ? "checked" : ""} />
              Top venues
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="citationBands" ${featureToggles.citationBands !== false ? "checked" : ""} />
              Citation bands
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="topics" ${featureToggles.topics !== false ? "checked" : ""} />
              Topics
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="filterBadges" ${featureToggles.filterBadges !== false ? "checked" : ""} />
              Badge filters
            </label>
            <label class="su-panel-toggle">
              <input type="checkbox" data-panel-toggle="researchIntel" ${featureToggles.researchIntel !== false ? "checked" : ""} />
              Research intelligence
            </label>
          </div>
        </div>
      </span>`;

    // Co-author stats
    if (show("coauthors") && (stats.uniqueCoAuthors || 0) > 0) {
      statsParts.push(`<span class="su-stat-item"><span class="su-stat-label">Co-authors:</span> <strong>${stats.uniqueCoAuthors}</strong></span>`);
    }

    // Collaboration shape: % solo / % first / % middle / % last
    if (show("collabShape") && (stats.totalPublications || 0) > 0) {
      const total = stats.totalPublications || 1;
      const soloPct = Math.round(((stats.soloAuthored || 0) / total) * 100);
      const firstPct = Math.round(((stats.firstAuthor || 0) / total) * 100);
      let middlePct = Math.round(((stats.middleAuthor || 0) / total) * 100);
      let lastPct = Math.round(((stats.lastAuthor || 0) / total) * 100);
      const sumPct = soloPct + firstPct + middlePct + lastPct;
      if (sumPct !== 100) lastPct = Math.max(0, lastPct + (100 - sumPct));
      statsParts.push(`
        <span class="su-stat-item su-collab-shape" title="Collaboration shape: % solo, % first-author, % middle-author">
          <span class="su-stat-label">Collab:</span>
          <span class="su-collab-legend">${soloPct}% solo · ${firstPct}% first · ${middlePct}% middle · ${lastPct}% last</span>
        </span>
      `);
    }

    if (show("drift") && stats.authorshipDrift) {
      statsParts.push(`
        <span class="su-stat-item su-stat-item-with-tooltip" data-stat-tooltip="1">
          <span class="su-stat-label">Drift:</span>
          <strong>${stats.authorshipDrift.label}</strong>
          <span class="su-author-stat-tooltip">
            <div class="su-tooltip-title">Authorship drift</div>
            <div>Compares early vs. late publications to see how your role mix changes over time.</div>
            <div>We split your papers in half by year (early vs. late) and compute the share of roles: solo, first, middle, last.</div>
            <div><strong>Drift</strong> reports the dominant role in each half (e.g., “first → middle”), or “stable” if there’s no meaningful shift.</div>
            <div class="su-tooltip-muted">${stats.authorshipDrift.detail}</div>
          </span>
        </span>
      `);
    }

    if (statsParts.length) groups.push(statsParts);
    const hasFilterBadges = filterBadgeCount > 0;
    for (const group of groups) {
      if (!group.length) continue;
      if (parts.length > 0) parts.push('<span class="su-stat-separator">|</span>');
      parts.push(...group);
    }

    // Close the stats row and add co-author table
    if (parts.length > 0) {
      statsContainer.style.display = "block";
      const coStats = Array.isArray(stats.coAuthorStats) ? stats.coAuthorStats : [];
      const coSort = window.suCoauthorSort || { key: "count", dir: "desc" };
      window.suCoauthorSort = coSort;
      const sortKey = coSort.key || "count";
      const sortDir = coSort.dir === "asc" ? "asc" : "desc";
      const sortedCo = coStats.slice().sort((a, b) => {
        if (sortKey === "name") {
          const cmp = String(a?.name || "").localeCompare(String(b?.name || ""));
          return sortDir === "asc" ? cmp : -cmp;
        }
        const va = Number(a?.[sortKey]) || 0;
        const vb = Number(b?.[sortKey]) || 0;
        if (va === vb) return String(a?.name || "").localeCompare(String(b?.name || ""));
        return sortDir === "asc" ? va - vb : vb - va;
      });
      const coauthorDisplayLimit = 10;
      const topCoAuthors = sortedCo.slice(0, coauthorDisplayLimit);
      const sortArrow = (k) => (sortKey === k ? (sortDir === "asc" ? " ▲" : " ▼") : "");
      const topics = Array.isArray(stats.topTitleTokens) ? stats.topTitleTokens.slice(0, 10) : [];
      const maxTopicCount = topics.length ? Math.max(...topics.map((t) => Number(t.count) || 1)) : 1;
      const activeTopicList = normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter);
      const activeTopicSet = new Set(activeTopicList);
      const topicHtml = featureToggles.topics !== false && topics.length
        ? `<div class="su-topic-tags"><span class="su-topic-label">Topics:</span>${topics.map((t) => {
            const count = Number(t.count) || 1;
            const weight = maxTopicCount ? count / maxTopicCount : 1;
            const opacity = Math.max(0.6, Math.min(1, 0.6 + weight * 0.4));
            const display = escapeHtml(t.display || t.token || "");
            const token = String(t.token || "").toLowerCase();
            const isActive = activeTopicSet.has(token);
            const title = `${count} title${count === 1 ? "" : "s"} · click to filter`;
            return `<span class="su-topic-tag${isActive ? " su-topic-tag-active" : ""}" data-topic-filter="1" data-topic-token="${escapeHtml(token)}" style="opacity:${opacity.toFixed(2)}" title="${title}">${display}</span>`;
          }).join("")}${activeTopicList.length ? `<span class="su-topic-tag su-topic-tag-clear" data-topic-clear="1" title="Clear topic filters">Clear</span>` : ""}</div>`
        : "";

      const topVenues = Array.isArray(stats.topVenues) ? stats.topVenues.slice(0, 3) : [];
      const activeVenue = String(window.suAuthorVenueFilter || "").trim().toLowerCase();
      const topVenueHtml = topVenues.length
        ? `<div class="su-top-venues"><span class="su-top-venues-label">Top venues:</span>${topVenues.map(v => {
            const label = normalizeVenueDisplay(String(v.venue || "").trim());
            const count = Number(v.count) || 0;
            const key = String(v.key || label.toLowerCase());
            const isActive = activeVenue && key === activeVenue;
            const display = `${label} (${count})`;
            return `<span class="su-top-venue-chip${isActive ? " su-top-venue-chip-active" : ""}" data-venue-filter="${escapeHtml(key)}" title="${escapeHtml(display)}"><span class="su-top-venue-text">${escapeHtml(display)}</span></span>`;
          }).join("")}${activeVenue ? `<span class="su-top-venue-chip su-top-venue-chip-clear" data-venue-filter="" title="Clear venue filter"><span class="su-top-venue-text">Clear</span></span>` : ""}</div>`
        : "";
      const selfCiteHtml = "";
      const popReportBtn = showResearchIntel
        ? `<button type="button" class="su-compare-authors-btn" data-pop-report="1">PoP report</button>`
        : "";

      const activeCoauthor = String(window.suAuthorCoauthorFilter || "").trim();
      const coauthorInsights = window.suState?.authorFeatureToggles?.coauthors !== false;
      const coauthorQuery = coauthorInsights ? String(window.suCoauthorQuery || "") : "";
      const activeSortKey = window.suCoauthorSort?.key || "count";
      const coauthorsHtml = coauthorInsights && topCoAuthors.length > 0 ? `
        <div class="su-coauthors-table">
          <div class="su-coauthors-header">Top Collaborators</div>
          ${coauthorInsights ? `<div class="su-coauthors-controls">
            <input type="search" id="su-coauthor-search" class="su-coauthor-search" placeholder="Search co-authors" value="${escapeHtml(coauthorQuery)}" />
            <div class="su-coauthor-sort-row">
              ${[
                { key: "score", label: "Rank" },
                { key: "count", label: "Papers" },
                { key: "citations", label: "Cites" },
                { key: "recent", label: "Recent" }
              ].map((opt) => {
                const active = activeSortKey === opt.key ? " su-coauthor-sort-active" : "";
                return `<button type="button" class="su-coauthor-sort-chip${active}" data-coauthor-sort="${opt.key}">${opt.label}</button>`;
              }).join("")}
            </div>
          </div>` : ""}
          <table class="su-coauthors-table-inner">
          <colgroup>
            <col class="su-co-col-rank" />
            <col class="su-co-col-author" />
            <col class="su-co-col-papers" />
            <col class="su-co-col-cites" />
            <col class="su-co-col-h" />
          </colgroup>
          <thead>
            <tr>
              <th class="su-coauthor-rank"></th>
              <th class="su-coauthor-name" data-coauthor-sort="name" data-coauthor-label="Co-Author">Co-Author${sortArrow("name")}</th>
              <th class="su-coauthor-metric" data-coauthor-sort="count" data-coauthor-label="Co-Papers">Co-Papers${sortArrow("count")}</th>
              <th class="su-coauthor-metric" data-coauthor-sort="citations" data-coauthor-label="Co-Cites">Co-Cites${sortArrow("citations")}</th>
              <th class="su-coauthor-metric" data-coauthor-sort="hIndex" data-coauthor-label="Co-h">Co-h${sortArrow("hIndex")}</th>
              
            </tr>
          </thead>
          <tbody>
            ${topCoAuthors.map((coAuthor, idx) => {
              const coKey = normalizeAuthorName(String(coAuthor?.name ?? ""));
              const isActive = activeCoauthor && coKey === activeCoauthor;
              return `
              <tr>
                <td class="su-coauthor-rank">${idx + 1}.</td>
                <td class="su-coauthor-name${isActive ? " su-coauthor-name-active" : ""}" data-coauthor-filter="${escapeHtml(coKey)}">${String(coAuthor?.name ?? "")}</td>
                <td class="su-coauthor-count"><strong>${Number(coAuthor?.count) || 0}</strong></td>
                <td class="su-coauthor-cites"><strong>${Number(coAuthor?.citations) || 0}</strong></td>
                <td class="su-coauthor-h"><strong>${Number(coAuthor?.hIndex) || 0}</strong></td>
                
              </tr>
            `;
            }).join("")}
          </tbody>
        </table>
        </div>
      ` : "";

      statsContainer.innerHTML = `
        ${hasFilterBadges ? '<div class="su-filter-hint">Click a badge to filter papers. If it doesn’t respond, try clicking again.</div>' : ''}
        <div class="su-stats-row">${parts.join("")}</div>
        ${topicHtml}
        <div class="su-compare-authors-row"><button type="button" class="su-compare-authors-btn" id="su-compare-authors-btn">Compare authors</button><button type="button" class="su-compare-authors-btn" data-author-export="1">Download CSV</button>${popReportBtn}${metricsDropdownHtml}${panelsDropdownHtml}</div>
      `;
      window.suLastCoauthorsHtml = coauthorsHtml || "";
      renderRightPanel(stats, coauthorsHtml);
      applyAuthorFeatureToggles(window.suState);
      ensureCitedByChartObserver();
      if (window.suKeepViewSettingsOpen) {
        const dropdown = statsContainer.querySelector(".su-view-settings-dropdown");
        const trigger = dropdown?.querySelector(".su-view-settings-trigger");
        const panel = dropdown?.querySelector(".su-metrics-panel");
        if (dropdown && trigger && panel) {
          dropdown.classList.add("su-metrics-open");
          trigger.setAttribute("aria-expanded", "true");
          const rect = trigger.getBoundingClientRect();
          panel.style.left = rect.left + "px";
          panel.style.top = (rect.bottom + 4) + "px";
          requestAnimationFrame(() => {
            const pr = panel.getBoundingClientRect();
            if (pr.right > window.innerWidth - 8) {
              panel.style.left = Math.max(8, window.innerWidth - pr.width - 8) + "px";
            }
            if (pr.bottom > window.innerHeight - 8) {
              panel.style.top = Math.max(8, rect.top - pr.height - 4) + "px";
            }
          });
        }
        window.suKeepViewSettingsOpen = false;
      }
      // Self-citation estimate handled via metrics dropdown.
      
    } else {
      statsContainer.style.display = "none";
    }
    } catch (_) {
      statsContainer.style.display = "none";
    }
  }

  function enhanceCitedByChart() {
    const container = document.getElementById("gsc_rsb_cit");
    if (!container) return;
    const bars = Array.from(container.querySelectorAll(".gsc_g_a"));
    if (!bars.length) return;
    container.classList.add("su-citedby-enhanced");
    const theme = document.body?.getAttribute("data-su-theme") || "light";
    const isDark = theme === "dark";
    const scheme = window.suState?.citedByColorScheme || DEFAULT_CITEDBY_COLOR_SCHEME;
    ensureCitedByColorMenu(container, scheme);
    const widthScale = 1.5;
    const heights = bars.map((bar) => bar.getBoundingClientRect().height || 0);
    const maxH = Math.max(...heights, 0);
    const minH = Math.min(...heights.filter((h) => h > 0), maxH || 0);
    const getGradient = (t) => {
      const clamp = (v) => Math.max(0, Math.min(1, v));
      const mix = (a, b, x) => a + (b - a) * x;
      const tt = clamp(t);
      let hue;
      let sat = 78;
      if (scheme === "red-green") {
        hue = Math.round(120 * (1 - tt));
      } else if (scheme === "sunset") {
        hue = Math.round(45 * (1 - tt));
        sat = 80;
      } else if (scheme === "purple-teal") {
        hue = Math.round(180 + 100 * tt);
        sat = 70;
      } else if (scheme === "slate") {
        hue = 215;
        sat = 10;
      } else {
        // red-blue (default)
        hue = Math.round(210 * (1 - tt));
      }
      const baseMid = isDark ? 40 : 46;
      const lightMid = mix(baseMid + 4, baseMid - 2, tt);
      const lightTop = Math.min(isDark ? 62 : 68, lightMid + 12);
      const lightBot = Math.max(isDark ? 24 : 30, lightMid - 12);
      const top = `hsl(${hue}, ${sat}%, ${lightTop}%)`;
      const bot = `hsl(${hue}, ${sat}%, ${lightBot}%)`;
      const border = `hsl(${hue}, ${sat}%, ${Math.max(isDark ? 20 : 26, lightBot - 6)}%)`;
      return { top, bot, border };
    };
    bars.forEach((bar) => {
      if (bar.dataset.suOrigRight === undefined) {
        bar.dataset.suOrigRight = bar.style.right || "";
      }
      if (bar.dataset.suOrigLeft === undefined) {
        bar.dataset.suOrigLeft = bar.style.left || "";
      }
      if (bar.dataset.suOrigWidth === undefined) {
        bar.dataset.suOrigWidth = bar.style.width || "";
      }
      if (bar.dataset.suOrigBg === undefined) {
        bar.dataset.suOrigBg = bar.style.background || "";
      }
      if (bar.dataset.suOrigBorder === undefined) {
        bar.dataset.suOrigBorder = bar.style.borderColor || "";
      }
      bar.classList.add("su-citedby-bar");
      bar.classList.remove(
        "su-citedby-recent-1",
        "su-citedby-recent-2",
        "su-citedby-recent-3",
        "su-citedby-recent-4",
        "su-citedby-recent-5"
      );
      if (bar.dataset.suSlimScale !== String(widthScale)) {
        const style = window.getComputedStyle(bar);
        const w = parseFloat(style.width || "0") || 15;
        const newW = Math.max(12, w * widthScale);
        const styleRight = style.right;
        const styleLeft = style.left;
        const inlineRight = bar.style.right;
        const inlineLeft = bar.style.left;
        const rightDefined = inlineRight || (styleRight && styleRight !== "auto" && styleLeft === "auto");
        const leftDefined = inlineLeft || (styleLeft && styleLeft !== "auto" && styleRight === "auto");
        const canUseRight = !!rightDefined && !leftDefined;
        const canUseLeft = !!leftDefined && !canUseRight;
        if (canUseRight) {
          const rightVal = parseFloat(inlineRight || styleRight || "0");
          if (Number.isFinite(rightVal)) {
            const newRight = rightVal + (w - newW) / 2;
            bar.style.right = `${newRight}px`;
          }
        } else if (canUseLeft) {
          const leftVal = parseFloat(inlineLeft || styleLeft || "0");
          if (Number.isFinite(leftVal)) {
            const newLeft = leftVal - (w - newW) / 2;
            bar.style.left = `${newLeft}px`;
          }
        }
        bar.style.width = `${newW}px`;
        bar.dataset.suSlimScale = String(widthScale);
      }
      // Dynamic color scale: best years (tallest bars) → red, worst years → blue/green.
      const h = bar.getBoundingClientRect().height || 0;
      const t = maxH > minH ? Math.max(0, Math.min(1, (h - minH) / (maxH - minH))) : 0.5;
      const grad = getGradient(t);
      bar.style.setProperty("background", `linear-gradient(180deg, ${grad.top}, ${grad.bot})`, "important");
      bar.style.setProperty("border-color", grad.border, "important");
      bar.dataset.suTheme = isDark ? "dark" : "light";
    });
    const lastFive = bars.slice(-5);
    lastFive.forEach((bar, idx) => {
      bar.classList.add(`su-citedby-recent-${idx + 1}`);
    });
    adjustCitedByScroll(container);
  }

  function resetCitedByChart() {
    const container = document.getElementById("gsc_rsb_cit");
    if (!container) return;
    const bars = Array.from(container.querySelectorAll(".gsc_g_a"));
    bars.forEach((bar) => {
      bar.classList.remove(
        "su-citedby-bar",
        "su-citedby-recent-1",
        "su-citedby-recent-2",
        "su-citedby-recent-3",
        "su-citedby-recent-4",
        "su-citedby-recent-5"
      );
      if (bar.dataset.suOrigRight !== undefined) {
        bar.style.right = bar.dataset.suOrigRight;
      } else {
        bar.style.right = "";
      }
      if (bar.dataset.suOrigLeft !== undefined) {
        bar.style.left = bar.dataset.suOrigLeft;
      } else {
        bar.style.left = "";
      }
      if (bar.dataset.suOrigWidth !== undefined) {
        bar.style.width = bar.dataset.suOrigWidth;
      } else {
        bar.style.width = "";
      }
      if (bar.dataset.suOrigBg !== undefined) {
        bar.style.background = bar.dataset.suOrigBg;
      } else {
        bar.style.background = "";
      }
      if (bar.dataset.suOrigBorder !== undefined) {
        bar.style.borderColor = bar.dataset.suOrigBorder;
      } else {
        bar.style.borderColor = "";
      }
      delete bar.dataset.suSlimScale;
    });
    container.classList.remove("su-citedby-enhanced");
    const histWrap = container.querySelector(".gsc_md_hist_w");
    const histBody = histWrap?.querySelector(".gsc_md_hist_b");
    if (histBody) histBody.style.minWidth = "";
  }

  function resetCitedByStats() {
    const container = document.getElementById("gsc_rsb_cit");
    if (!container) return;
    const table = container.querySelector("#gsc_rsb_st");
    if (!table) return;
    table.querySelectorAll('[data-su-delta="1"]').forEach((el) => el.remove());
    table.querySelectorAll(".su-citedby-num").forEach((el) => {
      el.classList.remove("su-citedby-num", "su-citedby-delta");
    });
    table.querySelectorAll(".su-citedby-metric-badge").forEach((el) => {
      el.classList.remove("su-citedby-metric-badge");
    });
    table.classList.remove("su-citedby-table-enhanced");
  }

  function applyAuthorFeatureToggles(state) {
    const toggles = state?.authorFeatureToggles || DEFAULT_AUTHOR_FEATURE_TOGGLES;
    if (toggles.citedBy !== false) {
      enhanceCitedByChart();
      enhanceCitedByStats();
    } else {
      resetCitedByChart();
      resetCitedByStats();
    }
    updateCoauthorTable(window.suFullAuthorStats);
  }

  function enhanceCitedByStats() {
    const container = document.getElementById("gsc_rsb_cit");
    if (!container) return;
    const table = container.querySelector("#gsc_rsb_st");
    if (!table) return;
    const headerCells = table.querySelectorAll("thead th");
    const allLabelRaw = headerCells[1] ? text(headerCells[1]) : "All";
    const sinceLabelRaw = headerCells[2] ? text(headerCells[2]) : "Since";
    const allLabel = allLabelRaw?.trim() || "All";
    const sinceLabel = sinceLabelRaw?.trim() || "Since";
    const rows = Array.from(table.querySelectorAll("tbody tr"));
    if (!rows.length) return;

    const toNumber = (val) => {
      const n = parseInt(String(val || "").replace(/[^0-9]/g, ""), 10);
      return Number.isFinite(n) ? n : null;
    };
    const fmtPct = (n) => {
      if (!Number.isFinite(n)) return "—";
      const rounded = n >= 10 ? Math.round(n) : Math.round(n * 10) / 10;
      return `${rounded}%`;
    };

    const existingDeltaHeader = table.querySelector('thead th[data-su-delta="1"]');
    if (!existingDeltaHeader) {
      const deltaTh = document.createElement("th");
      deltaTh.className = "gsc_rsb_sth su-citedby-delta-head";
      deltaTh.dataset.suDelta = "1";
      deltaTh.title = `${sinceLabel} as % of ${allLabel}`;
      deltaTh.textContent = "Δ%";
      table.querySelector("thead tr")?.appendChild(deltaTh);
    }

    rows.forEach((row) => {
      const labelCell = row.querySelector(".gsc_rsb_sc1");
      if (labelCell) {
        const labelLink = labelCell.querySelector("a");
        if (labelLink) labelLink.classList.add("su-citedby-metric-badge");
      }
      const values = row.querySelectorAll(".gsc_rsb_std");
      if (values[0]) values[0].classList.add("su-citedby-num");
      if (values[1]) values[1].classList.add("su-citedby-num");
      if (row.querySelector('td[data-su-delta="1"]')) return;
      const allVal = toNumber(values[0]?.textContent);
      const sinceVal = toNumber(values[1]?.textContent);
      const ratio = allVal && sinceVal != null ? (sinceVal / allVal) * 100 : null;
      const deltaTd = document.createElement("td");
      deltaTd.className = "gsc_rsb_std su-citedby-num su-citedby-delta";
      deltaTd.dataset.suDelta = "1";
      deltaTd.textContent = fmtPct(ratio);
      row.appendChild(deltaTd);
    });

    const cards = container.querySelector("#su-citedby-cards");
    if (cards) cards.remove();
    table.classList.remove("su-citedby-hidden");
    table.removeAttribute("aria-hidden");
    table.classList.add("su-citedby-table-enhanced");
    container.classList.add("su-citedby-enhanced");
  }

  function adjustCitedByScroll(container) {
    const histWrap = container.querySelector(".gsc_md_hist_w");
    const histBody = histWrap?.querySelector(".gsc_md_hist_b");
    if (!histWrap || !histBody) return;
    const items = Array.from(histBody.querySelectorAll(".gsc_g_a, .gsc_g_t"));
    if (!items.length) return;
    const bodyRect = histBody.getBoundingClientRect();
    let maxRight = 0;
    for (const el of items) {
      const rect = el.getBoundingClientRect();
      const span = rect.right - bodyRect.left;
      if (span > maxRight) maxRight = span;
    }
    const minWidth = Math.ceil(maxRight + 16);
    if (Number.isFinite(minWidth) && minWidth > histWrap.clientWidth) {
      histBody.style.minWidth = `${minWidth}px`;
    } else {
      histBody.style.minWidth = "";
    }
  }

  function ensureCitedByColorMenu(container, activeScheme) {
    if (!container) return;
    const existing = container.querySelector(".su-citedby-color-menu");
    const scheme = activeScheme || DEFAULT_CITEDBY_COLOR_SCHEME;
    const options = [
      { value: "red-blue", label: "Red → Blue" },
      { value: "red-green", label: "Red → Green" },
      { value: "sunset", label: "Red → Gold" },
      { value: "purple-teal", label: "Purple → Teal" },
      { value: "slate", label: "Slate" }
    ];
    const menu = existing || document.createElement("div");
    if (!existing) {
      menu.className = "su-citedby-color-menu";
      menu.innerHTML = `
        <label class="su-citedby-color-label" for="su-citedby-color-select">Colors</label>
        <select id="su-citedby-color-select" class="su-citedby-color-select">
          ${options.map((opt) => `<option value="${opt.value}">${opt.label}</option>`).join("")}
        </select>
      `;
      container.appendChild(menu);
    }
    const select = menu.querySelector(".su-citedby-color-select");
    if (select) {
      if (select.value !== scheme) select.value = scheme;
      if (!select.dataset.bound) {
        select.dataset.bound = "1";
        select.addEventListener("change", async (e) => {
          const next = String(e.target.value || DEFAULT_CITEDBY_COLOR_SCHEME);
          if (window.suState) window.suState.citedByColorScheme = next;
          const scholarId = new URL(window.location.href).searchParams.get("user");
          if (scholarId) await setAuthorCitedByColorScheme(scholarId, next);
          enhanceCitedByChart();
        });
      }
    }
  }

  function ensureCitedByChartObserver() {
    if (window.__suCitedByObserver) return;
    const container = document.getElementById("gsc_rsb_cit");
    if (!container) return;
    const observer = new MutationObserver(() => {
      applyAuthorFeatureToggles(window.suState);
    });
    observer.observe(container, { childList: true, subtree: true });
    window.__suCitedByObserver = observer;
  }

  function updateCoauthorTable(stats) {
    if (!stats) return;
    const panel = document.getElementById("su-right-panel");
    if (!panel) return;
    const table = panel.querySelector(".su-coauthors-table-inner");
    if (!table) return;
    const tbody = table.querySelector("tbody");
    if (!tbody) return;
    const coStatsRaw = Array.isArray(stats.coAuthorStats) ? stats.coAuthorStats : [];
    const insightsOn = window.suState?.authorFeatureToggles?.coauthors !== false;
    const query = insightsOn ? String(window.suCoauthorQuery || "").trim().toLowerCase() : "";
    const coStats = query
      ? coStatsRaw.filter((c) => String(c?.name || "").toLowerCase().includes(query))
      : coStatsRaw.slice();

    const coSort = window.suCoauthorSort || { key: "count", dir: "desc" };
    if (!insightsOn && (coSort.key === "score" || coSort.key === "recent")) {
      coSort.key = "count";
      coSort.dir = "desc";
    }
    window.suCoauthorSort = coSort;
    const sortKey = coSort.key || "count";
    const sortDir = coSort.dir === "asc" ? "asc" : "desc";

    const currentYear = new Date().getFullYear();
    const scoreRawByKey = new Map();
    let maxScoreRaw = 1;
    if (insightsOn) {
      for (const c of coStatsRaw) {
        const count = Number(c?.count) || 0;
        const cites = Number(c?.citations) || 0;
        const lastYear = Number(c?.lastYear) || null;
        const yearsSince = lastYear ? Math.max(0, currentYear - lastYear) : 10;
        const recencyScore = Math.max(0, 10 - yearsSince);
        const raw = (count * 2) + (Math.log1p(cites) * 4) + recencyScore;
        scoreRawByKey.set(c.key, raw);
        if (raw > maxScoreRaw) maxScoreRaw = raw;
      }
    }

    const getScore = (c) => {
      if (!insightsOn) return 0;
      const raw = scoreRawByKey.get(c.key) || 0;
      return Math.round((raw / maxScoreRaw) * 100);
    };

    const sortedCo = coStats.slice().sort((a, b) => {
      if (sortKey === "name") {
        const cmp = String(a?.name || "").localeCompare(String(b?.name || ""));
        return sortDir === "asc" ? cmp : -cmp;
      }
      if (sortKey === "recent") {
        const va = Number(a?.lastYear) || 0;
        const vb = Number(b?.lastYear) || 0;
        if (va === vb) return String(a?.name || "").localeCompare(String(b?.name || ""));
        return sortDir === "asc" ? va - vb : vb - va;
      }
      if (sortKey === "score") {
        const va = getScore(a);
        const vb = getScore(b);
        if (va === vb) return String(a?.name || "").localeCompare(String(b?.name || ""));
        return sortDir === "asc" ? va - vb : vb - va;
      }
      const va = Number(a?.[sortKey]) || 0;
      const vb = Number(b?.[sortKey]) || 0;
      if (va === vb) return String(a?.name || "").localeCompare(String(b?.name || ""));
      return sortDir === "asc" ? va - vb : vb - va;
    });

    const displayLimit = insightsOn && query ? 20 : 10;
    const topCoAuthors = sortedCo.slice(0, displayLimit);
    const activeCoauthor = String(window.suAuthorCoauthorFilter || "").trim();
    const sharedCache = stats.__sharedCoauthorsCache || new Map();
    stats.__sharedCoauthorsCache = sharedCache;

    const getMentorshipBadge = (coAuthor) => {
      const count = Number(coAuthor?.positionDeltaCount) || 0;
      const sum = Number(coAuthor?.positionDeltaSum) || 0;
      if (!count) return { label: "Collaborator", cls: "su-coauthor-type-collab", avg: null };
      const avg = sum / Math.max(1, count);
      const threshold = 0.15;
      const minCount = 2;
      if (count >= minCount && Math.abs(avg) >= threshold) {
        if (avg < 0) return { label: "Mentor", cls: "su-coauthor-type-mentor", avg };
        return { label: "Mentee", cls: "su-coauthor-type-mentee", avg };
      }
      return { label: "Collaborator", cls: "su-coauthor-type-collab", avg };
    };

    const getShared = (key, limit = 2) => {
      if (!insightsOn) return [];
      const cacheKey = `${key}|${limit}`;
      if (sharedCache.has(cacheKey)) return sharedCache.get(cacheKey);
      const sets = Array.isArray(stats.__coauthorPaperSets) ? stats.__coauthorPaperSets : [];
      const counts = new Map();
      for (const set of sets) {
        if (!Array.isArray(set) || !set.includes(key)) continue;
        for (const other of set) {
          if (other === key) continue;
          counts.set(other, (counts.get(other) || 0) + 1);
        }
      }
      const sorted = Array.from(counts.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, limit)
        .map(([k, c]) => {
          const name = stats.coAuthors?.get(k)?.name || k;
          return { key: k, name, count: c };
        });
      sharedCache.set(cacheKey, sorted);
      return sorted;
    };

    const getCoauthorLinkMap = () => {
      const map = new Map();
      const items = document.querySelectorAll(".gsc_rsb_a_desc a");
      items.forEach((a) => {
        const name = text(a).trim();
        const href = a.getAttribute("href") || "";
        if (!name || !href) return;
        map.set(normalizeAuthorName(name), href);
      });
      return map;
    };
    const getCoauthorAffiliationMap = () => {
      const map = new Map();
      const items = document.querySelectorAll(".gsc_rsb_a_desc");
      items.forEach((el) => {
        const link = el.querySelector("a");
        const name = link ? text(link).trim() : "";
        if (!name) return;
        let full = text(el).trim();
        let aff = full.replace(name, "").trim();
        aff = aff.replace(/^[-–—,]/, "").trim();
        if (aff) map.set(normalizeAuthorName(name), aff);
      });
      return map;
    };
    const coauthorLinks = getCoauthorLinkMap();
    const coauthorAffiliations = getCoauthorAffiliationMap();

    tbody.innerHTML = topCoAuthors.map((coAuthor, idx) => {
      const coKey = coAuthor.key || normalizeAuthorName(String(coAuthor?.name ?? ""));
      const isActive = activeCoauthor && coKey === activeCoauthor;
      const score = getScore(coAuthor);
      const lastYear = Number(coAuthor?.lastYear) || null;
      const firstYear = Number(coAuthor?.firstYear) || null;
      const recent = lastYear && (currentYear - lastYear <= 2);
      const relationBadge = getMentorshipBadge(coAuthor);
      const shared = getShared(coKey, 2);
      const sharedNames = shared.map((s) => String(s.name || "")).filter(Boolean);
      const sharedLabel = sharedNames.length ? `With ${sharedNames.join(", ")}` : "With —";
      const metaText = `Score ${score} · Last ${lastYear || "—"} · ${sharedLabel}`;
      const metaLine = metaText;
      const metaHtml = insightsOn ? `<div class="su-coauthor-meta-line ${recent ? "su-coauthor-recent" : ""}" title="${escapeHtml(metaLine)}">${escapeHtml(metaText)}</div>` : "";

      const roleCounts = coAuthor?.roleCounts || { solo: 0, first: 0, middle: 0, last: 0 };
      const roleLine = `Co-author roles: first ${Number(roleCounts.first) || 0}, middle ${Number(roleCounts.middle) || 0}, last ${Number(roleCounts.last) || 0}, solo ${Number(roleCounts.solo) || 0}`;
      const avgDelta = relationBadge.avg;
      const avgDeltaLabel = avgDelta == null ? null : `${avgDelta >= 0 ? "+" : ""}${avgDelta.toFixed(2)}`;
      const tooltipLines = [
        `Co-author: ${String(coAuthor?.name ?? "")}`,
        `Co-papers: ${Number(coAuthor?.count) || 0}`,
        `Co-cites: ${Number(coAuthor?.citations) || 0}`,
        `Co-h: ${Number(coAuthor?.hIndex) || 0}`,
        roleLine
      ];
      if (insightsOn) {
        if (avgDeltaLabel) {
          let direction = "near";
          if (avgDelta < -0.05) direction = "before";
          else if (avgDelta > 0.05) direction = "after";
          tooltipLines.push(`Avg position Δ: ${avgDeltaLabel} (${direction} you)`);
        }
        tooltipLines.push(`Score: ${score}`);
        tooltipLines.push(`__EQ__Score = 2×papers + 4×ln(1+cites) + max(0, 10 − years since last)`);
        tooltipLines.push(`First year: ${firstYear || "—"}`);
        tooltipLines.push(`Last year: ${lastYear || "—"}`);
        tooltipLines.push(`Shared with: ${sharedNames.length ? sharedNames.join(", ") : "—"}`);
      }
      const affiliation = coauthorAffiliations.get(coKey) || "";
      if (affiliation) tooltipLines.push(`Affiliation: ${affiliation}`);
      const rorKey = affiliation ? makeExternalKey("ror", affiliation) : null;
      const rorEntry = rorKey ? getExternalSignalEntry(rorKey, "ror") : null;
      if (insightsOn && affiliation && !rorEntry) enqueueExternalSignalFetch(rorKey, "ror", fetchRorSignal, affiliation);
      const ror = rorEntry?.data || null;
      if (ror?.name) {
        const country = ror.country ? ` (${ror.country})` : "";
        tooltipLines.push(`ROR: ${ror.name}${country}`);
      }
      const dblpKey = makeExternalKey("dblp", String(coAuthor?.name || ""));
      const dblpEntry = dblpKey ? getExternalSignalEntry(dblpKey, "dblp") : null;
      if (insightsOn && dblpKey && !dblpEntry) enqueueExternalSignalFetch(dblpKey, "dblp", fetchDblpAuthorSignal, String(coAuthor?.name || ""));
      const dblp = dblpEntry?.data || null;
      if (dblp?.url) {
        tooltipLines.push(`DBLP: ${dblp.url}`);
        const dblpCountEntry = getExternalSignalEntry(dblpKey, "dblp_sparql");
        if (insightsOn && !dblpCountEntry) enqueueExternalSignalFetch(dblpKey, "dblp_sparql", fetchDblpSparqlSignal, dblp.url);
        const dblpCount = dblpCountEntry?.data?.count;
        if (Number.isFinite(dblpCount) && dblpCount > 0) {
          tooltipLines.push(`DBLP pubs: ${dblpCount}`);
        }
      }
      const tooltipRaw = tooltipLines.join("\n");
      const tooltipAttr = escapeHtml(tooltipRaw).replace(/\n/g, "&#10;");

      const link = coauthorLinks.get(coKey);
      const safeName = escapeHtml(String(coAuthor?.name ?? ""));
      const nameHtml = link
        ? `<a class="su-coauthor-link" href="${escapeHtml(link)}" target="_blank" rel="noopener" title="${tooltipAttr}">${safeName}</a>`
        : safeName;

      return `
              <tr class="su-coauthor-row" data-coauthor-tooltip="${tooltipAttr}">
                <td class="su-coauthor-rank">${idx + 1}.</td>
                <td class="su-coauthor-name${isActive ? " su-coauthor-name-active" : ""}" data-coauthor-filter="${escapeHtml(coKey)}" data-coauthor-tooltip="${tooltipAttr}" title="${tooltipAttr}">
                  <div class="su-coauthor-name-text" title="${tooltipAttr}">${nameHtml}</div>
                  ${metaHtml}
                </td>
                <td class="su-coauthor-count"><strong>${Number(coAuthor?.count) || 0}</strong></td>
                <td class="su-coauthor-cites"><strong>${Number(coAuthor?.citations) || 0}</strong></td>
                <td class="su-coauthor-h"><strong>${Number(coAuthor?.hIndex) || 0}</strong></td>
              </tr>
      `;
    }).join("");

    table.querySelectorAll("th[data-coauthor-sort]").forEach((th) => {
      const label = th.dataset.coauthorLabel || String(th.textContent || "").replace(/[▲▼]/g, "").trim();
      th.dataset.coauthorLabel = label;
      const key = String(th.dataset.coauthorSort || "").trim();
      const arrow = key && key === sortKey ? (sortDir === "asc" ? " ▲" : " ▼") : "";
      th.textContent = `${label}${arrow}`;
    });

    if (insightsOn) {
      panel.querySelectorAll(".su-coauthor-sort-chip").forEach((btn) => {
        const key = String(btn.dataset.coauthorSort || "");
        btn.classList.toggle("su-coauthor-sort-active", key === sortKey);
      });

      const searchInput = panel.querySelector("#su-coauthor-search");
      if (searchInput && searchInput.value !== String(window.suCoauthorQuery || "")) {
        searchInput.value = String(window.suCoauthorQuery || "");
      }
    }
  }

  function renderRightPanel(stats, coauthorsHtml) {
    const sidebar = document.querySelector(".gsc_rsb") || document.getElementById("gsc_rsb");
    if (!sidebar) return;
    let container = document.getElementById("su-right-panel");
    if (!container) {
      container = document.createElement("div");
      container.id = "su-right-panel";
      container.className = "su-right-panel";
    }

    const citedBox = document.getElementById("gsc_rsb_cit") || sidebar.querySelector("#gsc_rsb_cit") || sidebar.querySelector("#gsc_rsb_st")?.closest(".gsc_rsb_s");
    const publicAccessBox = document.getElementById("gsc_rsb_mnd") || Array.from(sidebar.querySelectorAll(".gsc_rsb_s,div,section,table"))
      .find((el) => /public access/i.test(el.textContent || "")) || null;

    if (!container.parentElement) {
      if (citedBox) {
        citedBox.insertAdjacentElement("afterend", container);
      } else {
        sidebar.insertAdjacentElement("afterbegin", container);
      }
    }
    if (publicAccessBox) {
      publicAccessBox.remove();
    }

    const featureToggles = window.suState?.authorFeatureToggles || DEFAULT_AUTHOR_FEATURE_TOGGLES;
    const showVenues = featureToggles.venues !== false;
    const showBands = featureToggles.citationBands !== false;
    const showGraph = featureToggles.graph !== false;
    const topVenues = Array.isArray(stats.topVenues) ? stats.topVenues.slice(0, 3) : [];
    const activeVenue = String(window.suAuthorVenueFilter || "").trim().toLowerCase();
    const topVenueHtml = showVenues && topVenues.length
      ? `<div class="su-right-card">
          <div class="su-right-title">Top venues</div>
          <div class="su-right-chiplist">${topVenues.map(v => {
            const label = normalizeVenueDisplay(String(v.venue || "").trim());
            const count = Number(v.count) || 0;
            const key = String(v.key || label.toLowerCase());
            const isActive = activeVenue && key === activeVenue;
            const display = `${label} (${count})`;
            return `<span class="su-top-venue-chip${isActive ? " su-top-venue-chip-active" : ""}" data-venue-filter="${escapeHtml(key)}" title="${escapeHtml(display)}"><span class="su-top-venue-text">${escapeHtml(display)}</span></span>`;
          }).join("")}${activeVenue ? `<span class="su-top-venue-chip su-top-venue-chip-clear" data-venue-filter="" title="Clear venue filter"><span class="su-top-venue-text">Clear</span></span>` : ""}</div>
        </div>`
      : "";

    const bands = Array.isArray(stats.citationBands) ? stats.citationBands : [];
    const maxBand = bands.length ? Math.max(...bands.map((b) => b.count || 0)) : 1;
    const activeBand = window.suAuthorCitationBand || null;
    const bandsHtml = showBands && bands.length
      ? `<div class="su-right-card">
          <div class="su-right-title">Citation bands</div>
          <div class="su-right-bands">${bands.map((b) => {
            const w = maxBand ? Math.round((b.count / maxBand) * 100) : 0;
            const isActive = activeBand && activeBand.min === b.min && activeBand.max === b.max;
            return `<div class="su-right-band${isActive ? " su-right-band-active" : ""}" data-cite-band-min="${b.min}" data-cite-band-max="${Number.isFinite(b.max) ? b.max : ""}">
              <span class="su-right-band-label">${escapeHtml(b.label)}</span>
              <span class="su-right-band-bar"><span class="su-right-band-fill" style="width:${w}%"></span></span>
              <span class="su-right-band-count">${b.count}</span>
            </div>`;
          }).join("")}</div>
        </div>`
      : "";

    const graphState = window.suGraphState || null;
    const graphNodeCount = graphState?.nodes ? graphState.nodes.size : 0;
    const graphEdgeCount = graphState?.edges ? graphState.edges.length : 0;
    const graphHtml = showGraph
      ? `<div class="su-right-card su-graph-card">
          <div class="su-right-title">Citation Atlas</div>
          <div class="su-graph-card-body">
            <div class="su-graph-card-meta">${graphNodeCount ? `${graphNodeCount} nodes · ${graphEdgeCount} edges` : "Build a local citation map from your top papers."}</div>
            <div class="su-graph-card-actions">
              <button type="button" class="su-graph-btn" data-graph-open="1" title="Open the interactive citation map">Open map</button>
              <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-build="1" data-graph-build-count="10" title="Build a map from the 10 most-cited papers on this page">Build top 10</button>
            </div>
          </div>
        </div>`
      : "";

    const authorName = (window.suState?.authorVariations?.[0] || extractAuthorName() || "").trim();
    const authorVariations = window.suState?.authorVariations || (authorName ? generateAuthorNameVariations(authorName) : []);
    ensureGenealogyMatchAsync(authorName, authorVariations);
    const genealogyMatch = getGenealogyMatchState();
    const genealogyKey = genealogyMatch?.datasetKey || "merged";
    const genealogyLabel = GENEALOGY_SOURCES[genealogyKey]?.label || "Academic lineage";
    const genealogyHtml = genealogyMatch?.status === "ready" && genealogyMatch?.matchIndex != null
      ? `<div class="su-right-card su-lineage-card">
          <div class="su-right-title">Academic lineage</div>
          <div class="su-lineage-card-body">
            <div class="su-lineage-card-name">${escapeHtml(genealogyMatch.matchName || authorName)}</div>
            <div class="su-lineage-card-meta">${escapeHtml(genealogyLabel)} · advisor/student lineage.</div>
            <button type="button" class="su-graph-btn" data-lineage-open="1">View lineage</button>
          </div>
        </div>`
      : "";
    container.innerHTML = `
      ${graphHtml}
      ${genealogyHtml}
      ${coauthorsHtml || ""}
      ${topVenueHtml}
      ${bandsHtml}
    `;
  }

  const GRAPH_MAX_NODES = 90;
  const GRAPH_MAX_EDGES = 200;

  function hashString(str) {
    let h = 0;
    const s = String(str || "");
    for (let i = 0; i < s.length; i++) {
      h = (h << 5) - h + s.charCodeAt(i);
      h |= 0;
    }
    return (h >>> 0).toString(36);
  }

  async function mapWithConcurrency(items, limit, fn) {
    const results = [];
    const queue = Array.isArray(items) ? items.slice() : [];
    let active = 0;
    return await new Promise((resolve) => {
      const next = () => {
        if (!queue.length && active === 0) return resolve(results);
        while (active < limit && queue.length) {
          const item = queue.shift();
          const idx = results.length;
          results.push(null);
          active += 1;
          Promise.resolve()
            .then(() => fn(item, idx))
            .then((res) => { results[idx] = res; })
            .catch(() => { results[idx] = null; })
            .finally(() => { active -= 1; next(); });
        }
      };
      next();
    });
  }

  function createGraphState(authorId, authorName) {
    return {
      authorId,
      authorName,
      nodes: new Map(),
      edges: [],
      edgeKeys: new Set(),
      seedIds: new Set(),
      layout: new Map(),
      selectedId: null,
      view: "papers",
      loading: false,
      lastUpdated: null,
      concepts: [],
      conceptColors: new Map(),
      recommendations: [],
      authorGraph: null,
      collections: [],
      viewTransforms: {},
      annotations: {},
      alerts: [],
      unreadCount: 0,
      monitorEnabled: false,
      lastMonitorCheck: null
    };
  }

  function serializeGraphState(graph) {
    if (!graph) return null;
    const nodes = Array.from(graph.nodes.values()).map((n) => {
      const pos = graph.layout.get(n.id);
      return { ...n, _pos: pos || null };
    });
    return {
      nodes,
      edges: graph.edges || [],
      seedIds: Array.from(graph.seedIds || []),
      selectedId: graph.selectedId || null,
      view: graph.view || "papers",
      lastUpdated: graph.lastUpdated || null,
      viewTransforms: graph.viewTransforms || {},
      annotations: graph.annotations || {}
    };
  }

  function deserializeGraphState(raw, authorId, authorName) {
    const graph = createGraphState(authorId, authorName);
    if (!raw || typeof raw !== "object") return graph;
    const nodes = Array.isArray(raw.nodes) ? raw.nodes : [];
    nodes.forEach((n) => {
      if (!n || !n.id) return;
      const pos = n._pos || null;
      const clean = { ...n };
      delete clean._pos;
      graph.nodes.set(clean.id, clean);
      if (pos && Number.isFinite(pos.x) && Number.isFinite(pos.y)) {
        graph.layout.set(clean.id, { x: pos.x, y: pos.y, vx: pos.vx || 0, vy: pos.vy || 0 });
      }
    });
    graph.edges = Array.isArray(raw.edges) ? raw.edges : [];
    graph.edgeKeys = new Set(graph.edges.map((e) => `${e.source}|${e.target}|${e.type}`));
    graph.seedIds = new Set(Array.isArray(raw.seedIds) ? raw.seedIds : []);
    graph.selectedId = raw.selectedId || null;
    graph.view = raw.view || "papers";
    graph.lastUpdated = raw.lastUpdated || null;
    graph.viewTransforms = raw.viewTransforms && typeof raw.viewTransforms === "object" ? raw.viewTransforms : {};
    graph.annotations = raw.annotations && typeof raw.annotations === "object" ? raw.annotations : {};
    return graph;
  }

  function getGraphTransform(graph, mode) {
    if (!graph.viewTransforms) graph.viewTransforms = {};
    const key = mode || "papers";
    const t = graph.viewTransforms[key];
    if (t && Number.isFinite(t.x) && Number.isFinite(t.y) && Number.isFinite(t.scale)) return t;
    const next = { x: 0, y: 0, scale: 1 };
    graph.viewTransforms[key] = next;
    return next;
  }

  function setGraphTransform(graph, mode, t) {
    if (!graph.viewTransforms) graph.viewTransforms = {};
    const key = mode || "papers";
    graph.viewTransforms[key] = {
      x: Number(t?.x) || 0,
      y: Number(t?.y) || 0,
      scale: Number(t?.scale) || 1
    };
  }

  async function ensureGraphStateForAuthor(authorId, authorName) {
    if (window.suGraphState && window.suGraphState.authorId === authorId) return window.suGraphState;
    const stored = await getAuthorGraphState(authorId);
    const graph = deserializeGraphState(stored, authorId, authorName);
    graph.collections = await getAuthorGraphCollections(authorId);
    const alerts = await getAuthorGraphAlerts(authorId);
    if (alerts) {
      graph.alerts = Array.isArray(alerts.alerts) ? alerts.alerts : [];
      graph.unreadCount = Number(alerts.unreadCount) || 0;
      graph.monitorEnabled = !!alerts.enabled;
      graph.lastMonitorCheck = alerts.lastCheck || null;
    }
    window.suGraphState = graph;
    return graph;
  }

  function graphAddNode(graph, node) {
    if (!graph || !node || !node.id) return null;
    const existing = graph.nodes.get(node.id);
    if (existing) {
      Object.assign(existing, node);
      return existing;
    }
    if (graph.nodes.size >= GRAPH_MAX_NODES) return null;
    graph.nodes.set(node.id, node);
    return node;
  }

  function graphAddEdge(graph, source, target, type) {
    if (!graph || !source || !target || source === target) return;
    if (graph.edges.length >= GRAPH_MAX_EDGES) return;
    const key = `${source}|${target}|${type || "link"}`;
    if (graph.edgeKeys.has(key)) return;
    graph.edgeKeys.add(key);
    graph.edges.push({ source, target, type: type || "link" });
  }

  function nodeFromOpenAlex(work, isSeed) {
    const id = work?.openalexId || normalizeOpenAlexId(work?.id || "");
    if (!id) return null;
    return {
      id,
      openalexId: id,
      type: "paper",
      title: work.title || "",
      year: work.year || null,
      doi: work.doi || "",
      url: work.url || "",
      citedByCount: work.citedByCount || 0,
      referencedWorks: work.referencedWorks || [],
      relatedWorks: work.relatedWorks || [],
      citedByApi: work.citedByApi || "",
      hostVenue: work.hostVenue || "",
      authors: Array.isArray(work.authors) ? work.authors.map((a) => a.name).filter(Boolean) : [],
      concepts: Array.isArray(work.concepts) ? work.concepts : [],
      isSeed: !!isSeed
    };
  }

  function nodeFromPaperFallback(paper, isSeed) {
    const title = paper?.title || "Untitled";
    const year = Number(paper?.year) || null;
    const id = `seed_${hashString(`${title}_${year || ""}`)}`;
    return {
      id,
      type: "paper",
      title,
      year,
      doi: normalizeDoi(paper?.doi || "") || "",
      url: paper?.url || "",
      citedByCount: 0,
      referencedWorks: [],
      relatedWorks: [],
      citedByApi: "",
      hostVenue: paper?.venue || "",
      authors: [],
      concepts: [],
      isSeed: !!isSeed
    };
  }

  async function resolveNodeWork(node, authorName) {
    if (!node || node._resolved) return node?._resolved || null;
    let work = null;
    if (node.openalexId) work = await fetchOpenAlexWorkById(node.openalexId);
    if (!work && node.doi) work = await fetchOpenAlexWorkByDoi(node.doi);
    if (!work && node.title) work = await searchOpenAlexWorkByTitle(node.title, node.year, authorName);
    if (work) {
      const merged = nodeFromOpenAlex(work, node.isSeed);
      graphAddNode(window.suGraphState, merged);
      Object.assign(node, merged);
    }
    node._resolved = work || null;
    return work;
  }

  function getSeedPapersFromAuthorPage(limit = 10) {
    const rows = Array.from(document.querySelectorAll(".gsc_a_tr"));
    const items = rows.map((tr) => {
      const paper = getCachedAuthorPaper(tr);
      const citations = getCachedAuthorCitationCount(tr) || 0;
      return { paper, citations };
    }).filter((p) => p.paper && p.paper.title);
    items.sort((a, b) => b.citations - a.citations);
    return items.slice(0, Math.max(3, limit)).map((p) => p.paper);
  }

  async function hydrateGraphNodes(graph, ids) {
    if (!graph || !ids || !ids.length) return;
    const unique = Array.from(new Set(ids));
    await mapWithConcurrency(unique, 2, async (id) => {
      const work = await fetchOpenAlexWorkById(id);
      if (!work) return null;
      const node = nodeFromOpenAlex(work, false);
      graphAddNode(graph, node);
      return node;
    });
  }

  function computeGraphConcepts(graph) {
    if (!graph) return;
    const counts = new Map();
    graph.nodes.forEach((n) => {
      if (n.type !== "paper") return;
      const concepts = Array.isArray(n.concepts) ? n.concepts : [];
      if (!concepts.length) return;
      const top = concepts.slice().sort((a, b) => (b.score || 0) - (a.score || 0))[0];
      if (top?.name) {
        n.primaryConcept = top.name;
        counts.set(top.name, (counts.get(top.name) || 0) + 1);
      }
    });
    const list = Array.from(counts.entries()).map(([name, count]) => ({ name, count }));
    list.sort((a, b) => b.count - a.count);
    graph.concepts = list.slice(0, 10);
    const palette = ["#1a73e8", "#d93025", "#188038", "#f9ab00", "#9334e6", "#12b5cb", "#d56e0c", "#3c4043", "#c5221f", "#7b1fa2"];
    graph.conceptColors = new Map();
    graph.concepts.forEach((c, idx) => graph.conceptColors.set(c.name, palette[idx % palette.length]));
  }

  function buildAuthorGraph(graph) {
    if (!graph) return null;
    const counts = new Map();
    const edgeCounts = new Map();
    const addEdge = (a, b) => {
      const [x, y] = a < b ? [a, b] : [b, a];
      const key = `${x}|${y}`;
      edgeCounts.set(key, (edgeCounts.get(key) || 0) + 1);
    };
    graph.nodes.forEach((n) => {
      if (n.type !== "paper") return;
      const authors = Array.isArray(n.authors) ? n.authors.filter(Boolean) : [];
      authors.forEach((a) => counts.set(a, (counts.get(a) || 0) + 1));
      for (let i = 0; i < authors.length; i++) {
        for (let j = i + 1; j < authors.length; j++) addEdge(authors[i], authors[j]);
      }
    });
    const extra = window.suFullAuthorStats?.coAuthorStats || [];
    extra.forEach((c) => {
      const name = String(c?.name || "").trim();
      if (!name) return;
      counts.set(name, Math.max(counts.get(name) || 0, Number(c?.count) || 0));
    });
    const topAuthors = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 30);
    const keep = new Set(topAuthors.map(([name]) => name));
    const authorGraph = {
      nodes: new Map(),
      edges: [],
      edgeKeys: new Set(),
      layout: new Map()
    };
    topAuthors.forEach(([name, count]) => {
      authorGraph.nodes.set(name, { id: name, type: "author", name, count });
    });
    edgeCounts.forEach((count, key) => {
      const [a, b] = key.split("|");
      if (!keep.has(a) || !keep.has(b)) return;
      const edgeKey = `${a}|${b}|co`;
      if (authorGraph.edgeKeys.has(edgeKey)) return;
      authorGraph.edgeKeys.add(edgeKey);
      authorGraph.edges.push({ source: a, target: b, type: "co", weight: count });
    });
    return authorGraph;
  }

  function computeForceLayout(nodes, edges, width, height, opts = {}) {
    const list = Array.isArray(nodes) ? nodes : Array.from(nodes.values());
    const existingLayout = opts.existingLayout instanceof Map ? opts.existingLayout : null;
    const iterations = Number.isFinite(opts.iterations) ? Math.max(10, opts.iterations) : Math.min(90, 30 + Math.floor(list.length / 2));
    const pos = new Map();
    const w = Math.max(300, width || 600);
    const h = Math.max(200, height || 400);
    const centerX = w / 2;
    const centerY = h / 2;
    const ring = Math.min(w, h) * 0.18;
    list.forEach((n, idx) => {
      const existing = existingLayout?.get(n.id) || n._pos;
      if (existing) {
        pos.set(n.id, existing);
      } else {
        const angle = (idx / Math.max(1, list.length)) * Math.PI * 2;
        pos.set(n.id, {
          x: centerX + Math.cos(angle) * ring,
          y: centerY + Math.sin(angle) * ring,
          vx: 0,
          vy: 0
        });
      }
    });
    const repulsion = 650;
    const spring = 0.0022;
    const damp = 0.82;
    for (let iter = 0; iter < iterations; iter++) {
      for (let i = 0; i < list.length; i++) {
        const a = list[i];
        const pa = pos.get(a.id);
        for (let j = i + 1; j < list.length; j++) {
          const b = list[j];
          const pb = pos.get(b.id);
          const dx = pa.x - pb.x;
          const dy = pa.y - pb.y;
          const dist2 = dx * dx + dy * dy + 0.01;
          const force = repulsion / dist2;
          pa.vx += (dx / Math.sqrt(dist2)) * force;
          pa.vy += (dy / Math.sqrt(dist2)) * force;
          pb.vx -= (dx / Math.sqrt(dist2)) * force;
          pb.vy -= (dy / Math.sqrt(dist2)) * force;
        }
      }
      edges.forEach((e) => {
        const pa = pos.get(e.source);
        const pb = pos.get(e.target);
        if (!pa || !pb) return;
        const dx = pb.x - pa.x;
        const dy = pb.y - pa.y;
        pa.vx += dx * spring;
        pa.vy += dy * spring;
        pb.vx -= dx * spring;
        pb.vy -= dy * spring;
      });
      pos.forEach((p) => {
        p.vx *= damp;
        p.vy *= damp;
        p.x += p.vx;
        p.y += p.vy;
        p.x = Math.max(20, Math.min(w - 20, p.x));
        p.y = Math.max(20, Math.min(h - 20, p.y));
      });
    }
    return pos;
  }

  function updateGraphTransformLayer(graph, mode) {
    const svg = document.getElementById("su-graph-canvas");
    if (!svg || !graph) return;
    const layer = svg.querySelector("#su-graph-layer");
    if (!layer) return;
    const transform = getGraphTransform(graph, mode);
    layer.setAttribute("transform", `translate(${transform.x.toFixed(1)} ${transform.y.toFixed(1)}) scale(${transform.scale.toFixed(3)})`);
  }

  function buildSimilarityEdges(graph, nodesOverride) {
    if (!graph) return [];
    const nodes = Array.isArray(nodesOverride)
      ? nodesOverride
      : Array.from(graph.nodes.values()).filter((n) => n.type === "paper");
    const refMap = new Map();
    nodes.forEach((n) => {
      const refs = Array.isArray(n.referencedWorks) ? n.referencedWorks : [];
      const set = new Set(refs.map((r) => normalizeOpenAlexId(r)).filter(Boolean));
      refMap.set(n.id, set);
    });
    const citeMap = new Map();
    nodes.forEach((n) => {
      const citing = Array.isArray(n._citingIds) ? n._citingIds : [];
      citeMap.set(n.id, new Set(citing));
    });
    const edges = [];
    const edgeKeys = new Set();
    for (let i = 0; i < nodes.length; i++) {
      const a = nodes[i];
      const refsA = refMap.get(a.id);
      const citesA = citeMap.get(a.id);
      for (let j = i + 1; j < nodes.length; j++) {
        const b = nodes[j];
        const refsB = refMap.get(b.id);
        const citesB = citeMap.get(b.id);
        let coupling = 0;
        if (refsA && refsB) {
          for (const r of refsA) if (refsB.has(r)) coupling += 1;
        }
        let cocite = 0;
        if (citesA && citesB) {
          for (const r of citesA) if (citesB.has(r)) cocite += 1;
        }
        const score = coupling + cocite * 0.7;
        if (score < 2) continue;
        const key = `${a.id}|${b.id}|sim`;
        if (edgeKeys.has(key)) continue;
        edgeKeys.add(key);
        edges.push({ source: a.id, target: b.id, type: "sim", weight: score, coupling, cocite });
      }
    }
    edges.sort((a, b) => b.weight - a.weight);
    return edges.slice(0, GRAPH_MAX_EDGES);
  }

  async function ensureSimilarityData(graph) {
    if (!graph) return;
    const nodes = Array.from(graph.nodes.values()).filter((n) => n.type === "paper");
    const needRefs = nodes.filter((n) => !Array.isArray(n.referencedWorks) || n.referencedWorks.length === 0);
    await mapWithConcurrency(needRefs.slice(0, 12), 2, async (n) => {
      const work = await resolveNodeWork(n, graph.authorName);
      if (work && Array.isArray(work.referencedWorks)) n.referencedWorks = work.referencedWorks;
    });
    const withCites = nodes
      .filter((n) => !Array.isArray(n._citingIds))
      .sort((a, b) => (b.citedByCount || 0) - (a.citedByCount || 0))
      .slice(0, 12);
    await mapWithConcurrency(withCites, 2, async (n) => {
      const work = await resolveNodeWork(n, graph.authorName);
      const citing = await fetchOpenAlexCitingWorks(work, 15);
      n._citingIds = citing.map((cw) => cw.openalexId || normalizeOpenAlexId(cw.id || "")).filter(Boolean);
    });
  }

  function renderGraphCanvas(graph, mode) {
    const svg = document.getElementById("su-graph-canvas");
    if (!svg || !graph) return;
    const container = svg.parentElement;
    const width = container?.clientWidth || 640;
    const height = container?.clientHeight || 360;
    let nodes = [];
    let edges = [];
    let layout = null;
    if (mode === "authors") {
      const authorGraph = graph.authorGraph || buildAuthorGraph(graph);
      graph.authorGraph = authorGraph;
      nodes = Array.from(authorGraph.nodes.values());
      edges = authorGraph.edges || [];
      layout = authorGraph.layout;
      if (!layout || layout.size === 0) {
        const pos = computeForceLayout(nodes, edges, width, height, { iterations: 24 });
        authorGraph.layout = pos;
        graph.authorLayoutVersion = (graph.authorLayoutVersion || 0) + 1;
        layout = pos;
      }
    } else if (mode === "similarity") {
      nodes = Array.isArray(graph.similarityNodes)
        ? graph.similarityNodes
        : Array.from(graph.nodes.values()).filter((n) => n.type === "paper");
      edges = graph.similarityEdges || [];
      layout = graph.similarityLayout;
      if (!layout || layout.size === 0) {
        const pos = computeForceLayout(nodes, edges, width, height, { iterations: 24 });
        graph.similarityLayout = pos;
        graph.similarityLayoutVersion = (graph.similarityLayoutVersion || 0) + 1;
        layout = pos;
      }
    } else {
      nodes = Array.from(graph.nodes.values());
      edges = graph.edges || [];
      layout = graph.layout;
      if (!layout || layout.size === 0) {
        const pos = computeForceLayout(nodes, edges, width, height, { iterations: 28 });
        graph.layout = pos;
        graph.layoutVersion = (graph.layoutVersion || 0) + 1;
        layout = pos;
      }
    }
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("width", String(width));
    svg.setAttribute("height", String(height));
    const transform = getGraphTransform(graph, mode);
    const edgeHtml = edges.map((e) => {
      const a = layout.get(e.source);
      const b = layout.get(e.target);
      if (!a || !b) return "";
      const typeClass = e.type ? ` su-graph-edge-${e.type}` : "";
      const width = e.weight ? Math.min(3.2, 0.7 + e.weight * 0.25) : 1;
      return `<line class="su-graph-edge${typeClass}" x1="${a.x.toFixed(1)}" y1="${a.y.toFixed(1)}" x2="${b.x.toFixed(1)}" y2="${b.y.toFixed(1)}" style="stroke-width:${width.toFixed(2)}" />`;
    }).join("");
    const nodeHtml = nodes.map((n) => {
      const p = layout.get(n.id);
      if (!p) return "";
      const isSeed = n.isSeed;
      const selected = graph.selectedId === n.id;
      const radius = n.type === "author"
        ? Math.max(8, Math.min(20, 7 + Math.log1p(n.count || 1)))
        : Math.max(8, Math.min(20, 7 + Math.log1p(n.citedByCount || 1)));
      const concept = n.primaryConcept;
      const color = concept && graph.conceptColors ? graph.conceptColors.get(concept) : null;
      const classes = ["su-graph-node"];
      if (isSeed) classes.push("su-graph-node-seed");
      if (selected) classes.push("su-graph-node-selected");
      return `<circle class="${classes.join(" ")}" data-node-id="${escapeHtml(n.id)}" data-node-type="${escapeHtml(n.type || "paper")}" cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${radius}"${color ? ` style=\\"fill:${color}\\"` : ""}></circle>`;
    }).join("");
    const labelHtml = nodes.filter((n) => n.isSeed || graph.selectedId === n.id).map((n) => {
      const p = layout.get(n.id);
      if (!p) return "";
      const label = n.type === "author" ? n.name : n.title;
      const short = String(label || "").slice(0, 40);
      return `<text class="su-graph-label" x="${(p.x + 6).toFixed(1)}" y="${(p.y - 6).toFixed(1)}">${escapeHtml(short)}</text>`;
    }).join("");
    svg.innerHTML = `<g id="su-graph-layer" transform="translate(${transform.x.toFixed(1)} ${transform.y.toFixed(1)}) scale(${transform.scale.toFixed(3)})">${edgeHtml}${nodeHtml}${labelHtml}</g>`;
  }

  function renderGraphSidebar(graph) {
    const side = document.getElementById("su-graph-side");
    if (!side || !graph) return;
    if (graph.view === "similarity") {
      const edgeCount = Array.isArray(graph.similarityEdges) ? graph.similarityEdges.length : 0;
      side.innerHTML = `
        <div class="su-graph-side-title">Similarity graph</div>
        <div class="su-graph-side-meta">Edges are based on shared references (bibliographic coupling) and shared citations (co‑citation). Heavier lines = stronger similarity.</div>
        <div class="su-graph-side-meta">${edgeCount ? `${edgeCount} similarity links` : "Build similarity links by selecting this tab."}</div>
      `;
      return;
    }
    if (graph.view === "alerts") {
      const alerts = Array.isArray(graph.alerts) ? graph.alerts : [];
      side.innerHTML = `
        <div class="su-graph-side-title">New papers</div>
        <div class="su-graph-side-meta">Papers published since your last check.</div>
        <div class="su-graph-alert-actions">
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-alerts-read="1">Mark all read</button>
        </div>
        <div class="su-graph-alert-list">
          ${alerts.length ? alerts.map((a) => `
            <div class="su-graph-alert-item">
              <div class="su-graph-alert-title">${escapeHtml(a.title || "Untitled")}</div>
              <div class="su-graph-alert-meta">${a.year || "—"} · ${escapeHtml(a.reason || "Related")}</div>
            </div>
          `).join("") : '<div class="su-graph-empty">No new papers yet.</div>'}
        </div>
      `;
      return;
    }
    if (graph.view === "topics") {
      const topics = graph.concepts || [];
      const topicHtml = topics.map((t) => {
        const color = graph.conceptColors.get(t.name) || "#5f6368";
        return `<div class="su-graph-topic"><span class="su-graph-topic-dot" style="background:${color}"></span><span>${escapeHtml(t.name)}</span><span class="su-graph-topic-count">${t.count}</span></div>`;
      }).join("");
      side.innerHTML = `
        <div class="su-graph-side-title">Top concepts</div>
        <div class="su-graph-topic-list">
          ${topicHtml || '<div class="su-graph-empty">No concepts yet.</div>'}
        </div>
      `;
      return;
    }
    if (graph.view === "recs") {
      if (!graph.recsComputed && !graph.loading) {
        graph.loading = true;
        renderGraphOverlay(graph);
        computeGraphRecommendations(graph).then(() => {
          graph.recsComputed = true;
          graph.loading = false;
          renderGraphOverlay(graph);
        }).catch(() => {
          graph.loading = false;
          renderGraphOverlay(graph);
        });
      }
      const recs = graph.recommendations || [];
      side.innerHTML = `
        <div class="su-graph-side-title">Recommendations</div>
        <div class="su-graph-rec-list">
          ${recs.map((r) => `
            <div class="su-graph-rec">
              <div class="su-graph-rec-title">${escapeHtml(r.title || "Untitled")}</div>
              <div class="su-graph-rec-meta">Score ${r.score} · ${r.year || "—"} · ${escapeHtml(r.reason || "")}</div>
              <button type="button" class="su-graph-rec-add" data-graph-add="${escapeHtml(r.id)}">Add to graph</button>
            </div>
          `).join("") || '<div class="su-graph-empty">No recommendations yet.</div>'}
        </div>
      `;
      return;
    }
    if (graph.view === "collections") {
      const list = Array.isArray(graph.collections) ? graph.collections : [];
      side.innerHTML = `
        <div class="su-graph-side-title">Collections</div>
        <div class="su-graph-collection-row">
          <input id="su-graph-collection-name" class="su-graph-input" placeholder="Collection name" />
          <button type="button" class="su-graph-btn" data-graph-save="1">Save</button>
        </div>
        <div class="su-graph-collection-list">
          ${list.map((c) => `
            <div class="su-graph-collection-item">
              <div class="su-graph-collection-name">${escapeHtml(c.name || "Untitled")}</div>
              <div class="su-graph-collection-meta">${new Date(c.createdAt || Date.now()).toLocaleDateString()}</div>
              <div class="su-graph-collection-actions">
                <button type="button" class="su-graph-btn" data-graph-load="${escapeHtml(c.id)}">Load</button>
                <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-delete="${escapeHtml(c.id)}">Delete</button>
              </div>
            </div>
          `).join("") || '<div class="su-graph-empty">No saved collections yet.</div>'}
        </div>
      `;
      return;
    }
    const node = graph.selectedId
      ? (graph.view === "authors" ? graph.authorGraph?.nodes?.get(graph.selectedId) : graph.nodes.get(graph.selectedId))
      : null;
    if (!node) {
      side.innerHTML = `<div class="su-graph-empty">Select a node to see details.</div>`;
      return;
    }
    if (graph.view === "authors") {
      side.innerHTML = `
        <div class="su-graph-side-title">${escapeHtml(node.name || "Author")}</div>
        <div class="su-graph-side-meta">Co-authored papers: ${node.count || 0}</div>
      `;
      return;
    }
    const prior = Array.isArray(node._priorWorks) ? node._priorWorks : [];
    const deriv = Array.isArray(node._derivativeWorks) ? node._derivativeWorks : [];
    if (!node._contextLoading && (prior.length === 0 || deriv.length === 0)) {
      loadGraphNodeContext(node, graph);
    }
    side.innerHTML = `
      <div class="su-graph-side-title">${escapeHtml(node.title || "Paper")}</div>
      <div class="su-graph-side-meta">${node.year || "—"} · ${escapeHtml(node.hostVenue || "")}</div>
      <div class="su-graph-side-meta">Citations: ${node.citedByCount || 0}</div>
      <div class="su-graph-side-authors">${Array.isArray(node.authors) && node.authors.length ? escapeHtml(node.authors.slice(0, 6).join(", ")) : "Authors unavailable"}</div>
      ${node.doi ? `<div class="su-graph-side-link">DOI: ${escapeHtml(node.doi)}</div>` : ""}
      <div class="su-graph-side-section">
        <div class="su-graph-side-section-title">Prior works (references)</div>
        ${prior.length ? prior.map((w) => `
          <div class="su-graph-side-item">${escapeHtml(w.title || "Untitled")} <span class="su-graph-side-item-meta">${w.year || "—"}</span></div>
        `).join("") : `<div class="su-graph-empty">${node._contextLoading ? "Loading…" : "No references loaded."}</div>`}
      </div>
      <div class="su-graph-side-section">
        <div class="su-graph-side-section-title">Derivative works (citations)</div>
        ${deriv.length ? deriv.map((w) => `
          <div class="su-graph-side-item">${escapeHtml(w.title || "Untitled")} <span class="su-graph-side-item-meta">${w.year || "—"}</span></div>
        `).join("") : `<div class="su-graph-empty">${node._contextLoading ? "Loading…" : "No citations loaded."}</div>`}
      </div>
    `;
  }

  function renderGraphOverlay(graph) {
    const overlay = ensureGraphOverlay();
    const status = overlay.querySelector("#su-graph-status");
    if (status) {
      status.textContent = graph.loading ? "Loading…" : (graph.lastUpdated ? `Updated ${new Date(graph.lastUpdated).toLocaleTimeString()}` : "");
    }
    const alertBadge = overlay.querySelector("[data-graph-alert-count]");
    if (alertBadge) {
      const count = Number(graph.unreadCount) || 0;
      alertBadge.textContent = count > 0 ? `(${count})` : "";
    }
    const monitorToggle = overlay.querySelector("[data-graph-monitor]");
    if (monitorToggle) monitorToggle.checked = !!graph.monitorEnabled;
    overlay.querySelectorAll("[data-graph-tab]").forEach((btn) => {
      const key = String(btn.dataset.graphTab || "");
      btn.classList.toggle("su-graph-tab-active", key === graph.view);
    });
    const canvasWrap = overlay.querySelector(".su-graph-canvas-wrap");
    if (canvasWrap) canvasWrap.style.display = (graph.view === "papers" || graph.view === "authors" || graph.view === "similarity") ? "block" : "none";
    let renderKey = graph.view;
    if (graph.view === "authors") {
      renderKey += `|a${graph.authorGraph?.nodes?.size || 0}|e${graph.authorGraph?.edges?.length || 0}|v${graph.authorLayoutVersion || 0}`;
    } else if (graph.view === "similarity") {
      renderKey += `|s${graph.similarityNodes?.length || 0}|e${graph.similarityEdges?.length || 0}|v${graph.similarityLayoutVersion || 0}`;
    } else {
      renderKey += `|n${graph.nodes.size}|e${graph.edges.length}|v${graph.layoutVersion || 0}`;
    }
    if (graph.view === "papers" || graph.view === "authors" || graph.view === "similarity") {
      if (graph.__renderKey !== renderKey) {
        renderGraphCanvas(graph, graph.view);
        graph.__renderKey = renderKey;
      } else {
        updateGraphSelection(graph, graph.view);
      }
    }
    renderGraphSidebar(graph);
  }

  function updateGraphSelection(graph, mode) {
    const svg = document.getElementById("su-graph-canvas");
    if (!svg || !graph) return;
    const prev = graph.__lastSelectedId;
    if (prev && prev !== graph.selectedId) {
      const elPrev = svg.querySelector(`[data-node-id="${CSS.escape(prev)}"]`);
      if (elPrev) elPrev.classList.remove("su-graph-node-selected");
    }
    if (graph.selectedId) {
      const el = svg.querySelector(`[data-node-id="${CSS.escape(graph.selectedId)}"]`);
      if (el) el.classList.add("su-graph-node-selected");
    }
    graph.__lastSelectedId = graph.selectedId || null;
  }

  async function loadGraphNodeContext(node, graph) {
    if (!node || node._contextLoading) return;
    node._contextLoading = true;
    const work = await resolveNodeWork(node, graph.authorName);
    if (work && Array.isArray(work.referencedWorks)) {
      const refs = work.referencedWorks.slice(0, 8).map((r) => normalizeOpenAlexId(r)).filter(Boolean);
      const priorWorks = await mapWithConcurrency(refs, 2, async (id) => await fetchOpenAlexWorkById(id));
      node._priorWorks = priorWorks.filter(Boolean);
    }
    const citing = await fetchOpenAlexCitingWorks(work, 8);
    node._derivativeWorks = citing.filter(Boolean);
    node._contextLoading = false;
    renderGraphOverlay(graph);
  }

  function ensureGraphOverlay() {
    let overlay = document.getElementById("su-graph-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "su-graph-overlay";
    overlay.className = "su-graph-overlay";
    overlay.innerHTML = `
      <div class="su-graph-backdrop" data-graph-close="1"></div>
      <div class="su-graph-panel">
        <div class="su-graph-header">
          <div>
            <div class="su-graph-title">Citation Atlas</div>
            <div class="su-graph-subtitle">Local map from OpenAlex + OpenCitations</div>
          </div>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-close="1">Close</button>
        </div>
        <div class="su-graph-controls">
          <label class="su-graph-label-text">Seed count</label>
          <input id="su-graph-seed-count" class="su-graph-input su-graph-input-small" type="number" min="3" max="50" value="10" />
          <button type="button" class="su-graph-btn" data-graph-build="1" title="Build a map from the top papers on this page">Build</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-expand="refs" title="Add references cited by the selected paper">Expand refs</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-expand="cites" title="Add papers that cite the selected paper">Expand cites</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-expand="related" title="Add OpenAlex related works for the selected paper">Expand related</button>
          <label class="su-graph-monitor-toggle"><input type="checkbox" data-graph-monitor="1" /> Monitor new papers</label>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-check="1" title="Check for new papers now">Check now</button>
          <span id="su-graph-status" class="su-graph-status"></span>
        </div>
        <div class="su-graph-help">
          Start with your most‑cited papers, then expand a node to explore references, citations, or related works. Use tabs to switch between paper, author, topic, and recommendation views.
        </div>
        <div class="su-graph-tabs">
          <button type="button" class="su-graph-tab" data-graph-tab="papers">Papers</button>
          <button type="button" class="su-graph-tab" data-graph-tab="similarity">Similarity</button>
          <button type="button" class="su-graph-tab" data-graph-tab="authors">Authors</button>
          <button type="button" class="su-graph-tab" data-graph-tab="topics">Topics</button>
          <button type="button" class="su-graph-tab" data-graph-tab="recs">Recommendations</button>
          <button type="button" class="su-graph-tab" data-graph-tab="alerts">Alerts <span class="su-graph-alert-count" data-graph-alert-count></span></button>
          <button type="button" class="su-graph-tab" data-graph-tab="collections">Collections</button>
        </div>
        <div class="su-graph-body">
          <div class="su-graph-canvas-wrap">
            <svg id="su-graph-canvas" class="su-graph-canvas"></svg>
          </div>
          <div id="su-graph-side" class="su-graph-side"></div>
        </div>
        <div class="su-graph-footer">
          <div class="su-graph-export">
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-export="csv">CSV</button>
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-export="bib">BibTeX</button>
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-export="ris">RIS</button>
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-graph-export="png">PNG</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", async (e) => {
      const close = e.target.closest("[data-graph-close]");
      if (close) {
        closeGraphOverlay();
        return;
      }
      const tab = e.target.closest("[data-graph-tab]");
      if (tab) {
        const key = String(tab.dataset.graphTab || "");
        if (window.suGraphState) {
          window.suGraphState.view = key || "papers";
          if (key === "similarity") {
            window.suGraphState.loading = true;
            renderGraphOverlay(window.suGraphState);
            await ensureSimilarityData(window.suGraphState);
            const simNodes = Array.from(window.suGraphState.nodes.values())
              .filter((n) => n.type === "paper")
              .sort((a, b) => (b.citedByCount || 0) - (a.citedByCount || 0))
              .slice(0, 50);
            window.suGraphState.similarityNodes = simNodes;
            window.suGraphState.similarityEdges = buildSimilarityEdges(window.suGraphState, simNodes);
            window.suGraphState.similarityLayout = computeForceLayout(simNodes, window.suGraphState.similarityEdges, 640, 360, { iterations: 28 });
            window.suGraphState.similarityLayoutVersion = (window.suGraphState.similarityLayoutVersion || 0) + 1;
            window.suGraphState.loading = false;
          }
          if (key === "recs" && !window.suGraphState.recsComputed) {
            window.suGraphState.loading = true;
            renderGraphOverlay(window.suGraphState);
            await computeGraphRecommendations(window.suGraphState);
            window.suGraphState.recsComputed = true;
            window.suGraphState.loading = false;
          }
          renderGraphOverlay(window.suGraphState);
        }
        return;
      }
      const monitorToggle = e.target.closest("[data-graph-monitor]");
      if (monitorToggle && window.suGraphState) {
        const enabled = !!monitorToggle.checked;
        window.suGraphState.monitorEnabled = enabled;
        const alerts = (await getAuthorGraphAlerts(window.suGraphState.authorId)) || {};
        alerts.enabled = enabled;
        alerts.seedIds = Array.from(window.suGraphState.seedIds || []);
        if (enabled && !alerts.lastCheck) alerts.lastCheck = new Date().toISOString();
        if (!Array.isArray(alerts.alerts)) alerts.alerts = [];
        alerts.unreadCount = Number(alerts.unreadCount) || 0;
        await setAuthorGraphAlerts(window.suGraphState.authorId, alerts);
        window.suGraphState.alerts = alerts.alerts;
        window.suGraphState.unreadCount = Number(alerts.unreadCount) || 0;
        window.suGraphState.lastMonitorCheck = alerts.lastCheck || null;
        renderGraphOverlay(window.suGraphState);
        return;
      }
      const checkNow = e.target.closest("[data-graph-check]");
      if (checkNow && window.suGraphState) {
        window.suGraphState.loading = true;
        renderGraphOverlay(window.suGraphState);
        try {
          await chrome.runtime.sendMessage({ action: "graphMonitorCheck", authorId: window.suGraphState.authorId });
        } catch (_) {}
        const alerts = await getAuthorGraphAlerts(window.suGraphState.authorId);
        if (alerts) {
          window.suGraphState.alerts = Array.isArray(alerts.alerts) ? alerts.alerts : [];
          window.suGraphState.unreadCount = Number(alerts.unreadCount) || 0;
          window.suGraphState.lastMonitorCheck = alerts.lastCheck || null;
        }
        window.suGraphState.loading = false;
        renderGraphOverlay(window.suGraphState);
        return;
      }
      const markRead = e.target.closest("[data-graph-alerts-read]");
      if (markRead && window.suGraphState) {
        const alerts = (await getAuthorGraphAlerts(window.suGraphState.authorId)) || {};
        alerts.unreadCount = 0;
        await setAuthorGraphAlerts(window.suGraphState.authorId, alerts);
        window.suGraphState.unreadCount = 0;
        renderGraphOverlay(window.suGraphState);
        return;
      }
      const build = e.target.closest("[data-graph-build]");
      if (build) {
        const countInput = overlay.querySelector("#su-graph-seed-count");
        const count = Math.min(50, Math.max(3, Number(countInput?.value) || 10));
        await buildGraphFromSeeds(count);
        return;
      }
      const expand = e.target.closest("[data-graph-expand]");
      if (expand) {
        const kind = String(expand.dataset.graphExpand || "");
        await expandGraph(kind);
        return;
      }
      const add = e.target.closest("[data-graph-add]");
      if (add) {
        const id = String(add.dataset.graphAdd || "");
        await addRecommendationToGraph(id);
        return;
      }
      const save = e.target.closest("[data-graph-save]");
      if (save) {
        await saveGraphCollection();
        return;
      }
      const load = e.target.closest("[data-graph-load]");
      if (load) {
        const id = String(load.dataset.graphLoad || "");
        await loadGraphCollection(id);
        return;
      }
      const del = e.target.closest("[data-graph-delete]");
      if (del) {
        const id = String(del.dataset.graphDelete || "");
        await deleteGraphCollection(id);
        return;
      }
      const exp = e.target.closest("[data-graph-export]");
      if (exp) {
        const kind = String(exp.dataset.graphExport || "");
        exportGraph(kind);
      }
    });
    overlay.addEventListener("click", (e) => {
      const nodeEl = e.target.closest("[data-node-id]");
      if (!nodeEl || !window.suGraphState) return;
      const id = String(nodeEl.dataset.nodeId || "");
      window.suGraphState.selectedId = id;
      renderGraphOverlay(window.suGraphState);
    });
    const svg = overlay.querySelector("#su-graph-canvas");
    if (svg && !svg.dataset.suGraphZoom) {
      svg.dataset.suGraphZoom = "1";
      svg.addEventListener("wheel", (e) => {
        const graph = window.suGraphState;
        if (!graph) return;
        if (!["papers", "authors", "similarity"].includes(graph.view)) return;
        e.preventDefault();
        const rect = svg.getBoundingClientRect();
        const cx = e.clientX - rect.left;
        const cy = e.clientY - rect.top;
        const mode = graph.view || "papers";
        const t = getGraphTransform(graph, mode);
        const scale = t.scale || 1;
        const factor = e.deltaY > 0 ? 0.9 : 1.1;
        const nextScale = Math.min(2.8, Math.max(0.4, scale * factor));
        const worldX = (cx - t.x) / scale;
        const worldY = (cy - t.y) / scale;
        const next = {
          scale: nextScale,
          x: cx - worldX * nextScale,
          y: cy - worldY * nextScale
        };
        setGraphTransform(graph, mode, next);
        updateGraphTransformLayer(graph, mode);
      }, { passive: false });
      let panState = null;
      let panRaf = null;
      svg.addEventListener("pointerdown", (e) => {
        if (e.button !== 0) return;
        if (e.target.closest("[data-node-id]")) return;
        const graph = window.suGraphState;
        if (!graph) return;
        const mode = graph.view || "papers";
        const t = getGraphTransform(graph, mode);
        panState = {
          mode,
          startX: e.clientX,
          startY: e.clientY,
          x: t.x,
          y: t.y
        };
        svg.classList.add("su-graph-panning");
        svg.setPointerCapture(e.pointerId);
      });
      svg.addEventListener("pointermove", (e) => {
        if (!panState || !window.suGraphState) return;
        const graph = window.suGraphState;
        const dx = e.clientX - panState.startX;
        const dy = e.clientY - panState.startY;
        setGraphTransform(graph, panState.mode, {
          x: panState.x + dx,
          y: panState.y + dy,
          scale: getGraphTransform(graph, panState.mode).scale
        });
        if (panRaf) return;
        panRaf = requestAnimationFrame(() => {
          panRaf = null;
          updateGraphTransformLayer(graph, panState.mode);
        });
      });
      const endPan = () => {
        panState = null;
        svg.classList.remove("su-graph-panning");
      };
      svg.addEventListener("pointerup", endPan);
      svg.addEventListener("pointercancel", endPan);
      svg.addEventListener("pointerleave", endPan);
    }
    return overlay;
  }

  async function openGraphOverlay() {
    const scholarId = new URL(window.location.href).searchParams.get("user") || "author";
    const authorName = getScholarAuthorName();
    const graph = await ensureGraphStateForAuthor(scholarId, authorName);
    if (graph.nodes.size) {
      computeGraphConcepts(graph);
      if (!graph.authorGraph) graph.authorGraph = buildAuthorGraph(graph);
      if (!graph.layout || graph.layout.size === 0) {
        graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360, { iterations: 30 });
        graph.layoutVersion = (graph.layoutVersion || 0) + 1;
      }
    }
    const overlay = ensureGraphOverlay();
    overlay.classList.add("su-visible");
    renderGraphOverlay(graph);
  }

  function closeGraphOverlay() {
    const overlay = document.getElementById("su-graph-overlay");
    if (overlay) overlay.classList.remove("su-visible");
  }

  async function buildGraphFromSeeds(count) {
    const scholarId = new URL(window.location.href).searchParams.get("user") || "author";
    const authorName = getScholarAuthorName();
    const graph = createGraphState(scholarId, authorName);
    graph.loading = true;
    window.suGraphState = graph;
    renderGraphOverlay(graph);
    const seeds = getSeedPapersFromAuthorPage(count);
    const works = await mapWithConcurrency(seeds, 2, async (paper) => {
      const work = await fetchOpenAlexWorkForPaper(paper, authorName);
      return { paper, work };
    });
    works.forEach(({ paper, work }) => {
      const node = work ? nodeFromOpenAlex(work, true) : nodeFromPaperFallback(paper, true);
      if (!node) return;
      graphAddNode(graph, node);
      graph.seedIds.add(node.id);
    });
    graph.nodes.forEach((n) => {
      if (!n.referencedWorks || !n.referencedWorks.length) return;
      n.referencedWorks.forEach((ref) => {
        const refId = normalizeOpenAlexId(ref);
        if (refId && graph.seedIds.has(refId)) graphAddEdge(graph, n.id, refId, "ref");
      });
      if (!n.relatedWorks || !n.relatedWorks.length) return;
      n.relatedWorks.forEach((rel) => {
        const relId = normalizeOpenAlexId(rel);
        if (relId && graph.seedIds.has(relId)) graphAddEdge(graph, n.id, relId, "rel");
      });
    });
    computeGraphConcepts(graph);
    graph.authorGraph = null;
    graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360, { iterations: 40 });
    graph.layoutVersion = (graph.layoutVersion || 0) + 1;
    graph.recsComputed = false;
    graph.loading = false;
    graph.lastUpdated = Date.now();
    await setAuthorGraphState(scholarId, serializeGraphState(graph));
    await syncGraphMonitorSeeds(graph);
    renderGraphOverlay(graph);
  }

  async function expandGraph(kind) {
    const graph = window.suGraphState;
    if (!graph) return;
    const targetId = graph.selectedId || Array.from(graph.seedIds)[0];
    if (!targetId) return;
    if (kind === "refs") await expandGraphReferences(graph, targetId);
    if (kind === "cites") await expandGraphCitations(graph, targetId);
    if (kind === "related") await expandGraphRelated(graph, targetId);
  }

  async function expandGraphReferences(graph, nodeId) {
    const node = graph.nodes.get(nodeId);
    if (!node || node.refsLoaded) return;
    graph.loading = true;
    renderGraphOverlay(graph);
    const work = await resolveNodeWork(node, graph.authorName);
    const refs = Array.isArray(work?.referencedWorks) ? work.referencedWorks.slice(0, 25) : [];
    const newIds = [];
    refs.forEach((ref) => {
      const refId = normalizeOpenAlexId(ref);
      if (!refId) return;
      graphAddEdge(graph, node.id, refId, "ref");
      if (!graph.nodes.has(refId)) {
        graphAddNode(graph, { id: refId, openalexId: refId, type: "paper", title: "Loading…", year: null, citedByCount: 0, authors: [], concepts: [], referencedWorks: [], relatedWorks: [], citedByApi: "" });
        newIds.push(refId);
      }
    });
    node.refsLoaded = true;
    await hydrateGraphNodes(graph, newIds);
    computeGraphConcepts(graph);
    graph.authorGraph = null;
    graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360, { existingLayout: graph.layout, iterations: 30 });
    graph.layoutVersion = (graph.layoutVersion || 0) + 1;
    graph.loading = false;
    graph.lastUpdated = Date.now();
    await setAuthorGraphState(graph.authorId, serializeGraphState(graph));
    renderGraphOverlay(graph);
  }

  async function expandGraphRelated(graph, nodeId) {
    const node = graph.nodes.get(nodeId);
    if (!node || node.relatedLoaded) return;
    graph.loading = true;
    renderGraphOverlay(graph);
    const work = await resolveNodeWork(node, graph.authorName);
    const rels = Array.isArray(work?.relatedWorks) ? work.relatedWorks.slice(0, 25) : [];
    const newIds = [];
    rels.forEach((rel) => {
      const relId = normalizeOpenAlexId(rel);
      if (!relId) return;
      graphAddEdge(graph, node.id, relId, "rel");
      if (!graph.nodes.has(relId)) {
        graphAddNode(graph, { id: relId, openalexId: relId, type: "paper", title: "Loading…", year: null, citedByCount: 0, authors: [], concepts: [], referencedWorks: [], relatedWorks: [], citedByApi: "" });
        newIds.push(relId);
      }
    });
    node.relatedLoaded = true;
    await hydrateGraphNodes(graph, newIds);
    computeGraphConcepts(graph);
    graph.authorGraph = null;
    graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360, { existingLayout: graph.layout, iterations: 30 });
    graph.layoutVersion = (graph.layoutVersion || 0) + 1;
    graph.loading = false;
    graph.lastUpdated = Date.now();
    await setAuthorGraphState(graph.authorId, serializeGraphState(graph));
    renderGraphOverlay(graph);
  }

  async function fetchOpenAlexCitingWorks(work, limit = 20) {
    if (!work) return [];
    if (work.citedByApi) {
      const url = formatOpenAlexUrl(`${work.citedByApi}?per_page=${limit}`);
      const data = await fetchExternalJson(url, { timeoutMs: 15000 });
      const results = Array.isArray(data?.results) ? data.results : [];
      return results.map((w) => compactOpenAlexWork(w)).filter(Boolean);
    }
    if (work.doi) {
      const dois = await fetchOpenCitationsCociList(work.doi);
      const slice = Array.isArray(dois) ? dois.slice(0, limit) : [];
      const works = await mapWithConcurrency(slice, 2, async (d) => await fetchOpenAlexWorkByDoi(d));
      return works.filter(Boolean);
    }
    return [];
  }

  async function expandGraphCitations(graph, nodeId) {
    const node = graph.nodes.get(nodeId);
    if (!node || node.citesLoaded) return;
    graph.loading = true;
    renderGraphOverlay(graph);
    const work = await resolveNodeWork(node, graph.authorName);
    const citingWorks = await fetchOpenAlexCitingWorks(work, 20);
    const newIds = [];
    citingWorks.forEach((cw) => {
      const nodeId = cw.openalexId || normalizeOpenAlexId(cw.id || "");
      if (!nodeId) return;
      graphAddEdge(graph, nodeId, node.id, "cite");
      if (!graph.nodes.has(nodeId)) {
        const nodeObj = nodeFromOpenAlex(cw, false);
        if (nodeObj) {
          graphAddNode(graph, nodeObj);
        } else {
          graphAddNode(graph, { id: nodeId, openalexId: nodeId, type: "paper", title: "Loading…", year: null, citedByCount: 0, authors: [], concepts: [], referencedWorks: [], relatedWorks: [], citedByApi: "" });
        }
      }
      newIds.push(nodeId);
    });
    node.citesLoaded = true;
    await hydrateGraphNodes(graph, newIds);
    computeGraphConcepts(graph);
    graph.authorGraph = null;
    graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360, { existingLayout: graph.layout, iterations: 30 });
    graph.layoutVersion = (graph.layoutVersion || 0) + 1;
    graph.loading = false;
    graph.lastUpdated = Date.now();
    await setAuthorGraphState(graph.authorId, serializeGraphState(graph));
    renderGraphOverlay(graph);
  }

  async function computeGraphRecommendations(graph) {
    if (!graph) return;
    const candidates = new Map();
    const seedNodes = Array.from(graph.seedIds).map((id) => graph.nodes.get(id)).filter(Boolean);
    for (const seed of seedNodes) {
      const work = await resolveNodeWork(seed, graph.authorName);
      if (!work) continue;
      const add = (id, weight, reason) => {
        if (!id) return;
        const key = normalizeOpenAlexId(id) || id;
        if (!key || graph.nodes.has(key)) return;
        const entry = candidates.get(key) || { id: key, score: 0, reasons: new Set() };
        entry.score += weight;
        if (reason) entry.reasons.add(reason);
        candidates.set(key, entry);
      };
      (work.relatedWorks || []).slice(0, 20).forEach((rid) => add(rid, 3, "related"));
      (work.referencedWorks || []).slice(0, 20).forEach((rid) => add(rid, 1, "refs"));
      const citing = await fetchOpenAlexCitingWorks(work, 10);
      citing.forEach((cw) => add(cw.openalexId || cw.id, 2, "co-cited"));
    }
    const sorted = Array.from(candidates.values()).sort((a, b) => b.score - a.score).slice(0, 12);
    const works = await mapWithConcurrency(sorted, 2, async (cand) => {
      const work = await fetchOpenAlexWorkById(cand.id);
      if (!work) return null;
      return { cand, work };
    });
    graph.recommendations = works.filter(Boolean).map(({ cand, work }) => ({
      id: work.openalexId || cand.id,
      title: work.title,
      year: work.year,
      score: cand.score,
      reason: Array.from(cand.reasons).join(", "),
      work
    }));
  }

  async function addRecommendationToGraph(id) {
    const graph = window.suGraphState;
    if (!graph || !id) return;
    const rec = (graph.recommendations || []).find((r) => r.id === id);
    if (!rec || !rec.work) return;
    const node = nodeFromOpenAlex(rec.work, false);
    if (!node) return;
    graphAddNode(graph, node);
    graph.layout = computeForceLayout(Array.from(graph.nodes.values()), graph.edges, 640, 360);
    await setAuthorGraphState(graph.authorId, serializeGraphState(graph));
    renderGraphOverlay(graph);
  }

  async function syncGraphMonitorSeeds(graph) {
    if (!graph || !graph.authorId) return;
    const alerts = (await getAuthorGraphAlerts(graph.authorId)) || {};
    alerts.seedIds = Array.from(graph.seedIds || []);
    if (alerts.enabled && !alerts.lastCheck) alerts.lastCheck = new Date().toISOString();
    if (!Array.isArray(alerts.alerts)) alerts.alerts = [];
    alerts.unreadCount = Number(alerts.unreadCount) || 0;
    await setAuthorGraphAlerts(graph.authorId, alerts);
    graph.monitorEnabled = !!alerts.enabled;
    graph.alerts = alerts.alerts;
    graph.unreadCount = alerts.unreadCount;
    graph.lastMonitorCheck = alerts.lastCheck || null;
  }

  async function saveGraphCollection() {
    const graph = window.suGraphState;
    if (!graph) return;
    const nameInput = document.getElementById("su-graph-collection-name");
    const name = String(nameInput?.value || "").trim() || `Collection ${new Date().toLocaleDateString()}`;
    const entry = {
      id: `col_${Date.now()}`,
      name,
      createdAt: Date.now(),
      graph: serializeGraphState(graph)
    };
    graph.collections = Array.isArray(graph.collections) ? graph.collections : [];
    graph.collections.unshift(entry);
    await setAuthorGraphCollections(graph.authorId, graph.collections);
    renderGraphSidebar(graph);
    if (nameInput) nameInput.value = "";
  }

  async function loadGraphCollection(id) {
    const graph = window.suGraphState;
    if (!graph) return;
    const entry = (graph.collections || []).find((c) => c.id === id);
    if (!entry) return;
    const loaded = deserializeGraphState(entry.graph, graph.authorId, graph.authorName);
    loaded.collections = graph.collections;
    window.suGraphState = loaded;
    await setAuthorGraphState(graph.authorId, serializeGraphState(loaded));
    await syncGraphMonitorSeeds(loaded);
    renderGraphOverlay(loaded);
  }

  async function deleteGraphCollection(id) {
    const graph = window.suGraphState;
    if (!graph) return;
    graph.collections = (graph.collections || []).filter((c) => c.id !== id);
    await setAuthorGraphCollections(graph.authorId, graph.collections);
    renderGraphSidebar(graph);
  }

  function downloadBlob(filename, mime, content) {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }

  function exportGraph(kind) {
    const graph = window.suGraphState;
    if (!graph) return;
    const nodes = Array.from(graph.nodes.values()).filter((n) => n.type === "paper");
    const authorName = (graph.authorName || "author").replace(/[^a-z0-9]+/gi, "-").toLowerCase();
    if (kind === "csv") {
      const rows = [["Title", "Year", "Citations", "DOI", "URL"]];
      nodes.forEach((n) => rows.push([n.title || "", n.year || "", n.citedByCount || 0, n.doi || "", n.url || ""]));
      const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\\n");
      downloadBlob(`${authorName}-graph.csv`, "text/csv;charset=utf-8", csv);
      return;
    }
    if (kind === "bib") {
      const entries = nodes.map((n) => {
        const key = `${authorName}${n.year || ""}${hashString(n.title || "")}`.slice(0, 30);
        const authors = Array.isArray(n.authors) ? n.authors.join(" and ") : "";
        return `@article{${key},\\n  title={${n.title || ""}},\\n  author={${authors}},\\n  year={${n.year || ""}},\\n  doi={${n.doi || ""}},\\n  url={${n.url || ""}}\\n}`;
      }).join("\\n\\n");
      downloadBlob(`${authorName}-graph.bib`, "text/plain;charset=utf-8", entries);
      return;
    }
    if (kind === "ris") {
      const entries = nodes.map((n) => {
        const authors = Array.isArray(n.authors) ? n.authors : [];
        const lines = [
          "TY  - JOUR",
          `TI  - ${n.title || ""}`,
          ...authors.map((a) => `AU  - ${a}`),
          `PY  - ${n.year || ""}`,
          n.doi ? `DO  - ${n.doi}` : "",
          n.url ? `UR  - ${n.url}` : "",
          "ER  -"
        ].filter(Boolean);
        return lines.join("\\n");
      }).join("\\n\\n");
      downloadBlob(`${authorName}-graph.ris`, "application/x-research-info-systems;charset=utf-8", entries);
      return;
    }
    if (kind === "png") {
      const svg = document.getElementById("su-graph-canvas");
      if (!svg) return;
      const xml = new XMLSerializer().serializeToString(svg);
      const img = new Image();
      const svg64 = btoa(unescape(encodeURIComponent(xml)));
      img.onload = () => {
        const canvas = document.createElement("canvas");
        canvas.width = img.width;
        canvas.height = img.height;
        const ctx = canvas.getContext("2d");
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.drawImage(img, 0, 0);
        canvas.toBlob((blob) => {
          if (!blob) return;
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `${authorName}-graph.png`;
          document.body.appendChild(a);
          a.click();
          a.remove();
          URL.revokeObjectURL(url);
        });
      };
      img.src = `data:image/svg+xml;base64,${svg64}`;
    }
  }

  async function ensureReviewState() {
    if (window.suReviewState) return window.suReviewState;
    const projects = await getReviewProjects();
    const ids = Object.keys(projects);
    let dirty = false;
    for (const id of ids) {
      const normalized = normalizeReviewProject(projects[id]);
      if (normalized) {
        projects[id] = normalized;
        dirty = true;
      }
    }
    let activeId = ids[0] || null;
    if (!activeId) {
      const q = getScholarSearchQuery();
      const project = createReviewProject("New review", q || "");
      projects[project.id] = project;
      activeId = project.id;
      dirty = true;
    }
    if (dirty) {
      await setReviewProjects(projects);
    }
    window.suReviewState = { projects, activeId, tab: "overview" };
    return window.suReviewState;
  }

  function getActiveReviewProject() {
    const state = window.suReviewState;
    if (!state) return null;
    return state.projects?.[state.activeId] || null;
  }

  async function saveReviewProject(project) {
    if (!project || !project.id) return;
    const state = await ensureReviewState();
    normalizeReviewProject(project);
    project.updatedAt = new Date().toISOString();
    state.projects[project.id] = project;
    await setReviewProjects(state.projects);
  }

  function extractReviewPaperFromResult(row, isAuthorProfile) {
    const paper = isAuthorProfile ? getCachedAuthorPaper(row) : extractPaperFromResult(row);
    if (!paper || !paper.title) return null;
    const snippetText = getSnippetText(row);
    const authorsPart = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0]?.trim() || "";
    const citations = isAuthorProfile ? getCachedAuthorCitationCount(row) : getCitationCountFromResult(row);
    const doi = normalizeDoi(extractDOIFromResult(row));
    const pdfInfo = getBestPdfUrl(row);
    return {
      id: paper.key,
      title: paper.title || "",
      year: paper.year || "",
      authors: authorsPart,
      venue: paper.venue || "",
      url: paper.url || "",
      citedByUrl: paper.citedByUrl || "",
      pdfUrl: pdfInfo?.url || "",
      pdfLabel: pdfInfo?.label || "",
      doi: doi || "",
      citations: citations || 0,
      abstract: (snippetText || "").trim()
    };
  }

  function paperFingerprint(p) {
    const doi = normalizeDoi(p?.doi || "");
    if (doi) return `doi:${doi}`;
    const titleKey = normalizeTitleForMatch(p?.title || "");
    return titleKey ? `title:${titleKey}` : `key:${p?.id || ""}`;
  }

  function addPapersToProject(project, papers) {
    if (!project || !Array.isArray(papers)) return { added: 0, duplicates: 0 };
    let added = 0;
    let duplicates = 0;
    for (const p of papers) {
      if (!p || !p.title) continue;
      const fp = paperFingerprint(p);
      if (project.fingerprints?.[fp]) {
        duplicates += 1;
        project.duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
        project.duplicates.push({
          fingerprint: fp,
          existingId: project.fingerprints[fp],
          paper: { ...p },
          addedAt: new Date().toISOString()
        });
        continue;
      }
      project.papers[p.id] = p;
      project.fingerprints[fp] = p.id;
      added += 1;
    }
    project.dedupeCount = (project.dedupeCount || 0) + duplicates;
    return { added, duplicates };
  }

  function getActiveReviewer(project) {
    normalizeReviewProject(project);
    const reviewers = project.reviewers || [];
    let active = reviewers.find((r) => r.id === project.activeReviewerId) || reviewers[0];
    if (!active) {
      active = { ...REVIEW_DEFAULT_REVIEWERS[0] };
      reviewers.push(active);
      project.activeReviewerId = active.id;
    }
    return active;
  }

  function normalizeDecisionEntry(decision) {
    if (!decision || typeof decision !== "object") return { status: "unscreened", reason: "", votes: [] };
    decision.status = decision.status || "unscreened";
    decision.reason = decision.reason || "";
    decision.overrideStatus = decision.overrideStatus || "";
    decision.votes = Array.isArray(decision.votes) ? decision.votes : [];
    return decision;
  }

  function getDecisionVotes(project, id) {
    const decision = normalizeDecisionEntry(project.decisions?.[id]);
    return Array.isArray(decision.votes) ? decision.votes : [];
  }

  function getConsensusStatus(votes) {
    if (!Array.isArray(votes) || votes.length === 0) return "unscreened";
    const counts = { include: 0, exclude: 0, maybe: 0 };
    votes.forEach((v) => {
      if (v?.status === "include") counts.include += 1;
      else if (v?.status === "exclude") counts.exclude += 1;
      else if (v?.status === "maybe") counts.maybe += 1;
    });
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (entries[0][1] === 0) return "unscreened";
    if (entries[1][1] === entries[0][1]) return "conflict";
    return entries[0][0];
  }

  function getDecisionStatus(project, id, opts = {}) {
    const decision = normalizeDecisionEntry(project.decisions?.[id]);
    const reviewerId = opts.reviewerId;
    const blind = !!opts.blind;
    if (blind && reviewerId) {
      const vote = decision.votes.find((v) => v.reviewerId === reviewerId);
      return vote?.status || "unscreened";
    }
    if (decision.overrideStatus) return decision.overrideStatus;
    if (decision.votes && decision.votes.length) {
      return getConsensusStatus(decision.votes);
    }
    return decision.status || "unscreened";
  }

  function getReviewerVote(decision, reviewerId) {
    if (!decision || !reviewerId) return null;
    const votes = Array.isArray(decision.votes) ? decision.votes : [];
    return votes.find((v) => v.reviewerId === reviewerId) || null;
  }

  function upsertReviewerVote(project, id, reviewerId, nextStatus, nextReason) {
    if (!project || !id || !reviewerId) return;
    project.decisions = project.decisions || {};
    const decision = normalizeDecisionEntry(project.decisions[id]);
    const votes = decision.votes;
    const existing = votes.find((v) => v.reviewerId === reviewerId);
    const now = new Date().toISOString();
    if (nextStatus && decision.overrideStatus) {
      decision.overrideStatus = "";
    }
    if (existing) {
      if (nextStatus) existing.status = nextStatus;
      if (nextReason != null) existing.reason = nextReason;
      existing.updatedAt = now;
    } else {
      votes.push({
        reviewerId,
        status: nextStatus || "unscreened",
        reason: nextReason || "",
        createdAt: now,
        updatedAt: now
      });
    }
    decision.status = getConsensusStatus(votes);
    project.decisions[id] = decision;
  }

  function computeReviewStats(project) {
    const ids = Object.keys(project.papers || {});
    let included = 0;
    let excluded = 0;
    let maybe = 0;
    let unscreened = 0;
    let conflicts = 0;
    for (const id of ids) {
      const status = getDecisionStatus(project, id, { blind: false });
      if (status === "include") included += 1;
      else if (status === "exclude") excluded += 1;
      else if (status === "maybe") maybe += 1;
      else if (status === "conflict") conflicts += 1;
      else unscreened += 1;
    }
    return {
      total: ids.length,
      included,
      excluded,
      maybe,
      unscreened,
      conflicts,
      duplicates: project.dedupeCount || 0
    };
  }

  function computeReviewerStats(project) {
    const reviewers = Array.isArray(project.reviewers) ? project.reviewers : [];
    const stats = {};
    reviewers.forEach((r) => {
      stats[r.id] = { name: r.name, total: 0, include: 0, exclude: 0, maybe: 0 };
    });
    const decisions = project.decisions || {};
    for (const [id, decisionRaw] of Object.entries(decisions)) {
      const decision = normalizeDecisionEntry(decisionRaw);
      const votes = Array.isArray(decision.votes) ? decision.votes : [];
      votes.forEach((v) => {
        const entry = stats[v.reviewerId] || (stats[v.reviewerId] = { name: v.reviewerId, total: 0, include: 0, exclude: 0, maybe: 0 });
        entry.total += 1;
        if (v.status === "include") entry.include += 1;
        else if (v.status === "exclude") entry.exclude += 1;
        else if (v.status === "maybe") entry.maybe += 1;
      });
    }
    return Object.entries(stats).map(([id, s]) => ({ id, ...s }));
  }

  function getConflictPapers(project) {
    const out = [];
    const papers = project.papers || {};
    for (const id of Object.keys(papers)) {
      const decision = normalizeDecisionEntry(project.decisions?.[id]);
      const votes = Array.isArray(decision.votes) ? decision.votes : [];
      if (votes.length < 2) continue;
      const consensus = getConsensusStatus(votes);
      if (consensus !== "conflict") continue;
      out.push({ id, paper: papers[id], votes });
    }
    return out;
  }

  function buildTermWeights(project) {
    const papers = project.papers || {};
    const pos = new Map();
    const neg = new Map();
    const tokenize = (txt) => String(txt || "").toLowerCase().replace(/[^a-z0-9\\s]/g, " ").split(/\\s+/).filter((t) => t.length >= 3);
    Object.keys(papers).forEach((id) => {
      const status = getDecisionStatus(project, id, { blind: false });
      if (status !== "include" && status !== "exclude") return;
      const text = `${papers[id]?.title || ""} ${papers[id]?.abstract || ""}`;
      const tokens = tokenize(text);
      const target = status === "include" ? pos : neg;
      tokens.forEach((t) => target.set(t, (target.get(t) || 0) + 1));
    });
    const weights = new Map();
    const allTokens = new Set([...pos.keys(), ...neg.keys()]);
    allTokens.forEach((t) => {
      const a = (pos.get(t) || 0) + 1;
      const b = (neg.get(t) || 0) + 1;
      weights.set(t, Math.log(a / b));
    });
    return weights;
  }

  function scorePaperForActiveLearning(paper, weights) {
    if (!paper) return 0;
    if (!weights || weights.size === 0) return 0;
    const tokens = String(`${paper.title || ""} ${paper.abstract || ""}`)
      .toLowerCase()
      .replace(/[^a-z0-9\\s]/g, " ")
      .split(/\\s+/)
      .filter((t) => t.length >= 3);
    let score = 0;
    tokens.forEach((t) => { score += weights.get(t) || 0; });
    return Math.round(score * 10) / 10;
  }

  function normalizeExtractionField(field, idx) {
    if (!field) return { key: `field_${idx}`, label: `Field ${idx + 1}`, type: "text", options: [] };
    if (typeof field === "string") {
      const key = field.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || `field_${idx}`;
      return { key, label: field, type: "text", options: [] };
    }
    const key = String(field.key || field.label || `field_${idx}`).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || `field_${idx}`;
    return {
      key,
      label: field.label || field.key || `Field ${idx + 1}`,
      type: field.type || "text",
      options: Array.isArray(field.options) ? field.options : []
    };
  }

  function getExtractionFields(project) {
    const fields = Array.isArray(project.extractionFields) ? project.extractionFields : [];
    return fields.map((f, idx) => normalizeExtractionField(f, idx));
  }

  function renderPrismaDiagram(stats) {
    const total = stats.total;
    const dupes = stats.duplicates;
    const screened = total - stats.unscreened;
    const included = stats.included;
    return `
      <svg class="su-review-prisma" viewBox="0 0 520 240" role="img" aria-label="PRISMA diagram">
        <rect x="20" y="20" width="200" height="50" rx="8"></rect>
        <text x="120" y="48" text-anchor="middle">Records: ${total}</text>
        <rect x="300" y="20" width="200" height="50" rx="8"></rect>
        <text x="400" y="48" text-anchor="middle">Duplicates: ${dupes}</text>
        <rect x="20" y="110" width="200" height="50" rx="8"></rect>
        <text x="120" y="138" text-anchor="middle">Screened: ${screened}</text>
        <rect x="300" y="110" width="200" height="50" rx="8"></rect>
        <text x="400" y="138" text-anchor="middle">Excluded: ${stats.excluded}</text>
        <rect x="160" y="190" width="200" height="50" rx="8"></rect>
        <text x="260" y="218" text-anchor="middle">Included: ${included}</text>
        <line x1="120" y1="70" x2="120" y2="110"></line>
        <line x1="220" y1="135" x2="300" y2="135"></line>
        <line x1="220" y1="135" x2="260" y2="190"></line>
      </svg>
    `;
  }

  function exportReviewDecisionsCsv(project) {
    if (!project) return;
    const rows = [["Title", "Year", "Decision", "Consensus", "Reviewer", "Reason", "Tags", "URL"]];
    for (const [id, paper] of Object.entries(project.papers || {})) {
      const decision = normalizeDecisionEntry(project.decisions?.[id]);
      const consensus = getDecisionStatus(project, id, { blind: false });
      const tags = Array.isArray(project.tags?.[id]) ? project.tags[id].join("; ") : "";
      const votes = Array.isArray(decision.votes) && decision.votes.length ? decision.votes : [{ reviewerId: "", status: decision.status, reason: decision.reason }];
      votes.forEach((v) => {
        const reviewerName = (project.reviewers || []).find((r) => r.id === v.reviewerId)?.name || v.reviewerId || "";
        rows.push([
          paper.title || "",
          paper.year || "",
          v.status || "",
          consensus,
          reviewerName,
          v.reason || "",
          tags,
          paper.url || ""
        ]);
      });
    }
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-decisions.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }

  function exportReviewExtractionCsv(project) {
    if (!project) return;
    const fields = getExtractionFields(project);
    const headers = ["Title", "Year", ...fields.map((f) => f.label)];
    const rows = [headers];
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    includedIds.forEach((id) => {
      const paper = project.papers[id];
      const data = project.extraction[id] || {};
      rows.push([
        paper?.title || "",
        paper?.year || "",
        ...fields.map((f) => data[f.key] || "")
      ]);
    });
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-extraction.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }

  function exportReviewQualityCsv(project) {
    if (!project) return;
    const checklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
    const headers = ["Title", "Year", "Risk", ...checklist.map((c) => c.label), "Notes"];
    const rows = [headers];
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    includedIds.forEach((id) => {
      const paper = project.papers[id];
      const q = project.quality[id] || {};
      const checks = q.checks || {};
      rows.push([
        paper?.title || "",
        paper?.year || "",
        q.risk || "",
        ...checklist.map((c) => (checks[c.key] ? "Yes" : "")),
        q.notes || ""
      ]);
    });
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-quality.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }

  function exportReviewBibTeX(project) {
    if (!project) return;
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const entries = includedIds.map((id) => {
      const p = project.papers[id];
      const key = `${(project.name || "review").replace(/\\W+/g, "")}${p.year || ""}${hashString(p.title || "")}`.slice(0, 32);
      const authors = p.authors ? p.authors.split(/\\s*[,;]\\s*|\\s+and\\s+/i).join(" and ") : "";
      return `@article{${key},\\n  title={${p.title || ""}},\\n  author={${authors}},\\n  year={${p.year || ""}},\\n  doi={${p.doi || ""}},\\n  url={${p.url || ""}}\\n}`;
    }).join("\\n\\n");
    downloadBlob(`${project.name}-included.bib`.replace(/\\s+/g, "-").toLowerCase(), "text/plain;charset=utf-8", entries || "");
  }

  function exportReviewReport(project, stats) {
    if (!project) return;
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const lines = [
      `# Systematic Review: ${project.name}`,
      "",
      `Query: ${project.query || "—"}`,
      `Generated: ${new Date().toLocaleString()}`,
      "",
      `Total records: ${stats.total}`,
      `Included: ${stats.included}`,
      `Excluded: ${stats.excluded}`,
      `Maybe: ${stats.maybe}`,
      `Conflicts: ${stats.conflicts}`,
      "",
      "## Included papers",
      ...includedIds.map((id) => `- ${project.papers[id]?.title || "Untitled"} (${project.papers[id]?.year || "—"})`)
    ];
    downloadBlob(`${project.name}-report.md`.replace(/\\s+/g, "-").toLowerCase(), "text/markdown;charset=utf-8", lines.join("\\n"));
  }

  function exportPrismaSvg(stats, name) {
    const svg = renderPrismaDiagram(stats);
    const filename = `${(name || "review")}-prisma.svg`.replace(/\\s+/g, "-").toLowerCase();
    downloadBlob(filename, "image/svg+xml;charset=utf-8", svg);
  }

  async function checkReviewUpdates(project) {
    if (!project) return;
    const lastCheck = project.lastUpdateCheck ? new Date(project.lastUpdateCheck) : new Date(0);
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const seeds = includedIds.slice(0, 8);
    const newUpdates = [];
    for (const id of seeds) {
      const paper = project.papers[id];
      if (!paper?.title) continue;
      const work = await fetchOpenAlexWorkForPaper(paper, "");
      const related = Array.isArray(work?.relatedWorks) ? work.relatedWorks.slice(0, 10) : [];
      for (const rid of related) {
        const w = await fetchOpenAlexWorkById(rid);
        if (!w || !w.year) continue;
        const dateStr = w?.id ? null : null;
        const pubYear = Number(w.year) || 0;
        if (pubYear && pubYear >= lastCheck.getFullYear()) {
          newUpdates.push({ id: w.openalexId || rid, title: w.title, year: w.year, url: w.url || "" });
        }
      }
    }
    const existing = Array.isArray(project.updates) ? project.updates : [];
    const existingIds = new Set(existing.map((u) => u.id));
    project.updates = [...newUpdates.filter((u) => !existingIds.has(u.id)), ...existing].slice(0, 50);
    project.lastUpdateCheck = new Date().toISOString();
  }

  function renderReviewOverlay(project) {
    const overlay = ensureReviewOverlay();
    const content = overlay.querySelector("#su-review-content");
    const state = window.suReviewState;
    if (!content || !state || !project) return;
    normalizeReviewProject(project);
    const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
    if (reviewerSelect) {
      reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
      reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
    }
    const blindToggleTop = overlay.querySelector("#su-review-blind-toggle-top");
    if (blindToggleTop) blindToggleTop.checked = !!project.blindMode;
    overlay.querySelectorAll(".su-review-tab").forEach((btn) => {
      const isActive = btn.dataset.reviewTab === (state.tab || "overview");
      btn.classList.toggle("su-review-tab-active", isActive);
    });
    const reviewer = getActiveReviewer(project);
    const reviewerId = reviewer?.id || "you";
    const stats = computeReviewStats(project);
    const tab = state.tab || "overview";
    const weights = buildTermWeights(project);
    const decisionFilter = overlay.querySelector("#su-review-filter-status")?.value || "all";
    const searchFilter = String(overlay.querySelector("#su-review-filter-search")?.value || "").toLowerCase().trim();
    const tagFilterRaw = String(overlay.querySelector("#su-review-filter-tag")?.value || "").toLowerCase().trim();
    const tagFilters = tagFilterRaw ? tagFilterRaw.split(",").map((t) => t.trim()).filter(Boolean) : [];
    const conflictOnly = !!overlay.querySelector("#su-review-filter-conflict")?.checked;
    const sortMode = overlay.querySelector("#su-review-filter-sort")?.value || "score";
    const picoFilter = {
      p: String(overlay.querySelector("#su-review-filter-p")?.value || "").toLowerCase().trim(),
      i: String(overlay.querySelector("#su-review-filter-i")?.value || "").toLowerCase().trim(),
      c: String(overlay.querySelector("#su-review-filter-c")?.value || "").toLowerCase().trim(),
      o: String(overlay.querySelector("#su-review-filter-o")?.value || "").toLowerCase().trim()
    };

    if (tab === "overview") {
      const screened = stats.total - stats.unscreened;
      const pct = stats.total ? Math.round((screened / stats.total) * 100) : 0;
      const tagCounts = new Map();
      Object.values(project.tags || {}).forEach((list) => {
        (list || []).forEach((t) => {
          const key = String(t || "").trim();
          if (!key) return;
          tagCounts.set(key, (tagCounts.get(key) || 0) + 1);
        });
      });
      const topTags = Array.from(tagCounts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
      const reviewerStats = computeReviewerStats(project);
      const sortedWeights = Array.from(weights.entries()).sort((a, b) => b[1] - a[1]);
      const topPos = sortedWeights.filter(([, w]) => w > 0).slice(0, 6);
      const topNeg = sortedWeights.filter(([, w]) => w < 0).slice(0, 6);
      content.innerHTML = `
        <div class="su-review-stats">
          <div><strong>${stats.total}</strong> records</div>
          <div><strong>${stats.included}</strong> included</div>
          <div><strong>${stats.excluded}</strong> excluded</div>
          <div><strong>${stats.maybe}</strong> maybe</div>
          <div><strong>${stats.unscreened}</strong> unscreened</div>
          <div><strong>${stats.duplicates}</strong> duplicates</div>
          <div><strong>${stats.conflicts}</strong> conflicts</div>
        </div>
        <div class="su-review-progress">
          <div class="su-review-progress-bar"><span style="width:${pct}%"></span></div>
          <div class="su-review-progress-meta">${pct}% screened (${screened}/${stats.total || 0})</div>
        </div>
        <div class="su-review-overview-grid">
          <div class="su-review-card">
            <div class="su-review-card-title">Team activity</div>
            ${reviewerStats.length ? reviewerStats.map((r) => `
              <div class="su-review-card-row">
                <span>${escapeHtml(r.name || r.id)}</span>
                <span>${r.total} screens · ${r.include} include</span>
              </div>
            `).join("") : '<div class="su-review-muted">No reviewers yet.</div>'}
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Top tags</div>
            <div class="su-review-tag-cloud">
              ${topTags.length ? topTags.map(([tag, count]) => `<span class="su-review-tag-chip">${escapeHtml(tag)} <em>${count}</em></span>`).join("") : '<span class="su-review-muted">No tags yet.</span>'}
            </div>
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Active learning signals</div>
            <div class="su-review-signal-grid">
              <div>
                <div class="su-review-signal-label">Positive terms</div>
                ${topPos.length ? topPos.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>+${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more includes.</div>'}
              </div>
              <div>
                <div class="su-review-signal-label">Negative terms</div>
                ${topNeg.length ? topNeg.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more excludes.</div>'}
              </div>
            </div>
          </div>
        </div>
        <div class="su-review-help">
          <p>Workflow: import results → screen (include/exclude) → extract data → assess quality → export.</p>
          <p>Active learning uses your include/exclude decisions to prioritize similar papers.</p>
        </div>
      `;
      return;
    }

    if (tab === "screening") {
      const ids = Object.keys(project.papers || {});
      let rows = ids.map((id) => {
        const p = project.papers[id];
        const decision = normalizeDecisionEntry(project.decisions[id]);
        const status = getDecisionStatus(project, id, { blind: project.blindMode, reviewerId });
        const consensus = getDecisionStatus(project, id, { blind: false });
        const score = scorePaperForActiveLearning(p, weights);
        const vote = getReviewerVote(decision, reviewerId);
        const tags = Array.isArray(project.tags?.[id]) ? project.tags[id] : [];
        return { id, p, decision, status, consensus, score, vote, tags };
      });
      if (decisionFilter !== "all") {
        if (decisionFilter === "conflict") rows = rows.filter((r) => r.consensus === "conflict");
        else rows = rows.filter((r) => r.status === decisionFilter);
      }
      if (conflictOnly) rows = rows.filter((r) => r.consensus === "conflict");
      if (searchFilter) rows = rows.filter((r) => (r.p.title || "").toLowerCase().includes(searchFilter));
      if (tagFilters.length) rows = rows.filter((r) => {
        const tagSet = new Set((r.tags || []).map((t) => String(t || "").toLowerCase()));
        return tagFilters.some((t) => tagSet.has(t));
      });
      rows = rows.filter((r) => {
        const pico = project.pico?.[r.id] || {};
        const match = (field, val) => !val || String(field || "").toLowerCase().includes(val);
        return match(pico.population, picoFilter.p) && match(pico.intervention, picoFilter.i) && match(pico.comparator, picoFilter.c) && match(pico.outcome, picoFilter.o);
      });
      if (sortMode === "recent") {
        rows.sort((a, b) => (Number(b.p.year) || 0) - (Number(a.p.year) || 0));
      } else if (sortMode === "citations") {
        rows.sort((a, b) => (Number(b.p.citations) || 0) - (Number(a.p.citations) || 0));
      } else if (sortMode === "title") {
        rows.sort((a, b) => String(a.p.title || "").localeCompare(String(b.p.title || "")));
      } else {
        rows.sort((a, b) => b.score - a.score);
      }
      const limited = rows.slice(0, 80);
      content.innerHTML = `
        <div class="su-review-filters">
          <select id="su-review-filter-status">
            <option value="all">All</option>
            <option value="unscreened">Unscreened</option>
            <option value="include">Include</option>
            <option value="exclude">Exclude</option>
            <option value="maybe">Maybe</option>
            <option value="conflict">Conflict</option>
          </select>
          <select id="su-review-filter-sort">
            <option value="score">Sort: active learning</option>
            <option value="recent">Sort: recent</option>
            <option value="citations">Sort: citations</option>
            <option value="title">Sort: title</option>
          </select>
          <input id="su-review-filter-search" class="su-review-input" placeholder="Search title" />
          <input id="su-review-filter-tag" class="su-review-input" placeholder="Tags (comma-separated)" />
          <label class="su-review-filter-check"><input type="checkbox" id="su-review-filter-conflict" ${conflictOnly ? "checked" : ""} /> Conflicts</label>
          <input id="su-review-filter-p" class="su-review-input" placeholder="P: population" />
          <input id="su-review-filter-i" class="su-review-input" placeholder="I: intervention" />
          <input id="su-review-filter-c" class="su-review-input" placeholder="C: comparator" />
          <input id="su-review-filter-o" class="su-review-input" placeholder="O: outcome" />
        </div>
        <div class="su-review-actions">
          <button type="button" class="su-graph-btn" data-review-autoscreen="1">Auto-screen suggestions</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-decisions="1">Export decisions CSV</button>
        </div>
        <div class="su-review-table">
          ${limited.map((r) => {
            const decision = r.decision || {};
            const pico = project.pico[r.id] || {};
            const vote = r.vote || {};
            const suggestion = r.score >= 1.2 ? "include" : (r.score <= -1.2 ? "exclude" : "");
            const statusClass = `su-review-status-${r.status}`;
            return `
              <details class="su-review-row" data-review-id="${escapeHtml(r.id)}">
                <summary>
                  <span class="su-review-title">
                    ${r.p.url ? `<a href="${escapeHtml(r.p.url)}" target="_blank" rel="noopener">${escapeHtml(r.p.title || "Untitled")}</a>` : escapeHtml(r.p.title || "Untitled")}
                  </span>
                  <span class="su-review-meta">${r.p.year || "—"} · ${r.p.citations || 0} cites · score ${r.score}</span>
                  <span class="su-review-badges">
                    <span class="su-review-status ${statusClass}">${r.status}</span>
                    ${suggestion ? `<span class="su-review-suggest su-review-suggest-${suggestion}">AI: ${suggestion}</span>` : ""}
                    ${r.tags.length ? r.tags.map((t) => `<span class="su-review-tag-chip">${escapeHtml(t)}</span>`).join("") : ""}
                  </span>
                  <select class="su-review-decision" data-review-decision>
                    <option value="unscreened" ${r.status === "unscreened" ? "selected" : ""}>Unscreened</option>
                    <option value="include" ${r.status === "include" ? "selected" : ""}>Include</option>
                    <option value="exclude" ${r.status === "exclude" ? "selected" : ""}>Exclude</option>
                    <option value="maybe" ${r.status === "maybe" ? "selected" : ""}>Maybe</option>
                  </select>
                </summary>
                <div class="su-review-row-body">
                  <label>Reason <input type="text" class="su-review-input" data-review-reason value="${escapeHtml(vote?.reason || decision.reason || "")}"></label>
                  <div class="su-review-pico">
                    <input class="su-review-input" placeholder="Population" data-review-pico="population" value="${escapeHtml(pico.population || "")}" />
                    <input class="su-review-input" placeholder="Intervention" data-review-pico="intervention" value="${escapeHtml(pico.intervention || "")}" />
                    <input class="su-review-input" placeholder="Comparator" data-review-pico="comparator" value="${escapeHtml(pico.comparator || "")}" />
                    <input class="su-review-input" placeholder="Outcome" data-review-pico="outcome" value="${escapeHtml(pico.outcome || "")}" />
                  </div>
                  <div class="su-review-tags">
                    <div class="su-review-tag-list">
                      ${(r.tags || []).map((t, idx) => `<span class="su-review-tag-chip">${escapeHtml(t)} <button type="button" data-review-remove-tag="${idx}">×</button></span>`).join("")}
                    </div>
                    <div class="su-review-tag-input-row">
                      <input class="su-review-input su-review-tag-input" placeholder="Add tag" data-review-tag-input value="" />
                      <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-add-tag="1">Add tag</button>
                    </div>
                  </div>
                  <label>Notes <textarea class="su-review-input su-review-notes" data-review-notes>${escapeHtml(project.notes[r.id] || "")}</textarea></label>
                  <div class="su-review-highlights">
                    <div class="su-review-highlights-header">
                      <span>PDF highlights</span>
                      ${r.p.pdfUrl ? `<a class="su-review-pdf-link" href="${escapeHtml(r.p.pdfUrl)}" target="_blank" rel="noopener">Open PDF</a>` : ""}
                    </div>
                    <div class="su-review-highlights-list">
                      ${(project.highlights?.[r.id] || []).map((h, hIdx) => `
                        <div class="su-review-highlight-item">
                          <div class="su-review-highlight-quote">“${escapeHtml(h.quote || "")}”</div>
                          <div class="su-review-highlight-meta">p.${escapeHtml(h.page || "")} · ${escapeHtml(h.note || "")}</div>
                          <button type="button" class="su-review-highlight-remove" data-review-remove-highlight="${hIdx}">Remove</button>
                        </div>
                      `).join("")}
                    </div>
                    <div class="su-review-highlight-form">
                      <input class="su-review-input su-review-highlight-page" placeholder="Page" data-review-highlight-page />
                      <input class="su-review-input su-review-highlight-quote" placeholder="Quote" data-review-highlight-quote />
                      <input class="su-review-input su-review-highlight-note" placeholder="Note" data-review-highlight-note />
                      <button type="button" class="su-graph-btn" data-review-add-highlight="1">Add highlight</button>
                    </div>
                  </div>
                  ${project.blindMode ? "" : `
                    <div class="su-review-votes">
                      <div class="su-review-votes-title">Votes</div>
                      ${(decision.votes || []).map((v) => {
                        const reviewerName = (project.reviewers || []).find((r2) => r2.id === v.reviewerId)?.name || v.reviewerId;
                        return `<div class="su-review-vote-item">${escapeHtml(reviewerName)}: ${escapeHtml(v.status || "")}</div>`;
                      }).join("") || '<div class="su-review-muted">No votes yet.</div>'}
                    </div>
                  `}
                </div>
              </details>
            `;
          }).join("")}
        </div>
      `;
      const statusSel = content.querySelector("#su-review-filter-status");
      if (statusSel) statusSel.value = decisionFilter;
      const sortSel = content.querySelector("#su-review-filter-sort");
      if (sortSel) sortSel.value = sortMode;
      const searchInput = content.querySelector("#su-review-filter-search");
      if (searchInput) searchInput.value = searchFilter;
      const tagInput = content.querySelector("#su-review-filter-tag");
      if (tagInput) tagInput.value = tagFilterRaw;
      const pInput = content.querySelector("#su-review-filter-p");
      const iInput = content.querySelector("#su-review-filter-i");
      const cInput = content.querySelector("#su-review-filter-c");
      const oInput = content.querySelector("#su-review-filter-o");
      if (pInput) pInput.value = picoFilter.p;
      if (iInput) iInput.value = picoFilter.i;
      if (cInput) cInput.value = picoFilter.c;
      if (oInput) oInput.value = picoFilter.o;
      const conflictCheck = content.querySelector("#su-review-filter-conflict");
      if (conflictCheck) conflictCheck.checked = conflictOnly;
      const inputs = content.querySelectorAll(".su-review-filters .su-review-input");
      inputs.forEach((input) => {
        input.addEventListener("input", () => renderReviewOverlay(project));
      });
      if (statusSel) statusSel.addEventListener("change", () => renderReviewOverlay(project));
      if (sortSel) sortSel.addEventListener("change", () => renderReviewOverlay(project));
      if (conflictCheck) conflictCheck.addEventListener("change", () => renderReviewOverlay(project));
      return;
    }

    if (tab === "dedupe") {
      const duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
      content.innerHTML = `
        <div class="su-review-help">
          <p>${duplicates.length} duplicate records detected via DOI/title fingerprinting.</p>
        </div>
        <div class="su-review-duplicates">
          ${duplicates.length ? duplicates.map((d, idx) => `
            <div class="su-review-duplicate-item" data-review-duplicate-index="${idx}">
              <div class="su-review-duplicate-title">${escapeHtml(d.paper?.title || "Untitled")}</div>
              <div class="su-review-duplicate-meta">Matches ${escapeHtml(d.fingerprint || "")}</div>
              <div class="su-review-duplicate-actions">
                <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-add-duplicate="${idx}">Add anyway</button>
                <button type="button" class="su-graph-btn" data-review-dismiss-duplicate="${idx}">Dismiss</button>
              </div>
            </div>
          `).join("") : '<div class="su-review-muted">No duplicates detected.</div>'}
        </div>
      `;
      return;
    }

    if (tab === "extraction") {
      const fields = getExtractionFields(project);
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      content.innerHTML = `
        <div class="su-review-extract-controls">
          <input id="su-review-field-name" class="su-review-input" placeholder="Add field (e.g., Sample Size)" />
          <select id="su-review-field-type" class="su-review-input">
            <option value="text">Text</option>
            <option value="number">Number</option>
            <option value="select">Select</option>
          </select>
          <input id="su-review-field-options" class="su-review-input" placeholder="Options (comma-separated)" />
          <button type="button" class="su-graph-btn" data-review-add-field="1">Add field</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-extraction="1">Export extraction CSV</button>
        </div>
        <div class="su-review-extract-table">
          <table>
            <thead>
              <tr>
                <th>Paper</th>
                ${fields.map((f) => `<th>${escapeHtml(f.label)}</th>`).join("")}
              </tr>
            </thead>
            <tbody>
              ${includedIds.map((id) => {
                const p = project.papers[id];
                const data = project.extraction[id] || {};
                return `<tr data-review-id="${escapeHtml(id)}">
                  <td class="su-review-extract-title">${escapeHtml(p?.title || "Untitled")}</td>
                  ${fields.map((f) => {
                    const value = data[f.key] || "";
                    if (f.type === "select") {
                      return `<td><select class="su-review-input" data-review-extract="${escapeHtml(f.key)}">
                        <option value=""></option>
                        ${f.options.map((opt) => `<option value="${escapeHtml(opt)}" ${String(opt) === String(value) ? "selected" : ""}>${escapeHtml(opt)}</option>`).join("")}
                      </select></td>`;
                    }
                    const inputType = f.type === "number" ? "number" : "text";
                    return `<td><input class="su-review-input" type="${inputType}" data-review-extract="${escapeHtml(f.key)}" value="${escapeHtml(value)}" /></td>`;
                  }).join("")}
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
      `;
      return;
    }

    if (tab === "quality") {
      const checklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      content.innerHTML = `
        <div class="su-review-quality-controls">
          <input id="su-review-quality-field" class="su-review-input" placeholder="Add checklist item" />
          <button type="button" class="su-graph-btn" data-review-add-quality="1">Add item</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-quality="1">Export quality CSV</button>
        </div>
        <div class="su-review-quality-table">
          <table>
            <thead>
              <tr>
                <th>Paper</th>
                <th>Risk</th>
                ${checklist.map((c) => `<th>${escapeHtml(c.label)}</th>`).join("")}
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              ${includedIds.map((id) => {
                const p = project.papers[id];
                const q = project.quality[id] || {};
                const checks = q.checks || {};
                return `<tr data-review-id="${escapeHtml(id)}">
                  <td class="su-review-extract-title">${escapeHtml(p?.title || "Untitled")}</td>
                  <td>
                    <select class="su-review-input" data-review-quality-risk>
                      <option value=""></option>
                      <option value="low" ${q.risk === "low" ? "selected" : ""}>Low</option>
                      <option value="some" ${q.risk === "some" ? "selected" : ""}>Some</option>
                      <option value="high" ${q.risk === "high" ? "selected" : ""}>High</option>
                    </select>
                  </td>
                  ${checklist.map((c) => `<td><input type="checkbox" data-review-quality-check="${escapeHtml(c.key)}" ${checks[c.key] ? "checked" : ""} /></td>`).join("")}
                  <td><input class="su-review-input" data-review-quality-notes value="${escapeHtml(q.notes || "")}" /></td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
      `;
      return;
    }

    if (tab === "prisma") {
      content.innerHTML = `
        <div class="su-review-prisma-wrap">
          ${renderPrismaDiagram(stats)}
        </div>
        <div class="su-review-actions">
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-prisma="1">Download PRISMA SVG</button>
        </div>
      `;
      return;
    }

    if (tab === "updates") {
      const updates = Array.isArray(project.updates) ? project.updates : [];
      content.innerHTML = `
        <div class="su-review-updates">
          <div class="su-review-updates-meta">Last check: ${project.lastUpdateCheck ? new Date(project.lastUpdateCheck).toLocaleString() : "Never"}</div>
          <button type="button" class="su-graph-btn" data-review-check-updates="1">Check updates</button>
          <div class="su-review-updates-list">
            ${updates.length ? updates.map((u) => `<div class="su-review-update-item">${escapeHtml(u.title || "Untitled")} <span>${u.year || ""}</span></div>`).join("") : '<div class="su-graph-empty">No updates yet.</div>'}
          </div>
        </div>
      `;
      return;
    }

    if (tab === "insights") {
      const screened = stats.total - stats.unscreened;
      const includeRate = screened ? stats.included / screened : 0;
      const projected = Math.round(includeRate * stats.unscreened);
      const scenarios = [25, 50, 100].map((n) => ({ n, expected: Math.round(includeRate * n) }));
      const sortedWeights = Array.from(weights.entries()).sort((a, b) => b[1] - a[1]);
      const topPos = sortedWeights.filter(([, w]) => w > 0).slice(0, 10);
      const topNeg = sortedWeights.filter(([, w]) => w < 0).slice(0, 10);
      content.innerHTML = `
        <div class="su-review-card">
          <div class="su-review-card-title">Screening simulation</div>
          <div class="su-review-card-row">Current include rate: <strong>${Math.round(includeRate * 100)}%</strong></div>
          <div class="su-review-card-row">Projected includes remaining: <strong>${projected}</strong></div>
          <div class="su-review-sim-grid">
            ${scenarios.map((s) => `<div class="su-review-sim-item">Next ${s.n} screens → ~${s.expected} includes</div>`).join("")}
          </div>
        </div>
        <div class="su-review-signal-grid">
          <div class="su-review-card">
            <div class="su-review-card-title">Include signals</div>
            ${topPos.length ? topPos.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>+${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more include/exclude decisions.</div>'}
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Exclude signals</div>
            ${topNeg.length ? topNeg.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more include/exclude decisions.</div>'}
          </div>
        </div>
      `;
      return;
    }

    if (tab === "team") {
      const reviewerStats = computeReviewerStats(project);
      const conflicts = getConflictPapers(project);
      content.innerHTML = `
        <div class="su-review-team-controls">
          <input id="su-review-reviewer-name" class="su-review-input" placeholder="Add reviewer name" />
          <button type="button" class="su-graph-btn" data-review-add-reviewer="1">Add reviewer</button>
          <label class="su-review-filter-check"><input type="checkbox" id="su-review-blind-toggle" ${project.blindMode ? "checked" : ""} /> Blind screening</label>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-decisions="1">Export team CSV</button>
        </div>
        <div class="su-review-team-list">
          ${reviewerStats.length ? reviewerStats.map((r) => `
            <div class="su-review-team-item">
              <strong>${escapeHtml(r.name || r.id)}</strong>
              <span>${r.total} screens · ${r.include} include · ${r.exclude} exclude</span>
            </div>
          `).join("") : '<div class="su-review-muted">No reviewers yet.</div>'}
        </div>
        <div class="su-review-conflicts">
          <div class="su-review-card-title">Conflicts</div>
          ${conflicts.length ? conflicts.map((c) => `
            <div class="su-review-conflict-item" data-review-id="${escapeHtml(c.id)}">
              <div class="su-review-conflict-title">${escapeHtml(c.paper?.title || "Untitled")}</div>
              <div class="su-review-conflict-votes">
                ${c.votes.map((v) => {
                  const name = (project.reviewers || []).find((r) => r.id === v.reviewerId)?.name || v.reviewerId;
                  return `${escapeHtml(name)}: ${escapeHtml(v.status || "")}`;
                }).join(" · ")}
              </div>
              <select class="su-review-input" data-review-conflict-resolve>
                <option value="">Resolve…</option>
                <option value="include">Include</option>
                <option value="exclude">Exclude</option>
                <option value="maybe">Maybe</option>
              </select>
            </div>
          `).join("") : '<div class="su-review-muted">No conflicts detected.</div>'}
        </div>
      `;
      return;
    }

    if (tab === "report") {
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      const lines = [
        `# Systematic Review: ${project.name}`,
        "",
        `Query: ${project.query || "—"}`,
        `Generated: ${new Date().toLocaleString()}`,
        "",
        `Total records: ${stats.total}`,
        `Included: ${stats.included}`,
        `Excluded: ${stats.excluded}`,
        `Maybe: ${stats.maybe}`,
        `Conflicts: ${stats.conflicts}`,
        "",
        "## Included papers",
        ...includedIds.map((id) => `- ${project.papers[id]?.title || "Untitled"} (${project.papers[id]?.year || "—"})`)
      ];
      const reportText = lines.join("\\n");
      content.innerHTML = `
        <textarea class="su-review-report" readonly>${reportText}</textarea>
        <div class="su-review-actions">
          <button type="button" class="su-graph-btn" data-review-copy-report="1">Copy report</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-report="1">Download report</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export-bib="1">Export BibTeX</button>
        </div>
      `;
      return;
    }
  }

  function ensureReviewOverlay() {
    let overlay = document.getElementById("su-review-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "su-review-overlay";
    overlay.className = "su-review-overlay";
    overlay.innerHTML = `
      <div class="su-review-backdrop" data-review-close="1"></div>
      <div class="su-review-panel">
        <div class="su-review-header">
          <div>
            <div class="su-review-title">Systematic Review Workspace</div>
            <div class="su-review-subtitle">Local screening, dedupe, extraction, quality, PRISMA, and updates</div>
          </div>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-close="1">Close</button>
        </div>
        <div class="su-review-project-bar">
          <select id="su-review-project-select"></select>
          <select id="su-review-reviewer-select" title="Active reviewer"></select>
          <button type="button" class="su-graph-btn" data-review-new="1">New</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-rename="1">Rename</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-add-page="1">Add page results</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-export="1">Export JSON</button>
          <button type="button" class="su-graph-btn su-graph-btn-secondary" data-review-import="1">Import JSON</button>
          <label class="su-review-blind-toggle"><input type="checkbox" id="su-review-blind-toggle-top" /> Blind</label>
          <input type="file" id="su-review-import-file" accept="application/json" style="display:none" />
        </div>
        <div class="su-review-tabs">
          <button type="button" class="su-review-tab" data-review-tab="overview">Overview</button>
          <button type="button" class="su-review-tab" data-review-tab="screening">Screening</button>
          <button type="button" class="su-review-tab" data-review-tab="dedupe">Dedupe</button>
          <button type="button" class="su-review-tab" data-review-tab="extraction">Extraction</button>
          <button type="button" class="su-review-tab" data-review-tab="quality">Quality</button>
          <button type="button" class="su-review-tab" data-review-tab="prisma">PRISMA</button>
          <button type="button" class="su-review-tab" data-review-tab="updates">Updates</button>
          <button type="button" class="su-review-tab" data-review-tab="insights">Insights</button>
          <button type="button" class="su-review-tab" data-review-tab="report">Report</button>
          <button type="button" class="su-review-tab" data-review-tab="team">Team</button>
        </div>
        <div id="su-review-content" class="su-review-content"></div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", async (e) => {
      const close = e.target.closest("[data-review-close]");
      if (close) {
        overlay.classList.remove("su-visible");
        return;
      }
      const tab = e.target.closest("[data-review-tab]");
      if (tab) {
        const state = await ensureReviewState();
        state.tab = String(tab.dataset.reviewTab || "overview");
        renderReviewOverlay(getActiveReviewProject());
        return;
      }
      const newBtn = e.target.closest("[data-review-new]");
      if (newBtn) {
        const state = await ensureReviewState();
        const name = prompt("Project name?", "New review");
        if (!name) return;
        const project = createReviewProject(name, getScholarSearchQuery() || "");
        state.projects[project.id] = project;
        state.activeId = project.id;
        await setReviewProjects(state.projects);
        renderReviewOverlay(project);
        return;
      }
      const renameBtn = e.target.closest("[data-review-rename]");
      if (renameBtn) {
        const project = getActiveReviewProject();
        if (!project) return;
        const name = prompt("Rename project", project.name);
        if (!name) return;
        project.name = name;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addPage = e.target.closest("[data-review-add-page]");
      if (addPage) {
        const project = getActiveReviewProject();
        if (!project) return;
        const { results, isAuthorProfile } = scanResults();
        const papers = results.map((row) => extractReviewPaperFromResult(row, isAuthorProfile)).filter(Boolean);
        addPapersToProject(project, papers);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const exportBtn = e.target.closest("[data-review-export]");
      if (exportBtn) {
        const project = getActiveReviewProject();
        if (!project) return;
        const json = JSON.stringify(project, null, 2);
        downloadBlob(`${project.name.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-review.json`, "application/json", json);
        return;
      }
      const importBtn = e.target.closest("[data-review-import]");
      if (importBtn) {
        const fileInput = overlay.querySelector("#su-review-import-file");
        if (fileInput) fileInput.click();
        return;
      }
      const addField = e.target.closest("[data-review-add-field]");
      if (addField) {
        const project = getActiveReviewProject();
        const fieldInput = overlay.querySelector("#su-review-field-name");
        const value = String(fieldInput?.value || "").trim();
        if (!project || !value) return;
        project.extractionFields = Array.isArray(project.extractionFields) ? project.extractionFields : [];
        const typeSelect = overlay.querySelector("#su-review-field-type");
        const optionsInput = overlay.querySelector("#su-review-field-options");
        const type = String(typeSelect?.value || "text");
        const options = type === "select"
          ? String(optionsInput?.value || "").split(",").map((s) => s.trim()).filter(Boolean)
          : [];
        project.extractionFields.push({ label: value, type, options });
        if (fieldInput) fieldInput.value = "";
        if (optionsInput) optionsInput.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const checkUpdates = e.target.closest("[data-review-check-updates]");
      if (checkUpdates) {
        const project = getActiveReviewProject();
        if (!project) return;
        await checkReviewUpdates(project);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const copyReport = e.target.closest("[data-review-copy-report]");
      if (copyReport) {
        const textarea = overlay.querySelector(".su-review-report");
        if (textarea) {
          textarea.select();
          document.execCommand("copy");
        }
        return;
      }
      const exportReport = e.target.closest("[data-review-export-report]");
      if (exportReport) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewReport(project, computeReviewStats(project));
        return;
      }
      const exportBib = e.target.closest("[data-review-export-bib]");
      if (exportBib) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewBibTeX(project);
        return;
      }
      const exportDecisions = e.target.closest("[data-review-export-decisions]");
      if (exportDecisions) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewDecisionsCsv(project);
        return;
      }
      const exportExtraction = e.target.closest("[data-review-export-extraction]");
      if (exportExtraction) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewExtractionCsv(project);
        return;
      }
      const exportQuality = e.target.closest("[data-review-export-quality]");
      if (exportQuality) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewQualityCsv(project);
        return;
      }
      const exportPrisma = e.target.closest("[data-review-export-prisma]");
      if (exportPrisma) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportPrismaSvg(computeReviewStats(project), project.name);
        return;
      }
      const autoScreen = e.target.closest("[data-review-autoscreen]");
      if (autoScreen) {
        const project = getActiveReviewProject();
        if (!project) return;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        const weights = buildTermWeights(project);
        const candidates = [];
        for (const id of Object.keys(project.papers || {})) {
          const status = getDecisionStatus(project, id, { blind: project.blindMode, reviewerId });
          if (status !== "unscreened") continue;
          const score = scorePaperForActiveLearning(project.papers[id], weights);
          const suggestion = score >= 1.2 ? "include" : (score <= -1.2 ? "exclude" : "");
          if (!suggestion) continue;
          candidates.push({ id, suggestion });
        }
        const toApply = candidates.slice(0, 30);
        if (!toApply.length) return;
        if (!window.confirm(`Auto-screen ${toApply.length} papers based on active-learning suggestions?`)) return;
        toApply.forEach((c) => {
          upsertReviewerVote(project, c.id, reviewerId, c.suggestion, "");
        });
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addReviewer = e.target.closest("[data-review-add-reviewer]");
      if (addReviewer) {
        const project = getActiveReviewProject();
        if (!project) return;
        const input = overlay.querySelector("#su-review-reviewer-name");
        const name = String(input?.value || "").trim();
        if (!name) return;
        const id = `rev_${Date.now().toString(36)}`;
        project.reviewers = Array.isArray(project.reviewers) ? project.reviewers : [];
        project.reviewers.push({ id, name });
        project.activeReviewerId = id;
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addTag = e.target.closest("[data-review-add-tag]");
      if (addTag) {
        const row = addTag.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const input = row.querySelector("[data-review-tag-input]");
        const value = String(input?.value || "").trim();
        if (!value) return;
        project.tags[id] = Array.isArray(project.tags[id]) ? project.tags[id] : [];
        if (!project.tags[id].includes(value)) project.tags[id].push(value);
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const removeTagBtn = e.target.closest("[data-review-remove-tag]");
      if (removeTagBtn) {
        const row = removeTagBtn.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const idx = parseInt(removeTagBtn.getAttribute("data-review-remove-tag") || "-1", 10);
        if (!Array.isArray(project.tags[id])) return;
        project.tags[id].splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addHighlight = e.target.closest("[data-review-add-highlight]");
      if (addHighlight) {
        const row = addHighlight.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const pageInput = row.querySelector("[data-review-highlight-page]");
        const quoteInput = row.querySelector("[data-review-highlight-quote]");
        const noteInput = row.querySelector("[data-review-highlight-note]");
        const quote = String(quoteInput?.value || "").trim();
        if (!quote) return;
        const page = String(pageInput?.value || "").trim();
        const note = String(noteInput?.value || "").trim();
        project.highlights[id] = Array.isArray(project.highlights[id]) ? project.highlights[id] : [];
        project.highlights[id].push({ quote, page, note, createdAt: new Date().toISOString() });
        if (pageInput) pageInput.value = "";
        if (quoteInput) quoteInput.value = "";
        if (noteInput) noteInput.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const removeHighlight = e.target.closest("[data-review-remove-highlight]");
      if (removeHighlight) {
        const row = removeHighlight.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const idx = parseInt(removeHighlight.getAttribute("data-review-remove-highlight") || "-1", 10);
        if (!Array.isArray(project.highlights[id])) return;
        project.highlights[id].splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addDuplicate = e.target.closest("[data-review-add-duplicate]");
      if (addDuplicate) {
        const project = getActiveReviewProject();
        if (!project) return;
        const idx = parseInt(addDuplicate.getAttribute("data-review-add-duplicate") || "-1", 10);
        const dup = Array.isArray(project.duplicates) ? project.duplicates[idx] : null;
        if (!dup || !dup.paper) return;
        const newId = `${dup.paper.id || dup.paper.title || "dup"}_${Date.now().toString(36)}`;
        project.papers[newId] = { ...dup.paper, id: newId, duplicateOf: dup.existingId || "" };
        project.duplicates.splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const dismissDuplicate = e.target.closest("[data-review-dismiss-duplicate]");
      if (dismissDuplicate) {
        const project = getActiveReviewProject();
        if (!project) return;
        const idx = parseInt(dismissDuplicate.getAttribute("data-review-dismiss-duplicate") || "-1", 10);
        if (Array.isArray(project.duplicates)) project.duplicates.splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addQuality = e.target.closest("[data-review-add-quality]");
      if (addQuality) {
        const project = getActiveReviewProject();
        if (!project) return;
        const input = overlay.querySelector("#su-review-quality-field");
        const label = String(input?.value || "").trim();
        if (!label) return;
        const key = label.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
        project.qualityChecklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
        project.qualityChecklist.push({ key, label });
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
    });
    overlay.addEventListener("change", async (e) => {
      const select = e.target.closest("#su-review-project-select");
      if (select) {
        const state = await ensureReviewState();
        state.activeId = select.value;
        const project = getActiveReviewProject();
        const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
        if (reviewerSelect && project) {
          reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
          reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
        }
        const blindToggle = overlay.querySelector("#su-review-blind-toggle-top");
        if (blindToggle) blindToggle.checked = !!project?.blindMode;
        renderReviewOverlay(project);
        return;
      }
      const reviewerSelect = e.target.closest("#su-review-reviewer-select");
      if (reviewerSelect) {
        const project = getActiveReviewProject();
        if (!project) return;
        project.activeReviewerId = reviewerSelect.value;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const blindToggle = e.target.closest("#su-review-blind-toggle-top, #su-review-blind-toggle");
      if (blindToggle) {
        const project = getActiveReviewProject();
        if (!project) return;
        project.blindMode = !!blindToggle.checked;
        const topToggle = overlay.querySelector("#su-review-blind-toggle-top");
        if (topToggle && topToggle !== blindToggle) topToggle.checked = !!project.blindMode;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const fileInput = e.target.closest("#su-review-import-file");
      if (fileInput && fileInput.files?.length) {
        const file = fileInput.files[0];
        const text = await file.text();
        try {
          const project = JSON.parse(text);
          if (project?.id) {
            normalizeReviewProject(project);
            const state = await ensureReviewState();
            state.projects[project.id] = project;
            state.activeId = project.id;
            await setReviewProjects(state.projects);
            renderReviewOverlay(project);
          }
        } catch {}
        fileInput.value = "";
        return;
      }
      const decision = e.target.closest("[data-review-decision]");
      if (decision) {
        const row = decision.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        upsertReviewerVote(project, id, reviewerId, decision.value, null);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const conflictResolve = e.target.closest("[data-review-conflict-resolve]");
      if (conflictResolve) {
        const row = conflictResolve.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const decision = normalizeDecisionEntry(project.decisions[id]);
        decision.overrideStatus = conflictResolve.value || "";
        project.decisions[id] = decision;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const extractInput = e.target.closest("[data-review-extract]");
      if (extractInput) {
        const row = extractInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = extractInput.dataset.reviewExtract;
        project.extraction[id] = project.extraction[id] || {};
        project.extraction[id][key] = extractInput.value;
        await saveReviewProject(project);
        return;
      }
      const qualityRisk = e.target.closest("[data-review-quality-risk]");
      if (qualityRisk) {
        const row = qualityRisk.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.quality[id] = project.quality[id] || {};
        project.quality[id].risk = qualityRisk.value;
        await saveReviewProject(project);
        return;
      }
      const qualityCheck = e.target.closest("[data-review-quality-check]");
      if (qualityCheck) {
        const row = qualityCheck.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = qualityCheck.getAttribute("data-review-quality-check");
        project.quality[id] = project.quality[id] || {};
        project.quality[id].checks = project.quality[id].checks || {};
        project.quality[id].checks[key] = !!qualityCheck.checked;
        await saveReviewProject(project);
        return;
      }
    });
    overlay.addEventListener("input", async (e) => {
      const reason = e.target.closest("[data-review-reason]");
      if (reason) {
        const row = reason.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        upsertReviewerVote(project, id, reviewerId, null, reason.value);
        await saveReviewProject(project);
        return;
      }
      const picoInput = e.target.closest("[data-review-pico]");
      if (picoInput) {
        const row = picoInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = picoInput.dataset.reviewPico;
        project.pico[id] = project.pico[id] || {};
        project.pico[id][key] = picoInput.value;
        await saveReviewProject(project);
        return;
      }
      const notesInput = e.target.closest("[data-review-notes]");
      if (notesInput) {
        const row = notesInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.notes[id] = notesInput.value;
        await saveReviewProject(project);
        return;
      }
      const qualityNotes = e.target.closest("[data-review-quality-notes]");
      if (qualityNotes) {
        const row = qualityNotes.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.quality[id] = project.quality[id] || {};
        project.quality[id].notes = qualityNotes.value;
        await saveReviewProject(project);
        return;
      }
    });
    return overlay;
  }

  async function openReviewOverlay() {
    await ensureReviewState();
    const overlay = ensureReviewOverlay();
    const state = window.suReviewState;
    const select = overlay.querySelector("#su-review-project-select");
    if (select) {
      select.innerHTML = Object.values(state.projects).map((p) => `<option value="${escapeHtml(p.id)}">${escapeHtml(p.name)}</option>`).join("");
      select.value = state.activeId;
    }
    const project = getActiveReviewProject();
    const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
    if (reviewerSelect && project) {
      reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
      reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
    }
    const blindToggle = overlay.querySelector("#su-review-blind-toggle-top");
    if (blindToggle) blindToggle.checked = !!project?.blindMode;
    overlay.classList.add("su-visible");
    renderReviewOverlay(getActiveReviewProject());
  }

  function openAuthorCompareOverlay(state) {
    let overlay = document.getElementById("su-author-compare-overlay");
    if (!overlay) {
      overlay = document.createElement("div");
      overlay.id = "su-author-compare-overlay";
      overlay.className = "su-author-compare-overlay";
      overlay.innerHTML = `
        <div class="su-author-compare-backdrop"></div>
        <div class="su-author-compare-panel">
          <h3 class="su-author-compare-title">Compare two authors</h3>
          <p class="su-author-compare-desc">Author A is the current page. Paste a Google Scholar author profile URL for Author B.</p>
          <div class="su-author-compare-a">Author A (current): <strong id="su-compare-name-a"></strong></div>
          <label class="su-author-compare-field">Author B profile URL: <input type="url" id="su-compare-url-b" placeholder="https://scholar.google.[tld]/citations?user=..." class="su-author-compare-input" /></label>
          <div class="su-author-compare-actions">
            <button type="button" id="su-compare-do">Compare</button>
            <button type="button" id="su-compare-close">Close</button>
          </div>
          <div id="su-compare-result" class="su-author-compare-result"></div>
        </div>
      `;
      document.body.appendChild(overlay);
      overlay.querySelector(".su-author-compare-backdrop").addEventListener("click", () => { overlay.classList.remove("su-visible"); });
      overlay.querySelector("#su-compare-close").addEventListener("click", () => { overlay.classList.remove("su-visible"); });
      overlay.querySelector("#su-compare-do").addEventListener("click", async () => {
        const urlInput = overlay.querySelector("#su-compare-url-b");
        const url = (urlInput?.value || "").trim();
        let parsed = null;
        try {
          parsed = url ? new URL(url) : null;
        } catch {
          parsed = null;
        }
        if (!parsed || parsed.protocol !== "https:" || !isScholarHostname(parsed.hostname)) {
          const resultEl = overlay.querySelector("#su-compare-result");
          if (resultEl) resultEl.innerHTML = '<p class="su-compare-error">Please enter a Google Scholar author profile URL (https://scholar.google.[tld]/...).</p>';
          return;
        }
        const resultEl = overlay.querySelector("#su-compare-result");
        if (resultEl) resultEl.innerHTML = "<p>Loading Author B...</p>";
        try {
          const PAGE_SIZE = 20;
          const MAX_PAGES = 100;

          const fetchPage = (cstart) => {
            const u = new URL(url);
            u.searchParams.set("cstart", String(cstart));
            u.searchParams.set("pagesize", String(PAGE_SIZE));
            return fetch(u.toString(), { credentials: "include" }).then((r) => r.text());
          };

          const html0 = await fetchPage(0);
          const doc0 = new DOMParser().parseFromString(html0, "text/html");
          const nameB = (doc0.querySelector("#gsc_prf_in")?.textContent || "").trim() || "Author B";
          let allRows = Array.from(doc0.querySelectorAll(".gsc_a_tr"));

          const rowId = (tr) => tr.getAttribute("data-row") || tr.querySelector(".gsc_a_at")?.getAttribute("href") || tr.textContent?.slice(0, 80) || "";
          for (let page = 1; page < MAX_PAGES; page++) {
            const cstart = page * PAGE_SIZE;
            if (resultEl) resultEl.innerHTML = `<p>Loading Author B... (${allRows.length} papers so far)</p>`;
            const html = await fetchPage(cstart);
            const doc = new DOMParser().parseFromString(html, "text/html");
            const rows = Array.from(doc.querySelectorAll(".gsc_a_tr"));
            if (rows.length === 0) break;
            if (allRows.length > 0 && rows.length > 0 && rowId(rows[0]) === rowId(allRows[0])) {
              break;
            }
            allRows = allRows.concat(rows);
            if (rows.length < PAGE_SIZE) break;
          }

          const stateB = {
            authorVariations: nameB ? generateAuthorNameVariations(nameB) : [],
            qIndex: (state && state.qIndex) || null
          };
          const statsB = computeAuthorStats(allRows, stateB, "all");
          const { results, isAuthorProfile } = scanResults();
          const statsA = isAuthorProfile && results.length ? computeAuthorStats(results, state, "all") : null;
          const nameA = (state && state.authorVariations && state.authorVariations[0]) || extractAuthorName() || "Author A";
          const table = buildAuthorCompareTable(nameA, statsA, nameB, statsB);
          if (resultEl) resultEl.innerHTML = table;
        } catch (err) {
          if (resultEl) resultEl.innerHTML = `<p class="su-compare-error">Could not load profile: ${String(err?.message || err)}</p>`;
        }
      });
    }
    const nameA = (state?.authorVariations && state.authorVariations[0]) || extractAuthorName() || "Current author";
    const nameAEl = overlay.querySelector("#su-compare-name-a");
    if (nameAEl) nameAEl.textContent = nameA;
    overlay.querySelector("#su-compare-url-b").value = "";
    overlay.querySelector("#su-compare-result").innerHTML = "";
    overlay.classList.add("su-visible");
    overlay.querySelector("#su-compare-url-b")?.focus();
  }

  function buildAuthorCompareTable(nameA, statsA, nameB, statsB) {
    const s = (st) => (st && typeof st === "object" ? st : null);
    const n = (v) => (v != null && !Number.isNaN(v) ? Number(v) : 0);
    const a = s(statsA);
    const b = s(statsB);
    const rows = [
      ["Total publications", n(a?.totalPublications), n(b?.totalPublications)],
      ["Total citations", n(a?.totalCitations), n(b?.totalCitations)],
      ["Most cited (single paper)", n(a?.mostCited), n(b?.mostCited)],
      ["First-author papers", n(a?.firstAuthor), n(b?.firstAuthor)],
      ["Last-author papers", n(a?.lastAuthor), n(b?.lastAuthor)],
      ["Solo-authored papers", n(a?.soloAuthored), n(b?.soloAuthored)],
      ["Q1 papers", n(a?.qualityCounts?.q1), n(b?.qualityCounts?.q1)],
      ["Last 5 years", n(a?.recentActivity?.last5Years), n(b?.recentActivity?.last5Years)]
    ];
    return `
      <table class="su-author-compare-table">
        <thead><tr><th>Metric</th><th>${escapeHtml(nameA)}</th><th>${escapeHtml(nameB)}</th></tr></thead>
        <tbody>
          ${rows.map(([label, valA, valB]) => `<tr><td>${escapeHtml(label)}</td><td>${valA}</td><td>${valB}</td></tr>`).join("")}
        </tbody>
      </table>
    `;
  }

  function closePoPOverlay() {
    const overlay = document.getElementById("su-pop-overlay");
    if (overlay) overlay.classList.remove("su-visible");
  }

  function bindPoPOverlayEvents(overlay) {
    if (!overlay || overlay.__suPopBound) return;
    overlay.__suPopBound = true;
    overlay.addEventListener("click", (e) => {
      if (e.target.closest("[data-pop-close]")) {
        closePoPOverlay();
        return;
      }
      const peersBtn = e.target.closest("[data-pop-peers]");
      if (peersBtn) {
        const kind = String(peersBtn.dataset.popPeers || "");
        loadPopPeers({ force: kind === "refresh" });
        return;
      }
      const exportBtn = e.target.closest("[data-pop-export]");
      if (exportBtn) {
        const kind = String(exportBtn.dataset.popExport || "");
        const payload = window.suPopOverlayData || null;
        if (!payload) return;
        if (kind === "metrics-csv") exportPoPMetricsCsv(payload);
        if (kind === "papers-csv") exportPoPPapersCsv(payload);
        if (kind === "json") exportPoPJson(payload);
        if (kind === "md") exportPoPMarkdown(payload);
      }
    });
    overlay.addEventListener("change", (e) => {
      const select = e.target.closest("[data-pop-peer-select]");
      if (!select) return;
      if (select.dataset.popPeerSelect === "concept") {
        const preset = select.value;
        setPopPeerConfig({ preset });
        window.suPopPeerState = {};
      } else if (select.dataset.popPeerSelect === "window") {
        const yearWindow = Math.max(0, Math.min(2, Number(select.value) || 0));
        setPopPeerConfig({ yearWindow });
        window.suPopPeerState = {};
      }
      renderPoPOverlay(window.suFullAuthorStats || {});
    });
    overlay.addEventListener("input", (e) => {
      const input = e.target.closest("[data-pop-peer-custom]");
      if (!input) return;
      setPopPeerConfig({ customConcepts: input.value || "" });
      window.suPopPeerState = {};
      if (overlay.__suPopInputTimer) clearTimeout(overlay.__suPopInputTimer);
      overlay.__suPopInputTimer = setTimeout(() => {
        renderPoPOverlay(window.suFullAuthorStats || {});
      }, 200);
    });
    overlay.addEventListener("mouseenter", (e) => {
      const item = e.target.closest(".su-stat-item-with-tooltip[data-stat-tooltip]");
      if (!item) return;
      const tip = item.querySelector(".su-author-stat-tooltip");
      if (tip) tip.classList.add("su-author-stat-tooltip-visible");
    }, true);
    overlay.addEventListener("mouseleave", (e) => {
      const item = e.target.closest(".su-stat-item-with-tooltip[data-stat-tooltip]");
      if (!item) return;
      const tip = item.querySelector(".su-author-stat-tooltip");
      if (tip) tip.classList.remove("su-author-stat-tooltip-visible");
    }, true);
  }

  function ensurePoPOverlay() {
    let overlay = document.getElementById("su-pop-overlay");
    if (overlay) {
      bindPoPOverlayEvents(overlay);
      return overlay;
    }
    overlay = document.createElement("div");
    overlay.id = "su-pop-overlay";
    overlay.className = "su-pop-overlay";
    overlay.innerHTML = `
      <div class="su-pop-backdrop" data-pop-close="1"></div>
      <div class="su-pop-panel">
        <div class="su-pop-header">
          <div>
            <h3 class="su-pop-title">Publish or Perish report</h3>
            <div class="su-pop-subtitle">Local-only metrics based on loaded papers.</div>
          </div>
          <button type="button" class="su-pop-close" data-pop-close="1">Close</button>
        </div>
        <div id="su-pop-content" class="su-pop-content"></div>
      </div>
    `;
    document.body.appendChild(overlay);
    bindPoPOverlayEvents(overlay);
    return overlay;
  }

  function formatPopValue(val, digits = 2) {
    if (val == null || val === "") return "—";
    const num = Number(val);
    if (!Number.isFinite(num)) return String(val);
    if (Number.isInteger(num)) return String(num);
    return num.toFixed(digits);
  }

  function buildPoPTooltips(metrics) {
    const years = metrics?.yearsSinceFirst || "n";
    return {
      citesPerYear: `<strong>Cites/year</strong><br>Total citations / years since first publication (${years}).`,
      citesPerPaperMean: `<strong>Cites/paper (mean)</strong><br>Mean citations per paper.`,
      citesPerPaperMedian: `<strong>Cites/paper (median)</strong><br>Median citations per paper.`,
      citesPerPaperMode: `<strong>Cites/paper (mode)</strong><br>Most frequent citation count.`,
      citesPerAuthor: `<strong>Cites/author</strong><br>Total citations / total authors across all papers.`,
      papersPerAuthor: `<strong>Papers/author</strong><br>Total papers / total authors across all papers.`,
      authorsPerPaperMean: `<strong>Authors/paper (mean)</strong><br>Mean authors per paper.`,
      authorsPerPaperMedian: `<strong>Authors/paper (median)</strong><br>Median authors per paper.`,
      authorsPerPaperMode: `<strong>Authors/paper (mode)</strong><br>Most frequent team size.`,
      hIndex: `<strong>h-index</strong><br>Largest h such that h papers have ≥ h citations.`,
      gIndex: `<strong>g-index</strong><br>Largest g such that the top g papers have ≥ g² citations.`,
      hINorm: `<strong>hI,norm</strong><br>Compute citations per author for each paper, then apply h-index.`,
      hIAnnual: `<strong>hI,annual</strong><br>hI,norm / years since first publication (${years}).`,
      hAIndex: `<strong>hA-index</strong><br>h / avg. authors in the h-core.`,
      eIndex: `<strong>e-index</strong><br>Excess citations in the h-core: √(Σ h-core cites − h²).`,
      awcr: `<strong>AWCR</strong><br>Sum of citations / (age in years) per paper.`,
      awcrpA: `<strong>AWCR/author</strong><br>AWCR divided by total authors.`,
      awIndex: `<strong>AW-index</strong><br>sqrt(AWCR).`,
      hContemporary: `<strong>Contemporary h</strong><br>Score = (4 / age) × cites; apply h-index.`,
      acc: `<strong>ACC</strong><br>Count of papers with ≥1/2/5/10/20 citations.`
    };
  }

  function buildPoPPeersSection(stats) {
    const cfg = getPopPeerConfig();
    const startYear = Number(stats?.firstYear) || null;
    const state = window.suPopPeerState || {};
    const isActiveYear = state.year && startYear && state.year === startYear;
    const loading = isActiveYear && state.loading;
    const error = isActiveYear ? state.error : "";
    const peers = isActiveYear && Array.isArray(state.data) ? state.data : [];
    const hasSearched = !!(isActiveYear && state.hasSearched);
    const yearWindow = isActiveYear && state.yearWindow != null ? Number(state.yearWindow) || 0 : cfg.yearWindow;
    const scanned = isActiveYear ? state.scanned : null;
    const pages = isActiveYear ? state.pages : null;
    const status = isActiveYear ? state.status : "";
    const scannedWorks = isActiveYear ? state.scannedWorks : null;
    const candidateCount = isActiveYear ? state.candidateCount : null;
    const conceptName = (isActiveYear && state.conceptName) ? state.conceptName : "Information Systems";
    const conceptList = isActiveYear && Array.isArray(state.concepts) ? state.concepts.filter(Boolean) : getPopPeerConceptQueries(cfg);
    const metaBits = [];
    if (scanned) metaBits.push(`${scanned} authors scanned`);
    if (scannedWorks) metaBits.push(`${scannedWorks} works scanned`);
    if (candidateCount) metaBits.push(`${candidateCount} candidates`);
    if (pages) metaBits.push(`${pages} page${pages === 1 ? "" : "s"}`);
    const meta = metaBits.length ? `(${metaBits.join(", ")})` : "";
    const btnLabel = loading ? "Loading peers…" : (startYear ? `Find peers for ${startYear}` : "Find peers");
    const refreshBtn = peers.length ? `<button type="button" class="su-graph-btn su-graph-btn-secondary" data-pop-peers="refresh" ${loading ? "disabled" : ""}>Refresh</button>` : "";
    const disabledAttr = startYear && !loading ? "" : "disabled";
    const yearRange = startYear && yearWindow ? `${startYear - yearWindow}–${startYear + yearWindow}` : (startYear || "—");
    const conceptsLabel = conceptList.length ? conceptList.join(", ") : conceptName;
    const note = `Data via OpenAlex concepts "${escapeHtml(conceptsLabel)}". Start year range: ${yearRange}. Inferred from OpenAlex counts_by_year; list may be incomplete.`;
    const displayPeers = peers.slice(0, 40);
    const showingLine = peers.length > 40 ? `Showing top 40 of ${peers.length}.` : "";

    let bodyHtml = "";
    if (!startYear) {
      bodyHtml = `<div class="su-pop-peers-empty">Start year unavailable for this author.</div>`;
    } else if (loading) {
      const statusLine = status ? `<div class="su-pop-peers-status">${escapeHtml(status)}</div>` : "";
      bodyHtml = `<div class="su-pop-peers-loading">Searching OpenAlex…</div>${statusLine}`;
    } else if (error) {
      bodyHtml = `<div class="su-pop-peers-error">${escapeHtml(error)}</div>`;
    } else if (peers.length) {
      const rows = displayPeers.map((p) => {
        const oaLink = p.id ? `<a href="${escapeHtml(p.id)}" target="_blank" rel="noopener">OpenAlex</a>` : "";
        const scholarQuery = p.name ? `https://scholar.google.com/citations?view_op=search_authors&mauthors=${encodeURIComponent(p.name)}` : "";
        const scholarLink = scholarQuery ? `<a href="${scholarQuery}" target="_blank" rel="noopener">Scholar</a>` : "";
        const links = [oaLink, scholarLink].filter(Boolean).join(" · ");
        return `<tr>
          <td>${escapeHtml(p.name || "")}</td>
          <td>${escapeHtml(p.institution || "")}</td>
          <td>${p.firstYear || "—"}</td>
          <td>${p.worksCount || 0}</td>
          <td>${p.citedByCount || 0}</td>
          <td>${p.hIndex ?? "—"}</td>
          <td>${links}</td>
        </tr>`;
      }).join("");
      bodyHtml = `
        <div class="su-pop-peers-meta">Found ${peers.length} peers ${meta} ${showingLine}</div>
        <div class="su-pop-table-wrap">
          <table class="su-pop-table su-pop-peers-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Institution</th>
                <th>Start</th>
                <th>Works</th>
                <th>Cites</th>
                <th>h-index</th>
                <th>Links</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>`;
    } else {
      bodyHtml = hasSearched
        ? `<div class="su-pop-peers-empty">No peers found for ${yearRange}. Try refresh or broaden the concept.</div>`
        : `<div class="su-pop-peers-empty">No peers loaded yet. Click “Find peers” to search.</div>`;
    }

    return `
      <div class="su-pop-section su-pop-peers">
        <div class="su-pop-section-title">Peer comparison (${escapeHtml(cfg.preset || "Field")})</div>
        <div class="su-pop-peers-controls">
          <label class="su-pop-peers-label">Field</label>
          <select class="su-pop-peers-select" data-pop-peer-select="concept">
            ${Object.keys(POP_CONCEPT_PRESETS).map((name) => {
              const selected = cfg.preset === name ? "selected" : "";
              return `<option value="${escapeHtml(name)}" ${selected}>${escapeHtml(name)}</option>`;
            }).join("")}
          </select>
          <input type="text" class="su-pop-peers-input" data-pop-peer-custom placeholder="Custom concepts (comma-separated)" value="${escapeHtml(cfg.customConcepts || "")}" ${cfg.preset === "Custom…" ? "" : "style=\"display:none\""} />
          <label class="su-pop-peers-label">± years</label>
          <select class="su-pop-peers-select" data-pop-peer-select="window">
            ${[0,1,2].map((w) => `<option value="${w}" ${w === cfg.yearWindow ? "selected" : ""}>${w}</option>`).join("")}
          </select>
          <button type="button" class="su-graph-btn" data-pop-peers="load" ${disabledAttr}>${btnLabel}</button>
          ${refreshBtn}
        </div>
        <div class="su-pop-peers-note">${note}</div>
        <div class="su-pop-peers-content">${bodyHtml}</div>
      </div>
    `;
  }

  function renderPoPOverlay(stats) {
    const overlay = ensurePoPOverlay();
    const content = overlay.querySelector("#su-pop-content");
    if (!content) return;
    let metrics = null;
    try {
      metrics = getPoPMetricsCached(stats);
    } catch (_) {
      metrics = null;
    }
    const authorName = (window.suState?.authorVariations?.[0] || extractAuthorName() || "Author").trim();
    if (!metrics) {
      content.innerHTML = `<div class="su-pop-empty">No publication data available.</div>`;
      window.suPopOverlayData = null;
      return;
    }
    const tips = buildPoPTooltips(metrics);
    const partial = window.suState?.authorStatsPartial;
    const papers = Array.isArray(stats.papers) ? stats.papers : [];
    const rows = papers.slice().sort((a, b) => (b.citations || 0) - (a.citations || 0));
    const summaryItems = [
      ["Total papers", metrics.totalPubs],
      ["Total cites", metrics.totalCites],
      ["h-index", metrics.hIndex],
      ["g-index", metrics.gIndex ?? "—"],
      ["hI,norm", metrics.hINorm],
      ["hI,annual", formatPopValue(metrics.hIAnnual, 3)],
      ["hA-index", formatPopValue(metrics.hAIndex, 3)],
      ["AWCR", formatPopValue(metrics.awcr, 2)],
      ["AW-index", formatPopValue(metrics.awIndex, 2)],
      ["Contemporary h", metrics.hContemporary]
    ];
    const acc = metrics.acc || {};
    const accLine = `≥1: ${acc.c1 ?? 0} · ≥2: ${acc.c2 ?? 0} · ≥5: ${acc.c5 ?? 0} · ≥10: ${acc.c10 ?? 0} · ≥20: ${acc.c20 ?? 0}`;
    let peersHtml = "";
    try {
      peersHtml = buildPoPPeersSection(stats);
    } catch (_) {
      peersHtml = `
        <div class="su-pop-section su-pop-peers">
          <div class="su-pop-section-title">Peer comparison (Information Systems)</div>
          <div class="su-pop-peers-error">Unable to render peer comparison.</div>
        </div>`;
    }
    content.innerHTML = `
      ${partial ? `<div class="su-pop-warning">Partial data: stats based on loaded papers only. Use “Load all for full stats” to complete.</div>` : ""}
      <div class="su-pop-actions">
        <button type="button" class="su-graph-btn" data-pop-export="metrics-csv">Export metrics CSV</button>
        <button type="button" class="su-graph-btn su-graph-btn-secondary" data-pop-export="papers-csv">Export papers CSV</button>
        <button type="button" class="su-graph-btn su-graph-btn-secondary" data-pop-export="json">Export JSON</button>
        <button type="button" class="su-graph-btn su-graph-btn-secondary" data-pop-export="md">Export report (MD)</button>
      </div>
      <div class="su-pop-summary">
        ${summaryItems.map(([label, val]) => `<div class="su-pop-card"><span>${label}</span><strong>${formatPopValue(val, 2)}</strong></div>`).join("")}
      </div>
      <div class="su-pop-section">
        <div class="su-pop-section-title">Publish or Perish metrics</div>
        <table class="su-pop-table">
          <thead><tr><th>Metric</th><th>Value</th></tr></thead>
          <tbody>
            ${[
              ["Cites/year", formatPopValue(metrics.citesPerYear, 2), tips.citesPerYear],
              ["Cites/paper (mean)", formatPopValue(metrics.citesPerPaperMean, 2), tips.citesPerPaperMean],
              ["Cites/paper (median)", formatPopValue(metrics.citesPerPaperMedian, 2), tips.citesPerPaperMedian],
              ["Cites/paper (mode)", formatPopValue(metrics.citesPerPaperMode, 2), tips.citesPerPaperMode],
              ["Cites/author", formatPopValue(metrics.citesPerAuthor, 3), tips.citesPerAuthor],
              ["Papers/author", formatPopValue(metrics.papersPerAuthor, 4), tips.papersPerAuthor],
              ["Authors/paper (mean)", formatPopValue(metrics.authorsPerPaperMean, 2), tips.authorsPerPaperMean],
              ["Authors/paper (median)", formatPopValue(metrics.authorsPerPaperMedian, 2), tips.authorsPerPaperMedian],
              ["Authors/paper (mode)", formatPopValue(metrics.authorsPerPaperMode, 2), tips.authorsPerPaperMode],
              ["h-index", metrics.hIndex, tips.hIndex],
              ["g-index", metrics.gIndex ?? "—", tips.gIndex],
              ["hI,norm", metrics.hINorm, tips.hINorm],
              ["hI,annual", formatPopValue(metrics.hIAnnual, 3), tips.hIAnnual],
              ["hA-index", formatPopValue(metrics.hAIndex, 3), tips.hAIndex],
              ["e-index", metrics.eIndex ?? "—", tips.eIndex],
              ["AWCR", formatPopValue(metrics.awcr, 2), tips.awcr],
              ["AWCR/author", formatPopValue(metrics.awcrpA, 3), tips.awcrpA],
              ["AW-index", formatPopValue(metrics.awIndex, 2), tips.awIndex],
              ["Contemporary h", metrics.hContemporary, tips.hContemporary]
            ].map(([label, val, tip]) => `
              <tr>
                <td><span class="su-stat-item-with-tooltip" data-stat-tooltip="1">${label}${tip ? `<span class="su-author-stat-tooltip">${tip}</span>` : ""}</span></td>
                <td>${val}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
      <div class="su-pop-section">
        <div class="su-pop-section-title">ACC thresholds</div>
        <div class="su-pop-acc">${accLine}</div>
      </div>
      ${peersHtml}
      <div class="su-pop-section">
        <div class="su-pop-section-title">Papers (sorted by citations)</div>
        <div class="su-pop-table-wrap">
          <table class="su-pop-table su-pop-table-papers">
            <thead>
              <tr>
                <th>Title</th>
                <th>Year</th>
                <th>Cites</th>
                <th>Cites/yr</th>
                <th>Authors</th>
                <th>DOI</th>
                <th>URL</th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((p) => {
                const cpy = computeVelocityValue(p.citations, p.year);
                const url = p.url ? `<a href="${escapeHtml(p.url)}" target="_blank" rel="noopener">link</a>` : "";
                return `<tr>
                  <td>${escapeHtml(p.title || "")}</td>
                  <td>${p.year || "—"}</td>
                  <td>${Number(p.citations) || 0}</td>
                  <td>${cpy?.velocity != null ? cpy.velocity.toFixed(2) : "—"}</td>
                  <td>${p.authorsCount || 1}</td>
                  <td>${escapeHtml(p.doi || "")}</td>
                  <td>${url}</td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
      </div>
    `;
    window.suPopOverlayData = {
      authorName,
      metrics,
      papers: rows
    };
  }

  async function loadPopPeers({ force = false } = {}) {
    const stats = window.suFullAuthorStats;
    const startYear = Number(stats?.firstYear) || null;
    if (!stats || !startYear) {
      window.suPopPeerState = { year: startYear, loading: false, error: "Author start year not available." };
      try { renderPoPOverlay(stats || {}); } catch (_) {}
      return;
    }
    const cfg = getPopPeerConfig();
    const yearWindow = Number.isFinite(cfg.yearWindow) ? Math.max(0, Math.min(2, cfg.yearWindow)) : 2;
    const perPage = 200;
    const conceptQueries = getPopPeerConceptQueries(cfg);
    if (!conceptQueries.length) {
      window.suPopPeerState = { year: startYear, loading: false, error: "Please select a field or enter custom concepts.", hasSearched: true };
      try { renderPoPOverlay(stats || {}); } catch (_) {}
      return;
    }
    const conceptKey = conceptQueries.map((c) => String(c).toLowerCase().replace(/\s+/g, "_")).join("+");
    const years = [startYear - yearWindow, startYear, startYear + yearWindow].filter((y) => Number.isFinite(y));
    const pagesPerYear = 2;
    const maxCandidates = 200;
    const concurrency = 3;
    const requestId = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const updateState = (next) => {
      if (window.suPopPeerState?.requestId && window.suPopPeerState.requestId !== requestId) return;
      window.suPopPeerState = { year: startYear, requestId, yearWindow, ...next };
      try { renderPoPOverlay(stats); } catch (_) {}
    };
    window.suPopPeerState = { year: startYear, loading: true, requestId, yearWindow };
    try { renderPoPOverlay(stats); } catch (_) {}
    const timeoutId = setTimeout(() => {
      updateState({ loading: false, error: "Peer search timed out. Please try again." });
    }, 45000);
    try {
      const cache = await getPopPeerCache();
      const cacheKey = `is|${startYear}|w${yearWindow}|c${conceptKey}|y${years.join("-")}|py${pagesPerYear}|n${maxCandidates}`;
      const cached = cache.peers?.[cacheKey];
      if (!force && cached?.data && cached.ts && (Date.now() - cached.ts < POP_PEER_TTL_MS)) {
        clearTimeout(timeoutId);
        updateState({
          loading: false,
          data: cached.data,
          scanned: cached.scanned,
          pages: cached.pages,
          conceptName: cached.conceptName || "Information Systems",
          concepts: Array.isArray(cached.concepts) ? cached.concepts : [],
          scannedWorks: cached.scannedWorks,
          candidateCount: cached.candidateCount,
          source: "cache",
          hasSearched: true
        });
        return;
      }
      const conceptInfos = await Promise.all(conceptQueries.map((q) => getOpenAlexConceptInfo(q)));
      const concepts = conceptInfos.filter((c) => c && c.id);
      if (!concepts.length) {
        clearTimeout(timeoutId);
        updateState({ loading: false, error: "OpenAlex concept lookup failed.", hasSearched: true });
        return;
      }
      updateState({ loading: true, status: "Scanning papers..." });
      const candidateRes = await fetchCandidateAuthorsFromWorks(concepts, years, { perPage, pagesPerYear });
      if (candidateRes.error) {
        clearTimeout(timeoutId);
        updateState({ loading: false, error: candidateRes.error, hasSearched: true });
        return;
      }
      if (!candidateRes.authors || candidateRes.authors.length === 0) {
        clearTimeout(timeoutId);
        updateState({ loading: false, error: "No authors returned from OpenAlex works. Try again later.", hasSearched: true });
        return;
      }
      const allCandidates = candidateRes.authors || [];
      const scannedAuthors = allCandidates.length;
      const candidateList = allCandidates.slice(0, maxCandidates);
      updateState({ loading: true, status: `Checking ${candidateList.length} authors...`, scanned: scannedAuthors, scannedWorks: candidateRes.scannedWorks, candidateCount: candidateList.length });

      const mapWithConcurrency = async (items, limit, worker) => {
        const results = [];
        let idx = 0;
        const runners = Array.from({ length: Math.max(1, limit) }, async () => {
          while (idx < items.length) {
            const current = items[idx++];
            try {
              const out = await worker(current);
              if (out) results.push(out);
            } catch (_) {}
          }
        });
        await Promise.all(runners);
        return results;
      };

      const withFirstYear = await mapWithConcurrency(candidateList, concurrency, async (cand) => {
        const firstYear = await fetchOpenAlexAuthorFirstYear(cand.id);
        return { ...cand, firstYear };
      });
      const missingCount = withFirstYear.filter((c) => !c.firstYear).length;
      if (candidateList.length && missingCount / candidateList.length > 0.8) {
        clearTimeout(timeoutId);
        updateState({ loading: false, error: "OpenAlex author-year lookups failed. Try again later.", hasSearched: true });
        return;
      }
      const minYear = startYear - yearWindow;
      const maxYear = startYear + yearWindow;
      const inWindow = withFirstYear.filter((c) => c.firstYear && c.firstYear >= minYear && c.firstYear <= maxYear);

      updateState({ loading: true, status: `Loading metrics for ${inWindow.length} peers...`, scanned: scannedAuthors, scannedWorks: candidateRes.scannedWorks, candidateCount: candidateList.length });
      const enriched = await mapWithConcurrency(inWindow, concurrency, async (cand) => {
        const raw = await fetchOpenAlexAuthorById(cand.id);
        const compact = compactOpenAlexAuthor(raw || {});
        return {
          ...compact,
          id: cand.id,
          name: cand.name || compact?.name || "",
          firstYear: cand.firstYear,
          sampleCount: cand.count
        };
      });

      const peers = enriched.filter((p) => p && p.firstYear != null);
      cache.peers[cacheKey] = {
        ts: Date.now(),
        data: peers,
        scanned: scannedAuthors,
        pages: pagesPerYear * years.length * concepts.length,
        conceptName: concepts[0]?.displayName || "Information Systems",
        conceptId: concepts[0]?.id || "",
        concepts: concepts.map((c) => c.displayName || c.name || "").filter(Boolean),
        scannedWorks: candidateRes.scannedWorks,
        candidateCount: candidateList.length
      };
      schedulePopPeerCacheSave();
      clearTimeout(timeoutId);
      updateState({
        loading: false,
        data: peers,
        scanned: scannedAuthors,
        pages: pagesPerYear * years.length * concepts.length,
        conceptName: concepts[0]?.displayName || "Information Systems",
        concepts: concepts.map((c) => c.displayName || c.name || "").filter(Boolean),
        scannedWorks: candidateRes.scannedWorks,
        candidateCount: candidateList.length,
        source: "openalex",
        hasSearched: true
      });
    } catch (err) {
      clearTimeout(timeoutId);
      updateState({ loading: false, error: "Failed to load peers from OpenAlex.", hasSearched: true });
    }
  }

  function openPoPOverlay() {
    const stats = window.suFullAuthorStats;
    if (!stats) return;
    const toggles = window.suState?.authorFeatureToggles || DEFAULT_AUTHOR_FEATURE_TOGGLES;
    const settings = window.suState?.settings || {};
    if (settings.showResearchIntel === false || toggles.researchIntel === false) return;
    const overlay = ensurePoPOverlay();
    overlay.classList.add("su-visible");
    renderPoPOverlay(stats);
  }

  function exportPoPMetricsCsv(payload) {
    const { authorName, metrics } = payload;
    if (!metrics) return;
    const rows = [
      ["Metric", "Value"],
      ["Total papers", metrics.totalPubs],
      ["Total citations", metrics.totalCites],
      ["Cites/year", metrics.citesPerYear],
      ["Cites/paper (mean)", metrics.citesPerPaperMean],
      ["Cites/paper (median)", metrics.citesPerPaperMedian],
      ["Cites/paper (mode)", metrics.citesPerPaperMode ?? ""],
      ["Cites/author", metrics.citesPerAuthor],
      ["Papers/author", metrics.papersPerAuthor],
      ["Authors/paper (mean)", metrics.authorsPerPaperMean],
      ["Authors/paper (median)", metrics.authorsPerPaperMedian],
      ["Authors/paper (mode)", metrics.authorsPerPaperMode ?? ""],
      ["h-index", metrics.hIndex],
      ["g-index", metrics.gIndex ?? ""],
      ["hI,norm", metrics.hINorm],
      ["hI,annual", metrics.hIAnnual],
      ["hA-index", metrics.hAIndex ?? ""],
      ["e-index", metrics.eIndex ?? ""],
      ["AWCR", metrics.awcr],
      ["AWCR/author", metrics.awcrpA],
      ["AW-index", metrics.awIndex],
      ["Contemporary h", metrics.hContemporary],
      ["ACC >=1", metrics.acc?.c1 ?? 0],
      ["ACC >=2", metrics.acc?.c2 ?? 0],
      ["ACC >=5", metrics.acc?.c5 ?? 0],
      ["ACC >=10", metrics.acc?.c10 ?? 0],
      ["ACC >=20", metrics.acc?.c20 ?? 0]
    ];
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${authorName.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-pop-metrics.csv`, "text/csv;charset=utf-8", csv);
  }

  function exportPoPPapersCsv(payload) {
    const { authorName, papers } = payload;
    const rows = [["Title", "Year", "Citations", "Cites/Year", "Authors", "DOI", "URL", "Cluster ID"]];
    for (const p of papers || []) {
      const cpy = computeVelocityValue(p.citations, p.year);
      rows.push([
        p.title || "",
        p.year || "",
        Number(p.citations) || 0,
        cpy?.velocity != null ? cpy.velocity.toFixed(2) : "",
        p.authorsCount || 1,
        p.doi || "",
        p.url || "",
        p.clusterId || ""
      ]);
    }
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${authorName.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-pop-papers.csv`, "text/csv;charset=utf-8", csv);
  }

  function exportPoPJson(payload) {
    const { authorName, metrics, papers } = payload;
    const json = JSON.stringify({
      author: authorName,
      exportedAt: new Date().toISOString(),
      metrics,
      papers
    }, null, 2);
    downloadBlob(`${authorName.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-pop-report.json`, "application/json", json);
  }

  const GENEALOGY_MAX_UP = 2;
  const GENEALOGY_MAX_DOWN = 2;
  const GENEALOGY_CLICK_DOWN = 10;
  const GENEALOGY_CLICK_UP = 10;
  const GENEALOGY_MAX_PER_LEVEL = 10;
  const GENEALOGY_DESC_LIMIT = 10000;
  const GENEALOGY_SOURCES = {
    merged: {
      label: "Unified genealogy (AFT + SE + Econ + OAI)",
      namesUrl: "src/data/genealogy_merged.names.json.gz",
      edgesUrl: "src/data/genealogy_merged.edges.bin.gz"
    }
  };

  function getGenealogyDataState() {
    if (!window.suGenealogyData) {
      window.suGenealogyData = { datasets: {} };
    }
    return window.suGenealogyData;
  }

  function getGenealogyDatasetState(key) {
    const root = getGenealogyDataState();
    if (!root.datasets[key]) {
      root.datasets[key] = {
        status: "idle",
        names: [],
        nameIndex: null,
        edgesLoaded: false,
        forward: null,
        reverse: null,
        orientation: null
      };
    }
    return root.datasets[key];
  }

  function getGenealogyMatchState() {
    if (!window.suGenealogyMatch) {
      window.suGenealogyMatch = { status: "idle", authorKey: "", matchIndex: null, matchName: "", datasetKey: "" };
    }
    return window.suGenealogyMatch;
  }

  function getLineageViewState() {
    if (!window.suLineageView) {
      window.suLineageView = { rootIndex: null, maxUp: GENEALOGY_MAX_UP, maxDown: GENEALOGY_MAX_DOWN, stack: [], datasetKey: "aft" };
    }
    return window.suLineageView;
  }

  async function fetchGzipText(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Failed to load ${url}`);
    if (!res.body || typeof DecompressionStream === "undefined") {
      return await res.text();
    }
    const stream = res.body.pipeThrough(new DecompressionStream("gzip"));
    return await new Response(stream).text();
  }

  async function fetchGzipArrayBuffer(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Failed to load ${url}`);
    if (!res.body || typeof DecompressionStream === "undefined") {
      return await res.arrayBuffer();
    }
    const stream = res.body.pipeThrough(new DecompressionStream("gzip"));
    return await new Response(stream).arrayBuffer();
  }

  async function ensureGenealogyNamesLoaded(key) {
    const source = GENEALOGY_SOURCES[key];
    if (!source) throw new Error("Unknown genealogy source.");
    const data = getGenealogyDatasetState(key);
    if (data.status === "ready" || data.status === "loading") return data;
    data.status = "loading";
    const url = chrome.runtime.getURL(source.namesUrl);
    const raw = await fetchGzipText(url);
    const parsed = JSON.parse(raw || "{}");
    const names = Array.isArray(parsed?.n) ? parsed.n : (parsed?.names || []);
    const index = new Map();
    for (let i = 0; i < names.length; i++) {
      const norm = normalizeAuthorName(names[i]);
      if (!norm) continue;
      if (index.has(norm)) {
        const existing = index.get(norm);
        if (Array.isArray(existing)) {
          existing.push(i);
        } else {
          index.set(norm, [existing, i]);
        }
      } else {
        index.set(norm, i);
      }
    }
    data.names = names;
    data.nameIndex = index;
    data.status = "ready";
    return data;
  }

  async function ensureGenealogyEdgesLoaded(key) {
    const source = GENEALOGY_SOURCES[key];
    if (!source) throw new Error("Unknown genealogy source.");
    const data = getGenealogyDatasetState(key);
    if (data.edgesLoaded) return data;
    const url = chrome.runtime.getURL(source.edgesUrl);
    const buf = await fetchGzipArrayBuffer(url);
    const arr = new Uint32Array(buf);
    const forward = new Map();
    const reverse = new Map();
    for (let i = 0; i < arr.length; i += 2) {
      const advisor = arr[i];
      const student = arr[i + 1];
      if (!Number.isFinite(advisor) || !Number.isFinite(student)) continue;
      if (!forward.has(advisor)) forward.set(advisor, []);
      forward.get(advisor).push(student);
      if (!reverse.has(student)) reverse.set(student, []);
      reverse.get(student).push(advisor);
    }
    data.forward = forward;
    data.reverse = reverse;
    const forwardAvg = forward.size ? arr.length / 2 / forward.size : 0;
    const reverseAvg = reverse.size ? arr.length / 2 / reverse.size : 0;
    data.orientation = forwardAvg < reverseAvg ? "student->advisor" : "advisor->student";
    data.edgesLoaded = true;
    return data;
  }

  function normalizeFullNameForMatch(name) {
    return stripTrailingSuffixTokens(stripNameCredentials(String(name || "")))
      .replace(/[^\p{L}\p{N}]+/gu, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
  }

  function resolveGenealogyMatch(authorName, authorVariations, data) {
    const variations = Array.isArray(authorVariations) && authorVariations.length
      ? authorVariations
      : (authorName ? generateAuthorNameVariations(authorName) : []);
    const normalized = variations.map((v) => normalizeAuthorName(v)).filter(Boolean);
    const index = data?.nameIndex;
    if (!index || !normalized.length) return null;
    for (const key of normalized) {
      if (!index.has(key)) continue;
      const found = index.get(key);
      if (Array.isArray(found)) {
        const variationSet = new Set(variations.map((v) => normalizeFullNameForMatch(v)).filter(Boolean));
        const exactMatches = found.filter((idx) => variationSet.has(normalizeFullNameForMatch(data.names?.[idx] || "")));
        if (exactMatches.length === 1) return exactMatches[0];
        return null;
      }
      return found;
    }
    return null;
  }

  function getGenealogyMaps(data) {
    if (!data) return { advisorsMap: new Map(), studentsMap: new Map() };
    if (data.orientation === "student->advisor") {
      return { advisorsMap: data.forward || new Map(), studentsMap: data.reverse || new Map() };
    }
    return { advisorsMap: data.reverse || new Map(), studentsMap: data.forward || new Map() };
  }

  function ensureGenealogyMatchAsync(authorName, authorVariations) {
    if (!authorName) return;
    const match = getGenealogyMatchState();
    const authorKey = normalizeAuthorName(authorName);
    if (match.authorKey && match.authorKey !== authorKey) {
      match.status = "idle";
      match.matchIndex = null;
      match.matchName = "";
      match.datasetKey = "";
    }
    match.authorKey = authorKey;
    if (match.status === "loading" || match.status === "ready" || match.status === "unmatched") return;
    match.status = "loading";
    Promise.resolve()
      .then(async () => {
        const keys = Object.keys(GENEALOGY_SOURCES);
        for (const key of keys) {
          const data = await ensureGenealogyNamesLoaded(key);
          const idx = resolveGenealogyMatch(authorName, authorVariations, data);
          if (idx == null) continue;
          match.matchIndex = idx;
          match.matchName = data.names?.[idx] || authorName;
          match.datasetKey = key;
          match.status = "ready";
          return;
        }
        match.status = "unmatched";
        match.matchIndex = null;
        match.matchName = "";
        match.datasetKey = "";
      })
      .catch(() => {
        match.status = "unmatched";
        match.matchIndex = null;
        match.matchName = "";
        match.datasetKey = "";
      })
      .finally(() => {
        if (match.status === "ready") {
          if (window.suFullAuthorStats) {
            renderRightPanel(window.suFullAuthorStats, window.suLastCoauthorsHtml || "");
          }
        }
      });
  }

  function countReachable(root, map, limit = GENEALOGY_DESC_LIMIT) {
    const visited = new Set();
    const queue = [root];
    let qi = 0;
    let count = 0;
    while (qi < queue.length) {
      const node = queue[qi++];
      const neighbors = map?.get(node) || [];
      for (const n of neighbors) {
        if (visited.has(n) || n === root) continue;
        visited.add(n);
        count += 1;
        if (count >= limit) return { count, truncated: true };
        queue.push(n);
      }
    }
    return { count, truncated: false };
  }

  function buildGenealogyTree(root, advisorsMap, studentsMap, maxUp, maxDown) {
    const levels = new Map();
    const edges = [];
    const overflow = new Map();
    levels.set(0, [root]);

    let frontier = [root];
    for (let depth = 1; depth <= maxUp; depth++) {
      const next = new Set();
      for (const node of frontier) {
        const parents = advisorsMap.get(node) || [];
        for (const p of parents) next.add(p);
      }
      let arr = Array.from(next);
      let extra = 0;
      if (arr.length > GENEALOGY_MAX_PER_LEVEL) {
        extra = arr.length - GENEALOGY_MAX_PER_LEVEL;
        arr = arr.slice(0, GENEALOGY_MAX_PER_LEVEL);
      }
      levels.set(-depth, arr);
      if (extra) overflow.set(-depth, extra);
      const selected = new Set(arr);
      for (const node of frontier) {
        const parents = advisorsMap.get(node) || [];
        for (const parent of parents) {
          if (selected.has(parent)) edges.push([parent, node]);
        }
      }
      frontier = arr;
    }

    frontier = [root];
    for (let depth = 1; depth <= maxDown; depth++) {
      const next = new Set();
      for (const node of frontier) {
        const kids = studentsMap.get(node) || [];
        for (const k of kids) next.add(k);
      }
      let arr = Array.from(next);
      let extra = 0;
      if (arr.length > GENEALOGY_MAX_PER_LEVEL) {
        extra = arr.length - GENEALOGY_MAX_PER_LEVEL;
        arr = arr.slice(0, GENEALOGY_MAX_PER_LEVEL);
      }
      levels.set(depth, arr);
      if (extra) overflow.set(depth, extra);
      const selected = new Set(arr);
      for (const node of frontier) {
        const kids = studentsMap.get(node) || [];
        for (const child of kids) {
          if (selected.has(child)) edges.push([node, child]);
        }
      }
      frontier = arr;
    }

    return { levels, edges, overflow };
  }

  function renderGenealogySvg(tree, data) {
    const width = 1200;
    const height = 720;
    const centerY = height / 2;
    const yGap = 70;
    const positions = new Map();
    const nodes = [];
    const levels = tree.levels;
    const depths = Array.from(levels.keys()).sort((a, b) => a - b);

    for (const depth of depths) {
      const row = levels.get(depth) || [];
      const y = centerY + depth * yGap;
      const count = row.length;
      row.forEach((node, idx) => {
        const x = count === 1 ? width / 2 : (idx + 1) * (width / (count + 1));
        positions.set(node, { x, y });
        nodes.push({ index: node, depth, x, y });
      });
      const extra = tree.overflow.get(depth) || 0;
      if (extra) {
        const x = width - 40;
        positions.set(`overflow-${depth}`, { x, y });
        nodes.push({ index: `overflow-${depth}`, depth, x, y, label: `+${extra} more`, overflow: true });
      }
    }

    const edgeHtml = tree.edges.map(([a, b]) => {
      const from = positions.get(a);
      const to = positions.get(b);
      if (!from || !to) return "";
      return `<line class="su-lineage-edge" x1="${from.x.toFixed(1)}" y1="${from.y.toFixed(1)}" x2="${to.x.toFixed(1)}" y2="${to.y.toFixed(1)}" />`;
    }).join("");

    const nodeHtml = nodes.map((n) => {
      const name = n.overflow ? n.label : (data.names?.[n.index] || "Unknown");
      const classes = ["su-lineage-node"];
      if (n.depth === 0) classes.push("su-lineage-node-root");
      if (n.overflow) classes.push("su-lineage-node-overflow");
      return `<g class="${classes.join(" ")}" transform="translate(${n.x.toFixed(1)} ${n.y.toFixed(1)})" data-lineage-name="${escapeHtml(name)}" data-lineage-index="${escapeHtml(String(n.index))}">
        <circle r="${n.depth === 0 ? 10 : 7}"></circle>
        <text class="su-lineage-node-text" x="${n.depth === 0 ? 14 : 12}" y="4">${escapeHtml(name)}</text>
      </g>`;
    }).join("");

    return `<svg class="su-lineage-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMid meet">
      <rect class="su-lineage-bg" x="0" y="0" width="${width}" height="${height}"></rect>
      ${edgeHtml}
      ${nodeHtml}
    </svg>`;
  }

  function ensureLineageOverlay() {
    let overlay = document.getElementById("su-lineage-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "su-lineage-overlay";
    overlay.className = "su-lineage-overlay";
    overlay.innerHTML = `
      <div class="su-lineage-backdrop" data-lineage-close="1"></div>
      <div class="su-lineage-panel">
        <div class="su-lineage-header">
          <div>
            <div class="su-lineage-title">Academic lineage</div>
            <div class="su-lineage-subtitle">Advisor/student genealogy based on public family-tree data.</div>
          </div>
          <div class="su-lineage-actions">
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-lineage-back="1">Back</button>
            <button type="button" class="su-graph-btn su-graph-btn-secondary" data-lineage-close="1">Close</button>
          </div>
        </div>
        <div class="su-lineage-meta">
          <div id="su-lineage-status" class="su-lineage-status"></div>
          <div class="su-lineage-controls">
            <label class="su-lineage-label">Dataset</label>
            <select id="su-lineage-dataset" class="su-lineage-select"></select>
          </div>
        </div>
        <div id="su-lineage-stats" class="su-lineage-stats"></div>
        <div id="su-lineage-tree" class="su-lineage-tree"></div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", (e) => {
      const backBtn = e.target.closest?.("[data-lineage-back]");
      if (backBtn) {
        const view = getLineageViewState();
        if (view.stack.length) {
          view.rootIndex = view.stack.pop();
          view.maxDown = GENEALOGY_MAX_DOWN;
          view.maxUp = GENEALOGY_MAX_UP;
          renderLineageOverlay();
        }
        return;
      }
      const node = e.target.closest?.(".su-lineage-node");
      if (!node || node.classList.contains("su-lineage-node-overflow")) return;
      const rawIndex = node.getAttribute("data-lineage-index");
      const index = rawIndex != null ? Number(rawIndex) : null;
      if (!Number.isFinite(index)) return;
      const view = getLineageViewState();
      if (view.rootIndex != null && view.rootIndex !== index) {
        view.stack.push(view.rootIndex);
      }
      view.rootIndex = index;
      view.maxUp = GENEALOGY_CLICK_UP;
      view.maxDown = GENEALOGY_CLICK_DOWN;
      renderLineageOverlay();
    });
    overlay.addEventListener("change", (e) => {
      const select = e.target?.closest?.("#su-lineage-dataset");
      if (!select) return;
      const key = String(select.value || "aft");
      const view = getLineageViewState();
      view.datasetKey = key;
      view.rootIndex = null;
      view.stack = [];
      view.maxUp = GENEALOGY_MAX_UP;
      view.maxDown = GENEALOGY_MAX_DOWN;
      renderLineageOverlay();
    });
    return overlay;
  }

  async function renderLineageOverlay() {
    const overlay = ensureLineageOverlay();
    const statusEl = overlay.querySelector("#su-lineage-status");
    const statsEl = overlay.querySelector("#su-lineage-stats");
    const treeEl = overlay.querySelector("#su-lineage-tree");
    const match = getGenealogyMatchState();
    const datasetSelect = overlay.querySelector("#su-lineage-dataset");
    const view = getLineageViewState();
    const authorName = (window.suState?.authorVariations?.[0] || extractAuthorName() || "").trim();
    const authorVariations = window.suState?.authorVariations || (authorName ? generateAuthorNameVariations(authorName) : []);
    const keys = Object.keys(GENEALOGY_SOURCES);
    if (datasetSelect) {
      datasetSelect.innerHTML = keys.map((key) => {
        const label = GENEALOGY_SOURCES[key]?.label || key;
        const selected = (view.datasetKey || match?.datasetKey || "aft") === key ? "selected" : "";
        return `<option value="${escapeHtml(key)}" ${selected}>${escapeHtml(label)}</option>`;
      }).join("");
    }
    const activeKey = (datasetSelect?.value || view.datasetKey || match?.datasetKey || "aft");
    view.datasetKey = activeKey;
    if (!authorName) {
      treeEl.innerHTML = `<div class="su-lineage-empty">No author detected.</div>`;
      statsEl.innerHTML = "";
      statusEl.textContent = "";
      return;
    }
    statusEl.textContent = "Loading lineage data...";
    try {
      const dataNames = await ensureGenealogyNamesLoaded(activeKey);
      const resolved = resolveGenealogyMatch(authorName, authorVariations, dataNames);
      if (resolved == null) {
        treeEl.innerHTML = `<div class="su-lineage-empty">No genealogy match found in this dataset.</div>`;
        statsEl.innerHTML = "";
        statusEl.textContent = "";
        return;
      }
      const data = await ensureGenealogyEdgesLoaded(activeKey);
      const root = Number.isFinite(view.rootIndex) ? view.rootIndex : resolved;
      view.rootIndex = root;
      const { advisorsMap, studentsMap } = getGenealogyMaps(data);
      const parents = advisorsMap.get(root) || [];
      const kids = studentsMap.get(root) || [];
      const ancestors = countReachable(root, advisorsMap);
      const descendants = countReachable(root, studentsMap);
      const tree = buildGenealogyTree(root, advisorsMap, studentsMap, view.maxUp || GENEALOGY_MAX_UP, view.maxDown || GENEALOGY_MAX_DOWN);
      const svg = renderGenealogySvg(tree, data);
      const truncAnc = ancestors.truncated ? `${ancestors.count}+` : ancestors.count;
      const truncDesc = descendants.truncated ? `${descendants.count}+` : descendants.count;
      statusEl.textContent = "";
      statsEl.innerHTML = `
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Match</div>
          <div class="su-lineage-stat-value">${escapeHtml(data.names?.[root] || match.matchName || "Unknown")}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Advisors</div>
          <div class="su-lineage-stat-value">${parents.length}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Students</div>
          <div class="su-lineage-stat-value">${kids.length}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Ancestors</div>
          <div class="su-lineage-stat-value">${truncAnc}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Descendants</div>
          <div class="su-lineage-stat-value">${truncDesc}</div>
        </div>
      `;
      treeEl.innerHTML = svg;
    } catch (err) {
      statusEl.textContent = "";
      statsEl.innerHTML = "";
      treeEl.innerHTML = `<div class="su-lineage-empty">Unable to load lineage data.</div>`;
    }
  }

  async function openLineageOverlay() {
    const match = getGenealogyMatchState();
    if (!match?.matchIndex || match.status !== "ready") return;
    const view = getLineageViewState();
    if (!view.datasetKey) view.datasetKey = match.datasetKey || "aft";
    const overlay = ensureLineageOverlay();
    overlay.classList.add("su-visible");
    await renderLineageOverlay();
  }

  function closeLineageOverlay() {
    const overlay = document.getElementById("su-lineage-overlay");
    if (overlay) overlay.classList.remove("su-visible");
  }

  function exportPoPMarkdown(payload) {
    const { authorName, metrics } = payload;
    if (!metrics) return;
    const lines = [];
    lines.push(`# Publish or Perish Report`);
    lines.push(`Author: ${authorName}`);
    lines.push(`Generated: ${new Date().toISOString()}`);
    lines.push("");
    lines.push("## Summary");
    lines.push(`- Total papers: ${metrics.totalPubs}`);
    lines.push(`- Total citations: ${metrics.totalCites}`);
    lines.push(`- h-index: ${metrics.hIndex}`);
    lines.push(`- g-index: ${metrics.gIndex ?? "—"}`);
    lines.push(`- hI,norm: ${metrics.hINorm}`);
    lines.push(`- hI,annual: ${formatPopValue(metrics.hIAnnual, 3)}`);
    lines.push(`- hA-index: ${formatPopValue(metrics.hAIndex, 3)}`);
    lines.push(`- AWCR: ${formatPopValue(metrics.awcr, 2)}`);
    lines.push(`- AW-index: ${formatPopValue(metrics.awIndex, 2)}`);
    lines.push(`- Contemporary h: ${metrics.hContemporary}`);
    lines.push("");
    lines.push("## ACC thresholds");
    lines.push(`- ≥1: ${metrics.acc?.c1 ?? 0}`);
    lines.push(`- ≥2: ${metrics.acc?.c2 ?? 0}`);
    lines.push(`- ≥5: ${metrics.acc?.c5 ?? 0}`);
    lines.push(`- ≥10: ${metrics.acc?.c10 ?? 0}`);
    lines.push(`- ≥20: ${metrics.acc?.c20 ?? 0}`);
    lines.push("");
    lines.push("## Metrics table");
    lines.push("| Metric | Value |");
    lines.push("| --- | --- |");
    const metricRows = [
      ["Cites/year", metrics.citesPerYear],
      ["Cites/paper (mean)", metrics.citesPerPaperMean],
      ["Cites/paper (median)", metrics.citesPerPaperMedian],
      ["Cites/paper (mode)", metrics.citesPerPaperMode ?? ""],
      ["Cites/author", metrics.citesPerAuthor],
      ["Papers/author", metrics.papersPerAuthor],
      ["Authors/paper (mean)", metrics.authorsPerPaperMean],
      ["Authors/paper (median)", metrics.authorsPerPaperMedian],
      ["Authors/paper (mode)", metrics.authorsPerPaperMode ?? ""],
      ["h-index", metrics.hIndex],
      ["g-index", metrics.gIndex ?? ""],
      ["hI,norm", metrics.hINorm],
      ["hI,annual", metrics.hIAnnual],
      ["hA-index", metrics.hAIndex ?? ""],
      ["e-index", metrics.eIndex ?? ""],
      ["AWCR", metrics.awcr],
      ["AWCR/author", metrics.awcrpA],
      ["AW-index", metrics.awIndex],
      ["Contemporary h", metrics.hContemporary]
    ];
    for (const [label, val] of metricRows) {
      lines.push(`| ${label} | ${val == null ? "" : val} |`);
    }
    downloadBlob(`${authorName.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-pop-report.md`, "text/markdown;charset=utf-8", lines.join("\n"));
  }

  /** Get profile author's role for a paper: 'solo' | 'first' | 'last' | 'middle'. */
  function getAuthorRole(paper, authorVariations) {
    if (!paper || !authorVariations?.length) return "middle";
    const authors = parseAuthors(paper.authorsVenue.split(" - ")[0] || paper.authorsVenue);
    if (authors.length === 0) return "middle";
    const coAuthors = authors.filter(a => !isAuthorVariation(a, authorVariations));
    if (coAuthors.length === 0) return "solo";
    // Check if author is first (position 0) OR has "*" marker (corresponding author)
    const isFirst = authors.length > 0 && (
      isAuthorVariation(authors[0], authorVariations) ||
      authors.some(a => a.includes("*") && isAuthorVariation(a, authorVariations))
    );
    const isLast = authors.length > 1 && isAuthorVariation(authors[authors.length - 1], authorVariations);
    if (isFirst && isLast) return "solo";
    if (isFirst) return "first";
    if (isLast) return "last";
    return "middle";
  }

  /** True if paper's author role matches the position filter (e.g. 'first', 'first+last'). */
  function paperMatchesPositionFilter(paper, positionFilter, authorVariations) {
    if (!positionFilter || positionFilter === "all") return true;
    const role = getAuthorRole(paper, authorVariations);
    const want = String(positionFilter).toLowerCase().split("+").map(s => s.trim());
    return want.includes(role);
  }

  function escapeRegExp(s) {
    return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function paperMatchesTitleToken(paper, token) {
    const t = String(token || "").toLowerCase().trim();
    if (!t) return true;
    const title = String(paper?.title || "");
    if (!title) return false;
    const re = new RegExp(`\\b${escapeRegExp(t)}\\b`, "i");
    return re.test(title);
  }

  function normalizeTopicFilters(filters) {
    const raw = Array.isArray(filters)
      ? filters
      : (filters ? [filters] : []);
    const out = [];
    const seen = new Set();
    for (const item of raw) {
      const t = String(item || "").toLowerCase().trim();
      if (!t || seen.has(t)) continue;
      seen.add(t);
      out.push(t);
    }
    return out;
  }

  function paperMatchesTitleTokens(paper, tokens) {
    const list = normalizeTopicFilters(tokens);
    if (!list.length) return true;
    for (const t of list) {
      if (!paperMatchesTitleToken(paper, t)) return false;
    }
    return true;
  }

  function computeHIndexFromCitations(list) {
    if (!Array.isArray(list) || list.length === 0) return 0;
    const sorted = list
      .map((v) => (Number.isFinite(v) ? v : parseInt(v, 10) || 0))
      .sort((a, b) => b - a);
    let h = 0;
    for (let i = 0; i < sorted.length; i++) {
      if (sorted[i] >= i + 1) h = i + 1;
      else break;
    }
    return h;
  }

  function normalizeProceedingsVenue(venue) {
    const v = String(venue || "").trim();
    if (!v) return v;
    let out = v;
    // Trim to first segment before commas (drop page numbers / extra descriptors)
    if (out.includes(",")) {
      out = out.split(",")[0].trim();
    }
    // Drop trailing standalone numbers (volume/issue) like "Decision Support Systems 114"
    out = out.replace(/\s+\d{1,4}\s*$/g, "");
    // Strip truncation ellipses
    out = out.replace(/\u2026/g, "...");
    out = out.replace(/\s*\.{3}\s*$/, "");
    out = out.replace(/\s*\.{2,}\s*$/, "");
    // Normalize common proceedings prefixes
    out = out.replace(/^\s*proceedings\s+of\s+the\s+/i, "");
    out = out.replace(/^\s*proceedings\s+of\s+/i, "");
    out = out.replace(/^\s*proceedings\s+/i, "");
    out = out.replace(/^\s*proc\.\s+of\s+the\s+/i, "");
    out = out.replace(/^\s*proc\.\s+of\s+/i, "");
    out = out.replace(/^\s*proc\.\s+/i, "");
    out = out.replace(/^\s*in:\s*/i, "");
    // Remove any standalone years (19xx or 20xx) in venue strings
    out = out.replace(/\b(19|20)\d{2}\b/g, "");
    // Remove years in parentheses
    out = out.replace(/\(\s*(19|20)\d{2}\s*\)/g, "");
    // Remove ordinals like 1st, 2nd, 3rd, 4th, 21st, 32nd, 43rd, 5th, 10th, etc.
    out = out.replace(/\b\d+(st|nd|rd|th)\b/gi, "");
    // Remove word ordinals like "Twenty-Third", "Thirtieth", etc.
    out = out.replace(/\b(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth|twentieth|twenty[-\s]?first|twenty[-\s]?second|twenty[-\s]?third|twenty[-\s]?fourth|twenty[-\s]?fifth|twenty[-\s]?sixth|twenty[-\s]?seventh|twenty[-\s]?eighth|twenty[-\s]?ninth|thirtieth|thirty[-\s]?first|thirty[-\s]?second|thirty[-\s]?third|thirty[-\s]?fourth|thirty[-\s]?fifth|thirty[-\s]?sixth|thirty[-\s]?seventh|thirty[-\s]?eighth|thirty[-\s]?ninth|fortieth|forty[-\s]?first|forty[-\s]?second|forty[-\s]?third|forty[-\s]?fourth|forty[-\s]?fifth|forty[-\s]?sixth|forty[-\s]?seventh|forty[-\s]?eighth|forty[-\s]?ninth|fiftieth|fifty[-\s]?first|fifty[-\s]?second|fifty[-\s]?third|fifty[-\s]?fourth|fifty[-\s]?fifth|fifty[-\s]?sixth|fifty[-\s]?seventh|fifty[-\s]?eighth|fifty[-\s]?ninth|sixtieth|sixty[-\s]?first|sixty[-\s]?second|sixty[-\s]?third|sixty[-\s]?fourth|sixty[-\s]?fifth|sixty[-\s]?sixth|sixty[-\s]?seventh|sixty[-\s]?eighth|sixty[-\s]?ninth|seventieth|seventy[-\s]?first|seventy[-\s]?second|seventy[-\s]?third|seventy[-\s]?fourth|seventy[-\s]?fifth|seventy[-\s]?sixth|seventy[-\s]?seventh|seventy[-\s]?eighth|seventy[-\s]?ninth|eightieth|eighty[-\s]?first|eighty[-\s]?second|eighty[-\s]?third|eighty[-\s]?fourth|eighty[-\s]?fifth|eighty[-\s]?sixth|eighty[-\s]?seventh|eighty[-\s]?eighth|eighty[-\s]?ninth|ninetieth|ninety[-\s]?first|ninety[-\s]?second|ninety[-\s]?third|ninety[-\s]?fourth|ninety[-\s]?fifth|ninety[-\s]?sixth|ninety[-\s]?seventh|ninety[-\s]?eighth|ninety[-\s]?ninth)\b/gi, "");
    // Remove bracketed years
    out = out.replace(/\[\s*(19|20)\d{2}\s*\]/g, "");
    // Normalize whitespace and trim punctuation
    out = out.replace(/\s{2,}/g, " ").trim();
    out = out.replace(/[\s,;-]+$/g, "").trim();
    return out || v;
  }

  const SU_VENUE_STOPWORDS = new Set([
    "proceedings", "proceeding", "proc", "conference", "conf", "symposium", "workshop", "meeting",
    "annual", "international", "intl", "on", "of", "the", "and", "for", "in",
    "journal", "transactions", "letters", "communications", "review", "reviews",
    "studies", "research", "science", "sciences", "technology", "technologies",
    "ieee", "acm", "ais", "association", "institute", "society"
  ]);

  const SU_VENUE_ACRONYMS = {
    hicss: "hawaii international conference on system sciences",
    jmis: "journal of management information systems",
    misq: "management information systems quarterly",
    icis: "international conference on information systems",
    amcis: "americas conference on information systems",
    pacis: "pacific asia conference on information systems",
    ecis: "european conference on information systems"
  };

  function normalizeVenueKey(venue) {
    const cleaned = normalizeProceedingsVenue(venue);
    if (!cleaned) return "";
    let out = String(cleaned).toLowerCase();
    if (/hawaii/.test(out) && /(conference|hicss|system|international|annual)/.test(out)) {
      return SU_VENUE_ACRONYMS.hicss;
    }
    out = out.replace(/&/g, " and ");
    out = out.replace(/[^a-z0-9\s]/g, " ");
    const rawTokens = out.split(/\s+/).filter(Boolean);
    for (const t of rawTokens) {
      if (SU_VENUE_ACRONYMS[t]) {
        return SU_VENUE_ACRONYMS[t];
      }
    }
    const tokens = out
      .split(/\s+/)
      .filter(Boolean)
      .map((t) => {
        if (t.endsWith("ies") && t.length > 4) return t.slice(0, -3) + "y";
        if (t.endsWith("s") && t.length > 4) return t.slice(0, -1);
        return t;
      })
      .filter(t => !SU_VENUE_STOPWORDS.has(t))
      .filter(t => !/^\d+$/.test(t));
    if (!tokens.length) {
      if (/^proceedings?$/.test(out.trim())) return "";
      return out.trim();
    }
    return tokens.join(" ");
  }

  function pickVenueDisplay(current, candidate) {
    if (!current) return candidate;
    if (!candidate) return current;
    if (current === candidate) return current;
    const score = (s) => {
      const letters = (s.match(/[A-Za-z]/g) || []).length;
      const uppers = (s.match(/[A-Z]/g) || []).length;
      return (uppers * 2) + letters + s.length;
    };
    const curScore = score(current);
    const candScore = score(candidate);
    if (candScore > curScore) return candidate;
    return current;
  }

  const SU_TITLECASE_LOWER = new Set([
    "a", "an", "and", "as", "at", "but", "by", "for", "if", "in", "nor", "of", "on", "or", "per", "the", "to", "vs", "via"
  ]);

  function normalizeVenueDisplay(venue) {
    const v = String(venue || "").trim();
    if (!v) return v;
    const acronymMap = {
      mis: "MIS",
      jmis: "JMIS",
      hicss: "HICSS",
      icis: "ICIS",
      amcis: "AMCIS",
      pacis: "PACIS",
      ecis: "ECIS"
    };
    const tokens = v.split(/(\s+|[-/])/);
    let wordIndex = 0;
    const out = tokens.map((tok) => {
      if (/^\s+$/.test(tok) || tok === "-" || tok === "/") return tok;
      const raw = tok;
      const word = raw.replace(/^[^A-Za-z0-9]+|[^A-Za-z0-9]+$/g, "");
      if (!word) return raw;
      const lowerWord = word.toLowerCase();
      if (acronymMap[lowerWord]) {
        return raw.replace(word, acronymMap[lowerWord]);
      }
      if (/[A-Z]{2,}/.test(word)) return raw; // preserve acronyms
      const lower = word.toLowerCase();
      const shouldLower = wordIndex > 0 && SU_TITLECASE_LOWER.has(lower);
      wordIndex += 1;
      const cased = shouldLower ? lower : lower.charAt(0).toUpperCase() + lower.slice(1);
      return raw.replace(word, cased);
    });
    return out.join("");
  }

  function paperMatchesFilter(paper, filter, state) {
    if (!filter || !state || !state.qIndex) return true;
    
    if (!paper || !paper.venue) return false;
    
    const badges = qualityBadgesForVenue(paper.venue, state.qIndex);
    const has = (kind, re) => badges.some((b) => b.kind === kind && (!re || re.test(String(b.text || ""))));
    
    switch (filter) {
      case "q1":
        // Check for Q1 quartile badge
        return has("quartile", /Q1/i);
      case "q2":
        return has("quartile", /Q2/i);
      case "q3":
        return has("quartile", /Q3/i);
      case "q4":
        return has("quartile", /Q4/i);
      case "jcr-q1":
        return has("jcr", /Q1/i);
      case "jcr-q2":
        return has("jcr", /Q2/i);
      case "jcr-q3":
        return has("jcr", /Q3/i);
      case "jcr-q4":
        return has("jcr", /Q4/i);
      case "a":
        // Check for ABDC A* or A rank
        return has("abdc", /^ABDC\s+(A\*|A)$/i);
      case "abdc-a*":
        return has("abdc", /^ABDC\s+A\*/i);
      case "abdc-a":
        return has("abdc", /^ABDC\s+A$/i);
      case "abdc-b":
        return has("abdc", /^ABDC\s+B$/i);
      case "abdc-c":
        return has("abdc", /^ABDC\s+C$/i);
      case "abdc-d":
        return has("abdc", /^ABDC\s+D$/i);
      case "vhb":
        return has("vhb");
      case "vhb-a+":
        return has("vhb", /^VHB\s+A\+$/i);
      case "vhb-a":
        return has("vhb", /^VHB\s+A$/i);
      case "vhb-b":
        return has("vhb", /^VHB\s+B$/i);
      case "vhb-c":
        return has("vhb", /^VHB\s+C$/i);
      case "vhb-d":
        return has("vhb", /^VHB\s+D$/i);
      case "vhb-e":
        return has("vhb", /^VHB\s+E$/i);
      case "utd24":
        return has("utd24");
      case "ft50":
        return has("ft50");
      case "abs4star":
        return has("abs", /^ABS\s+4\*$/i);
      case "abs-4":
        return has("abs", /^ABS\s+4$/i);
      case "abs-3":
        return has("abs", /^ABS\s+3$/i);
      case "abs-2":
        return has("abs", /^ABS\s+2$/i);
      case "abs-1":
        return has("abs", /^ABS\s+1$/i);
      case "core-a*":
        return has("core", /^CORE\s+A\*$/i);
      case "core-a":
        return has("core", /^CORE\s+A$/i);
      case "core-b":
        return has("core", /^CORE\s+B$/i);
      case "core-c":
        return has("core", /^CORE\s+C$/i);
      case "ccf-a":
        return has("ccf", /^CCF\s+A$/i);
      case "ccf-b":
        return has("ccf", /^CCF\s+B$/i);
      case "ccf-c":
        return has("ccf", /^CCF\s+C$/i);
      case "era":
        return has("era");
      case "norwegian-1":
        return has("norwegian", /Level\s+1/i);
      case "norwegian-2":
        return has("norwegian", /Level\s+2/i);
      case "preprint":
        return has("preprint");
      case "if":
        return has("if");
      case "h5":
        return has("h5");
      default:
        return true;
    }
  }

  function getSnippetText(container) {
    const snippetEl = container.querySelector(".gs_rs");
    if (!snippetEl) return "";
    return text(snippetEl).trim();
  }

  /** Search terms from current query (normalized, min length 2). */
  function getSearchTerms() {
    const q = getScholarSearchQuery();
    if (!q) return [];
    return q
      .split(/\s+/)
      .map((s) => s.replace(/^["']|["']$/g, "").trim().toLowerCase())
      .filter((s) => s.length >= 2);
  }

  /** Chrome Prompt API (Gemini Nano): availability and one-sentence contribution from snippet. */
  async function getLocalContributionStatement(snippetText, session) {
    if (!snippetText || snippetText.length < 20) return null;
    const truncated = snippetText.length > 2000 ? snippetText.slice(0, 1997) + "…" : snippetText;
    const prompt = `Based only on this academic abstract/snippet, state the paper's main contribution in one short sentence (Contribution Statement). No preamble.\n\n${truncated}`;
    try {
      const result = await session.prompt(prompt);
      return typeof result === "string" ? result.trim() : null;
    } catch {
      return null;
    }
  }

  async function runLocalSummaries() {
    const LM = globalThis.LanguageModel || window.LanguageModel;
    if (!LM?.availability) {
      showLocalSummaryMessage("Chrome 127+ with Gemini Nano is required. Enable chrome://flags/#prompt-api-for-gemini-nano and ensure the model is available.");
      return;
    }
    const opts = { expectedInputs: [{ type: "text", languages: ["en"] }], expectedOutputs: [{ type: "text", languages: ["en"] }] };
    let availability;
    try {
      availability = await LM.availability(opts);
    } catch {
      showLocalSummaryMessage("Prompt API is not available. Enable chrome://flags/#prompt-api-for-gemini-nano and check chrome://on-device-internals.");
      return;
    }
    if (availability === "unavailable" || availability === "downloadable") {
      showLocalSummaryMessage(availability === "downloadable" ? "Gemini Nano is not installed. Use the API once to trigger download (see chrome://on-device-internals)." : "Gemini Nano is unavailable on this device.");
      return;
    }
    if (availability === "downloading") {
      showLocalSummaryMessage("Gemini Nano is still downloading. Try again in a few minutes.");
      return;
    }
    let session;
    try {
      session = await LM.create(opts);
    } catch (e) {
      showLocalSummaryMessage("Could not create session: " + (e?.message || String(e)));
      return;
    }
    const { results, isAuthorProfile } = scanResults();
    if (isAuthorProfile || !results.length) return;
    const visible = results.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
    for (const r of visible) {
      const existing = r.querySelector(".su-ai-contribution");
      if (existing) existing.remove();
      const snippetText = getSnippetText(r);
      if (!snippetText) continue;
      const wrap = document.createElement("div");
      wrap.className = "su-ai-contribution";
      wrap.textContent = "…";
      const snippetEl = r.querySelector(".gs_rs");
      if (snippetEl) snippetEl.after(wrap);
      else r.querySelector(".gs_ri")?.appendChild(wrap) || r.appendChild(wrap);
      try {
        const statement = await getLocalContributionStatement(snippetText, session);
        wrap.textContent = statement || "(no summary)";
        wrap.classList.add("su-ai-contribution-done");
      } catch {
        wrap.textContent = "(error)";
      }
    }
    try {
      session.destroy?.();
    } catch {}
  }

  function showLocalSummaryMessage(msg) {
    const id = "su-ai-summary-message";
    let el = document.getElementById(id);
    if (!el) {
      el = document.createElement("div");
      el.id = id;
      el.className = "su-ai-summary-message";
      const container = document.querySelector("#gs_res_ccl_mid, #gs_res_ccl, #gs_bdy");
      if (container) container.insertBefore(el, container.firstChild);
      else document.body.insertBefore(el, document.body.firstChild);
    }
    el.textContent = msg;
    el.style.display = "block";
    setTimeout(() => { el.style.display = "none"; }, 8000);
  }

  function getScholarSearchQuery() {
    try {
      const url = new URL(window.location.href);
      if (!url.pathname.includes("/scholar")) return null;
      const q = url.searchParams.get("q");
      return q ? String(q).trim() : null;
    } catch {
      return null;
    }
  }

  /** Current Scholar search URL with optional year range (applies across all pages). */
  function getScholarSearchUrl(opts) {
    try {
      const url = new URL(window.location.href);
      if (!url.pathname.includes("/scholar")) return null;
      if (opts && (opts.yearMin != null || opts.yearMax != null)) {
        if (opts.yearMin != null && opts.yearMin !== "") url.searchParams.set("as_ylo", String(opts.yearMin));
        else url.searchParams.delete("as_ylo");
        if (opts.yearMax != null && opts.yearMax !== "") url.searchParams.set("as_yhi", String(opts.yearMax));
        else url.searchParams.delete("as_yhi");
        url.searchParams.delete("start"); // go to first page when changing year
      }
      return url.toString();
    } catch {
      return null;
    }
  }

  /** Read year filter from URL (as_ylo, as_yhi). */
  function readYearFilterFromUrl() {
    try {
      const url = new URL(window.location.href);
      if (!url.pathname.includes("/scholar")) return { yearMin: "", yearMax: "" };
      const ylo = url.searchParams.get("as_ylo");
      const yhi = url.searchParams.get("as_yhi");
      return { yearMin: ylo != null ? String(ylo).trim() : "", yearMax: yhi != null ? String(yhi).trim() : "" };
    } catch {
      return { yearMin: "", yearMax: "" };
    }
  }

  /** URL for the next page of results (start=current+10), or null. */
  function getNextPageUrl() {
    try {
      const url = new URL(window.location.href);
      if (!url.pathname.includes("/scholar")) return null;
      const start = parseInt(url.searchParams.get("start") || "0", 10);
      if (isNaN(start) || start < 0) return null;
      url.searchParams.set("start", String(start + 10));
      return url.toString();
    } catch {
      return null;
    }
  }

  /** Fetch next page HTML, append .gs_r rows to current page, re-apply filters. Returns { added, visible, hasMore }. */
  async function fetchNextPageAndStitch(state) {
    const nextUrl = getNextPageUrl();
    if (!nextUrl) return { added: 0, visible: 0, hasMore: false };
    const container = document.querySelector("#gs_res_ccl_mid");
    if (!container) return { added: 0, visible: 0, hasMore: false };
    let added = 0;
    try {
      const res = await fetch(nextUrl, { credentials: "include" });
      if (!res.ok) return { added: 0, visible: 0, hasMore: true };
      const html = await res.text();
      const parser = new DOMParser();
      const doc = parser.parseFromString(html, "text/html");
      const mid = doc.querySelector("#gs_res_ccl_mid");
      if (!mid) return { added: 0, visible: 0, hasMore: true };
      const rows = Array.from(mid.children).filter((el) => el.classList.contains("gs_r"));
      for (const row of rows) {
        const clone = row.cloneNode(true);
        clone.removeAttribute("id");
        clone.querySelectorAll("[id]").forEach((el) => el.removeAttribute("id"));
        container.appendChild(clone);
        added += 1;
      }
      history.replaceState(null, "", nextUrl);
      const { results, isAuthorProfile } = scanResults();
      if (!isAuthorProfile) applyResultFilters(results, state);
      const visible = results.filter((r) => !r.classList.contains("su-filtered-out")).length;
      if (typeof window.suProcessAll === "function") window.suProcessAll();
      return { added, visible, hasMore: getNextPageUrl() !== null };
    } catch (e) {
      return { added, visible: 0, hasMore: true };
    }
  }

  function highlightSearchQuerySyntax(q) {
    if (!q) return "";
    const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    let out = "";
    let i = 0;
    while (i < q.length) {
      const ch = q[i];
      if (ch === '"') {
        const end = q.indexOf('"', i + 1);
        const phrase = end === -1 ? q.slice(i) : q.slice(i, end + 1);
        out += `<span class="su-search-phrase">${esc(phrase)}</span>`;
        i += phrase.length;
        continue;
      }
      const rest = q.slice(i);
      const authorMatch = rest.match(/^author:[^\s"]+/i);
      if (authorMatch) {
        out += `<span class="su-search-author">${esc(authorMatch[0])}</span>`;
        i += authorMatch[0].length;
        continue;
      }
      const opMatch = rest.match(/^(AND|OR|NOT)\b/i);
      if (opMatch) {
        const prev = i === 0 ? " " : q[i - 1];
        const next = q[i + opMatch[0].length] || " ";
        const prevOk = /\s|\(|\)|"/.test(prev);
        const nextOk = /\s|\(|\)|"/.test(next);
        if (prevOk && nextOk) {
          out += `<span class="su-search-op">${esc(opMatch[0].toUpperCase())}</span>`;
          i += opMatch[0].length;
          continue;
        }
      }
      out += esc(ch);
      i += 1;
    }
    return out;
  }

  function ensureSearchSyntaxHighlighting() {
    const input = document.querySelector("#gs_hdr_ts_in, input[name='q']");
    if (!input) return;
    if (input.__suSyntaxHighlightAttached) return;
    input.__suSyntaxHighlightAttached = true;

    const overlay = document.createElement("div");
    overlay.className = "su-search-highlight-overlay";
    overlay.setAttribute("aria-hidden", "true");
    document.body.appendChild(overlay);

    let stylesSynced = false;
    let lastValue = null;
    let rafId = null;

    const syncStyles = () => {
      const cs = window.getComputedStyle(input);
      overlay.style.font = cs.font;
      overlay.style.fontSize = cs.fontSize;
      overlay.style.fontFamily = cs.fontFamily;
      overlay.style.fontWeight = cs.fontWeight;
      overlay.style.letterSpacing = cs.letterSpacing;
      overlay.style.lineHeight = cs.lineHeight;
      overlay.style.paddingTop = cs.paddingTop;
      overlay.style.paddingRight = cs.paddingRight;
      overlay.style.paddingBottom = cs.paddingBottom;
      overlay.style.paddingLeft = cs.paddingLeft;
      overlay.style.textAlign = cs.textAlign;
      overlay.style.borderRadius = cs.borderRadius;
    };

    const syncPosition = () => {
      const rect = input.getBoundingClientRect();
      overlay.style.left = `${rect.left}px`;
      overlay.style.top = `${rect.top}px`;
      overlay.style.width = `${rect.width}px`;
      overlay.style.height = `${rect.height}px`;
    };

    const update = () => {
      const value = input.value || "";
      if (!value) {
        overlay.style.display = "none";
        input.classList.remove("su-search-highlight-input");
        lastValue = value;
        return;
      }
      if (value === lastValue && overlay.style.display === "block") {
        return;
      }
      overlay.innerHTML = highlightSearchQuerySyntax(value);
      overlay.style.display = "block";
      input.classList.add("su-search-highlight-input");
      if (!stylesSynced) {
        syncStyles();
        stylesSynced = true;
      }
      syncPosition();
      lastValue = value;
    };

    const scheduleUpdate = () => {
      if (rafId != null) return;
      const raf = window.requestAnimationFrame || ((fn) => setTimeout(fn, 16));
      rafId = raf(() => {
        rafId = null;
        update();
      });
    };

    input.addEventListener("input", scheduleUpdate);
    input.addEventListener("focus", () => {
      stylesSynced = false;
      scheduleUpdate();
    });
    input.addEventListener("blur", () => {
      overlay.style.display = "none";
      input.classList.remove("su-search-highlight-input");
      lastValue = null;
    });
    window.addEventListener("resize", () => {
      stylesSynced = false;
      syncStyles();
      syncPosition();
    });
    window.addEventListener("scroll", syncPosition, true);
    update();
  }

  async function loadSemanticExpansionData() {
    try {
      const url = chrome.runtime.getURL("src/data/semantic_expansion.json");
      const r = await fetch(url);
      if (!r.ok) return null;
      const data = await r.json();
      return data?.terms ? data : null;
    } catch {
      return null;
    }
  }

  function getExpansionForQuery(query, termsMap) {
    if (!query || !termsMap || typeof termsMap !== "object") return null;
    const key = query.toLowerCase().trim();
    return termsMap[key] || null;
  }

  function buildSemanticExpansionPanel(expansion, currentQuery) {
    const baseUrl = "https://scholar.google.com/scholar";
    const link = (label) => {
      const a = document.createElement("a");
      a.href = `${baseUrl}?q=${encodeURIComponent(label)}`;
      a.textContent = label;
      a.className = "su-expansion-link";
      a.rel = "noopener";
      return a;
    };
    const section = (title, terms) => {
      if (!terms || terms.length === 0) return null;
      const div = document.createElement("div");
      div.className = "su-expansion-section";
      const heading = document.createElement("span");
      heading.className = "su-expansion-heading";
      heading.textContent = title + ": ";
      div.appendChild(heading);
      terms.forEach((term, i) => {
        div.appendChild(link(term));
        if (i < terms.length - 1) {
          div.appendChild(document.createTextNode(", "));
        }
      });
      return div;
    };
    const root = document.createElement("div");
    root.id = "su-semantic-expansion";
    root.className = "su-semantic-expansion";
    const header = document.createElement("div");
    header.className = "su-expansion-header";
    const title = document.createElement("div");
    title.className = "su-expansion-title";
    title.textContent = "Semantic expansion — try related terms to broaden or narrow your search:";
    header.appendChild(title);
    const dismissBtn = document.createElement("button");
    dismissBtn.type = "button";
    dismissBtn.className = "su-expansion-dismiss";
    dismissBtn.innerHTML = "×";
    dismissBtn.title = "Dismiss";
    dismissBtn.setAttribute("aria-label", "Dismiss");
    dismissBtn.addEventListener("click", () => {
      try {
        sessionStorage.setItem("su-semantic-dismissed", currentQuery || "");
        root.remove();
      } catch (_) {}
    });
    header.appendChild(dismissBtn);
    root.appendChild(header);
    const sym = section("Synonyms", expansion.synonyms);
    if (sym) root.appendChild(sym);
    const bro = section("Broader", expansion.broader);
    if (bro) root.appendChild(bro);
    const nar = section("Narrower", expansion.narrower);
    if (nar) root.appendChild(nar);
    return root;
  }

  async function ensureSemanticExpansionPanel(state) {
    if (!state.settings?.showSemanticExpansion) {
      document.getElementById("su-semantic-expansion")?.remove();
      return;
    }
    const query = getScholarSearchQuery();
    if (!query) {
      document.getElementById("su-semantic-expansion")?.remove();
      return;
    }
    const data = await loadSemanticExpansionData();
    const expansion = getExpansionForQuery(query, data?.terms);
    const existing = document.getElementById("su-semantic-expansion");
    existing?.remove();
    if (!expansion || (!expansion.synonyms?.length && !expansion.broader?.length && !expansion.narrower?.length)) return;
    try {
      if (sessionStorage.getItem("su-semantic-dismissed") === (query || "")) return;
    } catch (_) {}
    const panel = buildSemanticExpansionPanel(expansion, query);
    const container = document.querySelector("#gs_res_ccl_mid, #gs_res_ccl, #gs_bdy");
    if (container) {
      container.insertBefore(panel, container.firstChild);
    } else {
      document.body.insertBefore(panel, document.body.firstChild);
    }
  }

  function applyCitationSort() {
    const mode = window.suSearchSortByCitations;
    if (!mode || mode === "default") return;
    const container = document.querySelector("#gs_res_ccl_mid");
    if (!container) return;
    const rows = Array.from(container.children).filter((el) => el.classList && el.classList.contains("gs_r"));
    if (rows.length === 0) return;
    const getCite = (r) => getCitationCountFromResult(r) ?? 0;
    rows.sort((a, b) => {
      const ca = getCite(a);
      const cb = getCite(b);
      if (mode === "high") return cb - ca;
      if (mode === "low") return ca - cb;
      return 0;
    });
    rows.forEach((r) => container.appendChild(r));
  }

  function ensureFilterBar(state) {
    const q = getScholarSearchQuery();
    if (!q) return;
    let bar = document.getElementById("su-within-results-filter");
    if (bar) {
      bar.style.display = "";
      return;
    }
    state.filters = state.filters || {
      yearMin: "",
      yearMax: "",
      venueKeyword: "",
      hasPdf: false,
      hasCode: false,
      qualityFilter: "",
      hasFullText: false,
      hasFunding: false,
      hasOpenCitations: false,
      hasDatasetDoi: false,
      hasSoftwareDoi: false,
      hasUpdates: false,
      minCitations: "",
      authorMatch: "",
      maxCitations: "",
      minCitesPerYear: "",
      minInfluential: "",
      maxInfluential: "",
      affiliationContains: "",
      funderContains: ""
    };
    const f = state.filters;
    const showAdvancedFilters = state.settings?.showAdvancedFilters !== false;

    bar = document.createElement("div");
    bar.id = "su-within-results-filter";
    bar.className = "su-within-results-filter";

    const apply = () => {
      if (window.suSearchSortByCitations && window.suSearchSortByCitations !== "default") applyCitationSort();
      const { results, isAuthorProfile } = scanResults();
      if (!isAuthorProfile) applyResultFilters(results, state);
    };

    const yearMinEl = document.createElement("input");
    yearMinEl.type = "number";
    yearMinEl.placeholder = "From";
    yearMinEl.title = "Min year (applies to all pages; blur to apply)";
    yearMinEl.value = f.yearMin || "";
    yearMinEl.className = "su-filter-input su-filter-year";
    const yearMaxEl = document.createElement("input");
    yearMaxEl.type = "number";
    yearMaxEl.placeholder = "To";
    yearMaxEl.title = "Max year (applies to all pages; blur to apply)";
    yearMaxEl.value = f.yearMax || "";
    yearMaxEl.className = "su-filter-input su-filter-year";

    const venueEl = document.createElement("input");
    venueEl.type = "text";
    venueEl.placeholder = "Venue keyword";
    venueEl.value = f.venueKeyword || "";
    venueEl.className = "su-filter-input su-filter-venue";

    const pdfCheck = document.createElement("input");
    pdfCheck.type = "checkbox";
    pdfCheck.checked = !!f.hasPdf;
    pdfCheck.title = "Has PDF";
    pdfCheck.className = "su-filter-check";
    const pdfLabel = document.createElement("label");
    pdfLabel.className = "su-filter-label";
    pdfLabel.appendChild(pdfCheck);
    pdfLabel.appendChild(document.createTextNode(" PDF"));

    const codeCheck = document.createElement("input");
    codeCheck.type = "checkbox";
    codeCheck.checked = !!f.hasCode;
    codeCheck.title = "Has code link/repository";
    codeCheck.className = "su-filter-check";
    const codeLabel = document.createElement("label");
    codeLabel.className = "su-filter-label";
    codeLabel.appendChild(codeCheck);
    codeLabel.appendChild(document.createTextNode(" Code"));

    const citeEl = document.createElement("input");
    citeEl.type = "number";
    citeEl.placeholder = "≥ cites";
    citeEl.min = "0";
    citeEl.title = "Min citations";
    citeEl.value = f.minCitations || "";
    citeEl.className = "su-filter-input su-filter-cite";

    const authorEl = document.createElement("input");
    authorEl.type = "text";
    authorEl.placeholder = "Author match";
    authorEl.value = f.authorMatch || "";
    authorEl.className = "su-filter-input su-filter-author";

    const maxCiteEl = document.createElement("input");
    maxCiteEl.type = "number";
    maxCiteEl.placeholder = "≤ cites";
    maxCiteEl.min = "0";
    maxCiteEl.title = "Max citations";
    maxCiteEl.value = f.maxCitations || "";
    maxCiteEl.className = "su-filter-input su-filter-cite";

    const citesPerYearEl = document.createElement("input");
    citesPerYearEl.type = "number";
    citesPerYearEl.placeholder = "≥ cites/yr";
    citesPerYearEl.min = "0";
    citesPerYearEl.title = "Min citations per year";
    citesPerYearEl.value = f.minCitesPerYear || "";
    citesPerYearEl.className = "su-filter-input su-filter-cite";

    const inflMinEl = document.createElement("input");
    inflMinEl.type = "number";
    inflMinEl.placeholder = "≥ infl cites (proxy)";
    inflMinEl.min = "0";
    inflMinEl.title = "Influential citations (Semantic Scholar; attention proxy) min";
    inflMinEl.value = f.minInfluential || "";
    inflMinEl.className = "su-filter-input su-filter-cite";

    const inflMaxEl = document.createElement("input");
    inflMaxEl.type = "number";
    inflMaxEl.placeholder = "≤ infl cites (proxy)";
    inflMaxEl.min = "0";
    inflMaxEl.title = "Influential citations (Semantic Scholar; attention proxy) max";
    inflMaxEl.value = f.maxInfluential || "";
    inflMaxEl.className = "su-filter-input su-filter-cite";

    const affiliationEl = document.createElement("input");
    affiliationEl.type = "text";
    affiliationEl.placeholder = "Affiliation contains";
    affiliationEl.title = "Filter by author affiliation (OpenAlex)";
    affiliationEl.value = f.affiliationContains || "";
    affiliationEl.className = "su-filter-input su-filter-venue";

    const funderEl = document.createElement("input");
    funderEl.type = "text";
    funderEl.placeholder = "Funder contains";
    funderEl.title = "Filter by funder/award metadata (Crossref)";
    funderEl.value = f.funderContains || "";
    funderEl.className = "su-filter-input su-filter-venue";

    const fullTextCheck = document.createElement("input");
    fullTextCheck.type = "checkbox";
    fullTextCheck.checked = !!f.hasFullText;
    fullTextCheck.title = "Has full-text link (Crossref)";
    fullTextCheck.className = "su-filter-check";
    const fullTextLabel = document.createElement("label");
    fullTextLabel.className = "su-filter-label";
    fullTextLabel.appendChild(fullTextCheck);
    fullTextLabel.appendChild(document.createTextNode(" Full text"));

    const fundingCheck = document.createElement("input");
    fundingCheck.type = "checkbox";
    fundingCheck.checked = !!f.hasFunding;
    fundingCheck.title = "Has funder information (Crossref)";
    fundingCheck.className = "su-filter-check";
    const fundingLabel = document.createElement("label");
    fundingLabel.className = "su-filter-label";
    fundingLabel.appendChild(fundingCheck);
    fundingLabel.appendChild(document.createTextNode(" Funding"));

    const updatesCheck = document.createElement("input");
    updatesCheck.type = "checkbox";
    updatesCheck.checked = !!f.hasUpdates;
    updatesCheck.title = "Has corrections/updates (Crossref)";
    updatesCheck.className = "su-filter-check";
    const updatesLabel = document.createElement("label");
    updatesLabel.className = "su-filter-label";
    updatesLabel.appendChild(updatesCheck);
    updatesLabel.appendChild(document.createTextNode(" Updates"));

    const openCiteCheck = document.createElement("input");
    openCiteCheck.type = "checkbox";
    openCiteCheck.checked = !!f.hasOpenCitations;
    openCiteCheck.title = "Has open citations (OpenCitations)";
    openCiteCheck.className = "su-filter-check";
    const openCiteLabel = document.createElement("label");
    openCiteLabel.className = "su-filter-label";
    openCiteLabel.appendChild(openCiteCheck);
    openCiteLabel.appendChild(document.createTextNode(" Open cites"));

    const datasetCheck = document.createElement("input");
    datasetCheck.type = "checkbox";
    datasetCheck.checked = !!f.hasDatasetDoi;
    datasetCheck.title = "Has dataset DOI (DataCite)";
    datasetCheck.className = "su-filter-check";
    const datasetLabel = document.createElement("label");
    datasetLabel.className = "su-filter-label";
    datasetLabel.appendChild(datasetCheck);
    datasetLabel.appendChild(document.createTextNode(" Dataset DOI"));

    const softwareCheck = document.createElement("input");
    softwareCheck.type = "checkbox";
    softwareCheck.checked = !!f.hasSoftwareDoi;
    softwareCheck.title = "Has software DOI (DataCite)";
    softwareCheck.className = "su-filter-check";
    const softwareLabel = document.createElement("label");
    softwareLabel.className = "su-filter-label";
    softwareLabel.appendChild(softwareCheck);
    softwareLabel.appendChild(document.createTextNode(" Software DOI"));

    const oaCheck = document.createElement("input");
    oaCheck.type = "checkbox";
    oaCheck.checked = !!f.hasOa;
    oaCheck.title = "Open access (Unpaywall)";
    oaCheck.className = "su-filter-check";
    const oaLabel = document.createElement("label");
    oaLabel.className = "su-filter-label";
    oaLabel.appendChild(oaCheck);
    oaLabel.appendChild(document.createTextNode(" OA"));

    const preprintCheck = document.createElement("input");
    preprintCheck.type = "checkbox";
    preprintCheck.checked = !!f.hasPreprint;
    preprintCheck.title = "Has arXiv preprint";
    preprintCheck.className = "su-filter-check";
    const preprintLabel = document.createElement("label");
    preprintLabel.className = "su-filter-label";
    preprintLabel.appendChild(preprintCheck);
    preprintLabel.appendChild(document.createTextNode(" Preprint"));

    const pubmedCheck = document.createElement("input");
    pubmedCheck.type = "checkbox";
    pubmedCheck.checked = !!f.hasPubmed;
    pubmedCheck.title = "Listed in PubMed";
    pubmedCheck.className = "su-filter-check";
    const pubmedLabel = document.createElement("label");
    pubmedLabel.className = "su-filter-label";
    pubmedLabel.appendChild(pubmedCheck);
    pubmedLabel.appendChild(document.createTextNode(" PubMed"));

    const pmcCheck = document.createElement("input");
    pmcCheck.type = "checkbox";
    pmcCheck.checked = !!f.hasPmc;
    pmcCheck.title = "Available in PubMed Central (PMC)";
    pmcCheck.className = "su-filter-check";
    const pmcLabel = document.createElement("label");
    pmcLabel.className = "su-filter-label";
    pmcLabel.appendChild(pmcCheck);
    pmcLabel.appendChild(document.createTextNode(" PMC"));

    const meshCheck = document.createElement("input");
    meshCheck.type = "checkbox";
    meshCheck.checked = !!f.hasMesh;
    meshCheck.title = "Has MeSH terms (PubMed)";
    meshCheck.className = "su-filter-check";
    const meshLabel = document.createElement("label");
    meshLabel.className = "su-filter-label";
    meshLabel.appendChild(meshCheck);
    meshLabel.appendChild(document.createTextNode(" MeSH"));

    const qualitySelect = document.createElement("select");
    qualitySelect.className = "su-filter-select su-filter-quality";
    qualitySelect.title = "Venue quality filter";
    qualitySelect.innerHTML = `
      <option value="">Quality: Any</option>
      <optgroup label="Scimago Quartiles">
        <option value="q1">Q1</option>
        <option value="q2">Q2</option>
        <option value="q3">Q3</option>
        <option value="q4">Q4</option>
      </optgroup>
      <optgroup label="JCR Quartiles">
        <option value="jcr-q1">JCR Q1</option>
        <option value="jcr-q2">JCR Q2</option>
        <option value="jcr-q3">JCR Q3</option>
        <option value="jcr-q4">JCR Q4</option>
      </optgroup>
      <optgroup label="ABDC">
        <option value="abdc-a*">ABDC A*</option>
        <option value="abdc-a">ABDC A</option>
        <option value="abdc-b">ABDC B</option>
        <option value="abdc-c">ABDC C</option>
        <option value="abdc-d">ABDC D</option>
        <option value="a">ABDC A/A*</option>
      </optgroup>
      <optgroup label="VHB">
        <option value="vhb-a+">VHB A+</option>
        <option value="vhb-a">VHB A</option>
        <option value="vhb-b">VHB B</option>
        <option value="vhb-c">VHB C</option>
        <option value="vhb-d">VHB D</option>
        <option value="vhb-e">VHB E</option>
        <option value="vhb">VHB (any)</option>
      </optgroup>
      <optgroup label="ABS">
        <option value="abs4star">ABS 4*</option>
        <option value="abs-4">ABS 4</option>
        <option value="abs-3">ABS 3</option>
        <option value="abs-2">ABS 2</option>
        <option value="abs-1">ABS 1</option>
      </optgroup>
      <optgroup label="Conference Lists">
        <option value="core-a*">CORE A*</option>
        <option value="core-a">CORE A</option>
        <option value="core-b">CORE B</option>
        <option value="core-c">CORE C</option>
        <option value="ccf-a">CCF A</option>
        <option value="ccf-b">CCF B</option>
        <option value="ccf-c">CCF C</option>
      </optgroup>
      <optgroup label="Curated Lists">
        <option value="ft50">FT50</option>
        <option value="utd24">UTD24</option>
        <option value="era">ERA 2023</option>
        <option value="norwegian-2">Norwegian Level 2</option>
        <option value="norwegian-1">Norwegian Level 1</option>
      </optgroup>
      <optgroup label="Other">
        <option value="if">Impact Factor (IF)</option>
        <option value="h5">Google Scholar h5</option>
        <option value="preprint">Preprint</option>
      </optgroup>
    `;
    qualitySelect.value = f.qualityFilter || "";

    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "su-filter-clear";
    clearBtn.textContent = "Clear";
    clearBtn.title = "Clear all filters";

    const summarizeBtn = document.createElement("button");
    summarizeBtn.type = "button";
    summarizeBtn.className = "su-filter-clear su-summarize-ai";
    summarizeBtn.textContent = "Summarize (local AI)";
    summarizeBtn.title = "One-sentence contribution per result using Chrome Prompt API (Gemini Nano). Runs on your device; 100% private.";
    summarizeBtn.addEventListener("click", () => runLocalSummaries());

    const sortSelect = document.createElement("select");
    sortSelect.className = "su-filter-sort-citations";
    sortSelect.title = "Sort search results by citation count";
    sortSelect.innerHTML = `
      <option value="default">Sort: default</option>
      <option value="high">Sort: citations (high→low)</option>
      <option value="low">Sort: citations (low→high)</option>
    `;
    sortSelect.value = window.suSearchSortByCitations || "default";
    sortSelect.addEventListener("change", () => {
      window.suSearchSortByCitations = sortSelect.value;
      applyCitationSort();
      apply();
    });

    const loadMoreBtn = document.createElement("button");
    loadMoreBtn.type = "button";
    loadMoreBtn.className = "su-filter-clear su-load-more-pages";
    loadMoreBtn.textContent = "Load more pages";
    loadMoreBtn.title = "Fetch next pages and append results here so filter matches appear without clicking Next.";
    loadMoreBtn.addEventListener("click", async () => {
      if (loadMoreBtn.disabled) return;
      const maxPages = 5;
      const delayMs = 1500;
      loadMoreBtn.disabled = true;
      loadMoreBtn.textContent = "Loading…";
      let totalAdded = 0;
      let lastVisible = 0;
      for (let i = 0; i < maxPages; i++) {
        const next = getNextPageUrl();
        if (!next) break;
        const out = await fetchNextPageAndStitch(state);
        totalAdded += out.added;
        lastVisible = out.visible;
        if (!out.hasMore || out.added === 0) break;
        await new Promise((r) => setTimeout(r, delayMs));
      }
      loadMoreBtn.disabled = false;
      loadMoreBtn.textContent = totalAdded ? `Loaded ${totalAdded} more (${lastVisible} match)` : "Load more pages";
      if (totalAdded) setTimeout(() => { loadMoreBtn.textContent = "Load more pages"; }, 3000);
    });

    const MAX_BATCH_PDFS = 25;
    const openTabsInBackground = async (urls) => {
      if (!urls || urls.length === 0) return 0;
      try {
        const res = await chrome.runtime.sendMessage({ action: "openTabs", urls });
        if (res?.ok) return typeof res.opened === "number" ? res.opened : urls.length;
      } catch {}
      // Fallback: open directly (may focus the new tabs depending on browser settings)
      for (const url of urls) {
        try {
          window.open(url, "_blank", "noopener");
        } catch (_) {}
      }
      return urls.length;
    };

    const openPdfBtn = document.createElement("button");
    openPdfBtn.type = "button";
    openPdfBtn.className = "su-filter-clear su-batch-open-pdf";
    openPdfBtn.textContent = "Open PDFs";
    openPdfBtn.title = "Open the top N available PDFs in background tabs (asks for confirmation).";
    openPdfBtn.addEventListener("click", async () => {
      const defaultCount = Number(sessionStorage.getItem("su-batch-open-count") || "5") || 5;
      const raw = window.prompt(`Open how many PDFs? (max ${MAX_BATCH_PDFS})`, String(defaultCount));
      if (raw == null) return;
      const parsed = parseInt(raw, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        window.alert("Please enter a positive number.");
        return;
      }
      const desired = Math.min(parsed, MAX_BATCH_PDFS);
      sessionStorage.setItem("su-batch-open-count", String(desired));

      const { results, isAuthorProfile } = scanResults();
      if (isAuthorProfile) {
        window.alert("Batch PDF open is only available on search result pages.");
        return;
      }
      const urls = [];
      const seen = new Set();
      for (const r of results) {
        if (r.classList.contains("su-filtered-out") || r.classList.contains("su-hidden") || r.style.display === "none") continue;
        const best = getBestPdfUrl(r);
        if (!best || !best.url) continue;
        if (seen.has(best.url)) continue;
        seen.add(best.url);
        urls.push(best.url);
        if (urls.length >= desired) break;
      }
      if (!urls.length) {
        window.alert("No PDFs found in the current results.");
        return;
      }
      const msg = urls.length < desired
        ? `Found ${urls.length} PDFs. Open them in background tabs?`
        : `Open ${urls.length} PDFs in background tabs?`;
      if (!window.confirm(msg)) return;

      const prevText = openPdfBtn.textContent;
      openPdfBtn.disabled = true;
      openPdfBtn.textContent = "Opening…";
      const opened = await openTabsInBackground(urls);
      openPdfBtn.textContent = opened ? `Opened ${opened}` : "Opened";
      setTimeout(() => {
        openPdfBtn.textContent = prevText;
        openPdfBtn.disabled = false;
      }, 2500);
    });

    const reviewBtn = document.createElement("button");
    reviewBtn.type = "button";
    reviewBtn.className = "su-filter-clear su-review-workspace";
    reviewBtn.textContent = "Review workspace";
    reviewBtn.title = "Open the systematic review workspace for screening and extraction.";
    reviewBtn.addEventListener("click", () => openReviewOverlay());

    const readFilterValues = () => ({
      yearMin: yearMinEl.value.trim(),
      yearMax: yearMaxEl.value.trim(),
      venueKeyword: venueEl.value.trim(),
      hasPdf: pdfCheck.checked,
      hasCode: codeCheck.checked,
      qualityFilter: qualitySelect.value || "",
      hasFullText: fullTextCheck.checked,
      hasFunding: fundingCheck.checked,
      hasOpenCitations: openCiteCheck.checked,
      hasDatasetDoi: datasetCheck.checked,
      hasSoftwareDoi: softwareCheck.checked,
      hasUpdates: updatesCheck.checked,
      hasOa: oaCheck.checked,
      hasPreprint: preprintCheck.checked,
      hasPubmed: pubmedCheck.checked,
      hasPmc: pmcCheck.checked,
      hasMesh: meshCheck.checked,
      minCitations: citeEl.value.trim(),
      authorMatch: authorEl.value.trim(),
      maxCitations: showAdvancedFilters ? maxCiteEl.value.trim() : "",
      minCitesPerYear: showAdvancedFilters ? citesPerYearEl.value.trim() : "",
      minInfluential: showAdvancedFilters ? inflMinEl.value.trim() : "",
      maxInfluential: showAdvancedFilters ? inflMaxEl.value.trim() : "",
      affiliationContains: showAdvancedFilters ? affiliationEl.value.trim() : "",
      funderContains: showAdvancedFilters ? funderEl.value.trim() : ""
    });

    const quickButtons = new Map();
    const currentYear = new Date().getFullYear();
    const quickDefs = [
      {
        id: "recent5",
        label: "Last 5y",
        patch: { yearMin: String(currentYear - 4), yearMax: String(currentYear) },
        isActive: (v) => v.yearMin === String(currentYear - 4) && v.yearMax === String(currentYear)
      },
      {
        id: "recent10",
        label: "Last 10y",
        patch: { yearMin: String(currentYear - 9), yearMax: String(currentYear) },
        isActive: (v) => v.yearMin === String(currentYear - 9) && v.yearMax === String(currentYear)
      },
      {
        id: "cites100",
        label: "≥100 cites",
        patch: { minCitations: "100" },
        isActive: (v) => v.minCitations === "100"
      }
    ];

    function updateQuickButtons() {
      try {
        const vals = readFilterValues();
        for (const def of quickDefs) {
          const btn = quickButtons.get(def.id);
          if (!btn) continue;
          const active = !!def.isActive(vals);
          btn.classList.toggle("su-quick-active", active);
        }
      } catch (_) {}
    }

    const applyFilterValues = (next) => {
      try {
        state.filters.yearMin = next.yearMin;
        state.filters.yearMax = next.yearMax;
        state.filters.venueKeyword = next.venueKeyword;
        state.filters.hasPdf = next.hasPdf;
        state.filters.hasCode = next.hasCode;
        state.filters.qualityFilter = next.qualityFilter;
        state.filters.hasFullText = next.hasFullText;
        state.filters.hasFunding = next.hasFunding;
        state.filters.hasOpenCitations = next.hasOpenCitations;
        state.filters.hasDatasetDoi = next.hasDatasetDoi;
        state.filters.hasSoftwareDoi = next.hasSoftwareDoi;
        state.filters.hasUpdates = next.hasUpdates;
        state.filters.hasOa = next.hasOa;
        state.filters.hasPreprint = next.hasPreprint;
        state.filters.hasPubmed = next.hasPubmed;
        state.filters.hasPmc = next.hasPmc;
        state.filters.hasMesh = next.hasMesh;
        state.filters.minCitations = next.minCitations;
        state.filters.authorMatch = next.authorMatch;
        state.filters.maxCitations = next.maxCitations;
        state.filters.minCitesPerYear = next.minCitesPerYear;
        state.filters.minInfluential = next.minInfluential;
        state.filters.maxInfluential = next.maxInfluential;
        state.filters.affiliationContains = next.affiliationContains;
        state.filters.funderContains = next.funderContains;
        apply();
        saveClientFilterToStorage(state);
        updateToggleLabel();
        updateQuickButtons();
      } catch (_) {
        // Avoid breaking the page if apply/scanResults/filters throw
      }
    };

    let updateStateAndApply = () => {
      try {
        const next = readFilterValues();
        applyFilterValues(next);
      } catch (_) {
        // Avoid breaking the page if apply/scanResults/filters throw
      }
    };

    let filterDebounceId = null;
    const scheduleFilterApply = () => {
      if (filterDebounceId) clearTimeout(filterDebounceId);
      filterDebounceId = setTimeout(() => {
        filterDebounceId = null;
        updateStateAndApply();
      }, 180);
    };

    const onFilterInput = () => {
      updateToggleLabel();
      scheduleFilterApply();
    };

    [yearMinEl, yearMaxEl, venueEl, pdfCheck, codeCheck, qualitySelect, fullTextCheck, fundingCheck, updatesCheck, openCiteCheck, datasetCheck, softwareCheck, oaCheck, preprintCheck, pubmedCheck, pmcCheck, meshCheck, citeEl, authorEl, maxCiteEl, citesPerYearEl, inflMinEl, inflMaxEl, affiliationEl, funderEl].forEach((el) => {
      el.addEventListener("input", onFilterInput);
      el.addEventListener("change", updateStateAndApply);
    });

    const setFilterInputs = (vals) => {
      yearMinEl.value = vals.yearMin || "";
      yearMaxEl.value = vals.yearMax || "";
      venueEl.value = vals.venueKeyword || "";
      citeEl.value = vals.minCitations || "";
      authorEl.value = vals.authorMatch || "";
      pdfCheck.checked = !!vals.hasPdf;
      codeCheck.checked = !!vals.hasCode;
      qualitySelect.value = vals.qualityFilter || "";
      fullTextCheck.checked = !!vals.hasFullText;
      fundingCheck.checked = !!vals.hasFunding;
      updatesCheck.checked = !!vals.hasUpdates;
      openCiteCheck.checked = !!vals.hasOpenCitations;
      datasetCheck.checked = !!vals.hasDatasetDoi;
      softwareCheck.checked = !!vals.hasSoftwareDoi;
      oaCheck.checked = !!vals.hasOa;
      preprintCheck.checked = !!vals.hasPreprint;
      pubmedCheck.checked = !!vals.hasPubmed;
      pmcCheck.checked = !!vals.hasPmc;
      meshCheck.checked = !!vals.hasMesh;
      if (showAdvancedFilters) {
        maxCiteEl.value = vals.maxCitations || "";
        citesPerYearEl.value = vals.minCitesPerYear || "";
        inflMinEl.value = vals.minInfluential || "";
        inflMaxEl.value = vals.maxInfluential || "";
        affiliationEl.value = vals.affiliationContains || "";
        funderEl.value = vals.funderContains || "";
      }
      syncSlidersFromInputs();
    };

    function applyYearToAllPages() {
      const yMin = yearMinEl.value.trim();
      const yMax = yearMaxEl.value.trim();
      const urlYear = readYearFilterFromUrl();
      if (yMin === urlYear.yearMin && yMax === urlYear.yearMax) return;
      const newUrl = getScholarSearchUrl({ yearMin: yMin || null, yearMax: yMax || null });
      if (!newUrl || newUrl === window.location.href) return;
      saveClientFilterToStorage(state);
      window.location.assign(newUrl);
    }
    yearMinEl.addEventListener("blur", applyYearToAllPages);
    yearMaxEl.addEventListener("blur", applyYearToAllPages);

    clearBtn.addEventListener("click", () => {
      try {
        const url = new URL(window.location.href);
        const hadYear = url.searchParams.has("as_ylo") || url.searchParams.has("as_yhi");
        if (hadYear && url.pathname.includes("/scholar")) {
          url.searchParams.delete("as_ylo");
          url.searchParams.delete("as_yhi");
          url.searchParams.delete("start");
          sessionStorage.removeItem(FILTER_STORAGE_KEY);
          window.location.assign(url.toString());
          return;
        }
        state.filters.yearMin = state.filters.yearMax = "";
        state.filters.venueKeyword = state.filters.authorMatch = "";
        state.filters.hasPdf = false;
        state.filters.hasCode = false;
        state.filters.qualityFilter = "";
        state.filters.hasFullText = false;
        state.filters.hasFunding = false;
        state.filters.hasOpenCitations = false;
        state.filters.hasDatasetDoi = false;
        state.filters.hasSoftwareDoi = false;
        state.filters.hasUpdates = false;
        state.filters.hasOa = false;
        state.filters.hasPreprint = false;
        state.filters.hasPubmed = false;
        state.filters.hasPmc = false;
        state.filters.hasMesh = false;
        state.filters.minCitations = "";
        state.filters.maxCitations = "";
        state.filters.minCitesPerYear = "";
        state.filters.minInfluential = "";
        state.filters.maxInfluential = "";
        state.filters.affiliationContains = "";
        state.filters.funderContains = "";
        setFilterInputs({
          yearMin: "",
          yearMax: "",
          venueKeyword: "",
          hasPdf: false,
          hasCode: false,
          qualityFilter: "",
          hasFullText: false,
          hasFunding: false,
          hasOpenCitations: false,
          hasDatasetDoi: false,
          hasSoftwareDoi: false,
          hasUpdates: false,
          hasOa: false,
          hasPreprint: false,
          hasPubmed: false,
          hasPmc: false,
          hasMesh: false,
          minCitations: "",
          authorMatch: "",
          maxCitations: "",
          minCitesPerYear: "",
          minInfluential: "",
          maxInfluential: "",
          affiliationContains: "",
          funderContains: ""
        });
        applyFilterValues(readFilterValues());
      } catch (_) {}
    });

    const toggleBtn = document.createElement("button");
    toggleBtn.type = "button";
    toggleBtn.className = "su-filter-toggle";
    toggleBtn.setAttribute("aria-expanded", "false");
    toggleBtn.title = "Show/hide filter options";

    function updateToggleLabel() {
      try {
        const g = readFilterValues();
        const n = [
          g.yearMin,
          g.yearMax,
          g.venueKeyword,
          g.hasPdf,
          g.hasCode,
          g.qualityFilter,
          g.hasFullText,
          g.hasFunding,
          g.hasOpenCitations,
          g.hasDatasetDoi,
          g.hasSoftwareDoi,
          g.hasUpdates,
          g.hasOa,
          g.hasPreprint,
          g.hasPubmed,
          g.hasPmc,
          g.hasMesh,
          g.minCitations,
          g.authorMatch,
          g.maxCitations,
          g.minCitesPerYear,
          g.minInfluential,
          g.maxInfluential,
          g.affiliationContains,
          g.funderContains
        ].filter(
          (v) => (typeof v === "string" ? v.trim() : v)
        ).length;
        const arrow = bar && bar.classList.contains("su-filter-open") ? "▴" : "▾";
        if (toggleBtn) toggleBtn.textContent = n ? `Filter (${n}) ${arrow}` : `Filter ${arrow}`;
      } catch (_) {}
    }

    const applyQuickFilter = (def) => {
      const current = readFilterValues();
      const active = def.isActive(current);
      const next = { ...current };
      const boolKeys = new Set(["hasPdf", "hasCode"]);
      for (const [key, value] of Object.entries(def.patch || {})) {
        if (active) {
          next[key] = boolKeys.has(key) ? false : "";
        } else {
          next[key] = value;
        }
      }
      setFilterInputs(next);
      applyFilterValues(readFilterValues());
    };

    const row = document.createElement("div");
    row.className = "su-within-results-filter-row";
    try {
      row.appendChild(yearMinEl);
      row.appendChild(yearMaxEl);
      row.appendChild(qualitySelect);
      row.appendChild(venueEl);
      row.appendChild(pdfLabel);
      row.appendChild(codeLabel);
      row.appendChild(citeEl);
      row.appendChild(authorEl);
      row.appendChild(sortSelect);
      row.appendChild(clearBtn);
      row.appendChild(loadMoreBtn);
      row.appendChild(openPdfBtn);
      row.appendChild(reviewBtn);
      if (summarizeBtn) row.appendChild(summarizeBtn);
    } catch (_) {
      // If any append fails, still try to show the bar with what we have
    }

    const signalsRow = document.createElement("div");
    signalsRow.className = "su-within-results-filter-row su-filter-signals-row";
    const signalsLabel = document.createElement("span");
    signalsLabel.className = "su-filter-label su-filter-label-text";
    signalsLabel.textContent = "Signals:";
    signalsRow.appendChild(signalsLabel);
    signalsRow.appendChild(fullTextLabel);
    signalsRow.appendChild(fundingLabel);
    signalsRow.appendChild(updatesLabel);
    signalsRow.appendChild(openCiteLabel);
    signalsRow.appendChild(datasetLabel);
    signalsRow.appendChild(softwareLabel);

    const signalsRow2 = document.createElement("div");
    signalsRow2.className = "su-within-results-filter-row su-filter-signals-row";
    const signalsLabel2 = document.createElement("span");
    signalsLabel2.className = "su-filter-label su-filter-label-text";
    signalsLabel2.textContent = "Access:";
    signalsRow2.appendChild(signalsLabel2);
    signalsRow2.appendChild(oaLabel);
    signalsRow2.appendChild(preprintLabel);
    signalsRow2.appendChild(pubmedLabel);
    signalsRow2.appendChild(pmcLabel);
    signalsRow2.appendChild(meshLabel);

    const advRow = document.createElement("div");
    advRow.className = "su-within-results-filter-row su-filter-advanced-row";
    if (showAdvancedFilters) {
      const advLabel = document.createElement("span");
      advLabel.className = "su-filter-label su-filter-label-text";
      advLabel.textContent = "Intel:";
      advRow.appendChild(advLabel);
      advRow.appendChild(maxCiteEl);
      advRow.appendChild(citesPerYearEl);
      advRow.appendChild(inflMinEl);
      advRow.appendChild(inflMaxEl);
      advRow.appendChild(affiliationEl);
      advRow.appendChild(funderEl);
    }

    const quickRow = document.createElement("div");
    quickRow.className = "su-within-results-filter-row su-filter-quick-row";
    for (const def of quickDefs) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "su-filter-quick-btn";
      btn.textContent = def.label;
      btn.title = "Quick filter";
      btn.addEventListener("click", () => applyQuickFilter(def));
      quickRow.appendChild(btn);
      quickButtons.set(def.id, btn);
    }

    // --- Year range dual-thumb slider ---
    // Determine year bounds from current results (fall back to 1990–currentYear)
    const SLIDER_MIN_YEAR = 1990;
    const SLIDER_MAX_YEAR = currentYear;

    const sliderRow = document.createElement("div");
    sliderRow.className = "su-year-slider-row";

    const sliderLabel = document.createElement("span");
    sliderLabel.className = "su-year-slider-label";
    sliderLabel.textContent = "Years:";
    sliderRow.appendChild(sliderLabel);

    const sliderWrap = document.createElement("div");
    sliderWrap.className = "su-year-slider-wrap";

    const sliderTrack = document.createElement("div");
    sliderTrack.className = "su-year-slider-track";

    const sliderFill = document.createElement("div");
    sliderFill.className = "su-year-slider-fill";
    sliderTrack.appendChild(sliderFill);

    const sliderMin = document.createElement("input");
    sliderMin.type = "range";
    sliderMin.className = "su-year-slider-thumb su-year-slider-thumb-min";
    sliderMin.min = String(SLIDER_MIN_YEAR);
    sliderMin.max = String(SLIDER_MAX_YEAR);
    sliderMin.step = "1";
    sliderMin.value = String(f.yearMin ? Math.max(SLIDER_MIN_YEAR, parseInt(f.yearMin, 10)) : SLIDER_MIN_YEAR);

    const sliderMax = document.createElement("input");
    sliderMax.type = "range";
    sliderMax.className = "su-year-slider-thumb su-year-slider-thumb-max";
    sliderMax.min = String(SLIDER_MIN_YEAR);
    sliderMax.max = String(SLIDER_MAX_YEAR);
    sliderMax.step = "1";
    sliderMax.value = String(f.yearMax ? Math.min(SLIDER_MAX_YEAR, parseInt(f.yearMax, 10)) : SLIDER_MAX_YEAR);

    sliderTrack.appendChild(sliderMin);
    sliderTrack.appendChild(sliderMax);
    sliderWrap.appendChild(sliderTrack);
    sliderRow.appendChild(sliderWrap);

    const updateSliderFill = () => {
      const lo = parseInt(sliderMin.value, 10);
      const hi = parseInt(sliderMax.value, 10);
      const span = SLIDER_MAX_YEAR - SLIDER_MIN_YEAR;
      const leftPct = ((lo - SLIDER_MIN_YEAR) / span) * 100;
      const rightPct = ((SLIDER_MAX_YEAR - hi) / span) * 100;
      sliderFill.style.left = leftPct + "%";
      sliderFill.style.right = rightPct + "%";
    };

    // Sync sliders → text inputs
    sliderMin.addEventListener("input", () => {
      const lo = parseInt(sliderMin.value, 10);
      const hi = parseInt(sliderMax.value, 10);
      if (lo > hi) { sliderMin.value = String(hi); return; }
      yearMinEl.value = lo <= SLIDER_MIN_YEAR ? "" : String(lo);
      updateSliderFill();
      scheduleFilterApply();
    });
    sliderMax.addEventListener("input", () => {
      const lo = parseInt(sliderMin.value, 10);
      const hi = parseInt(sliderMax.value, 10);
      if (hi < lo) { sliderMax.value = String(lo); return; }
      yearMaxEl.value = hi >= SLIDER_MAX_YEAR ? "" : String(hi);
      updateSliderFill();
      scheduleFilterApply();
    });

    // Sync text inputs → sliders
    const syncSlidersFromInputs = () => {
      const lo = parseInt(yearMinEl.value, 10);
      const hi = parseInt(yearMaxEl.value, 10);
      sliderMin.value = String(Number.isFinite(lo) ? Math.max(SLIDER_MIN_YEAR, lo) : SLIDER_MIN_YEAR);
      sliderMax.value = String(Number.isFinite(hi) ? Math.min(SLIDER_MAX_YEAR, hi) : SLIDER_MAX_YEAR);
      updateSliderFill();
    };
    yearMinEl.addEventListener("input", syncSlidersFromInputs);
    yearMaxEl.addEventListener("input", syncSlidersFromInputs);

    updateSliderFill();
    // --- End year slider ---

    const body = document.createElement("div");
    body.className = "su-within-results-filter-body";
    body.appendChild(quickRow);
    body.appendChild(row);
    body.appendChild(signalsRow);
    body.appendChild(signalsRow2);
    if (showAdvancedFilters) body.appendChild(advRow);
    body.appendChild(sliderRow);

    toggleBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      if (bar) bar.classList.toggle("su-filter-open");
      const open = bar && bar.classList.contains("su-filter-open");
      toggleBtn.setAttribute("aria-expanded", String(open));
      updateToggleLabel();
      if (open) {
        const close = (e2) => {
          if (bar && !bar.contains(e2.target)) {
            bar.classList.remove("su-filter-open");
            toggleBtn.setAttribute("aria-expanded", "false");
            updateToggleLabel();
            document.removeEventListener("click", close);
          }
        };
        setTimeout(() => document.addEventListener("click", close), 0);
      }
    });

    state.filters = state.filters || {};
    updateToggleLabel();
    updateQuickButtons();
    bar.appendChild(toggleBtn);
    bar.appendChild(body);

    const origUpdate = updateStateAndApply;
    updateStateAndApply = () => {
      try {
        origUpdate();
      } catch (_) {}
      updateToggleLabel();
    };

    document.body.appendChild(bar);
  }

  const FILTER_STORAGE_KEY = "su-filter-client";

  function loadClientFilterFromStorage() {
    try {
      const q = getScholarSearchQuery();
      if (!q) return null;
      const raw = sessionStorage.getItem(FILTER_STORAGE_KEY);
      if (!raw) return null;
      const data = JSON.parse(raw);
      if (data && data.q === q) return data;
      return null;
    } catch {
      return null;
    }
  }

  function saveClientFilterToStorage(state) {
    try {
      const q = getScholarSearchQuery();
      if (!q || !state.filters) return;
      sessionStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify({
        q,
        venueKeyword: state.filters.venueKeyword || "",
        hasPdf: !!state.filters.hasPdf,
        hasCode: !!state.filters.hasCode,
        qualityFilter: state.filters.qualityFilter || "",
        hasFullText: !!state.filters.hasFullText,
        hasFunding: !!state.filters.hasFunding,
        hasOpenCitations: !!state.filters.hasOpenCitations,
        hasDatasetDoi: !!state.filters.hasDatasetDoi,
        hasSoftwareDoi: !!state.filters.hasSoftwareDoi,
        hasUpdates: !!state.filters.hasUpdates,
        hasOa: !!state.filters.hasOa,
        hasPreprint: !!state.filters.hasPreprint,
        hasPubmed: !!state.filters.hasPubmed,
        hasPmc: !!state.filters.hasPmc,
        hasMesh: !!state.filters.hasMesh,
        minCitations: state.filters.minCitations || "",
        authorMatch: state.filters.authorMatch || "",
        maxCitations: state.filters.maxCitations || "",
        minCitesPerYear: state.filters.minCitesPerYear || "",
        minInfluential: state.filters.minInfluential || "",
        maxInfluential: state.filters.maxInfluential || "",
        affiliationContains: state.filters.affiliationContains || "",
        funderContains: state.filters.funderContains || ""
      }));
    } catch (_) {}
  }

  async function run() {
    const state = { saved: {}, settings: null, qIndex: null, venueCache: new Map(), allPublicationsLoaded: false, authorAutoLoadDisabled: false, authorStatsPartial: false, activeFilter: null, filters: {} };
    await refreshState(state);
    await ensureExternalSignalCacheLoaded();
    if (window.matchMedia && !window.__suThemeListener) {
      window.__suThemeListener = true;
      window.matchMedia("(prefers-color-scheme: dark)").addListener(() => {
        if (window.suState?.settings?.theme === "auto") applyTheme("auto");
      });
    }

    const q = getScholarSearchQuery();
    if (q) {
      const urlYear = readYearFilterFromUrl();
      const stored = loadClientFilterFromStorage();
      state.filters = {
        yearMin: urlYear.yearMin,
        yearMax: urlYear.yearMax,
        venueKeyword: (stored && stored.venueKeyword) || "",
        hasPdf: !!(stored && stored.hasPdf),
        hasCode: !!(stored && stored.hasCode),
        qualityFilter: (stored && stored.qualityFilter) || "",
        hasFullText: !!(stored && stored.hasFullText),
        hasFunding: !!(stored && stored.hasFunding),
        hasOpenCitations: !!(stored && stored.hasOpenCitations),
        hasDatasetDoi: !!(stored && stored.hasDatasetDoi),
        hasSoftwareDoi: !!(stored && stored.hasSoftwareDoi),
        hasUpdates: !!(stored && stored.hasUpdates),
        hasOa: !!(stored && stored.hasOa),
      hasPreprint: !!(stored && stored.hasPreprint),
      hasPubmed: !!(stored && stored.hasPubmed),
      hasPmc: !!(stored && stored.hasPmc),
      hasMesh: !!(stored && stored.hasMesh),
      minCitations: (stored && stored.minCitations) || "",
      authorMatch: (stored && stored.authorMatch) || "",
      maxCitations: (stored && stored.maxCitations) || "",
      minCitesPerYear: (stored && stored.minCitesPerYear) || "",
      minInfluential: (stored && stored.minInfluential) || "",
      maxInfluential: (stored && stored.maxInfluential) || "",
      affiliationContains: (stored && stored.affiliationContains) || "",
      funderContains: (stored && stored.funderContains) || ""
    };
  }

    await ensureSemanticExpansionPanel(state);
    ensureSearchSyntaxHighlighting();

    // Extract author name if on author profile page
    let authorVariations = [];
    const isAuthorProfilePage = document.querySelector(".gsc_a_tr") !== null;
    if (isAuthorProfilePage) {
      const authorName = extractAuthorName();
      if (authorName) {
        authorVariations = generateAuthorNameVariations(authorName);
        state.authorVariations = authorVariations;
      }
      const scholarId = new URL(window.location.href).searchParams.get("user");
      state.authorFeatureToggles = await getAuthorFeatureToggles(scholarId);
      state.citedByColorScheme = await getAuthorCitedByColorScheme(scholarId);
      await ensureGraphStateForAuthor(scholarId, authorName || getScholarAuthorName());
    }

    let scheduled = false;
    let processing = false;
    let needsRerun = false;
    let forceFull = true;
    const pendingDirtyRows = new Set();
    const UI_BATCH_SIZE = 10;
    const VIEWPORT_MARGIN = 200;
    const AUTHOR_AUTOLOAD_MAX = 200;
    let viewObserver = null;
    const visibleRows = new Set();

    const markDirtyRow = (row) => {
      if (!row || !row.isConnected) return;
      row.dataset.suDirty = "1";
      invalidateRowCache(row);
      pendingDirtyRows.add(row);
    };

    const ensureViewObserver = () => {
      if (viewObserver || typeof IntersectionObserver === "undefined") return;
      viewObserver = new IntersectionObserver((entries) => {
        const newlyVisible = [];
        for (const entry of entries) {
          const row = entry.target;
          if (entry.isIntersecting) {
            row.dataset.suInView = "1";
            visibleRows.add(row);
            newlyVisible.push(row);
          } else {
            row.dataset.suInView = "0";
            visibleRows.delete(row);
          }
        }
        if (newlyVisible.length > 0) scheduleProcess({ rows: newlyVisible });
      }, { rootMargin: `${VIEWPORT_MARGIN}px`, threshold: 0 });
    };

    const observeRow = (row) => {
      ensureViewObserver();
      if (!viewObserver || row.__suObserved) return;
      row.__suObserved = true;
      viewObserver.observe(row);
    };

    const processDirtyRows = async (rows, isAuthorProfile) => {
      if (!rows || rows.length === 0) return;
      const filteredRows = rows.filter((r) => r && r.isConnected);
      if (filteredRows.length === 0) return;
      if (typeof IntersectionObserver !== "undefined") {
        for (const r of filteredRows) observeRow(r);
      }
      if (!isAuthorProfile) {
        applyResultFiltersToRows(filteredRows, state);
      }
      await Promise.all(
        filteredRows.map((r) =>
          ensureResultUI(r, state, isAuthorProfile, { inViewport: r.dataset.suInView === "1" })
            .then(() => {
              if (isAuthorProfile && state.authorVariations && state.authorVariations.length > 0) {
                highlightAuthorName(r, state.authorVariations);
              }
            })
            .catch(() => {})
        )
      );
    };

    const processAll = async () => {
      if (document.hidden) return;
      if (processing) {
        needsRerun = true;
        return;
      }
      processing = true;
      const doFull = forceFull || pendingDirtyRows.size === 0;
      const dirtyRows = doFull ? [] : Array.from(pendingDirtyRows);
      forceFull = false;
      pendingDirtyRows.clear();
      try {
        let { results, isAuthorProfile } = scanResults();
        if (!isAuthorProfile && window.suSearchSortByCitations && window.suSearchSortByCitations !== "default") {
          applyCitationSort();
          const next = scanResults();
          results = next.results;
        }
        if (isAuthorProfile) {
          const showMoreBtn = document.getElementById("gsc_bpf_more");
          const fullyLoadedNow = !showMoreBtn || showMoreBtn.disabled;
          if (fullyLoadedNow) {
            state.allPublicationsLoaded = true;
            state.authorStatsPartial = false;
            state.authorAutoLoadDisabled = false;
          }
        }
        
        if (!doFull) {
          await processDirtyRows(dirtyRows, isAuthorProfile);
          return;
        }

      // First pass: extract venues from all results and find the best one for each paper
      if (!isAuthorProfile) {
        const venueMap = new Map(); // clusterId -> best venue info
        for (const r of results) {
          try {
            let paper = getCachedPaperFast(r);
            if (!paper.authorsVenue || paper.authorsVenue.includes("…") || paper.authorsVenue.length < 20) {
              paper = getCachedPaperFull(r);
            }
            const clusterId = paper.clusterId || paper.key;
            if (clusterId) {
              const currentVenue = paper.venue || "";
              const existing = venueMap.get(clusterId);
              
              // Score venues: prefer complete, non-truncated venues with journal/conference names
              const scoreVenue = (venue) => {
                if (!venue) return 0;
                let score = venue.length; // Longer is generally better
                if (venue.includes("…") || venue.includes("…")) score -= 1000; // Heavily penalize truncated
                if (venue.toLowerCase().includes("journal")) score += 50;
                if (venue.toLowerCase().includes("conference")) score += 50;
                if (venue.match(/\d+\s*\([^)]*\)/)) score -= 20; // Penalize if still has volume/issue
                if (venue.length < 10) score -= 50; // Penalize very short venues
                return score;
              };
              
              const currentScore = scoreVenue(currentVenue);
              const existingScore = existing ? scoreVenue(existing) : -1;
              
              // Keep the best scored venue
              if (currentScore > existingScore) {
                venueMap.set(clusterId, currentVenue);
              }
            }
          } catch {
            // Skip errors
          }
        }
        // Update cache in state
        state.venueCache.clear();
        for (const [clusterId, venue] of venueMap) {
          state.venueCache.set(clusterId, venue);
        }
      }
      
      // Second pass: process all results with shared venue information
      // First, apply filters if active (quality filter and/or author position filter)
      const authorTopicFilters = normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter);
      const authorVenueFilter = String(window.suAuthorVenueFilter || "").trim().toLowerCase();
      const authorCoauthorFilter = String(window.suAuthorCoauthorFilter || "").trim();
      const authorCitationBand = window.suAuthorCitationBand || null;
      if (isAuthorProfile && (window.suActiveFilter || (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") || authorTopicFilters.length || authorVenueFilter || authorCoauthorFilter || authorCitationBand)) {
        for (const r of results) {
          try {
            const paper = getCachedAuthorPaper(r);
            let show = true;
            if (window.suActiveFilter) {
              show = show && paperMatchesFilter(paper, window.suActiveFilter, state);
            }
            if (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") {
              show = show && paperMatchesPositionFilter(paper, window.suAuthorPositionFilter, state.authorVariations);
            }
            if (authorTopicFilters.length) show = show && paperMatchesTitleTokens(paper, authorTopicFilters);
            if (authorVenueFilter) {
              const venueKey = normalizeVenueKey(paper.venue || "");
              show = show && venueKey === authorVenueFilter;
            }
            if (authorCoauthorFilter) {
              show = show && paperHasCoauthor(paper, authorCoauthorFilter);
            }
            if (authorCitationBand) {
              const citations = getCachedAuthorCitationCount(r);
              const min = Number(authorCitationBand.min || 0);
              const max = authorCitationBand.max === Infinity ? Infinity : Number(authorCitationBand.max || 0);
              show = show && citations >= min && citations <= max;
            }
            r.style.display = show ? "" : "none";
          } catch {
            r.style.display = "none";
          }
        }
      } else {
        for (const r of results) {
          r.style.display = "";
        }
      }

      // New since last visit: compute page key and which keys are new
      const pageKey = (location.pathname || "") + (location.search || "");
      if (state.settings.showNewSinceLastVisit) {
        try {
          const cache = await getPageVisitCache();
          const previous = cache[pageKey];
          const previousSeen = new Set(Array.isArray(previous?.seenKeys) ? previous.seenKeys : []);
          const currentKeys = [];
          for (const r of results) {
            if (r.style.display === "none") continue;
            try {
              let paper = isAuthorProfile ? getCachedAuthorPaper(r) : getCachedPaperFast(r);
              if (!isAuthorProfile && (!paper.authorsVenue || paper.authorsVenue.includes("…")) && !paper.clusterId && !paper.url) {
                paper = getCachedPaperFull(r);
              }
              if (paper?.key) currentKeys.push(paper.key);
            } catch {}
          }
          state.newSinceLastVisitKeys = new Set(currentKeys.filter((k) => !previousSeen.has(k)));
          state.currentResultKeys = currentKeys;
          state.currentPageKey = pageKey;
        } catch {
          state.newSinceLastVisitKeys = new Set();
          state.currentResultKeys = [];
          state.currentPageKey = pageKey;
        }
      } else {
        state.newSinceLastVisitKeys = new Set();
        state.currentResultKeys = [];
      }

      if (state.settings.showCitationSpike) {
        try {
          state.citationSnapshots = await getCitationSnapshots();
        } catch {
          state.citationSnapshots = {};
        }
      } else {
        state.citationSnapshots = {};
      }

      // Compute velocity bucket averages for trajectory arrows
      const needsVelocityBuckets = state.settings.viewMode !== "minimal";
      if (needsVelocityBuckets) {
        const bucketSums = { early: 0, mid: 0, late: 0 };
        const bucketCounts = { early: 0, mid: 0, late: 0 };
        for (const r of results) {
          if (r.style.display === "none") continue;
          let paper;
          try {
            paper = isAuthorProfile ? getCachedAuthorPaper(r) : getCachedPaperFast(r);
          } catch {
            continue;
          }
          const citations = isAuthorProfile ? getCachedAuthorCitationCount(r) : getCitationCountFromResult(r);
          const data = computeVelocityValue(citations, paper.year);
          if (!data) continue;
          const bucket = getVelocityBucket(data.yearsAgo);
          bucketSums[bucket] += data.velocity;
          bucketCounts[bucket] += 1;
        }
        state.velocityBucketAvg = {
          early: bucketCounts.early ? bucketSums.early / bucketCounts.early : 0,
          mid: bucketCounts.mid ? bucketSums.mid / bucketCounts.mid : 0,
          late: bucketCounts.late ? bucketSums.late / bucketCounts.late : 0
        };
      } else {
        state.velocityBucketAvg = { early: 0, mid: 0, late: 0 };
      }
      window.suState = state;
      
      // Now process UI: visible rows first, offscreen rows when they enter viewport
      const visibleResults = results.filter((r) => r.style.display !== "none");
      const processBatch = (rows, inViewportDefault = false) =>
        Promise.all(
          rows.map((r) =>
            ensureResultUI(r, state, isAuthorProfile, { inViewport: inViewportDefault || r.dataset.suInView === "1" }).then(() => {
              if (isAuthorProfile && state.authorVariations && state.authorVariations.length > 0) {
                highlightAuthorName(r, state.authorVariations);
              }
            }).catch(() => {})
          )
        );

      if (typeof IntersectionObserver !== "undefined") {
        ensureViewObserver();
        for (const r of visibleResults) observeRow(r);
        for (const r of Array.from(visibleRows)) {
          if (!r.isConnected) visibleRows.delete(r);
        }
        const initialRows = visibleRows.size > 0 ? Array.from(visibleRows) : visibleResults.slice(0, UI_BATCH_SIZE);
        if (initialRows.length > 0) {
          initialRows.forEach((r) => { r.dataset.suInView = "1"; });
          await processBatch(initialRows, true);
        }
      } else {
        // Fallback for older browsers: process above-the-fold rows only
        const viewportHeight = window.innerHeight;
        const initialRows = [];
        for (const r of visibleResults) {
          try {
            if (r.getBoundingClientRect().top < viewportHeight + VIEWPORT_MARGIN) {
              initialRows.push(r);
              if (initialRows.length >= UI_BATCH_SIZE) break;
            }
          } catch {
            initialRows.push(r);
            if (initialRows.length >= UI_BATCH_SIZE) break;
          }
        }
        if (initialRows.length > 0) {
          initialRows.forEach((r) => { r.dataset.suInView = "1"; });
          await processBatch(initialRows, true);
        }
      }

      // Smart-rename PDF: build url -> { author, year, title } for onDeterminingFilename in background
      try {
        const pdfUrlToMetadata = {};
        for (const r of results) {
          if (r.style.display === "none") continue;
          const pdfInfo = getBestPdfUrl(r);
          if (!pdfInfo?.url) continue;
          let paper;
          try {
            paper = isAuthorProfile ? getCachedAuthorPaper(r) : getCachedPaperFast(r);
          } catch {
            continue;
          }
          const authorsPart = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0] || "";
          const firstAuthor = authorsPart.split(/\s*,\s*|\s+and\s+/i).map((s) => s.trim()).filter(Boolean)[0] || "Unknown";
          const year = paper.year != null ? String(paper.year).replace(/\D/g, "").slice(0, 4) : "";
          pdfUrlToMetadata[pdfInfo.url] = { author: firstAuthor, year: year || undefined, title: paper.title || "Paper" };
        }
        if (chrome.storage?.session?.set) {
          try {
            chrome.storage.session.set({ pdfUrlToMetadata });
          } catch (e) {
            if (!String(e?.message || "").includes("Extension context invalidated")) throw e;
          }
        }
      } catch {
        // Non-fatal
      }

      // Re-apply sort by citations/year if toggle is on (e.g. after "Show more")
      if (isAuthorProfile && window.suAuthorSortByVelocity) {
        applyAuthorSort();
      }

      if (isAuthorProfile) {
        const filterBar = document.getElementById("su-within-results-filter");
        if (filterBar) filterBar.style.display = "none";
      } else {
        applyVersionGrouping(results, state);
        ensureFilterBar(state);
        applyResultFilters(results, state);
      }

      if (isAuthorProfile) markAuthorProfileTitleToolbar();

      // Update author stats if on author profile page (use only visible rows when a filter is active)
      if (isAuthorProfile && state.settings && state.settings.showQualityBadges) {
        const visibleAuthorResults = results.filter(
          (r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out")
        );
        if (!state.allPublicationsLoaded && !state.authorAutoLoadDisabled) {
          renderAuthorStats({ qualityCounts: { q1: 0, a: 0, vhb: 0, utd24: 0, ft50: 0, abs4star: 0 }, totalPublications: 0, totalCitations: 0, avgCitations: 0, recentActivity: { last5Years: 0 } }, true);
          const runLoadAll = () => {
            loadAllPublicationsRecursively({ maxPubs: AUTHOR_AUTOLOAD_MAX }).then(async (loadResult) => {
              const fullyLoaded = loadResult && loadResult.status === "complete";
              state.allPublicationsLoaded = fullyLoaded;
              state.authorStatsPartial = !fullyLoaded;
              state.authorAutoLoadDisabled = !fullyLoaded;
              const allResults = scanResults().results;
              const authorTopicFilters2 = normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter);
              const authorVenueFilter2 = String(window.suAuthorVenueFilter || "").trim().toLowerCase();
              const authorCoauthorFilter2 = String(window.suAuthorCoauthorFilter || "").trim();
              const authorCitationBand2 = window.suAuthorCitationBand || null;
              if (window.suActiveFilter || (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") || authorTopicFilters2.length || authorVenueFilter2 || authorCoauthorFilter2 || authorCitationBand2) {
                for (const r of allResults) {
                  try {
                    const paper = getCachedAuthorPaper(r);
                    let show = true;
                    if (window.suActiveFilter) show = show && paperMatchesFilter(paper, window.suActiveFilter, state);
                    if (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") show = show && paperMatchesPositionFilter(paper, window.suAuthorPositionFilter, state.authorVariations);
                    if (authorTopicFilters2.length) show = show && paperMatchesTitleTokens(paper, authorTopicFilters2);
                    if (authorVenueFilter2) {
                      const venueKey = normalizeVenueKey(paper.venue || "");
                      show = show && venueKey === authorVenueFilter2;
                    }
                    if (authorCoauthorFilter2) {
                      show = show && paperHasCoauthor(paper, authorCoauthorFilter2);
                    }
                    if (authorCitationBand2) {
                      const citations = getCachedAuthorCitationCount(r);
                      const min = Number(authorCitationBand2.min || 0);
                      const max = authorCitationBand2.max === Infinity ? Infinity : Number(authorCitationBand2.max || 0);
                      show = show && citations >= min && citations <= max;
                    }
                    r.style.display = show ? "" : "none";
                  } catch {
                    r.style.display = "none";
                  }
                }
              }
              const visible = allResults.filter((r) => r.style.display !== "none" && !r.classList.contains("su-filtered-out"));
              const fullStats = computeAuthorStats(visible, state, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            }).catch(async () => {
              state.authorStatsPartial = true;
              state.authorAutoLoadDisabled = true;
              const fullStats = computeAuthorStats(visibleAuthorResults, state, window.suAuthorPositionFilter);
              await renderAuthorStatsWithGrowth(fullStats);
            });
          };
          if (typeof requestIdleCallback === "function") {
            requestIdleCallback(runLoadAll, { timeout: 1500 });
          } else {
            setTimeout(runLoadAll, 600);
          }
        } else {
          const fullStats = computeAuthorStats(visibleAuthorResults, state, window.suAuthorPositionFilter);
          await renderAuthorStatsWithGrowth(fullStats);
        }
      }

      // Persist visit cache for "new since last visit" next time
      if (state.settings.showNewSinceLastVisit && state.currentPageKey && Array.isArray(state.currentResultKeys) && state.currentResultKeys.length >= 0) {
        setPageVisitCacheEntry(state.currentPageKey, state.currentResultKeys).catch(() => {});
      }
      } finally {
        processing = false;
        if (needsRerun) {
          needsRerun = false;
          scheduleProcess();
        }
      }
    };

    const scheduleProcess = (opts = {}) => {
      if (opts.full) forceFull = true;
      if (Array.isArray(opts.rows)) {
        opts.rows.forEach((r) => markDirtyRow(r));
      }
      if (scheduled) return;
      scheduled = true;
      if (typeof requestIdleCallback === 'function') {
        requestIdleCallback(async () => {
          scheduled = false;
          await processAll();
        }, { timeout: 500 });
      } else {
        setTimeout(async () => {
          scheduled = false;
          await processAll();
        }, 250);
      }
    };

    /** Mark the papers table header row (TITLE / CITED BY / YEAR) so dark mode can style it. */
    function markAuthorProfileTitleToolbar() {
      const tbody = document.getElementById("gsc_a_b");
      if (!tbody) return;
      const table = tbody.closest("table");
      if (!table) return;
      const hasTitleAndCited = (el) => {
        const t = (el.textContent || "").toUpperCase();
        return t.includes("TITLE") && t.includes("CITED BY");
      };
      let header = null;
      const container = table.parentElement;
      if (container) {
        for (const child of container.children) {
          if (child === table) break;
          if (hasTitleAndCited(child)) {
            header = child;
            break;
          }
        }
      }
      if (!header) header = table.querySelector("thead tr");
      if (!header) {
        for (const tr of table.querySelectorAll("tr")) {
          if (hasTitleAndCited(tr)) {
            header = tr;
            break;
          }
        }
      }
      if (!header) header = table.querySelector("tr:first-child");
      if (header && !header.classList.contains("su-title-toolbar")) {
        header.classList.add("su-title-toolbar");
        for (const cell of header.querySelectorAll("th, td, div")) cell.classList.add("su-title-toolbar-cell");
      }
    }

    const resultsRoot = () => {
      const mid = document.querySelector("#gs_res_ccl_mid");
      return mid || document.querySelector("#gsc_a_b");
    };

    const isSuNode = (node) => {
      if (!node || node.nodeType !== 1) return false;
      const el = node;
      if (el.id && el.id.startsWith("su-")) return true;
      if (el.classList && el.classList.length) {
        for (const cls of el.classList) {
          if (cls && cls.startsWith("su-")) return true;
        }
      }
      return false;
    };
    
    const mo = new MutationObserver((mutations) => {
      if (document.hidden) return;
      const root = resultsRoot();
      if (!root) return;
      const inResults = mutations.some((m) =>
        root.contains(m.target) || (typeof m.target.contains === "function" && m.target.contains(root))
      );
      if (!inResults) return;

      let structural = false;
      const dirtyRows = new Set();

      const markRowFromNode = (node) => {
        if (!node || node.nodeType !== Node.ELEMENT_NODE) return;
        const el = node;
        if (isSuNode(el)) return;
        if (el.matches && el.matches(".gs_r, .gsc_a_tr")) {
          structural = true;
          dirtyRows.add(el);
          return;
        }
        const row = el.closest && el.closest(".gs_r, .gsc_a_tr");
        if (row) dirtyRows.add(row);
        const inner = el.querySelector && el.querySelector(".gs_r, .gsc_a_tr");
        if (inner) {
          structural = true;
          dirtyRows.add(inner);
        }
      };

      for (const m of mutations) {
        if (!(root.contains(m.target) || (typeof m.target.contains === "function" && m.target.contains(root)))) continue;
        const nodes = [];
        if (m.addedNodes && m.addedNodes.length) nodes.push(...m.addedNodes);
        if (m.removedNodes && m.removedNodes.length) nodes.push(...m.removedNodes);
        const elementNodes = nodes.filter((n) => n.nodeType === Node.ELEMENT_NODE);
        if (elementNodes.length > 0 && elementNodes.every((n) => isSuNode(n))) {
          continue;
        }
        for (const n of elementNodes) markRowFromNode(n);
        const targetRow = m.target?.closest?.(".gs_r, .gsc_a_tr");
        if (targetRow && !isSuNode(targetRow)) dirtyRows.add(targetRow);
      }

      if (dirtyRows.size === 0 && !structural) return;
      scheduleProcess({ full: structural, rows: Array.from(dirtyRows) });
    });
    mo.observe(document.documentElement, { childList: true, subtree: true });
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) scheduleProcess();
    });
    
    // Store processAll function and state globally so filter badges can trigger it
    window.suProcessAll = () => {
      forceFull = true;
      return processAll();
    };
    window.suState = state;

    await processAll();
    await ensureReadingQueueSidebar();

    try {
      if (typeof chrome !== "undefined" && chrome.storage?.onChanged?.addListener) {
        const onChanged = chrome.storage.onChanged;
        if (onChanged) {
          onChanged.addListener(async (changes, area) => {
            try {
              if (area !== "local") return;
              if (changes.readingQueue) await ensureReadingQueueSidebar();
              const hiddenChanged = changes.hiddenPapers || changes.hiddenVenues || changes.hiddenAuthors;
              if (!changes.savedPapers && !changes.settings && !hiddenChanged) return;
              await refreshState(state);
              if (changes.savedPapers || changes.settings) await ensureSemanticExpansionPanel(state);
              scheduleProcess({ full: true });
              const guideBtn = document.getElementById("su-reading-guide-toggle");
              if (guideBtn) guideBtn.style.display = state.settings.showReadingGuide ? "" : "none";
            } catch (e) {
              if (!String(e?.message || "").includes("Extension context invalidated")) throw e;
            }
          });
        }
      }
    } catch (e) {
      const msg = String(e?.message || e || "");
      if (!msg.includes("Extension context invalidated") && !msg.includes("onChanged")) {
        throw e;
      }
      // Extension was reloaded or context invalidated; chrome.storage is no longer valid
    }

    // ——— Vim-style keyboard navigation (j/k, Enter, s, c, /) ———
    let keyboardSelectedIndex = null;

    function getVisibleResults() {
      const { results, isAuthorProfile } = scanResults();
      return results.filter(
        (r) =>
          r.style.display !== "none" &&
          !r.classList.contains("su-version-grouped-hidden") &&
          !r.classList.contains("su-filtered-out") &&
          !r.classList.contains("su-hidden")
      );
    }

    function updateKeyboardHighlight() {
      document.querySelectorAll(".su-keyboard-selected").forEach((el) => el.classList.remove("su-keyboard-selected"));
      const visible = getVisibleResults();
      if (visible.length === 0) return;
      if (keyboardSelectedIndex == null || keyboardSelectedIndex < 0) keyboardSelectedIndex = 0;
      if (keyboardSelectedIndex >= visible.length) keyboardSelectedIndex = visible.length - 1;
      const row = visible[keyboardSelectedIndex];
      if (row) {
        row.classList.add("su-keyboard-selected");
        row.scrollIntoView({ block: "nearest", behavior: "smooth" });
      }
    }

    document.addEventListener("keydown", (e) => {
      if (e.target?.closest?.("input, textarea, select") || (e.target?.isContentEditable && e.target?.isContentEditable)) return;
      const visible = getVisibleResults();
      if (visible.length === 0) return;

      const key = e.key?.toLowerCase();
      // Command palette (single key)
      if (key === "p") {
        e.preventDefault();
        openCommandPalette(visible);
        return;
      }
      if (key === "j") {
        e.preventDefault();
        if (keyboardSelectedIndex == null) keyboardSelectedIndex = 0;
        else keyboardSelectedIndex = Math.min(keyboardSelectedIndex + 1, visible.length - 1);
        updateKeyboardHighlight();
        return;
      }
      if (key === "k") {
        e.preventDefault();
        if (keyboardSelectedIndex == null) keyboardSelectedIndex = visible.length - 1;
        else keyboardSelectedIndex = Math.max(0, keyboardSelectedIndex - 1);
        updateKeyboardHighlight();
        return;
      }
      if (key === "enter" && keyboardSelectedIndex != null && visible[keyboardSelectedIndex]) {
        const row = visible[keyboardSelectedIndex];
        const titleLink = row.querySelector(".gs_rt a, .gsc_a_at");
        const href = titleLink?.getAttribute?.("href");
        if (href) {
          e.preventDefault();
          window.open(href, "_blank", "noopener");
        }
        return;
      }
      if (key === "s" && keyboardSelectedIndex != null && visible[keyboardSelectedIndex]) {
        e.preventDefault();
        const row = visible[keyboardSelectedIndex];
        const isAuthorProfile = document.querySelector(".gsc_a_tr") !== null;
        try {
          const paper = isAuthorProfile ? getCachedAuthorPaper(row) : extractPaperFromResult(row);
          const best = getBestPdfUrl(row);
          const pdfData = best?.url ? { pdfUrl: best.url, pdfLabel: best.label } : {};
          upsertPaper({ ...paper, ...pdfData, sourcePageUrl: window.location.href, savedAt: paper.savedAt || new Date().toISOString() });
          refreshState(state).then(() => window.suProcessAll?.());
          updateKeyboardHighlight();
        } catch {}
        return;
      }
      if (key === "c" && keyboardSelectedIndex != null && visible[keyboardSelectedIndex]) {
        e.preventDefault();
        const row = visible[keyboardSelectedIndex];
        for (const a of row.querySelectorAll("a")) {
          if (/^Cite$/i.test(text(a).trim())) {
            a.click();
            break;
          }
        }
        return;
      }
      if (key === "/") {
        e.preventDefault();
        const searchEl = document.querySelector("#gs_hdr_ts_in, input[name='q']");
        if (searchEl) {
          searchEl.focus();
          searchEl.select?.();
        }
        return;
      }
    });

    // Command palette (single key)
    let commandPaletteEl = null;
    let commandBackdropEl = null;
    let commandItems = [];
    let commandIndex = 0;
    let paletteOpen = false;

    function closeCommandPalette() {
      paletteOpen = false;
      if (commandBackdropEl) commandBackdropEl.classList.remove("su-command-visible");
      if (commandPaletteEl) commandPaletteEl.classList.remove("su-command-visible");
    }

    function ensureCommandPalette() {
      if (commandPaletteEl && commandBackdropEl) return;
      commandBackdropEl = document.createElement("div");
      commandBackdropEl.className = "su-command-backdrop";
      commandBackdropEl.addEventListener("click", closeCommandPalette);
      commandPaletteEl = document.createElement("div");
      commandPaletteEl.className = "su-command-palette";
      document.body.appendChild(commandBackdropEl);
      document.body.appendChild(commandPaletteEl);
      document.addEventListener("keydown", (e) => {
        if (!paletteOpen) return;
        const k = e.key?.toLowerCase();
        if (k === "escape") {
          e.preventDefault();
          closeCommandPalette();
          return;
        }
        if (k === "arrowdown" || k === "j") {
          e.preventDefault();
          commandIndex = (commandIndex + 1) % commandItems.length;
          renderCommandPalette();
          return;
        }
        if (k === "arrowup" || k === "k") {
          e.preventDefault();
          commandIndex = (commandIndex - 1 + commandItems.length) % commandItems.length;
          renderCommandPalette();
          return;
        }
        if (k === "enter") {
          e.preventDefault();
          commandItems[commandIndex]?.action?.();
          closeCommandPalette();
          return;
        }
      });
    }

    function renderCommandPalette() {
      if (!commandPaletteEl) return;
      commandPaletteEl.innerHTML = `
        <div class="su-command-title">Command Palette</div>
        ${commandItems.map((item, idx) => `
          <div class="su-command-item ${idx === commandIndex ? "su-command-active" : ""}" data-index="${idx}">
            ${item.label}
          </div>
        `).join("")}
      `;
      commandPaletteEl.querySelectorAll(".su-command-item").forEach((el) => {
        el.addEventListener("click", () => {
          const idx = parseInt(el.getAttribute("data-index") || "0", 10);
          commandItems[idx]?.action?.();
          closeCommandPalette();
        });
      });
    }

    function openCommandPalette(visible) {
      ensureCommandPalette();
      const { isAuthorProfile } = scanResults();
      const row = (keyboardSelectedIndex != null && visible[keyboardSelectedIndex]) ? visible[keyboardSelectedIndex] : visible[0];
      commandItems = [
        {
          label: "Hide venue",
          action: async () => {
            if (!row) return;
            try {
              const paper = isAuthorProfile ? getCachedAuthorPaper(row) : extractPaperFromResult(row);
              const venue = normalizeVenueName(paper.venue || "");
              if (venue) await addHiddenVenue(venue);
              await refreshState(state);
              await window.suProcessAll?.();
            } catch {}
          }
        },
        {
          label: "Toggle filters",
          action: () => {
            const bar = document.getElementById("su-within-results-filter");
            const toggle = bar?.querySelector(".su-filter-toggle");
            toggle?.click();
          }
        },
        {
          label: "Export Markdown",
          action: async () => {
            if (!row) return;
            try {
              const markdown = buildMarkdownFromRow(row, isAuthorProfile);
              await navigator.clipboard.writeText(markdown);
            } catch {}
          }
        },
        {
          label: "Clear queue",
          action: async () => {
            try {
              await clearReadingQueue();
              await ensureReadingQueueSidebar();
            } catch {}
          }
        }
      ];
      commandIndex = 0;
      renderCommandPalette();
      paletteOpen = true;
      commandBackdropEl.classList.add("su-command-visible");
      commandPaletteEl.classList.add("su-command-visible");
    }

    // ——— Funding tag tooltip (hover on search result; snippet-only) ———
    let fundingTooltipEl = document.getElementById("su-funding-tooltip");
    if (!fundingTooltipEl) {
      fundingTooltipEl = document.createElement("div");
      fundingTooltipEl.id = "su-funding-tooltip";
      fundingTooltipEl.className = "su-funding-tooltip";
      fundingTooltipEl.setAttribute("aria-hidden", "true");
      document.body.appendChild(fundingTooltipEl);
    }

    document.addEventListener("mouseover", (e) => {
      const row = e.target?.closest?.(".gs_r");
      if (!row) return;
      const st = window.suState?.settings;
      if (!st?.showFundingTag) return;
      const snippetEl = row.querySelector(".gs_rs");
      if (!snippetEl) return; // author profile or no snippet
      const snippetText = text(snippetEl);
      const funding = detectFunding(snippetText);
      if (!funding) {
        fundingTooltipEl.classList.remove("su-funding-tooltip-visible");
        return;
      }
      const typeClass =
        funding.type === "government"
          ? "su-funding-gov"
          : funding.type === "industry"
            ? "su-funding-industry"
            : "su-funding-unknown";
      fundingTooltipEl.className = `su-funding-tooltip ${typeClass}`;
      fundingTooltipEl.innerHTML = `<span class="su-funding-label">${escapeHtml(funding.label)}</span><span class="su-funding-excerpt">${escapeHtml(funding.excerpt)}</span>`;
      const rect = row.getBoundingClientRect();
      fundingTooltipEl.style.left = `${rect.left}px`;
      fundingTooltipEl.style.top = `${rect.bottom + 4}px`;
      fundingTooltipEl.classList.add("su-funding-tooltip-visible");
      requestAnimationFrame(() => clampFixedToViewport(fundingTooltipEl, 8));
    });

    document.addEventListener("mouseout", (e) => {
      const row = e.target?.closest?.(".gs_r");
      const related = e.relatedTarget;
      if (!row) return;
      if (related && (row.contains(related) || fundingTooltipEl.contains(related))) return;
      fundingTooltipEl.classList.remove("su-funding-tooltip-visible");
    });

    // ——— Hover-summary: fetch landing page and show abstract/description in tooltip ———
    const snippetCache = new Map();
    const SNIPPET_HOVER_DELAY_MS = 400;
    const SNIPPET_MAX_LEN = 520;
    let snippetHoverTimer = null;
    let snippetHoverRow = null;

    function parseAbstractFromHtml(html) {
      if (!html || typeof html !== "string") return null;
      try {
        const doc = new DOMParser().parseFromString(html, "text/html");
        const meta = (name, attr = "name") => doc.querySelector(`meta[${attr}="${name}"]`)?.getAttribute?.("content")?.trim();
        const citation = meta("citation_abstract");
        if (citation) return citation;
        const desc = meta("description");
        if (desc) return desc;
        const og = meta("og:description", "property");
        if (og) return og;
        const abstractEl = doc.querySelector("[id*='abstract' i], [class*='abstract' i]");
        if (abstractEl) {
          const t = (abstractEl.textContent || "").trim().replace(/\s+/g, " ");
          if (t.length > 50) return t;
        }
        return null;
      } catch {
        return null;
      }
    }

    let snippetTooltipEl = document.getElementById("su-snippet-tooltip");
    if (!snippetTooltipEl) {
      snippetTooltipEl = document.createElement("div");
      snippetTooltipEl.id = "su-snippet-tooltip";
      snippetTooltipEl.className = "su-snippet-tooltip";
      snippetTooltipEl.setAttribute("aria-hidden", "true");
      document.body.appendChild(snippetTooltipEl);
    }

    document.addEventListener("mouseover", (e) => {
      const row = e.target?.closest?.(".gs_r");
      if (!row) return;
      if (!window.suState?.settings?.showHoverSummary) return;
      const titleLink = row.querySelector(".gs_rt a");
      let url = titleLink?.href || "";
      if (!url) return;
      try {
        const u = new URL(url);
        if (isScholarHostname(u.hostname)) {
          const realUrl = u.searchParams.get("url") || u.searchParams.get("q") || "";
          if (!realUrl) return;
          try {
            url = decodeURIComponent(realUrl);
          } catch {
            url = realUrl;
          }
        }
      } catch {
        return;
      }
      if (snippetHoverTimer) clearTimeout(snippetHoverTimer);
      snippetHoverRow = row;
      snippetHoverTimer = setTimeout(async () => {
        snippetHoverTimer = null;
        const cached = snippetCache.get(url);
        let summary = cached ?? null;
        if (summary === undefined) {
          try {
            const res = await chrome.runtime.sendMessage({ action: "fetchSnippet", url });
            if (res?.ok && res.html) summary = parseAbstractFromHtml(res.html);
            else summary = null;
          } catch {
            summary = null;
          }
          snippetCache.set(url, summary);
        }
        if (!summary || snippetHoverRow !== row) return;
        const text = summary.length > SNIPPET_MAX_LEN ? summary.slice(0, SNIPPET_MAX_LEN).trim() + "…" : summary;
        snippetTooltipEl.textContent = text;
        const rect = row.getBoundingClientRect();
        snippetTooltipEl.style.left = `${rect.left}px`;
        snippetTooltipEl.style.top = `${rect.bottom + 4}px`;
        snippetTooltipEl.classList.add("su-snippet-tooltip-visible");
        requestAnimationFrame(() => clampFixedToViewport(snippetTooltipEl, 8));
      }, SNIPPET_HOVER_DELAY_MS);
    });

    document.addEventListener("mouseout", (e) => {
      const row = e.target?.closest?.(".gs_r");
      const related = e.relatedTarget;
      if (row && related && (row.contains(related) || snippetTooltipEl.contains(related))) return;
      if (snippetHoverTimer) {
        clearTimeout(snippetHoverTimer);
        snippetHoverTimer = null;
      }
      snippetHoverRow = null;
      snippetTooltipEl.classList.remove("su-snippet-tooltip-visible");
    });

    // ——— Reading guide (visor): horizontal band that follows the mouse ———
    let visorActive = false;
    let visorRaf = null;

    let visorEl = document.getElementById("su-reading-visor");
    if (!visorEl) {
      visorEl = document.createElement("div");
      visorEl.id = "su-reading-visor";
      visorEl.className = "su-reading-visor";
      visorEl.setAttribute("aria-hidden", "true");
      document.body.appendChild(visorEl);
    }

    // Floating toolbar: theme toggle above reading guide
    function themeLabel(t) {
      return (t === "dark" ? "Dark" : t === "light" ? "Light" : "Auto");
    }
    let toolbarEl = document.getElementById("su-theme-guide-toolbar");
    if (!toolbarEl) {
      toolbarEl = document.createElement("div");
      toolbarEl.id = "su-theme-guide-toolbar";
      toolbarEl.className = "su-theme-guide-toolbar";
      document.body.appendChild(toolbarEl);
    }

    let themeToggleEl = document.getElementById("su-theme-toggle");
    if (!themeToggleEl) {
      themeToggleEl = document.createElement("button");
      themeToggleEl.id = "su-theme-toggle";
      themeToggleEl.className = "su-theme-toggle";
      themeToggleEl.type = "button";
      themeToggleEl.title = "Cycle theme: Auto → Light → Dark";
      themeToggleEl.addEventListener("click", async () => {
        const next = { auto: "light", light: "dark", dark: "auto" }[state.settings.theme] || "auto";
        await setSettings({ theme: next });
        state.settings.theme = next;
        applyTheme(next);
        themeToggleEl.textContent = themeLabel(next);
      });
      toolbarEl.appendChild(themeToggleEl);
    }
    themeToggleEl.textContent = themeLabel(state.settings.theme);

    // Reading guide button removed per request; ensure any existing instance is removed.
    let guideToggleEl = document.getElementById("su-reading-guide-toggle");
    if (guideToggleEl) {
      guideToggleEl.parentNode?.removeChild(guideToggleEl);
      guideToggleEl = null;
    }

    function setVisorActive(active) {
      visorActive = active;
      if (visorActive) {
        visorEl.classList.add("su-reading-visor-active");
        if (guideToggleEl) guideToggleEl.classList.add("su-reading-guide-on");
      } else {
        visorEl.classList.remove("su-reading-visor-active");
        if (guideToggleEl) guideToggleEl.classList.remove("su-reading-guide-on");
      }
    }

    function updateVisorY(clientY) {
      if (!visorEl || !visorActive) return;
      visorEl.style.setProperty("--visor-y", `${clientY}px`);
    }

    const onVisorMouseMove = (e) => {
      if (!visorActive) return;
      if (visorRaf) cancelAnimationFrame(visorRaf);
      visorRaf = requestAnimationFrame(() => {
        updateVisorY(e.clientY);
        visorRaf = null;
      });
    };

    const onVisorKeyDown = (e) => {
      if (e.key === "Escape" && visorActive) {
        setVisorActive(false);
        document.removeEventListener("mousemove", onVisorMouseMove);
        document.removeEventListener("keydown", onVisorKeyDown);
      }
    };

    if (guideToggleEl) {
      guideToggleEl.addEventListener("click", () => {
        if (!state.settings.showReadingGuide) return;
        visorActive = !visorActive;
        setVisorActive(visorActive);
        if (visorActive) {
          document.addEventListener("mousemove", onVisorMouseMove);
          document.addEventListener("keydown", onVisorKeyDown);
          updateVisorY(typeof window.lastMouseY === "number" ? window.lastMouseY : window.innerHeight / 2);
        } else {
          document.removeEventListener("mousemove", onVisorMouseMove);
          document.removeEventListener("keydown", onVisorKeyDown);
        }
      });
    }

    document.addEventListener("mousemove", (e) => { window.lastMouseY = e.clientY; }, { passive: true });

    if (guideToggleEl) {
      guideToggleEl.style.display = state.settings.showReadingGuide ? "" : "none";
    } else if (visorActive) {
      setVisorActive(false);
      document.removeEventListener("mousemove", onVisorMouseMove);
      document.removeEventListener("keydown", onVisorKeyDown);
    }
  }

  function escapeHtml(s) {
    const div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
  }

  run();
})();
