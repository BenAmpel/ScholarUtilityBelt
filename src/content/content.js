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

  // Per-row parse cache to avoid repeated DOM parsing.
  const rowParseCache = new WeakMap();
  function getRowCache(row) {
    let cache = rowParseCache.get(row);
    if (!cache) {
      cache = { fast: null, full: null };
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
      const [eraText, norwegianText, absText] = await Promise.all([
        fetch(base + "era2023.txt").then((r) => (r.ok ? r.text() : "")).catch(() => ""),
        fetch(base + "norwegian_register.csv").then((r) => (r.ok ? r.text() : "")).catch(() => ""),
        fetch(base + "abs2024.csv").then((r) => (r.ok ? r.text() : "")).catch(() => "")
      ]);
      for (const line of (eraText || "").split(/\r?\n/)) {
        const t = line.trim();
        if (!t || t.startsWith("#")) continue;
        const n = normalizeVenueName(t);
        if (n) eraSet.add(n);
      }
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

  // ——— Retraction Watch via Crossref API (no Bloom filter; checks live) ———
  const retractionCheckCache = new Map();

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

    for (const a of links) {
      const rawHref = (a.getAttribute("href") || "").trim();
      const href = unwrapScholarUrl(rawHref);
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

  /** DOM-only: show/hide result rows by state.filters (year range, venue, has PDF, min citations, author). */
  function applyResultFiltersToRows(rows, state) {
    const f = state.filters || {};
    const yearMin = f.yearMin ? parseInt(f.yearMin, 10) : null;
    const yearMax = f.yearMax ? parseInt(f.yearMax, 10) : null;
    const venueKw = (f.venueKeyword || "").trim().toLowerCase();
    const hasPdf = !!f.hasPdf;
    const minCite = f.minCitations ? parseInt(f.minCitations, 10) : null;
    const authorKw = (f.authorMatch || "").trim().toLowerCase();
    const anyActive = yearMin != null || yearMax != null || venueKw.length > 0 || hasPdf || (minCite != null && !isNaN(minCite)) || authorKw.length > 0;

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
      if (minCite != null && !isNaN(minCite)) {
        const cite = getCitationCountFromResult(r);
        if (cite == null || cite < minCite) {
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
    
    tooltip.innerHTML = parts.join("<br>");
    return tooltip;
  }

  function buildMarkdownFromRow(row, isAuthorProfile) {
    const p = isAuthorProfile ? extractPaperFromAuthorProfile(row) : extractPaperFromResult(row);
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

  function renderQuality(container, paper, state, isAuthorProfile = false) {
    // Find the info element - try multiple strategies for robustness
    let info = null;
    if (isAuthorProfile) {
      // For author profile pages, try to find the venue div first
      if (paper._venueDiv) {
        info = paper._venueDiv;
      } else {
        // Fallback: look for the venue div in the title cell
        const titleCell = getCachedElement(container, ".gsc_a_t");
        if (titleCell) {
          const grayDivs = getCachedElements(titleCell, "div.gs_gray");
          if (grayDivs.length >= 2) {
            info = grayDivs[1]; // Second div contains venue info
          } else if (grayDivs.length === 1) {
            info = grayDivs[0];
          }
        }
        // Last resort: use the cited by cell
        if (!info) {
          info = getCachedElement(container, ".gsc_a_c");
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
          return;
        }
      }
    }

    // Remove ALL existing quality badges to prevent duplicates
    const existingBadges = getCachedElements(container, ".su-quality");
    existingBadges.forEach(badge => badge.remove());
    
    // Get quality badges (only if enabled)
    const rawBadges = state.settings.showQualityBadges
      ? qualityBadgesForVenue(paper.venue, state.qIndex)
      : [];
    const allowedKinds = state.settings.qualityBadgeKinds;
    const badges = allowedKinds && typeof allowedKinds === "object"
      ? rawBadges.filter((b) => allowedKinds[b.kind] !== false)
      : rawBadges;
    const citeUrl = !isAuthorProfile && paper ? getCiteUrlForResult(container, paper) : null;
    const showLookupButton = state.settings.showQualityBadges && !badges.length && citeUrl;
    
    if (!badges.length && !showLookupButton) {
      return;
    }
    
    // Create new badge container
    const root = document.createElement("div");
    root.className = "su-quality";
    root.setAttribute("data-su-quality", "true"); // Mark as quality badge

    // Short native tooltip for each badge kind (shown on hover)
    const badgeKindTitles = {
      quartile: "SCImago Journal Rank quartile: Q1 = top 25%, Q2 = next 25%, etc.",
      abdc: "ABDC Journal Quality List rank (A*, A, B, C).",
      jcr: "Clarivate JCR (Journal Citation Reports) impact quartile or indicator.",
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
        case "jcr": return /^(JIF|JCI|AIS|5Y)\s+Q[1-4]$/i.test(t);
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
        span.className = `su-badge su-${b.kind}`;
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
      // On author profile pages, .gsc_a_c is a table cell (<td>)
      // Insert the badges div after the cell (as a sibling, not inside the cell)
      if (info) {
        info.insertAdjacentElement("afterend", root);
      } else {
        // Fallback: append to container
        container.appendChild(root);
      }
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
        const paper = extractPaperFromAuthorProfile(tr);
        const citations = extractCitationCount(tr);
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
    const citations = isAuthorProfile
      ? extractCitationCount(container)
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
    anchor.appendChild(document.createTextNode(" "));
    anchor.appendChild(vel);
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
    const cite = isAuthorProfile ? extractCitationCount(container) : getCitationCountFromResult(container);
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

    const { code, data } = detectArtifacts(container);
    if (!code && !data) return;

    const wrap = document.createElement("span");
    wrap.className = "su-artifact-badge";
    wrap.setAttribute("aria-label", [code && "Has code/link", data && "Has data/supplementary"].filter(Boolean).join("; ") || "Artifacts");
    const parts = [];
    if (code) parts.push("💻");
    if (data) parts.push("📊");
    wrap.textContent = " " + parts.join(" ");
    wrap.title = [code && "Code/repository (e.g. GitHub) detected", data && "Data/supplementary (e.g. Zenodo, OSF) detected"].filter(Boolean).join(" · ") || "Artifacts detected";
    titleEl.appendChild(wrap);
  }

  /**
   * Contribution Signal Score (CSS): composite 0–100 estimate of intellectual contribution.
   * Formula: CSS = 40%·V + 40%·W + 20%·N (reference entropy E omitted—no data).
   * V = citation velocity (log-normalized, cap 20/yr → 1). W = venue tier weight [0,1]. N = artifact novelty (0, 0.1, or 0.2).
   */
  function computeCSS(container, paper, state, isAuthorProfile) {
    const citations = isAuthorProfile
      ? extractCitationCount(container)
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
    if (isAuthorProfile || !state.settings.showReadingLoadEstimator) return;

    const pageCount = state.readingLoadPageCounts && state.readingLoadPageCounts[paper.key];
    const label = computeReadingLoadLabel(paper, pageCount);
    const slug = label.replace(/\s+/g, "-");
    const badge = document.createElement("span");
    badge.className = `su-reading-load-badge su-reading-load-${slug} su-btn`;
    badge.setAttribute("aria-label", `Reading effort: ${label}`);
    badge.textContent = label;
    const tipParts = [
      `Reading load: ${label}. `,
      pageCount != null
        ? `Page count ${pageCount} was read from the PDF. `
        : "Page count unknown; estimate is from venue type. Click the PDF button and open the file to store page count from metadata. ",
      "Skim = ≤5 pages or preprint; Read = 6–15 pages or conference; Deep read = 16+ pages or journal."
    ];
    const tipText = tipParts.join(" ");
    attachFloatingTooltip(badge, tipText);

    const anchor = isAuthorProfile
      ? container.querySelector(".gsc_a_c")
      : container.querySelector(".gs_fl");
    if (!anchor) return;
    anchor.appendChild(document.createTextNode(" "));
    anchor.appendChild(badge);
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
      hiddenAuthors: [],
      readingLoadPageCounts: {}
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
    const readingLoadPageCounts = storageData.readingLoadPageCounts && typeof storageData.readingLoadPageCounts === "object" ? storageData.readingLoadPageCounts : {};
    
    state.saved = saved;
    state.settings = settings;
    state.quartilesIndex = quartiles.index;
    state.quartilesMeta = quartiles.meta;
    state.jcrIndex = jcr.index;
    state.jcrMeta = jcr.meta;
    
    // Create hash of settings that affect quality index compilation
    const settingsHash = JSON.stringify({
      qualityFt50List: settings.qualityFt50List || "",
      qualityUtd24List: settings.qualityUtd24List || "",
      qualityAbdcRanks: settings.qualityAbdcRanks || "",
      qualityQuartiles: settings.qualityQuartiles || "",
      qualityCoreRanks: settings.qualityCoreRanks || "",
      qualityCcfRanks: settings.qualityCcfRanks || "",
      quartilesIndexKeys: Object.keys(quartiles.index || {}).length,
      jcrIndexKeys: Object.keys(jcr.index || {}).length
    });
    
    // Only recompute quality index if settings changed or cache doesn't exist
    if (!state.qIndexCache || state.qIndexCache.settingsHash !== settingsHash) {
      // Load era/norwegian/h5 data (these are cached at window level, so fast on subsequent calls)
      const [eraNorwegian, h5Index] = await Promise.all([loadEraAndNorwegian(), loadH5Index()]);
      state.qIndex = compileQualityIndex(state.settings, {
        quartilesIndex: state.quartilesIndex,
        jcrIndex: state.jcrIndex,
        eraSet: eraNorwegian.eraSet,
        absIndex: eraNorwegian.absIndex,
        norwegianMap: eraNorwegian.norwegianMap,
        h5Index
      });
      // Cache the compiled index
      state.qIndexCache = {
        qIndex: state.qIndex,
        settingsHash
      };
    } else {
      // Use cached quality index
      state.qIndex = state.qIndexCache.qIndex;
    }
    state.hiddenPapers = new Set(hiddenPapers);
    state.hiddenVenues = new Set(hiddenVenues);
    state.hiddenAuthors = new Set(hiddenAuthors);
    state.readingLoadPageCounts = readingLoadPageCounts;
    applyTheme(settings.theme);
    const themeBtn = document.getElementById("su-theme-toggle");
    if (themeBtn) themeBtn.textContent = (settings.theme === "dark" ? "Dark" : settings.theme === "light" ? "Light" : "Auto");
    state.keywordHighlights = csvToTags(settings.keywordHighlightsCsv);
    state.keywordHighlightRegexes = (state.keywordHighlights || []).map((kw) => {
      const escaped = String(kw || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return new RegExp(`(${escaped})`, "gi");
    });
    state.keywordHighlightKey = `${settings.keywordHighlightsCsv || ""}::${settings.showSnippetCueEmphasis ? "1" : "0"}`;
    state.renderEpoch = (state.renderEpoch || 0) + 1;
  }

  function applyTheme(theme) {
    const resolved = theme === "dark" ? "dark" : theme === "light" ? "light" : (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    if (document.body) document.body.setAttribute("data-su-theme", resolved);
    if (document.documentElement) document.documentElement.setAttribute("data-su-theme", resolved);
  }

  async function ensureResultUI(container, state, isAuthorProfile = false, opts = {}) {
    const inViewport = opts.inViewport !== false;
    const renderEpoch = String(state.renderEpoch || 0);
    const isDirty = container.dataset.suDirty === "1";
    let fastPaper = isAuthorProfile
      ? extractPaperFromAuthorProfile(container)
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

    const isNewSinceVisit = !!state.settings.showNewSinceLastVisit && state.newSinceLastVisitKeys?.has(paper.key);
    if (isNewSinceVisit) container.classList.add("su-new-since-visit");
    else container.classList.remove("su-new-since-visit");
    let newBadge = getCachedElement(container, ".su-new-badge");
    const newBadgeTooltip = "You have never seen this paper on this page before (new since your last visit)";
    if (isNewSinceVisit) {
      if (!newBadge) {
        newBadge = document.createElement("span");
        newBadge.className = "su-new-badge";
        newBadge.textContent = "New";
        newBadge.title = newBadgeTooltip;
        newBadge.setAttribute("aria-label", newBadgeTooltip);
        const titleWrap = isAuthorProfile ? getCachedElement(container, ".gsc_a_t") : getCachedElement(container, ".gs_rt");
        if (titleWrap) titleWrap.appendChild(newBadge);
        else container.appendChild(newBadge);
      } else {
        newBadge.title = newBadgeTooltip;
        newBadge.setAttribute("aria-label", newBadgeTooltip);
      }
    } else if (newBadge) newBadge.remove();

    highlightKeywords(container, state, isAuthorProfile);

    const isSaved = !!state.saved[paper.key];
    if (isSaved && state.settings.highlightSaved) container.classList.add("su-saved");
    else container.classList.remove("su-saved");

    renderQuality(container, paper, state, isAuthorProfile);
    const isDetailed = state.settings.viewMode !== "minimal";
    if (isDetailed) {
      renderVelocity(container, paper, isAuthorProfile);
      renderSkimmabilityStrip(container, paper, state, isAuthorProfile);
      renderArtifactBadges(container, state);
      renderReadingLoadBadge(container, paper, state, isAuthorProfile);
    } else {
      container.querySelectorAll(".su-velocity, .su-skimmability-strip, .su-artifact-badge, .su-reading-load-badge").forEach((el) => el.remove());
    }
    renderAuthorshipHeatmap(container, state, isAuthorProfile);

    if (state.settings.showCitationSpike) {
      const citations = isAuthorProfile ? extractCitationCount(container) : getCitationCountFromResult(container);
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

    if (!isAuthorProfile && state.settings.showRetractionWatch) {
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
            ? extractPaperFromAuthorProfile(container)
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
          const p = isAuthorProfile ? extractPaperFromAuthorProfile(container) : extractPaperFromResult(container);
          const best = getBestPdfUrl(container);
          if (best) {
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
            ? extractPaperFromAuthorProfile(container)
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
            ? extractPaperFromAuthorProfile(container)
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
            ? extractPaperFromAuthorProfile(container)
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

  async function loadAllPublicationsRecursively(maxAttempts = 50) {
    // Load all publications by clicking "Show more" until it's disabled
    for (let i = 0; i < maxAttempts; i++) {
      const hasMore = await loadAllPublications();
      if (!hasMore) {
        break; // All publications loaded
      }
      // Small delay between clicks to avoid overwhelming the page
      await new Promise(resolve => setTimeout(resolve, 500));
    }
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
          const paper = extractPaperFromAuthorProfile(r);
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
      qualityCounts: { q1: 0, q2: 0, q3: 0, q4: 0, a: 0, utd24: 0, ft50: 0, abs4star: 0, core: { "A*": 0, "A": 0, "B": 0, "C": 0 }, era: 0 },
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
    const topicTokenCounts = new Map();
    
    for (const result of results) {
      // Extract paper info
      const paper = extractPaperFromAuthorProfile(result);
      if (!paper) continue;
      addTitleTokens(topicTokenCounts, paper.title);
      
      stats.totalPublications++;
      
      // Extract citation count
      const citations = extractCitationCount(result);
      stats.totalCitations += citations;
      if (citations > stats.mostCited) {
        stats.mostCited = citations;
      }
      
      // Parse authors
      const authors = parseAuthors(paper.authorsVenue.split(" - ")[0] || paper.authorsVenue);
      const coAuthorSet = new Set();
      const coAuthors = [];
      const authorRole = getAuthorRole(paper, authorVariations);
      for (const a of authors) {
        if (isAuthorVariation(a, authorVariations)) continue;
        const normalized = normalizeAuthorName(a);
        if (!normalized || coAuthorSet.has(normalized)) continue;
        coAuthorSet.add(normalized);
        coAuthors.push({ name: a, normalized });
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
        coAuthors.forEach(({ name, normalized }) => {
          const existing = stats.coAuthors.get(normalized);
          if (existing) {
            existing.count++;
            existing.citations = (existing.citations || 0) + citeVal;
            if (!Array.isArray(existing.citeList)) existing.citeList = [];
            existing.citeList.push(citeVal);
            if (!existing.roleCounts) existing.roleCounts = { solo: 0, first: 0, middle: 0, last: 0 };
            if (existing.roleCounts[authorRole] != null) existing.roleCounts[authorRole] += 1;
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
              name: name,
              count: 1,
              variations: [name],
              citations: citeVal,
              citeList: [citeVal],
              roleCounts: { solo: authorRole === "solo" ? 1 : 0, first: authorRole === "first" ? 1 : 0, middle: authorRole === "middle" ? 1 : 0, last: authorRole === "last" ? 1 : 0 }
            });
          }
        });
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
      
      papers.push({ paper, citations });
      stats.__citations.push(citations);
      stats.__authorCounts.push(Math.max(1, authors.length));
      if (paper.year != null && citations != null && citations >= 0) {
        stats.__yearCitations.push({ year: paper.year, citations });
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
        name: bestName,
        count: coAuthor.count,
        citations: coAuthor.citations || 0,
        hIndex,
        roleShare
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

    // h-index: largest h such that h papers have >= h citations each
    const sortedCitations = (stats.__citations || []).slice().sort((a, b) => b - a);
    let hIndex = 0;
    for (let i = 0; i < sortedCitations.length; i++) {
      if (sortedCitations[i] >= i + 1) hIndex = i + 1;
      else break;
    }
    stats.hIndex = hIndex;

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
    
    return stats;
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
      statsContainer.innerHTML = '<span class="su-stat-loading">Loading all publications...</span>';
      return;
    }

    if (!stats || !stats.qualityCounts) return;

    try {
    // Build stats HTML with badge-style boxes
    const parts = [];
    const qc = stats.qualityCounts;

    // Quality badges (existing) - make them clickable for filtering
    const activeFilter = window.suActiveFilter || null;
    if ((qc.q1 || 0) > 0) {
      const isActive = activeFilter === "q1";
      parts.push(`<span class="su-stat-badge su-badge su-quartile ${isActive ? 'su-filter-active' : ''}" data-filter="q1" style="cursor: pointer;" title="Click to filter by Q1 papers"><span class="su-stat-label">Q1:</span> <strong>${qc.q1}</strong></span>`);
    }
    if ((qc.a || 0) > 0) {
      const isActive = activeFilter === "a";
      parts.push(`<span class="su-stat-badge su-badge su-abdc ${isActive ? 'su-filter-active' : ''}" data-filter="a" style="cursor: pointer;" title="Click to filter by A-ranked papers"><span class="su-stat-label">ABDC:</span> <strong>${qc.a}</strong></span>`);
    }
    if ((qc.ft50 || 0) > 0) {
      const isActive = activeFilter === "ft50";
      parts.push(`<span class="su-stat-badge su-badge su-ft50 ${isActive ? 'su-filter-active' : ''}" data-filter="ft50" style="cursor: pointer;" title="Click to filter by FT50 papers"><span class="su-stat-label">FT50:</span> <strong>${qc.ft50}</strong></span>`);
    }
    if ((qc.utd24 || 0) > 0) {
      const isActive = activeFilter === "utd24";
      parts.push(`<span class="su-stat-badge su-badge su-utd24 ${isActive ? 'su-filter-active' : ''}" data-filter="utd24" style="cursor: pointer;" title="Click to filter by UTD24 papers"><span class="su-stat-label">UTD24:</span> <strong>${qc.utd24}</strong></span>`);
    }
    if ((qc.abs4star || 0) > 0) {
      const isActive = activeFilter === "abs4star";
      parts.push(`<span class="su-stat-badge su-badge su-abs4star ${isActive ? 'su-filter-active' : ''}" data-filter="abs4star" style="cursor: pointer;" title="Click to filter by ABS 4* papers"><span class="su-stat-label">ABS 4*:</span> <strong>${qc.abs4star}</strong></span>`);
    }
    
    // Add "Clear filter" button if a filter is active
    if (activeFilter) {
      parts.push(`<span class="su-stat-badge su-badge" style="cursor: pointer; opacity: 0.7;" data-filter="clear" title="Click to clear filter">Clear filter</span>`);
    }
    // Sort by citations/year toggle (author pages only)
    const sortByVelocity = !!window.suAuthorSortByVelocity;
    if (parts.length > 0) parts.push('<span class="su-stat-separator">|</span>');
    parts.push(`<span id="su-sort-toggle" class="su-stat-badge su-badge" data-sort-toggle="1" style="cursor: pointer;" title="Toggle sort by citations per year">${sortByVelocity ? "Sort: citations/yr ✓" : "Sort by citations/yr"}</span>`);
    // Citation view by author position
    const posFilter = window.suAuthorPositionFilter || "all";
    const posOptions = [
      { value: "all", label: "All" },
      { value: "first", label: "1st only" },
      { value: "last", label: "Last only" },
      { value: "middle", label: "Middle only" },
      { value: "first+last", label: "1st+Last" },
      { value: "first+middle+last", label: "1st+Mid+Last" }
    ];
    parts.push('<span class="su-stat-separator">|</span>');
    parts.push('<span class="su-stat-item"><span class="su-stat-label">View:</span></span>');
    for (const opt of posOptions) {
      const active = posFilter === opt.value ? " su-filter-active" : "";
      parts.push(`<span class="su-stat-badge su-badge su-position-filter${active}" data-position-filter="${opt.value}" style="cursor: pointer;" title="Show stats for ${opt.label}">${opt.label}</span>`);
    }

    // Add separator if we have quality badges and other stats
    if (parts.length > 0 && (stats.totalPublications || 0) > 0) {
      parts.push('<span class="su-stat-separator">|</span>');
    }
    
    // Citation and publication stats
    if ((stats.totalPublications || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">Papers:</span> <strong>${stats.totalPublications}</strong></span>`);
    }
    if ((stats.totalCitations || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">Citations:</span> <strong>${stats.totalCitations}</strong></span>`);
    }
    if ((stats.firstAuthor || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">1st Author:</span> <strong>${stats.firstAuthor}</strong></span>`);
    }
    if ((stats.firstAuthorCitations || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">1st Author Cites:</span> <strong>${stats.firstAuthorCitations}</strong></span>`);
    }
    if ((stats.soloAuthored || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">Solo:</span> <strong>${stats.soloAuthored}</strong></span>`);
    }
    if ((stats.soloCitations || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">Solo Cites:</span> <strong>${stats.soloCitations}</strong></span>`);
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

    // Co-author stats
    if ((stats.uniqueCoAuthors || 0) > 0) {
      parts.push(`<span class="su-stat-item"><span class="su-stat-label">Co-authors:</span> <strong>${stats.uniqueCoAuthors}</strong></span>`);
    }
    
    // Collaboration shape: % solo / % first / % middle / % last
    if ((stats.totalPublications || 0) > 0) {
      const total = stats.totalPublications || 1;
      const soloPct = Math.round(((stats.soloAuthored || 0) / total) * 100);
      const firstPct = Math.round(((stats.firstAuthor || 0) / total) * 100);
      let middlePct = Math.round(((stats.middleAuthor || 0) / total) * 100);
      let lastPct = Math.round(((stats.lastAuthor || 0) / total) * 100);
      const sumPct = soloPct + firstPct + middlePct + lastPct;
      if (sumPct !== 100) lastPct = Math.max(0, lastPct + (100 - sumPct));
      parts.push(`
        <span class="su-stat-item su-collab-shape" title="Collaboration shape: % solo, % first-author, % middle-author">
          <span class="su-stat-label">Collab:</span>
          <span class="su-collab-legend">${soloPct}% solo · ${firstPct}% first · ${middlePct}% middle · ${lastPct}% last</span>
        </span>
      `);
    }

    if (stats.authorshipDrift) {
      parts.push(`
        <span class="su-stat-item" title="Authorship drift over time (early vs late papers). Indicates shift from first-author dominance toward last/middle collaboration (or the reverse). ${stats.authorshipDrift.detail}">
          <span class="su-stat-label">Drift:</span>
          <strong>${stats.authorshipDrift.label}</strong>
        </span>
      `);
    }

    // Close the stats row and add co-author table
    if (parts.length > 0) {
      statsContainer.style.display = "block";
      const hasFilterBadges = (qc.q1 || 0) > 0 || (qc.a || 0) > 0 || (qc.utd24 || 0) > 0 || (qc.ft50 || 0) > 0 || (qc.abs4star || 0) > 0;
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
      const topCoAuthors = sortedCo.slice(0, 3);
      const sortArrow = (k) => (sortKey === k ? (sortDir === "asc" ? " ▲" : " ▼") : "");
      const topics = Array.isArray(stats.topTitleTokens) ? stats.topTitleTokens.slice(0, 10) : [];
      const maxTopicCount = topics.length ? Math.max(...topics.map((t) => Number(t.count) || 1)) : 1;
      const activeTopicList = normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter);
      const activeTopicSet = new Set(activeTopicList);
      const topicHtml = topics.length
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

      statsContainer.innerHTML = `
        ${hasFilterBadges ? '<div class="su-filter-hint">Click a badge to filter papers. If it doesn’t respond, try clicking again.</div>' : ''}
        <div class="su-stats-row">${parts.join("")}</div>
        ${topicHtml}
        <div class="su-compare-authors-row"><button type="button" class="su-compare-authors-btn" id="su-compare-authors-btn">Compare authors</button>${metricsDropdownHtml}</div>
        <div class="su-coauthors-wrap">
          ${topCoAuthors.length > 0 ? `
            <div class="su-coauthors-table">
              <div class="su-coauthors-header">Top Collaborators</div>
              <table class="su-coauthors-table-inner">
              <colgroup>
                <col class="su-co-col-rank" />
                <col class="su-co-col-author" />
                <col class="su-co-col-papers" />
                <col class="su-co-col-cites" />
                <col class="su-co-col-h" />
                <col class="su-co-col-dots" />
              </colgroup>
              <thead>
                <tr>
                  <th class="su-coauthor-rank"></th>
                  <th class="su-coauthor-name" data-coauthor-sort="name">Co-Author${sortArrow("name")}</th>
                  <th class="su-coauthor-metric" data-coauthor-sort="count">Co-Papers${sortArrow("count")}</th>
                  <th class="su-coauthor-metric" data-coauthor-sort="citations">Co-Cites${sortArrow("citations")}</th>
                  <th class="su-coauthor-metric" data-coauthor-sort="hIndex">Co-h${sortArrow("hIndex")}</th>
                  <th class="su-coauthor-metric">Co-Role</th>
                </tr>
              </thead>
              <tbody>
                ${topCoAuthors.map((coAuthor, idx) => `
                  <tr>
                    <td class="su-coauthor-rank">${idx + 1}.</td>
                    <td class="su-coauthor-name">${String(coAuthor?.name ?? "")}</td>
                    <td class="su-coauthor-count"><strong>${Number(coAuthor?.count) || 0}</strong></td>
                    <td class="su-coauthor-cites"><strong>${Number(coAuthor?.citations) || 0}</strong></td>
                    <td class="su-coauthor-h"><strong>${Number(coAuthor?.hIndex) || 0}</strong></td>
                    <td class="su-coauthor-dots">
                      ${(() => {
                        const rs = coAuthor.roleShare || {};
                        const dots = [
                          { cls: "su-role-dot-solo", label: "Solo", val: rs.solo || 0 },
                          { cls: "su-role-dot-first", label: "First", val: rs.first || 0 },
                          { cls: "su-role-dot-middle", label: "Middle", val: rs.middle || 0 },
                          { cls: "su-role-dot-last", label: "Last", val: rs.last || 0 }
                        ];
                        return `<div class="su-role-dots">${dots.map(d => {
                          const alpha = Math.max(0.2, Math.min(1, 0.2 + d.val * 0.8));
                          const title = `${d.label}: ${Math.round(d.val * 100)}%`;
                          return `<span class="su-role-dot ${d.cls}" style="opacity:${alpha.toFixed(2)}" title="${title}"></span>`;
                        }).join("")}</div>`;
                      })()}
                    </td>
                  </tr>
                `).join("")}
              </tbody>
            </table>
            </div>
          ` : ""}
          ${topVenueHtml}
        </div>
      `;
      
    } else {
      statsContainer.style.display = "none";
    }
    } catch (_) {
      statsContainer.style.display = "none";
    }
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
          <label class="su-author-compare-field">Author B profile URL: <input type="url" id="su-compare-url-b" placeholder="https://scholar.google.com/citations?user=..." class="su-author-compare-input" /></label>
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
        if (!url || !url.startsWith("https://scholar.google.com/")) {
          const resultEl = overlay.querySelector("#su-compare-result");
          if (resultEl) resultEl.innerHTML = '<p class="su-compare-error">Please enter a Google Scholar author profile URL (https://scholar.google.com/...).</p>';
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
    
    switch (filter) {
      case "q1":
        // Check for Q1 quartile badge
        return badges.some(b => {
          if (b.kind === "quartile") {
            const q = b.text.toUpperCase();
            return q === "Q1";
          }
          return false;
        });
      case "a":
        // Check for ABDC A* or A rank
        return badges.some(b => {
          if (b.kind === "abdc") {
            const rank = b.text.replace(/^ABDC\s+/i, "").trim().toUpperCase();
            return rank === "A*" || rank === "A";
          }
          return false;
        });
      case "utd24":
        return badges.some(b => b.kind === "utd24");
      case "ft50":
        return badges.some(b => b.kind === "ft50");
      case "abs4star":
        return badges.some(b => b.kind === "abs" && b.text === "ABS 4*");
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
    let bar = document.getElementById("su-within-results-filter");
    if (bar) {
      bar.style.display = "";
      return;
    }
    state.filters = state.filters || { yearMin: "", yearMax: "", venueKeyword: "", hasPdf: false, minCitations: "", authorMatch: "" };
    const f = state.filters;

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

    const readFilterValues = () => ({
      yearMin: yearMinEl.value.trim(),
      yearMax: yearMaxEl.value.trim(),
      venueKeyword: venueEl.value.trim(),
      hasPdf: pdfCheck.checked,
      minCitations: citeEl.value.trim(),
      authorMatch: authorEl.value.trim()
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
      },
      {
        id: "pdfonly",
        label: "PDF only",
        patch: { hasPdf: true },
        isActive: (v) => !!v.hasPdf
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
        state.filters.minCitations = next.minCitations;
        state.filters.authorMatch = next.authorMatch;
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

    [yearMinEl, yearMaxEl, venueEl, pdfCheck, citeEl, authorEl].forEach((el) => {
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
        state.filters.minCitations = "";
        setFilterInputs({
          yearMin: "",
          yearMax: "",
          venueKeyword: "",
          hasPdf: false,
          minCitations: "",
          authorMatch: ""
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
        const n = [g.yearMin, g.yearMax, g.venueKeyword, g.hasPdf, g.minCitations, g.authorMatch].filter(
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
      for (const [key, value] of Object.entries(def.patch || {})) {
        if (active) {
          next[key] = key === "hasPdf" ? false : "";
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
      row.appendChild(venueEl);
      row.appendChild(pdfLabel);
      row.appendChild(citeEl);
      row.appendChild(authorEl);
      row.appendChild(sortSelect);
      row.appendChild(clearBtn);
      row.appendChild(loadMoreBtn);
      row.appendChild(openPdfBtn);
      if (summarizeBtn) row.appendChild(summarizeBtn);
    } catch (_) {
      // If any append fails, still try to show the bar with what we have
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

    const body = document.createElement("div");
    body.className = "su-within-results-filter-body";
    body.appendChild(quickRow);
    body.appendChild(row);

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
        minCitations: state.filters.minCitations || "",
        authorMatch: state.filters.authorMatch || ""
      }));
    } catch (_) {}
  }

  async function run() {
    const state = { saved: {}, settings: null, qIndex: null, venueCache: new Map(), allPublicationsLoaded: false, activeFilter: null, filters: {} };
    await refreshState(state);
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
        minCitations: (stored && stored.minCitations) || "",
        authorMatch: (stored && stored.authorMatch) || ""
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
    }

    let scheduled = false;
    let processing = false;
    let needsRerun = false;
    let forceFull = true;
    const pendingDirtyRows = new Set();
    const UI_BATCH_SIZE = 10;
    const VIEWPORT_MARGIN = 200;
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
      if (isAuthorProfile && (window.suActiveFilter || (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") || authorTopicFilters.length || authorVenueFilter)) {
        for (const r of results) {
          try {
            const paper = extractPaperFromAuthorProfile(r);
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
              let paper = isAuthorProfile ? extractPaperFromAuthorProfile(r) : getCachedPaperFast(r);
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
            paper = isAuthorProfile ? extractPaperFromAuthorProfile(r) : getCachedPaperFast(r);
          } catch {
            continue;
          }
          const citations = isAuthorProfile ? extractCitationCount(r) : getCitationCountFromResult(r);
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
            paper = isAuthorProfile ? extractPaperFromAuthorProfile(r) : getCachedPaperFast(r);
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
        if (!state.allPublicationsLoaded) {
          renderAuthorStats({ qualityCounts: { q1: 0, a: 0, utd24: 0, ft50: 0, abs4star: 0 }, totalPublications: 0, totalCitations: 0, avgCitations: 0, recentActivity: { last5Years: 0 } }, true);
          const runLoadAll = () => {
            loadAllPublicationsRecursively().then(async () => {
              state.allPublicationsLoaded = true;
              const allResults = scanResults().results;
              const authorTopicFilters2 = normalizeTopicFilters(window.suAuthorTitleFilters || window.suAuthorTitleFilter);
              const authorVenueFilter2 = String(window.suAuthorVenueFilter || "").trim().toLowerCase();
              if (window.suActiveFilter || (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") || authorTopicFilters2.length || authorVenueFilter2) {
                for (const r of allResults) {
                  try {
                    const paper = extractPaperFromAuthorProfile(r);
                    let show = true;
                    if (window.suActiveFilter) show = show && paperMatchesFilter(paper, window.suActiveFilter, state);
                    if (window.suAuthorPositionFilter && window.suAuthorPositionFilter !== "all") show = show && paperMatchesPositionFilter(paper, window.suAuthorPositionFilter, state.authorVariations);
                    if (authorTopicFilters2.length) show = show && paperMatchesTitleTokens(paper, authorTopicFilters2);
                    if (authorVenueFilter2) {
                      const venueKey = normalizeVenueKey(paper.venue || "");
                      show = show && venueKey === authorVenueFilter2;
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
          const paper = isAuthorProfile ? extractPaperFromAuthorProfile(row) : extractPaperFromResult(row);
          upsertPaper({ ...paper, sourcePageUrl: window.location.href, savedAt: paper.savedAt || new Date().toISOString() });
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
              const paper = isAuthorProfile ? extractPaperFromAuthorProfile(row) : extractPaperFromResult(row);
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
        if (u.hostname === "scholar.google.com" || u.hostname?.endsWith(".scholar.google.com")) {
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

    let guideToggleEl = document.getElementById("su-reading-guide-toggle");
    if (!guideToggleEl) {
      guideToggleEl = document.createElement("button");
      guideToggleEl.id = "su-reading-guide-toggle";
      guideToggleEl.className = "su-reading-guide-toggle";
      guideToggleEl.type = "button";
      guideToggleEl.textContent = "Guide";
      guideToggleEl.title = "Toggle reading guide: dims the page except a band at your cursor (Esc to close)";
      toolbarEl.appendChild(guideToggleEl);
    } else if (guideToggleEl.parentNode !== toolbarEl) {
      guideToggleEl.parentNode?.removeChild(guideToggleEl);
      toolbarEl.appendChild(guideToggleEl);
    }

    function setVisorActive(active) {
      visorActive = active;
      if (visorActive) {
        visorEl.classList.add("su-reading-visor-active");
        guideToggleEl.classList.add("su-reading-guide-on");
      } else {
        visorEl.classList.remove("su-reading-visor-active");
        guideToggleEl.classList.remove("su-reading-guide-on");
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

    document.addEventListener("mousemove", (e) => { window.lastMouseY = e.clientY; }, { passive: true });

    if (state.settings.showReadingGuide) {
      guideToggleEl.style.display = "";
    } else {
      guideToggleEl.style.display = "none";
    }
  }

  function escapeHtml(s) {
    const div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
  }

  run();
})();
