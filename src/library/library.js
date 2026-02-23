import {
  csvToTags,
  getLibraryState,
  getQualityJcrIndex,
  getQualityQuartilesIndex,
  getSavedPapers,
  getSettings,
  removePaper,
  setLibraryState,
  uniqTags,
  upsertPaper
} from "../common/storage.js";
import {
  compileQualityIndex,
  extractVenueFromAuthorsVenue,
  normalizeVenueName,
  qualityBadgesForVenue
} from "../common/quality.js";

function el(id) {
  return document.getElementById(id);
}

function fmtDate(iso) {
  if (!iso) return "";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return String(iso);
  }
}

function normalize(s) {
  return String(s || "").toLowerCase();
}

function hashString(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h << 5) - h + str.charCodeAt(i);
    h |= 0;
  }
  return Math.abs(h).toString(36);
}

function normalizeTitleForMatch(title) {
  return String(title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function paperFingerprint(p) {
  const doi = String(p?.doi || "").toLowerCase().trim();
  if (doi) return `doi:${doi}`;
  const t = normalizeTitleForMatch(p?.title || "");
  return t ? `title:${t}` : `key:${p?.key || ""}`;
}

function parseAuthors(authorsVenue) {
  const raw = (authorsVenue || "").split(/\s*-\s*/)[0] || "";
  return raw
    .split(/\s*,\s*|\s+and\s+/i)
    .map((a) => a.trim())
    .filter(Boolean);
}

function lastName(author) {
  const parts = String(author || "").trim().split(/\s+/);
  return parts.length ? parts[parts.length - 1] : "Unknown";
}

function formatInText(p) {
  const authors = parseAuthors(p.authorsVenue);
  const first = authors[0] || "Unknown";
  const year = p.year || "n.d.";
  if (authors.length > 2) return `(${lastName(first)} et al., ${year})`;
  if (authors.length === 2) return `(${lastName(first)} & ${lastName(authors[1])}, ${year})`;
  return `(${lastName(first)}, ${year})`;
}

function formatApa(p) {
  const authors = parseAuthors(p.authorsVenue);
  const authorStr = authors.length ? authors.join(", ") : "Unknown";
  const year = p.year || "n.d.";
  const venue = p.venue || extractVenueFromAuthorsVenue(p.authorsVenue) || "";
  return `${authorStr} (${year}). ${p.title || "Untitled"}. ${venue}.`;
}

function formatMla(p) {
  const authors = parseAuthors(p.authorsVenue);
  const authorStr = authors.length ? authors.join(", ") : "Unknown";
  const year = p.year || "n.d.";
  const venue = p.venue || extractVenueFromAuthorsVenue(p.authorsVenue) || "";
  return `${authorStr}. "${p.title || "Untitled"}." ${venue}, ${year}.`;
}

function formatChicago(p) {
  const authors = parseAuthors(p.authorsVenue);
  const authorStr = authors.length ? authors.join(", ") : "Unknown";
  const year = p.year || "n.d.";
  const venue = p.venue || extractVenueFromAuthorsVenue(p.authorsVenue) || "";
  return `${authorStr}. "${p.title || "Untitled"}." ${venue} (${year}).`;
}

function formatBibTeX(p) {
  const authors = parseAuthors(p.authorsVenue).join(" and ");
  const key = `${lastName(parseAuthors(p.authorsVenue)[0] || "key")}${p.year || ""}${hashString(p.title || "")}`.slice(0, 30);
  return `@article{${key},\n  title={${p.title || ""}},\n  author={${authors}},\n  year={${p.year || ""}},\n  doi={${p.doi || ""}},\n  url={${p.url || ""}}\n}`;
}

function formatRIS(p) {
  const authors = parseAuthors(p.authorsVenue);
  const lines = [
    "TY  - JOUR",
    `TI  - ${p.title || ""}`,
    ...authors.map((a) => `AU  - ${a}`),
    `PY  - ${p.year || ""}`,
    p.doi ? `DO  - ${p.doi}` : "",
    p.url ? `UR  - ${p.url}` : "",
    "ER  -"
  ].filter(Boolean);
  return lines.join("\n");
}

async function copyText(txt) {
  try {
    await navigator.clipboard.writeText(txt);
  } catch {
    const ta = document.createElement("textarea");
    ta.value = txt;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    ta.remove();
  }
}

let qualityState = null;
let impactIndexCache = null;
let libraryState = null;
let activePdfKey = null;
let pendingLocalOpenKey = null;
let pendingFolderScanId = null;
let lastAppliedSearchId = null;
let pdfSummaryTimer = null;
let pdfObjectUrl = null;

function normalizeLibraryState(state) {
  return {
    collections: Array.isArray(state?.collections) ? state.collections : [],
    savedSearches: Array.isArray(state?.savedSearches) ? state.savedSearches : [],
    watchedFolders: Array.isArray(state?.watchedFolders) ? state.watchedFolders : [],
    activeCollectionId: state?.activeCollectionId || "",
    activeSavedSearchId: state?.activeSavedSearchId || "",
    showDuplicates: !!state?.showDuplicates
  };
}

async function loadLibraryState() {
  if (libraryState) return libraryState;
  const stored = await getLibraryState();
  libraryState = normalizeLibraryState(stored);
  return libraryState;
}

async function saveLibraryState(next) {
  libraryState = normalizeLibraryState({ ...libraryState, ...(next || {}) });
  await setLibraryState(libraryState);
}

async function loadImpactIndex() {
  if (impactIndexCache) return impactIndexCache;
  const map = new Map();
  try {
    const url = chrome.runtime.getURL("src/data/journal_impact_2024.csv");
    const r = await fetch(url);
    const text = r.ok ? await r.text() : "";
    for (const line of (text || "").split(/\r?\n/)) {
      if (!line || /^\s*Journal\s*Name\s*,/i.test(line)) continue;
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
    // ignore
  }
  impactIndexCache = map;
  return map;
}

async function ensureQualityState() {
  if (qualityState) return qualityState;
  const [s, quartiles, jcr, impactIndex] = await Promise.all([
    getSettings(),
    getQualityQuartilesIndex(),
    getQualityJcrIndex(),
    loadImpactIndex()
  ]);
  qualityState = {
    show: !!s.showQualityBadges,
    qIndex: compileQualityIndex(s, {
      quartilesIndex: quartiles.index || {},
      jcrIndex: jcr.index || {},
      impactIndex
    }),
    quartilesMeta: quartiles.meta || null
  };
  return qualityState;
}

function matchesQuery(p, q, collectionNames) {
  if (!q) return true;
  const pdfNotes = p.pdfNotes || {};
  const highlights = Array.isArray(pdfNotes.highlights) ? pdfNotes.highlights.map((h) => `${h.quote || ""} ${h.note || ""}`) : [];
  const hay = [
    p.title,
    p.authorsVenue,
    (p.tags || []).join(" "),
    p.notes,
    pdfNotes.summary,
    highlights.join(" "),
    (collectionNames || []).join(" "),
    p.localFile?.name,
    p.localFile?.relativePath
  ]
    .map(normalize)
    .join("\n");
  return hay.includes(q);
}

function compareBy(sortKey, a, b) {
  if (sortKey === "year") return (b.year || 0) - (a.year || 0);
  if (sortKey === "title") return String(a.title || "").localeCompare(String(b.title || ""));
  if (sortKey === "citations") return (b.citations || 0) - (a.citations || 0);
  return String(b.savedAt || "").localeCompare(String(a.savedAt || ""));
}

function collectionMap(state) {
  const map = new Map();
  (state.collections || []).forEach((c) => map.set(c.id, c));
  return map;
}

function applySavedSearch(state, search) {
  if (!search) return;
  el("q").value = search.query || "";
  el("tagFilter").value = (search.tags || []).join(", ");
  el("sort").value = search.sort || "savedAt";
  state.activeCollectionId = search.collectionId || "";
  state.activeSavedSearchId = search.id || "";
}

function computeDuplicateGroups(items) {
  const map = new Map();
  items.forEach((p) => {
    const fp = paperFingerprint(p);
    if (!fp) return;
    if (!map.has(fp)) map.set(fp, []);
    map.get(fp).push(p);
  });
  return Array.from(map.entries())
    .map(([fp, list]) => ({ fp, list }))
    .filter((g) => g.list.length > 1);
}

function renderActiveFilters(state, collectionName) {
  const filters = [];
  const qVal = normalize(el("q").value);
  const tagVal = el("tagFilter").value.trim();
  if (qVal) filters.push({ label: `Query: ${qVal}`, clear: () => { el("q").value = ""; render(); } });
  if (tagVal) filters.push({ label: `Tags: ${tagVal}`, clear: () => { el("tagFilter").value = ""; render(); } });
  if (collectionName) filters.push({ label: `Collection: ${collectionName}`, clear: () => { saveLibraryState({ activeCollectionId: "" }); render(); } });
  if (state.activeSavedSearchId) {
    filters.push({
      label: "Saved search",
      clear: () => {
        saveLibraryState({ activeSavedSearchId: "" });
        render();
      }
    });
  }
  const host = el("activeFilters");
  host.textContent = "";
  filters.forEach((f) => {
    const chip = document.createElement("span");
    chip.className = "filter-chip";
    chip.textContent = f.label;
    const btn = document.createElement("button");
    btn.textContent = "×";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      f.clear();
    });
    chip.appendChild(btn);
    host.appendChild(chip);
  });
}

function openModal(id) {
  el(id).classList.add("open");
}

function closeModal(id) {
  el(id).classList.remove("open");
  if (id === "pdfModal" && pdfObjectUrl) {
    try { URL.revokeObjectURL(pdfObjectUrl); } catch (_) {}
    pdfObjectUrl = null;
  }
  if (id === "pdfModal") {
    activePdfKey = null;
  }
}

function buildCiteMenu(paper) {
  const opts = [
    { id: "intext", label: "In-text", value: formatInText(paper) },
    { id: "apa", label: "APA", value: formatApa(paper) },
    { id: "mla", label: "MLA", value: formatMla(paper) },
    { id: "chicago", label: "Chicago", value: formatChicago(paper) },
    { id: "bib", label: "BibTeX", value: formatBibTeX(paper) },
    { id: "ris", label: "RIS", value: formatRIS(paper) }
  ];
  return opts.map((opt) => `<button class="cite-chip" data-cite="${opt.id}" data-value="${encodeURIComponent(opt.value)}">${opt.label}</button>`).join("");
}

async function openPdfReader(paper) {
  if (!paper) return;
  activePdfKey = paper.key;
  el("pdfModalTitle").textContent = paper.title || "PDF reader";
  el("pdfSummary").value = paper.pdfNotes?.summary || "";
  renderPdfHighlights(paper);
  const frame = el("pdfFrame");
  frame.src = "";
  if (paper.pdfUrl) {
    frame.src = paper.pdfUrl;
  }
  openModal("pdfModal");
}

function renderPdfHighlights(paper) {
  const list = el("pdfHighlightList");
  list.textContent = "";
  const highlights = Array.isArray(paper.pdfNotes?.highlights) ? paper.pdfNotes.highlights : [];
  if (!highlights.length) {
    const empty = document.createElement("div");
    empty.className = "panel-empty";
    empty.textContent = "No highlights yet.";
    list.appendChild(empty);
    return;
  }
  highlights.forEach((h, idx) => {
    const item = document.createElement("div");
    item.className = "pdf-highlight-item";
    item.innerHTML = `<div>${h.quote || ""}</div><small>p.${h.page || "?"} · ${h.note || ""}</small>`;
    const btn = document.createElement("button");
    btn.className = "btn";
    btn.textContent = "Remove";
    btn.addEventListener("click", async () => {
      const saved = await getSavedPapers();
      const p = saved[activePdfKey];
      if (!p) return;
      p.pdfNotes = p.pdfNotes || {};
      p.pdfNotes.highlights = Array.isArray(p.pdfNotes.highlights) ? p.pdfNotes.highlights : [];
      p.pdfNotes.highlights.splice(idx, 1);
      await upsertPaper(p);
      render();
      renderPdfHighlights(p);
    });
    item.appendChild(btn);
    list.appendChild(item);
  });
}

async function handlePdfSummaryInput() {
  if (!activePdfKey) return;
  const saved = await getSavedPapers();
  const p = saved[activePdfKey];
  if (!p) return;
  p.pdfNotes = p.pdfNotes || {};
  p.pdfNotes.summary = el("pdfSummary").value;
  await upsertPaper(p);
}

async function addPdfHighlight() {
  if (!activePdfKey) return;
  const quote = el("pdfHighlightQuote").value.trim();
  if (!quote) return;
  const page = el("pdfHighlightPage").value.trim();
  const note = el("pdfHighlightNote").value.trim();
  const saved = await getSavedPapers();
  const p = saved[activePdfKey];
  if (!p) return;
  p.pdfNotes = p.pdfNotes || {};
  p.pdfNotes.highlights = Array.isArray(p.pdfNotes.highlights) ? p.pdfNotes.highlights : [];
  p.pdfNotes.highlights.push({ quote, page, note, createdAt: new Date().toISOString() });
  await upsertPaper(p);
  el("pdfHighlightPage").value = "";
  el("pdfHighlightQuote").value = "";
  el("pdfHighlightNote").value = "";
  render();
  renderPdfHighlights(p);
}

async function openLocalPdf(paper) {
  if (!paper) return;
  pendingLocalOpenKey = paper.key;
  el("localPdfPicker").click();
}

async function handleLocalPdfSelection(file) {
  if (!file || !pendingLocalOpenKey) return;
  const url = URL.createObjectURL(file);
  if (activePdfKey === pendingLocalOpenKey) {
    if (pdfObjectUrl) {
      try { URL.revokeObjectURL(pdfObjectUrl); } catch (_) {}
    }
    pdfObjectUrl = url;
    el("pdfFrame").src = url;
  } else {
    window.open(url, "_blank", "noreferrer");
  }
  pendingLocalOpenKey = null;
}

async function handleFolderSelection(files) {
  const state = await loadLibraryState();
  if (!files || !files.length) return;
  const root = files[0].webkitRelativePath.split("/")[0] || "Folder";
  const folderId = pendingFolderScanId || `folder_${hashString(root)}`;
  const existing = state.watchedFolders.find((f) => f.id === folderId);
  const pdfFiles = Array.from(files).filter((f) => /\.pdf$/i.test(f.name));
  const entries = pdfFiles.map((f) => ({
    name: f.name,
    relativePath: f.webkitRelativePath,
    size: f.size,
    lastModified: f.lastModified
  }));
  const prev = new Set((existing?.files || []).map((f) => f.relativePath));
  const newOnes = entries.filter((e) => !prev.has(e.relativePath));
  if (newOnes.length) {
    for (const nf of newOnes) {
      const key = `local_${hashString(`${folderId}_${nf.relativePath}_${nf.size}`)}`;
      await upsertPaper({
        key,
        title: nf.name.replace(/\.pdf$/i, "").replace(/[_-]+/g, " ").trim(),
        authorsVenue: "Local PDF",
        year: "",
        pdfUrl: "",
        localFile: { folderId, ...nf },
        savedAt: new Date().toISOString()
      });
    }
  }
  const nextFolders = (state.watchedFolders || []).filter((f) => f.id !== folderId);
  nextFolders.push({
    id: folderId,
    name: root,
    lastScan: new Date().toISOString(),
    files: entries
  });
  pendingFolderScanId = null;
  await saveLibraryState({ watchedFolders: nextFolders });
  render();
}

async function renderSidebar(state, items) {
  const colList = el("collectionList");
  const searchList = el("savedSearchList");
  const folderList = el("folderList");
  const colCounts = new Map();
  items.forEach((p) => {
    (p.collections || []).forEach((id) => {
      colCounts.set(id, (colCounts.get(id) || 0) + 1);
    });
  });

  colList.textContent = "";
  if (!state.collections.length) {
    const empty = document.createElement("div");
    empty.className = "panel-empty";
    empty.textContent = "No collections yet.";
    colList.appendChild(empty);
  }
  state.collections.forEach((c) => {
    const row = document.createElement("div");
    row.className = `panel-item ${state.activeCollectionId === c.id ? "active" : ""}`;
    row.innerHTML = `<span>${c.name}</span><small>${colCounts.get(c.id) || 0}</small>`;
    row.addEventListener("click", async () => {
      await saveLibraryState({
        activeCollectionId: state.activeCollectionId === c.id ? "" : c.id,
        activeSavedSearchId: ""
      });
      render();
    });
    row.addEventListener("contextmenu", async (e) => {
      e.preventDefault();
      const next = window.prompt("Rename collection", c.name);
      if (next === null) return;
      const updated = state.collections.map((col) => (col.id === c.id ? { ...col, name: next } : col));
      await saveLibraryState({ collections: updated });
      render();
    });
    colList.appendChild(row);
  });

  searchList.textContent = "";
  if (!state.savedSearches.length) {
    const empty = document.createElement("div");
    empty.className = "panel-empty";
    empty.textContent = "No saved searches.";
    searchList.appendChild(empty);
  }
  state.savedSearches.forEach((s) => {
    const row = document.createElement("div");
    row.className = `panel-item ${state.activeSavedSearchId === s.id ? "active" : ""}`;
    row.innerHTML = `<span>${s.name}</span><small>${s.query ? s.query.slice(0, 12) : ""}</small>`;
    row.addEventListener("click", async () => {
      applySavedSearch(state, s);
      await saveLibraryState({ activeSavedSearchId: s.id, activeCollectionId: s.collectionId || "" });
      render();
    });
    row.addEventListener("contextmenu", async (e) => {
      e.preventDefault();
      const updated = state.savedSearches.filter((ss) => ss.id !== s.id);
      await saveLibraryState({ savedSearches: updated, activeSavedSearchId: "" });
      render();
    });
    searchList.appendChild(row);
  });

  folderList.textContent = "";
  if (!state.watchedFolders.length) {
    const empty = document.createElement("div");
    empty.className = "panel-empty";
    empty.textContent = "No watched folders.";
    folderList.appendChild(empty);
  }
  state.watchedFolders.forEach((f) => {
    const row = document.createElement("div");
    row.className = "panel-item";
    row.innerHTML = `<span>${f.name}</span><small>${(f.files || []).length}</small>`;
    row.addEventListener("click", () => {
      pendingFolderScanId = f.id;
      el("folderPicker").click();
    });
    folderList.appendChild(row);
  });
}

async function render() {
  const state = await loadLibraryState();
  const qs = await ensureQualityState();
  const saved = await getSavedPapers();
  const colMap = collectionMap(state);
  const dupBtn = el("toggleDuplicates");
  if (dupBtn) dupBtn.textContent = state.showDuplicates ? "All items" : "Duplicates";

  if (state.activeSavedSearchId && state.activeSavedSearchId !== lastAppliedSearchId) {
    const search = state.savedSearches.find((s) => s.id === state.activeSavedSearchId);
    if (search) applySavedSearch(state, search);
    lastAppliedSearchId = state.activeSavedSearchId;
  }

  const qVal = normalize(el("q").value);
  const tagFilter = el("tagFilter").value.trim();
  const sortKey = el("sort").value;

  const items = Object.values(saved);
  await renderSidebar(state, items);

  const tagFilters = tagFilter
    ? tagFilter.split(",").map((t) => t.trim().toLowerCase()).filter(Boolean)
    : [];

  const filtered = items
    .filter((p) => {
      const collectionNames = (p.collections || []).map((id) => colMap.get(id)?.name || "");
      if (!matchesQuery(p, qVal, collectionNames)) return false;
      if (state.activeCollectionId && !(p.collections || []).includes(state.activeCollectionId)) return false;
      if (tagFilters.length) {
        const tagSet = new Set((p.tags || []).map((t) => String(t || "").toLowerCase()));
        if (!tagFilters.some((t) => tagSet.has(t))) return false;
      }
      return true;
    })
    .sort((a, b) => compareBy(sortKey, a, b));

  renderActiveFilters(state, colMap.get(state.activeCollectionId)?.name || "");

  const list = el("list");
  list.textContent = "";
  if (state.showDuplicates) {
    const groups = computeDuplicateGroups(filtered);
    if (!groups.length) {
      list.textContent = "No duplicates detected.";
      return;
    }
    groups.forEach((group) => {
      const wrap = document.createElement("div");
      wrap.className = "duplicate-group";
      const title = document.createElement("h4");
      title.textContent = `Duplicate set (${group.list.length})`;
      wrap.appendChild(title);
      group.list.forEach((p) => {
        const row = document.createElement("div");
        row.className = "duplicate-item";
        row.innerHTML = `<span>${p.title || "Untitled"}</span>`;
        const btn = document.createElement("button");
        btn.className = "btn";
        btn.textContent = "Keep";
        btn.addEventListener("click", async () => {
          for (const other of group.list) {
            if (other.key === p.key) continue;
            await removePaper(other.key);
          }
          await render();
        });
        row.appendChild(btn);
        wrap.appendChild(row);
      });
      list.appendChild(wrap);
    });
    return;
  }

  const tpl = document.getElementById("itemTpl");
  for (const p of filtered) {
    const node = tpl.content.firstElementChild.cloneNode(true);
    const titleA = node.querySelector(".title");
    titleA.textContent = p.title || "(untitled)";
    titleA.href = p.url || p.sourcePageUrl || "#";

    const metaParts = [p.authorsVenue, p.year ? `(${p.year})` : "", p.citations ? `${p.citations} cites` : ""].filter(Boolean);
    node.querySelector(".meta").textContent = metaParts.join(" ");

    node.querySelector(".snippet").textContent = p.snippet || "";

    const tags = (p.tags || []).map((t) => `<span class="tag-chip">${t}</span>`).join(" ");
    const collectionNames = (p.collections || []).map((id) => colMap.get(id)?.name || id);
    const collections = collectionNames.map((c) => `<span class="collection-chip">${c}</span>`).join(" ");
    const pdfNotes = p.pdfNotes || {};
    const highlightCount = Array.isArray(pdfNotes.highlights) ? pdfNotes.highlights.length : 0;
    const venue = p.venue || extractVenueFromAuthorsVenue(p.authorsVenue);
    const badges = qs.show ? qualityBadgesForVenue(venue, qs.qIndex).map((b) => b.text) : [];
    const qualityStr = badges.length ? `Quality: ${badges.join(", ")}` : "";
    const notesStr = p.notes ? "Notes: yes" : "";
    const annotStr = highlightCount ? `Highlights: ${highlightCount}` : "";
    const localStr = p.localFile ? `Local: ${p.localFile.name}` : "";
    node.querySelector(".footer").innerHTML = [
      `Saved: ${fmtDate(p.savedAt)}`,
      tags,
      collections,
      notesStr,
      annotStr,
      localStr,
      qualityStr
    ].filter(Boolean).join(" | ");

    const citeMenu = node.querySelector(".cite-menu");
    citeMenu.innerHTML = buildCiteMenu(p);
    citeMenu.addEventListener("click", async (e) => {
      const btn = e.target.closest("[data-cite]");
      if (!btn) return;
      const value = decodeURIComponent(btn.getAttribute("data-value") || "");
      await copyText(value);
    });

    node.querySelectorAll("[data-act]").forEach((b) => {
      b.addEventListener("click", async (e) => {
        const act = b.getAttribute("data-act");
        if (act === "openScholar") {
          if (p.sourcePageUrl) window.open(p.sourcePageUrl, "_blank", "noreferrer");
          return;
        }
        if (act === "openPdf") {
          if (p.pdfUrl) window.open(p.pdfUrl, "_blank", "noreferrer");
          else if (p.localFile) await openLocalPdf(p);
          else window.alert("No PDF available for this item.");
          return;
        }
        if (act === "pdfReader") {
          await openPdfReader(p);
          return;
        }
        if (act === "remove") {
          await removePaper(p.key);
          qualityState = null;
          await render();
          return;
        }
        if (act === "tags") {
          const next = window.prompt("Comma-separated tags:", (p.tags || []).join(", "));
          if (next === null) return;
          await upsertPaper({ ...p, tags: uniqTags(csvToTags(next)) });
          qualityState = null;
          await render();
          return;
        }
        if (act === "notes") {
          const next = window.prompt("Notes:", p.notes || "");
          if (next === null) return;
          await upsertPaper({ ...p, notes: String(next) });
          qualityState = null;
          await render();
          return;
        }
        if (act === "collections") {
          await renderCollectionModal(p, state);
          return;
        }
        if (act === "cite") {
          citeMenu.classList.toggle("open");
          e.stopPropagation();
          return;
        }
      });
    });

    list.appendChild(node);
  }
}

async function renderCollectionModal(paper, state) {
  const list = el("collectionModalList");
  list.textContent = "";
  const collections = state.collections || [];
  if (!collections.length) {
    const empty = document.createElement("div");
    empty.className = "panel-empty";
    empty.textContent = "No collections yet.";
    list.appendChild(empty);
  }
  collections.forEach((c) => {
    const row = document.createElement("label");
    row.className = "panel-item";
    const checked = (paper.collections || []).includes(c.id);
    row.innerHTML = `<span>${c.name}</span><input type="checkbox" ${checked ? "checked" : ""} data-col-id="${c.id}"/>`;
    row.querySelector("input").addEventListener("change", async (e) => {
      const id = e.target.getAttribute("data-col-id");
      const saved = await getSavedPapers();
      const current = saved[paper.key] || paper;
      const next = new Set(current.collections || []);
      if (e.target.checked) next.add(id);
      else next.delete(id);
      await upsertPaper({ ...current, collections: Array.from(next) });
      await render();
    });
    list.appendChild(row);
  });
  openModal("collectionModal");
}

document.addEventListener("click", () => {
  document.querySelectorAll(".cite-menu.open").forEach((m) => m.classList.remove("open"));
});

document.querySelectorAll("[data-modal-close]").forEach((btn) => {
  btn.addEventListener("click", () => {
    closeModal("collectionModal");
    closeModal("pdfModal");
  });
});

el("pdfHighlightAdd").addEventListener("click", addPdfHighlight);
el("pdfSummary").addEventListener("input", () => {
  if (pdfSummaryTimer) clearTimeout(pdfSummaryTimer);
  pdfSummaryTimer = setTimeout(handlePdfSummaryInput, 300);
});
el("pdfModalOpenFile").addEventListener("click", () => {
  if (!activePdfKey) return;
  pendingLocalOpenKey = activePdfKey;
  el("localPdfPicker").click();
});
el("localPdfPicker").addEventListener("change", (e) => {
  const file = e.target.files?.[0];
  handleLocalPdfSelection(file);
  e.target.value = "";
});

el("folderPicker").addEventListener("change", (e) => {
  handleFolderSelection(Array.from(e.target.files || []));
  e.target.value = "";
});

el("scanFolder").addEventListener("click", () => {
  pendingFolderScanId = null;
  el("folderPicker").click();
});

el("saveSearch").addEventListener("click", async () => {
  const state = await loadLibraryState();
  const name = window.prompt("Saved search name?", "New search");
  if (!name) return;
  const entry = {
    id: `search_${Date.now().toString(36)}`,
    name,
    query: el("q").value.trim(),
    tags: el("tagFilter").value.split(",").map((t) => t.trim()).filter(Boolean),
    collectionId: state.activeCollectionId || "",
    sort: el("sort").value
  };
  const next = [...state.savedSearches, entry];
  await saveLibraryState({ savedSearches: next, activeSavedSearchId: entry.id });
  render();
});

el("newCollection").addEventListener("click", async () => {
  const state = await loadLibraryState();
  const name = window.prompt("Collection name?", "New collection");
  if (!name) return;
  const entry = { id: `col_${Date.now().toString(36)}`, name };
  await saveLibraryState({ collections: [...state.collections, entry], activeCollectionId: entry.id });
  render();
});

el("collectionModalAdd").addEventListener("click", async () => {
  const state = await loadLibraryState();
  const name = el("collectionModalNew").value.trim();
  if (!name) return;
  const entry = { id: `col_${Date.now().toString(36)}`, name };
  el("collectionModalNew").value = "";
  await saveLibraryState({ collections: [...state.collections, entry] });
  render();
  closeModal("collectionModal");
});

el("toggleDuplicates").addEventListener("click", async () => {
  const state = await loadLibraryState();
  await saveLibraryState({ showDuplicates: !state.showDuplicates });
  render();
});

el("exportLibrary").addEventListener("click", async () => {
  const state = await loadLibraryState();
  const saved = await getSavedPapers();
  const payload = { savedPapers: saved, libraryState: state };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "library-export.json";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
});

el("importLibrary").addEventListener("click", () => {
  el("importLibraryFile").click();
});

el("importLibraryFile").addEventListener("change", async (e) => {
  const file = e.target.files?.[0];
  if (!file) return;
  try {
    const text = await file.text();
    const data = JSON.parse(text);
    const saved = await getSavedPapers();
    const merged = { ...saved, ...(data.savedPapers || {}) };
    await chrome.storage.local.set({ savedPapers: merged });
    if (data.libraryState) {
      await saveLibraryState(data.libraryState);
    }
  } catch (_) {
    window.alert("Could not import library file.");
  }
  e.target.value = "";
  render();
});

el("q").addEventListener("input", async () => {
  const state = await loadLibraryState();
  if (state.activeSavedSearchId) await saveLibraryState({ activeSavedSearchId: "" });
  render();
});
el("tagFilter").addEventListener("input", async () => {
  const state = await loadLibraryState();
  if (state.activeSavedSearchId) await saveLibraryState({ activeSavedSearchId: "" });
  render();
});
el("sort").addEventListener("change", () => render());

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") return;
  if (changes.savedPapers || changes.settings || changes.libraryState) {
    qualityState = null;
    libraryState = null;
    render();
  }
});

render();
