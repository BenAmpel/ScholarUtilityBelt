import {
  clearHiddenAuthors,
  clearHiddenPapers,
  clearHiddenVenues,
  clearQualityQuartilesIndex,
  clearQualityJcrIndex,
  getHiddenAuthors,
  getHiddenPapers,
  getHiddenVenues,
  getQualityJcrIndex,
  getQualityQuartilesIndex,
  getSettings,
  removeHiddenAuthor,
  removeHiddenPaper,
  removeHiddenVenue,
  setQualityJcrIndex,
  setQualityQuartilesIndex,
  setSettings
} from "../common/storage.js";
import { normalizeVenueName } from "../common/quality.js";

function el(id) {
  return document.getElementById(id);
}

async function fetchExtText(path) {
  const url = chrome.runtime.getURL(path);
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Failed to load ${path}: ${r.status}`);
  return await r.text();
}

const QUALITY_BADGE_IDS = {
  quartile: "qbQuartile",
  abdc: "qbAbdc",
  jcr: "qbJcr",
  ft50: "qbFt50",
  utd24: "qbUtd24",
  core: "qbCore",
  ccf: "qbCcf",
  era: "qbEra",
  norwegian: "qbNorwegian",
  preprint: "qbPreprint",
  h5: "qbH5"
};

async function load() {
  const s = await getSettings();
  el("theme").value = s.theme || "auto";
  el("viewMode").value = s.viewMode || "detailed";
  const qb = s.qualityBadgeKinds || {};
  for (const [kind, id] of Object.entries(QUALITY_BADGE_IDS)) {
    const input = el(id);
    if (input) input.checked = qb[kind] !== false;
  }
  el("highlightSaved").checked = !!s.highlightSaved;
  el("defaultTagsCsv").value = s.defaultTagsCsv || "";
  el("keywordHighlightsCsv").value = s.keywordHighlightsCsv || "";
  el("hideTitleRegex").value = s.hideTitleRegex || "";
  el("hideAuthorsRegex").value = s.hideAuthorsRegex || "";
  el("showSemanticExpansion").checked = !!s.showSemanticExpansion;
  el("showFundingTag").checked = !!s.showFundingTag;
  el("showHoverSummary").checked = !!s.showHoverSummary;
  el("showReadingGuide").checked = !!s.showReadingGuide;
  el("showRetractionWatch").checked = !!s.showRetractionWatch;
  el("showArtifactBadge").checked = !!s.showArtifactBadge;
  el("showCSS").checked = !!s.showCSS;
  el("showAgeBiasHeatmap").checked = !!s.showAgeBiasHeatmap;
  el("showAuthorshipHeatmap").checked = !!s.showAuthorshipHeatmap;
  el("showRelevancySparkline").checked = !!s.showRelevancySparkline;
  el("showSnippetCueEmphasis").checked = !!s.showSnippetCueEmphasis;
  el("showSkimmabilityStrip").checked = !!s.showSkimmabilityStrip;
  el("showReadingLoadEstimator").checked = !!s.showReadingLoadEstimator;
  el("groupVersions").checked = !!s.groupVersions;
  el("versionOrder").value = s.versionOrder || "journal-first";
  el("showNewSinceLastVisit").checked = s.showNewSinceLastVisit !== false;
  el("showCitationSpike").checked = s.showCitationSpike !== false;
  el("citationSpikeThresholdPct").value = String(s.citationSpikeThresholdPct ?? 50);
  el("citationSpikeMonths").value = String(s.citationSpikeMonths ?? 6);
  el("showQualityBadges").checked = !!s.showQualityBadges;
  el("qualityFt50List").value = s.qualityFt50List || "";
  el("qualityUtd24List").value = s.qualityUtd24List || "";
  el("qualityAbdcRanks").value = s.qualityAbdcRanks || "";
  el("qualityQuartiles").value = s.qualityQuartiles || "";
  el("qualityCoreRanks").value = s.qualityCoreRanks || "";
  el("qualityCcfRanks").value = s.qualityCcfRanks || "";
  el("scimagoYear").value = String(new Date().getFullYear() - 1);

  // Show metadata about packaged sources if available.
  try {
    const metaText = await fetchExtText("src/data/quality_sources.json");
    const meta = JSON.parse(metaText);
    const when = meta?.fetchedAt ? String(meta.fetchedAt) : "";
    const sources = meta?.sources ? Object.keys(meta.sources).join(", ") : "";
    el("qualityMeta").textContent = when
      ? `Built-in lists last fetched: ${when}${sources ? ` (${sources})` : ""}`
      : "";
  } catch {
    el("qualityMeta").textContent = "";
  }

  // Show metadata about imported quartiles (if any).
  try {
    const q = await getQualityQuartilesIndex();
    const m = q.meta || null;
    el("scimagoMeta").textContent = m
      ? `Imported quartiles: ${m.source || "unknown"}${m.year ? ` (${m.year})` : ""} | rows: ${
          m.rowCount ?? "?"
        } | updated: ${m.importedAt || "?"}`
      : "Imported quartiles: none";
  } catch {
    el("scimagoMeta").textContent = "";
  }

  // Show metadata about imported JCR index (if any).
  try {
    const j = await getQualityJcrIndex();
    const m = j.meta || null;
    el("jcrMeta").textContent = m
      ? `Imported JCR: ${m.year || "?"} | files: ${m.fileCount ?? "?"} | rows: ${m.rowCount ?? "?"} | journals: ${
          m.journalCount ?? "?"
        } | updated: ${m.importedAt || "?"}`
      : "Imported JCR: none";
  } catch {
    el("jcrMeta").textContent = "";
  }

  await renderHiddenLists();
  await updateOptionalPermStatus();
}

async function updateOptionalPermStatus() {
  const statusEl = el("optionalPermStatus");
  const btn = el("grantOptionalPermission");
  if (!statusEl || !btn) return;
  try {
    const has = await chrome.permissions.contains({ origins: ["<all_urls>"] });
    statusEl.textContent = has ? "Granted ✓" : "";
    btn.textContent = "Grant permission";
    btn.style.display = has ? "none" : "";
  } catch {
    statusEl.textContent = "";
  }
}

function setStatus(msg) {
  el("status").textContent = msg;
  setTimeout(() => {
    if (el("status").textContent === msg) el("status").textContent = "";
  }, 2000);
}

function setQualityStatus(msg) {
  el("qualityStatus").textContent = msg;
  setTimeout(() => {
    if (el("qualityStatus").textContent === msg) el("qualityStatus").textContent = "";
  }, 2500);
}

function setScimagoStatus(msg) {
  el("scimagoStatus").textContent = msg;
  setTimeout(() => {
    if (el("scimagoStatus").textContent === msg) el("scimagoStatus").textContent = "";
  }, 3500);
}

function setJcrStatus(msg) {
  el("jcrStatus").textContent = msg;
  setTimeout(() => {
    if (el("jcrStatus").textContent === msg) el("jcrStatus").textContent = "";
  }, 3500);
}

function parseCsv(text) {
  // Minimal CSV parser (quotes + delimiter detection). Returns { headers, rows }.
  const lines = String(text || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/g)
    .filter((l) => l.trim().length > 0);
  if (!lines.length) return { headers: [], rows: [] };

  const headerLine = lines[0];
  const delims = [",", ";", "\t"];
  let delim = ",";
  let best = -1;
  for (const d of delims) {
    const c = (headerLine.match(new RegExp(`\\${d}`, "g")) || []).length;
    if (c > best) {
      best = c;
      delim = d;
    }
  }

  const parseLine = (line) => {
    const out = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQ) {
        if (ch === "\"") {
          const next = line[i + 1];
          if (next === "\"") {
            cur += "\"";
            i++;
          } else {
            inQ = false;
          }
        } else {
          cur += ch;
        }
      } else {
        if (ch === "\"") inQ = true;
        else if (ch === delim) {
          out.push(cur);
          cur = "";
        } else cur += ch;
      }
    }
    out.push(cur);
    return out.map((s) => String(s || "").trim());
  };

  const headers = parseLine(lines[0]);
  const rows = [];
  for (const line of lines.slice(1)) {
    rows.push(parseLine(line));
  }
  return { headers, rows };
}

function parseScimagoExport(text) {
  const t = String(text || "").trim();
  if (!t) return { index: {}, rowCount: 0 };

  // Many "xls" exports are actually HTML tables.
  if (t.startsWith("<") && /<table/i.test(t)) {
    const doc = new DOMParser().parseFromString(t, "text/html");
    const tables = Array.from(doc.querySelectorAll("table"));

    for (const table of tables) {
      const headerCells = Array.from(table.querySelectorAll("tr th")).map((th) =>
        (th.textContent || "").trim()
      );
      if (!headerCells.length) continue;

      const norm = headerCells.map((h) => h.toLowerCase());
      const idxTitle = norm.findIndex((h) => h === "title" || h.includes("journal title"));
      const idxBest = norm.findIndex((h) => h.includes("best quartile"));
      if (idxTitle < 0 || idxBest < 0) continue;

      const rows = Array.from(table.querySelectorAll("tr")).slice(1);
      const index = {};
      let rowCount = 0;

      for (const tr of rows) {
        const tds = Array.from(tr.querySelectorAll("td"));
        if (tds.length <= Math.max(idxTitle, idxBest)) continue;
        const title = (tds[idxTitle].textContent || "").trim();
        const q = (tds[idxBest].textContent || "").trim().toUpperCase();
        if (!title || !/^Q[1-4]$/.test(q)) continue;
        index[normalizeVenueName(title)] = q;
        rowCount++;
      }

      return { index, rowCount };
    }

    throw new Error("Could not find a table with Title + Best Quartile columns.");
  }

  // Try CSV.
  const { headers, rows } = parseCsv(t);
  const normHeaders = headers.map((h) => h.toLowerCase());
  const idxTitle = normHeaders.findIndex((h) => h === "title" || h.includes("journal title"));
  const idxBest = normHeaders.findIndex((h) => h.includes("best quartile"));
  if (idxTitle < 0 || idxBest < 0) {
    throw new Error("CSV missing columns: Title and Best Quartile.");
  }

  const index = {};
  let rowCount = 0;
  for (const row of rows) {
    const title = String(row[idxTitle] || "").trim();
    const q = String(row[idxBest] || "").trim().toUpperCase();
    if (!title || !/^Q[1-4]$/.test(q)) continue;
    index[normalizeVenueName(title)] = q;
    rowCount++;
  }

  return { index, rowCount };
}

function normalizeQuartile(x) {
  const s = String(x || "").trim().toUpperCase();
  const m = s.match(/\bQ([1-4])\b/);
  return m ? `Q${m[1]}` : "";
}

function bestQuartile(a, b) {
  const qa = normalizeQuartile(a);
  const qb = normalizeQuartile(b);
  if (!qa) return qb;
  if (!qb) return qa;
  return Number(qa.slice(1)) <= Number(qb.slice(1)) ? qa : qb;
}

function parseJcrCsv(text) {
  const { headers, rows } = parseCsv(text);
  if (!headers.length) throw new Error("Empty CSV.");

  const h = headers.map((x) => String(x || "").trim().toLowerCase());
  const pick = (...needles) =>
    h.findIndex((col) => needles.some((n) => col === n || col.includes(n)));

  const idxTitle = pick("journal name", "journal title", "title", "source title", "journal");
  if (idxTitle < 0) throw new Error("Could not find a journal title column.");

  const idxJifQ = pick("jif quartile", "jif quart") ?? -1;
  const idxJciQ = pick("jci quartile", "jci quart") ?? -1;
  const idxAisQ = pick("ais quartile", "ais quart") ?? -1;
  const idxFiveYQ =
    pick("5 year jif quartile", "five year jif quartile", "5-year jif quartile") ?? -1;

  const index = {};
  let rowCount = 0;

  for (const row of rows) {
    const titleRaw = String(row[idxTitle] || "").trim();
    if (!titleRaw) continue;
    const key = normalizeVenueName(titleRaw);
    if (!key) continue;

    const cur = index[key] || {};
    const next = { ...cur };

    if (idxJifQ >= 0) next.jifQ = bestQuartile(cur.jifQ, row[idxJifQ]);
    if (idxJciQ >= 0) next.jciQ = bestQuartile(cur.jciQ, row[idxJciQ]);
    if (idxAisQ >= 0) next.aisQ = bestQuartile(cur.aisQ, row[idxAisQ]);
    if (idxFiveYQ >= 0) next.fiveYJifQ = bestQuartile(cur.fiveYJifQ, row[idxFiveYQ]);

    index[key] = next;
    rowCount++;
  }

  return { index, rowCount, journalCount: Object.keys(index).length };
}

async function renderHiddenLists() {
  const [papers, venues, authors] = await Promise.all([
    getHiddenPapers(),
    getHiddenVenues(),
    getHiddenAuthors()
  ]);

  const papersUl = el("hiddenPapersList");
  const venuesUl = el("hiddenVenuesList");
  const authorsUl = el("hiddenAuthorsList");
  if (!papersUl || !venuesUl || !authorsUl) return;

  papersUl.innerHTML = papers.length
    ? papers
        .map(
          (key) =>
            `<li><span class="su-hidden-item">${escapeHtml(truncateKey(key))}</span> <button type="button" class="su-options-small su-unhide" data-kind="paper" data-value="${escapeAttr(
              key
            )}">Unhide</button></li>`
        )
        .join("")
    : "<li class='su-empty'>None</li>";
  venuesUl.innerHTML = venues.length
    ? venues
        .map(
          (v) =>
            `<li><span class="su-hidden-item">${escapeHtml(v)}</span> <button type="button" class="su-options-small su-unhide" data-kind="venue" data-value="${escapeAttr(
              v
            )}">Unhide</button></li>`
        )
        .join("")
    : "<li class='su-empty'>None</li>";
  authorsUl.innerHTML = authors.length
    ? authors
        .map(
          (a) =>
            `<li><span class="su-hidden-item">${escapeHtml(a)}</span> <button type="button" class="su-options-small su-unhide" data-kind="author" data-value="${escapeAttr(
              a
            )}">Unhide</button></li>`
        )
        .join("")
    : "<li class='su-empty'>None</li>";
}

function truncateKey(key) {
  if (!key || key.length <= 40) return key;
  return key.slice(0, 20) + "…" + key.slice(-16);
}

function escapeHtml(s) {
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function escapeAttr(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

el("grantOptionalPermission")?.addEventListener("click", async () => {
  try {
    const granted = await chrome.permissions.request({ origins: ["<all_urls>"] });
    el("optionalPermStatus").textContent = granted ? "Granted ✓" : "Denied";
    if (granted) el("grantOptionalPermission").style.display = "none";
  } catch (e) {
    el("optionalPermStatus").textContent = "Error: " + (e?.message || "unknown");
  }
});

el("save").addEventListener("click", async () => {
  const qualityBadgeKinds = {};
  for (const [kind, id] of Object.entries(QUALITY_BADGE_IDS)) {
    const input = el(id);
    qualityBadgeKinds[kind] = input ? input.checked : true;
  }
  await setSettings({
    theme: el("theme").value || "auto",
    viewMode: el("viewMode").value || "detailed",
    qualityBadgeKinds,
    showNewSinceLastVisit: el("showNewSinceLastVisit").checked,
    showCitationSpike: el("showCitationSpike").checked,
    citationSpikeThresholdPct: Math.max(10, Math.min(500, parseInt(el("citationSpikeThresholdPct").value, 10) || 50)),
    citationSpikeMonths: Math.max(1, Math.min(24, parseInt(el("citationSpikeMonths").value, 10) || 6)),
    highlightSaved: el("highlightSaved").checked,
    defaultTagsCsv: el("defaultTagsCsv").value,
    keywordHighlightsCsv: el("keywordHighlightsCsv").value,
    hideTitleRegex: el("hideTitleRegex").value,
    hideAuthorsRegex: el("hideAuthorsRegex").value,
    showSemanticExpansion: el("showSemanticExpansion").checked,
    showFundingTag: el("showFundingTag").checked,
    showHoverSummary: el("showHoverSummary").checked,
    showReadingGuide: el("showReadingGuide").checked,
    showRetractionWatch: el("showRetractionWatch").checked,
    showArtifactBadge: el("showArtifactBadge").checked,
    showCSS: el("showCSS").checked,
    showAgeBiasHeatmap: el("showAgeBiasHeatmap").checked,
    showAuthorshipHeatmap: el("showAuthorshipHeatmap").checked,
    showRelevancySparkline: el("showRelevancySparkline").checked,
    showSnippetCueEmphasis: el("showSnippetCueEmphasis").checked,
    showSkimmabilityStrip: el("showSkimmabilityStrip").checked,
    showReadingLoadEstimator: el("showReadingLoadEstimator").checked,
    groupVersions: el("groupVersions").checked,
    versionOrder: el("versionOrder").value || "journal-first",

    showQualityBadges: el("showQualityBadges").checked,
    qualityFt50List: el("qualityFt50List").value,
    qualityUtd24List: el("qualityUtd24List").value,
    qualityAbdcRanks: el("qualityAbdcRanks").value,
    qualityQuartiles: el("qualityQuartiles").value,
    qualityCoreRanks: el("qualityCoreRanks").value,
    qualityCcfRanks: el("qualityCcfRanks").value
  });
  setStatus("Saved.");
});

el("importScimagoFile").addEventListener("change", async (e) => {
  const file = e.target.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const { index, rowCount } = parseScimagoExport(text);
    if (!rowCount) throw new Error("Parsed 0 rows (file not recognized?)");

    const meta = {
      source: "SCImago (SJR export)",
      importedAt: new Date().toISOString(),
      rowCount,
      filename: file.name
    };
    await setQualityQuartilesIndex(index, meta);
    el("scimagoMeta").textContent = `Imported quartiles: ${meta.source} | rows: ${rowCount} | updated: ${meta.importedAt}`;
    setScimagoStatus("Imported quartiles.");
  } catch (err) {
    setScimagoStatus(`Import failed: ${err?.message || String(err)}`);
  } finally {
    e.target.value = "";
  }
});

el("tryDownloadScimago").addEventListener("click", async () => {
  const year = Number(el("scimagoYear").value) || new Date().getFullYear() - 1;
  const url = `https://www.scimagojr.com/journalrank.php?year=${encodeURIComponent(
    String(year)
  )}&type=all&out=xls`;

  try {
    setScimagoStatus("Downloading...");
    const r = await fetch(url, { method: "GET", credentials: "omit" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const text = await r.text();

    // Common failure mode: Cloudflare / bot challenge.
    if (/Just a moment/i.test(text) && /cloudflare/i.test(text)) {
      throw new Error(
        "Blocked by Cloudflare. Open SCImago in your browser, download the export, then use Import."
      );
    }

    const { index, rowCount } = parseScimagoExport(text);
    if (!rowCount) throw new Error("Parsed 0 rows (download not recognized?)");

    const meta = {
      source: "SCImago (download)",
      importedAt: new Date().toISOString(),
      rowCount,
      year
    };
    await setQualityQuartilesIndex(index, meta);
    el("scimagoMeta").textContent = `Imported quartiles: ${meta.source} (${year}) | rows: ${rowCount} | updated: ${meta.importedAt}`;
    setScimagoStatus("Downloaded + imported quartiles.");
  } catch (err) {
    setScimagoStatus(`Download failed: ${err?.message || String(err)}`);
  }
});

el("importJcrFiles").addEventListener("change", async (e) => {
  const files = Array.from(e.target.files || []);
  if (!files.length) return;

  try {
    setJcrStatus(`Importing ${files.length} file(s)...`);

    let merged = {};
    let totalRows = 0;
    for (const f of files) {
      const text = await f.text();
      const { index, rowCount } = parseJcrCsv(text);
      totalRows += rowCount;

      // Merge by best quartile per journal per metric.
      for (const [k, v] of Object.entries(index)) {
        const cur = merged[k] || {};
        merged[k] = {
          jifQ: bestQuartile(cur.jifQ, v.jifQ),
          jciQ: bestQuartile(cur.jciQ, v.jciQ),
          aisQ: bestQuartile(cur.aisQ, v.aisQ),
          fiveYJifQ: bestQuartile(cur.fiveYJifQ, v.fiveYJifQ)
        };
      }
    }

    const year = files
      .map((f) => f.name.match(/\b(20\d{2})\b/))
      .map((m) => (m ? m[1] : null))
      .find(Boolean);

    const meta = {
      importedAt: new Date().toISOString(),
      fileCount: files.length,
      rowCount: totalRows,
      journalCount: Object.keys(merged).length,
      year: year || null
    };

    await setQualityJcrIndex(merged, meta);
    el("jcrMeta").textContent = `Imported JCR: ${meta.year || "?"} | files: ${meta.fileCount} | rows: ${
      meta.rowCount
    } | journals: ${meta.journalCount} | updated: ${meta.importedAt}`;
    setJcrStatus("Imported JCR CSVs.");
  } catch (err) {
    setJcrStatus(`Import failed: ${err?.message || String(err)}`);
  } finally {
    e.target.value = "";
  }
});

el("clearJcrIndex").addEventListener("click", async () => {
  await clearQualityJcrIndex();
  el("jcrMeta").textContent = "Imported JCR: none";
  setJcrStatus("Cleared JCR index.");
});

el("clearScimagoQuartiles").addEventListener("click", async () => {
  await clearQualityQuartilesIndex();
  el("scimagoMeta").textContent = "Imported quartiles: none";
  setScimagoStatus("Cleared imported quartiles.");
});

el("loadQualityDefaults").addEventListener("click", async () => {
  try {
    const [ft50, utd24, abdc, core, corePortal] = await Promise.all([
      fetchExtText("src/data/ft50.txt"),
      fetchExtText("src/data/utd24.txt"),
      fetchExtText("src/data/abdc2022.csv"),
      fetchExtText("src/data/core_icore2026.csv"),
      fetchExtText("src/data/core_portal_ranks.csv")
    ]);
    const coreMerged = [core.trim(), (corePortal || "").trim()].filter(Boolean).join("\n");

    // Only fill empty fields to avoid clobbering custom edits.
    if (!el("qualityFt50List").value.trim()) el("qualityFt50List").value = ft50.trim() + "\n";
    if (!el("qualityUtd24List").value.trim()) el("qualityUtd24List").value = utd24.trim() + "\n";
    if (!el("qualityAbdcRanks").value.trim()) el("qualityAbdcRanks").value = abdc.trim() + "\n";
    if (!el("qualityCoreRanks").value.trim()) el("qualityCoreRanks").value = coreMerged + "\n";

    if (!el("showQualityBadges").checked) el("showQualityBadges").checked = true;

    setQualityStatus("Loaded built-in lists (filled empty fields).");
  } catch (err) {
    setQualityStatus(`Load failed: ${err?.message || String(err)}`);
  }
});

el("clearQualityLists").addEventListener("click", async () => {
  el("qualityFt50List").value = "";
  el("qualityUtd24List").value = "";
  el("qualityAbdcRanks").value = "";
  el("qualityQuartiles").value = "";
  el("qualityCoreRanks").value = "";
  setQualityStatus("Cleared (not saved yet).");
});

el("clearHiddenPapers").addEventListener("click", async () => {
  await clearHiddenPapers();
  await renderHiddenLists();
  setStatus("Cleared hidden papers.");
});

el("clearHiddenVenues").addEventListener("click", async () => {
  await clearHiddenVenues();
  await renderHiddenLists();
  setStatus("Cleared hidden venues.");
});

el("clearHiddenAuthors").addEventListener("click", async () => {
  await clearHiddenAuthors();
  await renderHiddenLists();
  setStatus("Cleared hidden authors.");
});

for (const listId of ["hiddenPapersList", "hiddenVenuesList", "hiddenAuthorsList"]) {
  el(listId)?.addEventListener("click", async (e) => {
    const btn = e.target?.closest(".su-unhide");
    if (!btn) return;
    const kind = btn.getAttribute("data-kind");
    const value = btn.getAttribute("data-value");
    if (!kind || value == null) return;
    if (kind === "paper") await removeHiddenPaper(value);
    else if (kind === "venue") await removeHiddenVenue(value);
    else if (kind === "author") await removeHiddenAuthor(value);
    await renderHiddenLists();
    setStatus("Unhidden.");
  });
}

load();
