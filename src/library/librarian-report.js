// Librarian batch report page (Pro). Names typed here go to api.openalex.org, only after the user
// presses "Find authors". Google Scholar is never fetched. The matching and report logic lives in
// src/common/openalex-authors.js.
import { showProPrompt } from "../common/pro-prompt.js";
import { getSettings } from "../common/storage.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import { buildReport, parseNameList, reportToCsv, searchAuthors } from "../common/openalex-authors.js";

const el = (id) => document.getElementById(id);

let rows = [];
let reportRows = [];
let cancelled = false;

function node(tag, text, className) {
  const n = document.createElement(tag);
  if (text != null) n.textContent = String(text);
  if (className) n.className = className;
  return n;
}

const fmt = (v) => (v === null || v === undefined ? "n/a" : String(v));

async function apiKey() {
  try {
    const s = await getSettings();
    return (s && s.openalexApiKey) || "";
  } catch {
    return "";
  }
}

function describeCandidate(c) {
  const parts = [];
  if (c.institution) parts.push(c.institution);
  parts.push(`${fmt(c.worksCount)} works`, `${fmt(c.citedBy)} citations`, `h-index ${fmt(c.hIndex)}`);
  if (c.topics.length) parts.push(`topics: ${c.topics.join(", ")}`);
  return parts.join(" | ");
}

function renderMatches() {
  const host = el("matchList");
  host.textContent = "";
  rows.forEach((row, i) => {
    const fs = document.createElement("fieldset");
    fs.className = "match-row";
    fs.appendChild(node("legend", `${row.entry.index}. ${row.entry.raw}`));
    if (row.status === "error" || row.status === "skipped") {
      fs.appendChild(node("p", row.status === "skipped" ? "Not searched (stopped)." : row.error, "match-error"));
    } else if (row.status === "none") {
      fs.appendChild(node("p", "OpenAlex returned no author for this line.", "match-error"));
    } else {
      const addOption = (value, labelNode, checked) => {
        const label = document.createElement("label");
        label.className = "match-opt";
        const input = document.createElement("input");
        input.type = "radio";
        input.name = `match-${i}`;
        input.value = String(value);
        input.checked = checked;
        input.addEventListener("change", updateCount);
        label.append(input, labelNode);
        fs.appendChild(label);
      };
      row.candidates.forEach((c, k) => {
        const text = document.createElement("span");
        const name = node("b", c.name || "(no name)");
        text.appendChild(name);
        if (c.match === "orcid") text.appendChild(node("span", "ORCID matches exactly", "match-tag"));
        else if (c.match === "suggested") text.appendChild(node("span", "Suggested, not confirmed", "match-tag"));
        text.appendChild(document.createElement("br"));
        text.appendChild(node("span", describeCandidate(c), "match-meta"));
        addOption(k, text, row.confirmedId === c.id);
      });
      addOption("none", node("span", "None of these"), !row.confirmedId);
    }
    host.appendChild(fs);
  });
  el("matches").hidden = false;
  updateCount();
}

function confirmed() {
  const out = [];
  rows.forEach((row, i) => {
    const picked = document.querySelector(`input[name="match-${i}"]:checked`);
    if (picked && picked.value !== "none" && row.candidates[Number(picked.value)]) {
      out.push({ entry: row.entry, candidate: row.candidates[Number(picked.value)] });
    }
  });
  return out;
}

function updateCount() {
  const n = confirmed().length;
  el("confirmCount").textContent = `${n} confirmed.`;
  el("buildReport").disabled = n === 0;
}

function setRunning(running) {
  el("findAuthors").disabled = running;
  el("buildReport").disabled = running || confirmed().length === 0;
  el("cancelRun").hidden = !running;
}

async function findAuthors() {
  const { entries, truncated } = parseNameList(el("nameText").value);
  const status = el("status");
  if (!entries.length) { status.textContent = "Type at least one name."; return; }
  cancelled = false;
  setRunning(true);
  el("results").hidden = true;
  reportRows = [];
  const key = await apiKey();
  rows = await searchAuthors(entries, {
    fetchFn: (url, init) => fetch(url, init),
    apiKey: key,
    shouldCancel: () => cancelled,
    onProgress: (i, n) => { status.textContent = `Searched ${i} of ${n}...`; },
  });
  renderMatches();
  const notes = [`${rows.filter((r) => r.status === "ok").length} of ${entries.length} lines have candidates.`];
  if (truncated) notes.push(`${truncated} more ${truncated === 1 ? "line was" : "lines were"} ignored (limit 50).`);
  if (cancelled) notes.push("Stopped.");
  status.textContent = notes.join(" ");
  setRunning(false);
}

function renderReport() {
  const body = el("reportTable").querySelector("tbody");
  body.textContent = "";
  for (const r of reportRows) {
    const tr = document.createElement("tr");
    const cells = [
      [r.typed, "wrap"], [`${r.name} (${r.openalexId.replace(/^https?:\/\/openalex\.org\//, "")})`, "wrap"], [r.institution || "n/a", "wrap"],
      [fmt(r.worksCount), "num"], [fmt(r.citedBy), "num"], [fmt(r.hIndex), "num"],
      [r.meanCitedness2y === null ? "n/a" : r.meanCitedness2y.toFixed(2), "num"], [fmt(r.worksLastFiveYears), "num"],
      [r.venueError ? `Venues not loaded: ${r.venueError}` : (r.topVenues.map((v) => `${v.name} (${v.count})`).join("; ") || "none listed"), "wrap"],
    ];
    for (const [text, cls] of cells) tr.appendChild(node("td", text, cls));
    body.appendChild(tr);
  }
  el("results").hidden = false;
}

async function runReport() {
  const list = confirmed();
  if (!list.length) return;
  cancelled = false;
  setRunning(true);
  const key = await apiKey();
  const status = el("status");
  reportRows = await buildReport(list, {
    fetchFn: (url, init) => fetch(url, init),
    apiKey: key,
    shouldCancel: () => cancelled,
    onProgress: (i, n) => { status.textContent = `Loaded ${i} of ${n}...`; },
  });
  renderReport();
  status.textContent = `Report for ${reportRows.length} confirmed ${reportRows.length === 1 ? "person" : "people"}.${cancelled ? " Stopped." : ""}`;
  setRunning(false);
}

function download(name, mime, content) {
  const url = URL.createObjectURL(new Blob([content], { type: mime }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function init() {
  const allowed = await getEntitlementStatus({ needPurchased: true })
    .then((e) => canUseFeature("librarianReport", e))
    .catch(() => false);
  if (!allowed) {
    el("lockedCard").hidden = false;
    el("unlockPro").addEventListener("click", () => showProPrompt({ featureId: "librarianReport" }));
    return;
  }
  el("toolBody").hidden = false;
  el("findAuthors").addEventListener("click", findAuthors);
  el("buildReport").addEventListener("click", runReport);
  el("cancelRun").addEventListener("click", () => { cancelled = true; });
  el("exportCsv").addEventListener("click", () => {
    if (reportRows.length) download("librarian-report.csv", "text/csv;charset=utf-8", reportToCsv(reportRows));
  });
}

init();
