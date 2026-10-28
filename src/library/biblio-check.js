// Bibliography checker page (Pro). All checks run locally against data shipped
// with the extension. The only network call is the optional Crossref lookup,
// which runs after an explicit click.
import { showProPrompt } from "../common/pro-prompt.js";
import { getQualityJcrIndex, getQualityQuartilesIndex, getSettings } from "../common/storage.js";
import { compileQualityIndex } from "../common/quality.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import {
  applyCrossref,
  checkReferences,
  crossrefLookupAll,
  MAX_INPUT_CHARS,
  parseReferenceList,
  resultsToCsv,
  retractionText
} from "../common/biblio-check.js";
import {
  bloomHasDoi,
  loadEraAndNorwegian,
  loadH5Index,
  loadImpactIndex,
  loadRetractionBloom,
  loadVhbIndex
} from "../content/data-loader.js";

const el = (id) => document.getElementById(id);

let report = null;
let ctx = null;
let cancelCrossref = false;

function node(tag, text, className) {
  const n = document.createElement(tag);
  if (text != null) n.textContent = String(text);
  if (className) n.className = className;
  return n;
}

async function buildContext() {
  if (ctx) return ctx;
  const [settings, quartiles, jcr, eraNorwegian, h5Index, vhbIndex, impactIndex, bloom] = await Promise.all([
    getSettings(),
    getQualityQuartilesIndex(),
    getQualityJcrIndex(),
    loadEraAndNorwegian(),
    loadH5Index(),
    loadVhbIndex(),
    loadImpactIndex(),
    loadRetractionBloom()
  ]);
  const qIndex = compileQualityIndex(settings, {
    quartilesIndex: quartiles.index || {},
    jcrIndex: jcr.index || {},
    vhbIndex,
    impactIndex,
    eraSet: eraNorwegian.eraSet,
    absIndex: eraNorwegian.absIndex,
    norwegianMap: eraNorwegian.norwegianMap,
    h5Index
  });
  ctx = { qIndex, bloom, bloomHasDoi };
  return ctx;
}

function renderSummary(r) {
  const box = el("summary");
  box.textContent = "";
  const s = r.summary;
  const grid = node("div", null, "sum-grid");
  const add = (value, label) => {
    const d = node("div", null, "sum-item");
    d.appendChild(node("b", value));
    d.appendChild(node("span", label));
    grid.appendChild(d);
  };
  add(s.total, "entries read");
  add(s.withDoi, "with a DOI");
  add(s.withoutDoi, "without a DOI");
  add(s.retractionFlagged, "match the retraction filter");
  add(s.listedInAnyIndex, "venue listed in at least one local index");
  add(s.venueUnparsed, "venue could not be read");
  add(s.preprints, "preprint server detected");
  add(s.duplicates, "possible duplicates");
  if (r.items.some((i) => i.crossref)) {
    add(s.crossrefNotFound, "DOI not found at Crossref");
    add(s.crossrefUpdatesRetraction, "Crossref lists a retraction-type update");
  }
  box.appendChild(grid);

  if (s.tierMix.length) {
    box.appendChild(node("h3", "Venue badges found (entries per badge)", "tool-h2"));
    const p = node("p");
    for (const t of s.tierMix) p.appendChild(node("span", `${t.label}: ${t.count}`, "chip"));
    box.appendChild(p);
  }
  box.appendChild(node("h3", "Entries needing attention", "tool-h2"));
  if (!s.attention.length) {
    box.appendChild(node("p", "None by the checks above. This is not a clean bill of health: missing DOIs, unread venues and the limits of the retraction filter still apply.", "tool-note"));
  } else {
    const ul = document.createElement("ul");
    for (const a of s.attention) ul.appendChild(node("li", `Entry ${a.index}: ${a.reasons.join("; ")}`));
    box.appendChild(ul);
  }
}

function renderTable(r) {
  const body = el("entryTable").querySelector("tbody");
  body.textContent = "";
  const attn = new Set(r.summary.attention.map((a) => a.index));
  for (const it of r.items) {
    const tr = document.createElement("tr");
    if (attn.has(it.index)) tr.className = "attn";
    tr.appendChild(node("td", it.index, "num"));
    tr.appendChild(node("td", it.raw, "wrap"));
    tr.appendChild(node("td", it.doi || "none", "wrap"));
    tr.appendChild(node("td", retractionText(it.retraction)));
    tr.appendChild(node("td", it.venue || `not read (${it.venueNote || "no venue found"})`, "wrap"));
    const badgeTd = document.createElement("td");
    if (it.venue && !it.badges.length) badgeTd.textContent = "none listed";
    for (const b of it.badges) {
      const chip = node("span", b.text, "chip");
      if (b.system) chip.title = b.system;
      badgeTd.appendChild(chip);
    }
    tr.appendChild(badgeTd);
    const notes = [...it.flags.filter((f) => f !== "no DOI" && f !== "venue not read" && f !== "matches retraction filter")];
    if (it.crossref) {
      if (it.crossref.status === "ok") notes.push(`Crossref: found${it.crossref.updateTypes.length ? `, updates: ${it.crossref.updateTypes.join(", ")}` : ""}`);
      else notes.push(`Crossref: ${it.crossref.status}`);
    }
    tr.appendChild(node("td", notes.join("; "), "wrap"));
    body.appendChild(tr);
  }
}

function show(r) {
  report = r;
  el("results").hidden = false;
  el("filterNote").textContent = r.filter.text;
  renderSummary(r);
  renderTable(r);
}

async function runCheck() {
  const text = el("refText").value || "";
  const status = el("status");
  if (text.length > MAX_INPUT_CHARS) {
    status.textContent = "That text is too long. Split the list and check it in parts.";
    return;
  }
  const entries = parseReferenceList(text);
  if (!entries.length) {
    status.textContent = "No references were found in that text.";
    el("results").hidden = true;
    report = null;
    return;
  }
  status.textContent = "Checking...";
  try {
    show(checkReferences(entries, await buildContext()));
    status.textContent = `${entries.length} entr${entries.length === 1 ? "y" : "ies"} read.`;
  } catch (e) {
    status.textContent = `Could not run the check: ${e && e.message ? e.message : e}`;
  }
}

async function runCrossref() {
  if (!report) return;
  const dois = report.items.map((i) => i.doi).filter(Boolean);
  const status = el("crossrefStatus");
  if (!dois.length) { status.textContent = "No DOIs in this list."; return; }
  cancelCrossref = false;
  el("runCrossref").disabled = true;
  el("cancelCrossref").hidden = false;
  const out = await crossrefLookupAll(dois, {
    fetchFn: (url, init) => fetch(url, init),
    shouldCancel: () => cancelCrossref,
    onProgress: (i, n) => { status.textContent = `Checked ${i} of ${n}...`; }
  });
  show(applyCrossref(report, out.results));
  const notes = [`Checked ${out.results.size} DOI${out.results.size === 1 ? "" : "s"}.`];
  if (out.stopped === "rate-limited") notes.push("Crossref asked us to slow down, so the lookup stopped. Try again later.");
  if (out.stopped === "cancelled") notes.push("Stopped.");
  if (out.skippedOverCap) notes.push(`${out.skippedOverCap} DOI${out.skippedOverCap === 1 ? " was" : "s were"} not checked (limit 200).`);
  status.textContent = notes.join(" ");
  el("runCrossref").disabled = false;
  el("cancelCrossref").hidden = true;
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
    .then((e) => canUseFeature("bibliographyCheck", e))
    .catch(() => false);
  if (!allowed) {
    el("lockedCard").hidden = false;
    el("unlockPro").addEventListener("click", () => showProPrompt({ featureId: "bibliographyCheck" }));
    return;
  }
  el("toolBody").hidden = false;
  el("runCheck").addEventListener("click", runCheck);
  el("clearAll").addEventListener("click", () => {
    el("refText").value = "";
    el("results").hidden = true;
    el("status").textContent = "";
    report = null;
  });
  el("refFile").addEventListener("change", async (e) => {
    const file = e.target.files && e.target.files[0];
    if (!file) return;
    if (file.size > MAX_INPUT_CHARS) {
      el("status").textContent = "That file is too large (limit about 2 MB).";
    } else {
      el("refText").value = await file.text();
      await runCheck();
    }
    e.target.value = "";
  });
  el("exportCsv").addEventListener("click", () => {
    if (report) download("bibliography-check.csv", "text/csv;charset=utf-8", resultsToCsv(report));
  });
  el("runCrossref").addEventListener("click", runCrossref);
  el("cancelCrossref").addEventListener("click", () => { cancelCrossref = true; });
}

init();
