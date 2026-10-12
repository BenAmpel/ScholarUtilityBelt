/**
 * Dialogs for the exports that need more than one click: the LaTeX citation pack and the reference
 * list. Building logic is in src/common/latex-pack.js and reference-list.js; this file reads form
 * fields, calls it, and shows or downloads the result.
 */

import { assignCitekeys, buildBib, buildCiteList, checkCitations } from "../common/latex-pack.js";
import { STYLES, blocksToHtml, blocksToPlain, buildDocHtml, buildReferenceBlocks } from "../common/reference-list.js";
import { buildOverleafMain, overleafDisclosure, submitToOverleaf, MAX_SOURCE_BYTES } from "../common/overleaf.js";
import { downloadText } from "./export-ui.js";

function appendList(host, heading, items, emptyText) {
  const h = document.createElement("h4");
  h.className = "export-result-heading";
  h.textContent = heading;
  host.appendChild(h);
  if (!items.length) {
    const p = document.createElement("p");
    p.className = "export-note";
    p.textContent = emptyText;
    host.appendChild(p);
    return;
  }
  const ul = document.createElement("ul");
  ul.className = "export-result-list";
  for (const text of items) {
    const li = document.createElement("li");
    li.textContent = text;
    ul.appendChild(li);
  }
  host.appendChild(ul);
}

/**
 * @param {ReturnType<import("./export-ui.js").createExportUi>} ui
 * @param {object} deps
 * @param {(id:string)=>HTMLElement|null} deps.el
 * @param {(id:string)=>void} deps.openModal
 * @param {()=>Promise<object>} deps.getSavedPapers
 * @param {(keys:Map<string,string>)=>Promise<number>} deps.saveCitekeys
 * @param {(text:string)=>Promise<void>} deps.copyText
 */
export function initLatexDialog(ui, deps) {
  const { el } = deps;

  /** Records of the chosen scope with citekeys. Persists new keys only when `persist` is true. */
  async function prepare(persist) {
    const { scope, papers, records } = await ui.selectedRecords();
    if (!records.length) return null;
    const inScope = new Set(papers.map((p) => p.key));
    const taken = Object.values(await deps.getSavedPapers()).filter((p) => p.citekey && !inScope.has(p.key)).map((p) => p.citekey);
    const byKey = new Map(records.map((r) => [r.key, r]));
    const { keys, added } = assignCitekeys(papers, (p) => byKey.get(p.key), taken);
    if (persist && added.length) {
      await deps.saveCitekeys(new Map(added.map((a) => [a.key, a.citekey])));
      return prepare(false); // read back what was actually stored
    }
    return { scope, records: records.map((r) => ({ ...r, citekey: r.citekey || keys.get(r.key) || "" })) };
  }

  ui.register("latexPack", async () => {
    const { scope, records } = await ui.selectedRecords();
    el("latexScope").textContent = `${scope.name}: ${records.length} ${records.length === 1 ? "paper" : "papers"}.`;
    el("latexResult").textContent = "";
    el("overleafPanel").hidden = true;
    deps.openModal("latexModal");
  });

  el("latexBib").addEventListener("click", async () => {
    try {
      const data = await prepare(true);
      if (!data) return ui.setStatus("Nothing to export in this scope.");
      downloadText(`library-${new Date().toISOString().slice(0, 10)}.bib`, buildBib(data.records), "application/x-bibtex");
      ui.setStatus(`Exported ${data.records.length} entries. Citation keys are saved on the papers and will not change.`);
    } catch (err) {
      ui.setStatus(`Export failed: ${err && err.message ? err.message : "unknown error"}`, true);
    }
  });

  el("latexCopy").addEventListener("click", async () => {
    try {
      const data = await prepare(true);
      if (!data) return ui.setStatus("Nothing to copy in this scope.");
      await deps.copyText(buildCiteList(data.records));
      ui.setStatus(`Copied a \\cite list with ${data.records.length} keys.`);
    } catch (err) {
      ui.setStatus(`Copy failed: ${err && err.message ? err.message : "unknown error"}`, true);
    }
  });

  // Open in Overleaf: two steps. The first click only builds the file and shows it; nothing is sent
  // until "Send to Overleaf" is clicked.
  let overleafSource = "";
  function overleafPanelClose() {
    overleafSource = "";
    el("overleafPanel").hidden = true;
    el("overleafSource").value = "";
  }
  async function overleafPanelRefresh() {
    const data = await prepare(true);
    if (!data) { overleafPanelClose(); return ui.setStatus("Nothing to send in this scope."); }
    const includeNotes = el("overleafNotes").checked;
    const built = buildOverleafMain(data.records, { includeNotes });
    const list = el("overleafSends");
    list.textContent = "";
    if (!built.ok) {
      overleafSource = "";
      el("overleafSource").value = "";
      el("overleafSend").disabled = true;
      el("overleafSummary").textContent = built.reason === "too-large"
        ? `This scope makes a ${Math.round(built.bytes / 1000)} KB file; the limit here is ${Math.round(MAX_SOURCE_BYTES / 1000)} KB. Choose a smaller collection.`
        : "Nothing to send in this scope.";
      el("overleafPanel").hidden = false;
      return;
    }
    overleafSource = built.source;
    el("overleafSource").value = built.source;
    el("overleafSend").disabled = false;
    el("overleafSummary").textContent = "If you press Send, this is posted to www.overleaf.com in a new tab:";
    for (const text of overleafDisclosure(built, { includeNotes })) {
      const li = document.createElement("li");
      li.textContent = text;
      list.appendChild(li);
    }
    el("overleafPanel").hidden = false;
  }
  el("latexOverleaf").addEventListener("click", async () => {
    try {
      if (!(await ui.requirePro("latexPack"))) return;
      el("overleafNotes").checked = false;
      await overleafPanelRefresh();
    } catch (err) {
      ui.setStatus(`Could not prepare the file: ${err && err.message ? err.message : "unknown error"}`, true);
    }
  });
  el("overleafNotes").addEventListener("change", () => {
    overleafPanelRefresh().catch((err) => ui.setStatus(`Could not prepare the file: ${err && err.message ? err.message : "unknown error"}`, true));
  });
  el("overleafCancel").addEventListener("click", overleafPanelClose);
  el("overleafSend").addEventListener("click", () => {
    if (!overleafSource) return;
    submitToOverleaf(overleafSource, document);
    ui.setStatus("Sent to Overleaf in a new tab. Overleaf's own terms and privacy policy apply to what it received.");
    overleafPanelClose();
  });

  el("latexCheck").addEventListener("click", async () => {
    const host = el("latexResult");
    host.textContent = "";
    const input = el("latexInput").value;
    if (!input.trim()) {
      host.textContent = "Paste .tex text or a list of cite keys first.";
      return;
    }
    const data = await prepare(false);
    if (!data) {
      host.textContent = "There are no saved papers in this scope to check against.";
      return;
    }
    const res = checkCitations(input, data.records);
    const summary = document.createElement("p");
    summary.textContent = `${res.cited.length} distinct ${res.cited.length === 1 ? "key" : "keys"} found in the text.`;
    host.appendChild(summary);
    appendList(host, "Cited keys with no saved paper",
      res.missing.map((m) => (m.suggestion ? `${m.key} (a saved paper has the key ${m.suggestion}; keys are case-sensitive)` : m.key)),
      "None.");
    appendList(host, "Saved papers not cited",
      res.uncited.map((r) => `${r.citekey}: ${r.title || "Untitled"}`),
      res.wildcard ? "\\nocite{*} is present, so every entry counts as cited." : "None.");
    const limits = document.createElement("p");
    limits.className = "export-note";
    limits.textContent = "This is a text scan. It reads \\cite, \\citep, \\citet, \\parencite, \\textcite and similar commands, skips commented lines, and does not follow \\input files or macros. Keys for papers you have not exported yet are shown as they would be assigned.";
    host.appendChild(limits);
  });
}

/**
 * Reference list dialog: style picker, optional quote sheet, preview, rich copy and a .doc download.
 * @param {ReturnType<import("./export-ui.js").createExportUi>} ui
 * @param {object} deps same as initLatexDialog (el, openModal, copyText)
 */
export function initRefListDialog(ui, deps) {
  const { el } = deps;

  async function current() {
    const { scope, records } = await ui.selectedRecords();
    const style = el("refStyle").value;
    const built = buildReferenceBlocks(records, { style, includeQuotes: el("refQuotes").checked });
    return { scope, records, style, blocks: built.blocks };
  }

  async function refreshPreview() {
    const { scope, records, blocks } = await current();
    el("refScope").textContent = `${scope.name}: ${records.length} ${records.length === 1 ? "paper" : "papers"}.`;
    el("refPreview").textContent = records.length ? blocksToPlain(blocks) : "No saved papers in this scope.";
  }

  const select = el("refStyle");
  select.textContent = "";
  for (const [id, def] of Object.entries(STYLES)) {
    const o = document.createElement("option");
    o.value = id;
    o.textContent = def.label;
    select.appendChild(o);
  }
  select.addEventListener("change", refreshPreview);
  el("refQuotes").addEventListener("change", refreshPreview);

  ui.register("wordRefList", async () => {
    await refreshPreview();
    deps.openModal("refModal");
  });

  el("refCopy").addEventListener("click", async () => {
    try {
      const { records, blocks } = await current();
      if (!records.length) return ui.setStatus("Nothing to copy in this scope.");
      const plain = blocksToPlain(blocks);
      const html = `<meta charset="utf-8">${blocksToHtml(blocks)}`;
      if (navigator.clipboard && typeof window.ClipboardItem !== "undefined") {
        await navigator.clipboard.write([new window.ClipboardItem({
          "text/html": new Blob([html], { type: "text/html" }),
          "text/plain": new Blob([plain], { type: "text/plain" }),
        })]);
        ui.setStatus("Copied with formatting. Paste into Word or Google Docs.");
      } else {
        await deps.copyText(plain);
        ui.setStatus("Copied as plain text. This browser did not allow formatted copy, so italics and indents are missing.");
      }
    } catch (err) {
      ui.setStatus(`Copy failed: ${err && err.message ? err.message : "unknown error"}`, true);
    }
  });

  el("refDoc").addEventListener("click", async () => {
    try {
      const { scope, records, style, blocks } = await current();
      if (!records.length) return ui.setStatus("Nothing to export in this scope.");
      downloadText(`references-${style}-${new Date().toISOString().slice(0, 10)}.doc`,
        buildDocHtml(blocks, { title: `${STYLES[style].heading}: ${scope.name}` }), "application/msword");
      ui.setStatus("Downloaded a .doc file. Word opens it directly; Google Docs can import it.");
    } catch (err) {
      ui.setStatus(`Export failed: ${err && err.message ? err.message : "unknown error"}`, true);
    }
  });
}
