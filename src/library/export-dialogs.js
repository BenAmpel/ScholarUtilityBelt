/**
 * Dialogs for the exports that need more than one click: the LaTeX citation pack and the reference
 * list. Building logic is in src/common/latex-pack.js and reference-list.js; this file reads form
 * fields, calls it, and shows or downloads the result.
 */

import { assignCitekeys, buildBib, buildCiteList, checkCitations } from "../common/latex-pack.js";
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
