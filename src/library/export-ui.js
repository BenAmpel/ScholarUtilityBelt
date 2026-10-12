/**
 * Wiring for the library page's Export group. Building logic lives in src/common/*; this file only
 * reads the scope, runs a builder, and downloads or copies the result.
 *
 * Pro items carry data-pro="<feature id>". When the feature is locked the button shows an Unlock
 * label and opens the upsell modal instead of exporting.
 */

import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import { selectPapers, sortRecords } from "../common/export-model.js";
import { buildVaultFiles } from "../common/obsidian-export.js";
import { buildZip } from "../common/zip-writer.js";
import { buildCslJson, buildRis } from "../common/zotero-export.js";
import { buildNotesHtml, buildNotesMarkdown } from "../common/notes-export.js";

export function downloadBlob(filename, blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function downloadText(filename, text, mime = "text/plain") {
  downloadBlob(filename, new Blob([text], { type: mime }));
}

function fileStamp(now = new Date()) {
  return new Date(now).toISOString().slice(0, 10);
}

function scopeSlug(name) {
  return String(name || "all").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 40) || "all";
}

/**
 * @param {object} deps
 * @param {(id:string)=>HTMLElement|null} deps.el
 * @param {()=>Promise<object>} deps.getSavedPapers
 * @param {()=>Promise<object>} deps.loadLibraryState
 * @param {(paper:object, state:object)=>object} deps.toRecord
 * @param {()=>Promise<void>} [deps.prepare] called before records are built (loads the quality index)
 * @param {(record:object)=>string} [deps.cite] citation formatter for the notes document
 */
export function createExportUi(deps) {
  const { el } = deps;
  const proAllowed = {};

  function setStatus(message, isError = false) {
    const node = el("exportStatus");
    if (!node) return;
    node.textContent = message || "";
    node.classList.toggle("is-error", !!isError);
  }

  async function currentScope() {
    const state = await deps.loadLibraryState();
    const id = el("exportScope") ? el("exportScope").value : "";
    const coll = (state.collections || []).find((c) => c.id === id);
    return { state, collectionId: coll ? coll.id : "", name: coll ? coll.name : "All saved papers" };
  }

  /** Papers in the chosen scope, as saved papers (not records). */
  async function selectedPapers() {
    const { collectionId } = await currentScope();
    return selectPapers(await deps.getSavedPapers(), collectionId);
  }

  /** Records for the chosen scope in a stable order. */
  async function selectedRecords() {
    const scope = await currentScope();
    if (deps.prepare) await deps.prepare();
    const papers = selectPapers(await deps.getSavedPapers(), scope.collectionId);
    return { scope, papers, records: sortRecords(papers.map((p) => deps.toRecord(p, scope.state))) };
  }

  function refreshScope(state) {
    const select = el("exportScope");
    if (!select) return;
    const prev = select.value;
    select.textContent = "";
    const all = document.createElement("option");
    all.value = "";
    all.textContent = "All saved papers";
    select.appendChild(all);
    for (const c of state.collections || []) {
      const o = document.createElement("option");
      o.value = c.id;
      o.textContent = c.name || "Untitled collection";
      select.appendChild(o);
    }
    select.value = [...select.options].some((o) => o.value === prev) ? prev : "";
  }

  async function refreshLocks() {
    let ent = null;
    try { ent = await getEntitlementStatus({ needPurchased: true }); } catch (_) { ent = null; }
    document.querySelectorAll("[data-pro]").forEach((btn) => {
      const id = btn.getAttribute("data-pro");
      const ok = !!ent && !!canUseFeature(id, ent);
      proAllowed[id] = ok;
      btn.classList.toggle("is-locked", !ok);
      const tag = btn.querySelector(".pro-tag");
      if (tag) tag.textContent = ok ? "Pro" : "Pro: Unlock";
      btn.title = ok ? "" : "Part of Pro. Opens the unlock options.";
    });
  }

  async function requirePro(featureId) {
    await refreshLocks();
    if (proAllowed[featureId]) return true;
    chrome.runtime.sendMessage({ action: "openUpsellModal" });
    setStatus("This export is part of Pro. The unlock options opened in a new tab.");
    return false;
  }

  const actions = {
    async notesMd() {
      const { scope, records } = await selectedRecords();
      if (!records.length) return setStatus("Nothing to export in this scope.");
      downloadText(`highlights-notes-${scopeSlug(scope.name)}-${fileStamp()}.md`,
        buildNotesMarkdown(records, { scope: scope.name, cite: deps.cite }), "text/markdown");
      setStatus(`Exported notes and highlights for ${records.length} ${records.length === 1 ? "paper" : "papers"}.`);
    },
    async notesHtml() {
      const { scope, records } = await selectedRecords();
      if (!records.length) return setStatus("Nothing to export in this scope.");
      downloadText(`highlights-notes-${scopeSlug(scope.name)}-${fileStamp()}.html`,
        buildNotesHtml(records, { scope: scope.name, cite: deps.cite }), "text/html");
      setStatus(`Exported notes and highlights for ${records.length} ${records.length === 1 ? "paper" : "papers"}.`);
    },
    async zoteroRis() {
      const { scope, records } = await selectedRecords();
      if (!records.length) return setStatus("Nothing to export in this scope.");
      downloadText(`zotero-${scopeSlug(scope.name)}-${fileStamp()}.ris`, buildRis(records), "application/x-research-info-systems");
      setStatus(`Exported ${records.length} ${records.length === 1 ? "paper" : "papers"} as RIS, including your notes. In Zotero: File, Import.`);
    },
    async zoteroCsl() {
      const { scope, records } = await selectedRecords();
      if (!records.length) return setStatus("Nothing to export in this scope.");
      downloadText(`zotero-${scopeSlug(scope.name)}-${fileStamp()}.json`, buildCslJson(records), "application/json");
      setStatus(`Exported ${records.length} ${records.length === 1 ? "paper" : "papers"} as CSL-JSON. In Zotero: File, Import.`);
    },
    async obsidianVault() {
      const { scope, records } = await selectedRecords();
      if (!records.length) return setStatus("Nothing to export in this scope.");
      const now = new Date();
      const zip = buildZip(buildVaultFiles(records, { now }), { date: now });
      downloadBlob(`scholar-vault-${scopeSlug(scope.name)}-${fileStamp(now)}.zip`, new Blob([zip], { type: "application/zip" }));
      setStatus(`Exported ${records.length} ${records.length === 1 ? "note" : "notes"} plus an index. Unzip into your vault; exporting again overwrites the same files.`);
    },
  };

  /** Later features register themselves here: register("zoteroRis", fn). */
  function register(name, fn) {
    actions[name] = fn;
  }

  function wire() {
    const group = el("exportGroup");
    if (!group) return;
    group.addEventListener("click", async (e) => {
      const btn = e.target.closest("[data-export]");
      if (!btn || !group.contains(btn)) return;
      const run = actions[btn.getAttribute("data-export")];
      if (!run) return;
      const pro = btn.getAttribute("data-pro");
      try {
        if (pro && !(await requirePro(pro))) return;
        await run();
      } catch (err) {
        setStatus(`Export failed: ${err && err.message ? err.message : "unknown error"}`, true);
      }
    });
    refreshLocks();
    window.addEventListener("focus", refreshLocks);
  }

  return { wire, register, refreshScope, selectedPapers, selectedRecords, setStatus, requirePro };
}
