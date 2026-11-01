/**
 * Zotero-ready exports: RIS and CSL-JSON for a set of export records. The Zotero Connector already
 * saves single papers from a page; this is the bulk version that also carries the user's tags, notes
 * and collections.
 *
 * Recalled from memory of Zotero's importers, not checked against a running Zotero:
 *  - RIS: KW becomes a tag, N1 becomes a child note (Zotero's own RIS export writes notes as one line
 *    of HTML, so notes here are written the same way). Zotero's RIS import has no collection field,
 *    so collections go into a note.
 *  - CSL-JSON: the "note" property is imported into the Extra field, not as a child note, and
 *    "keyword" may not be imported as tags by every Zotero version. The UI says so.
 *  - Preprints use RIS "GEN" and CSL "article" because I am not sure which preprint codes each Zotero
 *    version reads.
 * Private user notes ARE written here on purpose (the user asked for a notes-carrying export). The
 * UI warns that the file contains them.
 */

import { bibtexEntryType } from "./bibtex.js";
import { escapeHtml, splitName } from "./export-model.js";

const RIS_TYPE = { article: "JOUR", inproceedings: "CPAPER", misc: "GEN" };
const CSL_TYPE = { article: "article-journal", inproceedings: "paper-conference", misc: "article" };

function oneLine(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

/** Note text as the single line of HTML Zotero expects: paragraphs split on blank lines, line breaks kept. */
export function noteHtml(text) {
  return String(text || "")
    .split(/\r?\n\s*\r?\n/)
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => `<p>${escapeHtml(p).replace(/\r?\n/g, "<br/>")}</p>`)
    .join("");
}

function highlightsHtml(highlights) {
  if (!highlights.length) return "";
  const items = highlights.map((h) => {
    const tail = [h.page ? `p. ${h.page}` : "", h.note ? `Note: ${h.note}` : ""].filter(Boolean).join(". ");
    return `<blockquote><p>${escapeHtml(h.quote).replace(/\r?\n/g, "<br/>")}</p>${tail ? `<p>${escapeHtml(tail)}</p>` : ""}</blockquote>`;
  });
  return `<p><b>PDF highlights</b></p>${items.join("")}`;
}

function risAuthor(a) {
  const { family, given } = splitName(a);
  return given ? `${family}, ${given}` : family;
}

/** One RIS record (CRLF line ends, ER terminator). */
export function buildRisEntry(r, opts = {}) {
  const type = bibtexEntryType(r.venue);
  const lines = [`TY  - ${RIS_TYPE[type]}`];
  const add = (tag, value) => {
    const v = oneLine(value);
    if (v) lines.push(`${tag}  - ${v}`);
  };
  add("TI", r.title);
  r.authors.forEach((a) => add("AU", risAuthor(a)));
  add("PY", r.year);
  add("T2", r.venue);
  add("VL", r.volume);
  add("IS", r.issue);
  const [sp, ep] = String(r.pages || "").split(/\s*[-–—]+\s*/);
  add("SP", sp);
  add("EP", ep);
  add("PB", r.publisher);
  add("DO", r.doi);
  add("UR", r.url);
  add("AB", r.abstract);
  r.tags.forEach((t) => add("KW", t));
  if (opts.includeNotes !== false) {
    if (r.notes) add("N1", noteHtml(r.notes));
    if (r.summary) add("N1", `<p><b>PDF summary</b></p>${noteHtml(r.summary)}`);
    if (r.highlights.length) add("N1", highlightsHtml(r.highlights));
  }
  if (r.collections.length) add("N1", `<p>Scholar Utility Belt collections: ${escapeHtml(r.collections.join("; "))}</p>`);
  lines.push("ER  - ");
  return lines.join("\r\n");
}

export function buildRis(records, opts = {}) {
  return records.map((r) => buildRisEntry(r, opts)).join("\r\n\r\n") + (records.length ? "\r\n" : "");
}

/** One CSL-JSON item. */
export function buildCslItem(r, opts = {}) {
  const type = bibtexEntryType(r.venue);
  const item = { id: r.citekey || `su-${(r.key || r.title || "item").replace(/[^A-Za-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40)}`, type: CSL_TYPE[type] };
  if (r.title) item.title = oneLine(r.title);
  const authors = r.authors.map((a) => splitName(a)).filter((n) => n.family).map((n) => (n.given ? { family: n.family, given: n.given } : { family: n.family }));
  if (authors.length) item.author = authors;
  const year = parseInt(r.year, 10);
  if (Number.isFinite(year)) item.issued = { "date-parts": [[year]] };
  if (r.venue) item["container-title"] = oneLine(r.venue);
  if (r.doi) item.DOI = r.doi;
  if (r.url) item.URL = r.url;
  if (r.abstract) item.abstract = oneLine(r.abstract);
  if (r.tags.length) item.keyword = r.tags.join(", ");
  const note = [];
  if (opts.includeNotes !== false && r.notes) note.push(r.notes);
  if (r.collections.length) note.push(`Scholar Utility Belt collections: ${r.collections.join("; ")}`);
  if (note.length) item.note = note.join("\n\n");
  return item;
}

export function buildCslJson(records, opts = {}) {
  return JSON.stringify(records.map((r) => buildCslItem(r, opts)), null, 2) + "\n";
}
