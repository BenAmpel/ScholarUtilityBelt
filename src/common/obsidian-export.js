/**
 * Obsidian / Markdown vault export: one .md per paper plus an index note, as a list of files for the
 * zip writer. Pure. File names come from the DOI or the first author, year and title so re-exporting
 * the same library overwrites the same files; when two papers would share a name, both get a short
 * hash of their storage key so the result does not depend on export order.
 *
 * Re-export caveat: a file name derived from the title changes if the title is edited later.
 * DOI-based names do not. The UI says so.
 */

import { cleanDoi, lastName, slugify } from "./export-model.js";

export const VAULT_ROOT = "Scholar Utility Belt";

/** Double-quoted YAML scalar. JSON string syntax is valid YAML; the Unicode line separators are escaped too. */
export function yamlString(value) {
  return JSON.stringify(String(value ?? ""))
    .replace(/\u2028/g, "\\u2028")
    .replace(/\u2029/g, "\\u2029")
    .replace(/\u0085/g, "\\u0085");
}

function yamlList(key, items) {
  if (!items.length) return [`${key}: []`];
  return [`${key}:`, ...items.map((i) => `  - ${yamlString(i)}`)];
}

/** Obsidian tags allow letters, digits, _, - and /, and need one non-digit. Spaces become hyphens. */
export function obsidianTag(tag) {
  const t = String(tag || "")
    .trim()
    .replace(/^#+/, "")
    .replace(/\s+/g, "-")
    .replace(/[^\p{L}\p{N}_\-/]/gu, "")
    .replace(/^[-/]+|[-/]+$/g, "");
  return /\p{L}|_/u.test(t) ? t : "";
}

/** Short stable hash (FNV-1a, base 36, 6 chars). Not cryptographic; only disambiguates file names. */
export function shortHash(text) {
  let h = 0x811c9dc5;
  const s = String(text || "");
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(36).padStart(6, "0").slice(-6);
}

const RESERVED = new Set(["con", "prn", "aux", "nul", "com1", "com2", "com3", "lpt1", "lpt2", "lpt3", "index"]);
const MAX_BASE = 96;

/** File name without extension for one record, before collision handling. */
export function baseFileName(r) {
  const doi = cleanDoi(r.doi);
  let base;
  if (doi) {
    base = "doi-" + doi.toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  } else {
    const author = r.authors.length ? slugify(lastName(r.authors[0]), 20) : "";
    const parts = [author, r.year, r.title ? slugify(r.title, 60) : ""].filter(Boolean);
    base = parts.length ? parts.join("-") : "untitled";
  }
  base = base.slice(0, MAX_BASE).replace(/[-.]+$/g, "");
  if (!base || RESERVED.has(base)) base = `paper-${base || "untitled"}`;
  return base;
}

/** Unique, order-independent names for a set of records: Map(record.key -> base name). */
export function assignFileNames(records) {
  const bases = records.map((r) => baseFileName(r));
  const count = new Map();
  bases.forEach((b) => count.set(b.toLowerCase(), (count.get(b.toLowerCase()) || 0) + 1));
  const names = new Map();
  const used = new Set();
  records.forEach((r, i) => {
    let name = bases[i];
    if (count.get(name.toLowerCase()) > 1) name = `${name.slice(0, MAX_BASE - 7).replace(/[-.]+$/g, "")}-${shortHash(r.key || r.title)}`;
    let n = 2;
    const stem = name;
    while (used.has(name.toLowerCase())) name = `${stem}-${n++}`; // identical storage keys: should not happen
    used.add(name.toLowerCase());
    names.set(r, name);
  });
  return names;
}

function oneLine(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

/** Markdown text for a single paper. */
export function buildPaperNote(r) {
  const fm = ["---", `title: ${yamlString(oneLine(r.title) || "Untitled")}`];
  fm.push(...yamlList("authors", r.authors));
  if (/^\d{4}$/.test(r.year)) fm.push(`year: ${r.year}`);
  if (r.venue) fm.push(`venue: ${yamlString(oneLine(r.venue))}`);
  if (r.doi) fm.push(`doi: ${yamlString(r.doi)}`);
  if (r.url) fm.push(`url: ${yamlString(r.url)}`);
  if (r.citekey) fm.push(`citekey: ${yamlString(r.citekey)}`);
  fm.push(...yamlList("tags", [...new Set(r.tags.map(obsidianTag).filter(Boolean))]));
  fm.push(...yamlList("collections", r.collections));
  fm.push(...yamlList("quality", r.badges));
  fm.push("---", "");

  const body = [`# ${oneLine(r.title) || "Untitled"}`, ""];
  if (r.abstract) body.push("## Abstract", "", r.abstract.trim(), "");
  if (r.notes) body.push("## Notes", "", r.notes.trim(), "");
  if (r.summary) body.push("## Summary", "", r.summary.trim(), "");
  if (r.highlights.length) {
    body.push("## Highlights", "");
    for (const h of r.highlights) {
      body.push(`- > ${oneLine(h.quote)}${h.page ? ` (p. ${oneLine(h.page)})` : ""}`);
      if (h.note) body.push(`  - Note: ${oneLine(h.note)}`);
    }
    body.push("");
  }
  return fm.join("\n") + "\n" + body.join("\n").replace(/\n{3,}/g, "\n\n").trimEnd() + "\n";
}

function wikiLink(name, title) {
  const alias = oneLine(title).replace(/[[\]|#^]/g, " ").replace(/\s+/g, " ").trim();
  return alias ? `[[${name}|${alias}]]` : `[[${name}]]`;
}

function byline(r) {
  const a = r.authors.length ? (r.authors.length > 2 ? `${lastName(r.authors[0])} et al.` : r.authors.map(lastName).join(" and ")) : "";
  return [a, r.year].filter(Boolean).join(", ");
}

/** Index note: papers listed under each collection with wikilinks. */
export function buildIndexNote(records, names, opts = {}) {
  const date = new Date(opts.now || new Date()).toISOString().slice(0, 10);
  const lines = ["---", `title: ${yamlString("Scholar library index")}`, `exported: ${date}`, "---", "", "# Scholar library index", "",
    `${records.length} ${records.length === 1 ? "paper" : "papers"}, exported ${date}. A paper in several collections is listed under each.`, ""];
  const byColl = new Map();
  const none = [];
  for (const r of records) {
    if (!r.collections.length) none.push(r);
    for (const c of r.collections) {
      if (!byColl.has(c)) byColl.set(c, []);
      byColl.get(c).push(r);
    }
  }
  const item = (r) => `- ${wikiLink(names.get(r), r.title || "Untitled")}${byline(r) ? ` (${byline(r)})` : ""}`;
  for (const c of [...byColl.keys()].sort((a, b) => a.localeCompare(b))) {
    lines.push(`## ${oneLine(c).replace(/^#+\s*/, "")}`, "", ...byColl.get(c).map(item), "");
  }
  if (none.length) lines.push("## Not in a collection", "", ...none.map(item), "");
  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trimEnd() + "\n";
}

/** Files for the zip writer. `records` should already be in a stable order (sortRecords). */
export function buildVaultFiles(records, opts = {}) {
  const names = assignFileNames(records);
  const files = records.map((r) => ({ name: `${VAULT_ROOT}/Papers/${names.get(r)}.md`, data: buildPaperNote(r) }));
  files.unshift({ name: `${VAULT_ROOT}/Index.md`, data: buildIndexNote(records, names, opts) });
  return files;
}
