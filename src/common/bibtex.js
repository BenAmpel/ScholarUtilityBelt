/**
 * Pure BibTeX helpers for the library export. The caller supplies the author list,
 * venue and key (library.js owns the Scholar-specific parsing); this module only
 * decides the entry type and writes valid fields.
 *
 * Private notes are deliberately never written: a .bib is usually shared with
 * co-authors or uploaded to a publisher.
 */

const PREPRINT_RE = /\b(arxiv|biorxiv|medrxiv|chemrxiv|ssrn|preprint|research square|osf|repec|zenodo)\b/i;
const CONFERENCE_RE = /\b(proceedings|proc\.|conference|workshop|symposium|congress|meeting)\b|\b(acl|emnlp|naacl|eacl|neurips|nips|icml|iclr|cvpr|iccv|eccv|aaai|ijcai|kdd|sigir|www|chi|uist|icis|ecis|amcis)\b/i;

/** "article", "inproceedings" or "misc" from the venue string. No venue gives "misc". */
export function bibtexEntryType(venue) {
  const v = String(venue || "").trim();
  if (!v) return "misc";
  if (PREPRINT_RE.test(v)) return "misc";
  if (CONFERENCE_RE.test(v)) return "inproceedings";
  return "article";
}

/** Field name that carries the venue for each entry type. */
const VENUE_FIELD = { article: "journal", inproceedings: "booktitle", misc: "howpublished" };

/** BibTeX breaks on unbalanced braces; keep balanced ones (they protect capitals), drop the rest. */
function balanceBraces(s) {
  let depth = 0;
  for (const ch of s) {
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth < 0) return s.replace(/[{}]/g, "");
    }
  }
  return depth === 0 ? s : s.replace(/[{}]/g, "");
}

/** Text fields: collapse whitespace, balance braces, escape the characters LaTeX treats specially. */
export function escapeBibText(value) {
  const s = balanceBraces(String(value ?? "")).replace(/\s+/g, " ").trim();
  return s.replace(/\\/g, "").replace(/([&%#_$])/g, "\\$1");
}

/** Keys may not contain whitespace or BibTeX delimiters. */
export function sanitizeBibKey(key) {
  return String(key || "").replace(/[\s{},"'#%~\\]/g, "");
}

/**
 * @param {object} e
 * @param {string} e.key
 * @param {string} e.title
 * @param {string[]} e.authors
 * @param {string|number} e.year
 * @param {string} e.venue
 * @param {string} e.doi
 * @param {string} e.url
 * @param {string[]} e.tags   written as keywords
 * @param {string} e.abstract
 */
export function buildBibTeXEntry(e) {
  const type = bibtexEntryType(e.venue);
  const fields = [
    ["title", escapeBibText(e.title)],
    ["author", (e.authors || []).map(escapeBibText).filter(Boolean).join(" and ")],
    ["year", String(e.year || "").replace(/\D/g, "").slice(0, 4)],
    [VENUE_FIELD[type], escapeBibText(e.venue)],
    ["doi", String(e.doi || "").trim()],
    ["url", String(e.url || "").trim()],
    ["keywords", (e.tags || []).map(escapeBibText).filter(Boolean).join(", ")],
    ["abstract", escapeBibText(e.abstract)],
  ].filter(([, v]) => v);
  const body = fields.map(([k, v]) => `  ${k}={${v}}`).join(",\n");
  return `@${type}{${sanitizeBibKey(e.key) || "key"},\n${body}\n}`;
}
