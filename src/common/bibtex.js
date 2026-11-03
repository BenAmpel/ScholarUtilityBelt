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
 * @param {string} [e.volume]
 * @param {string} [e.number]   issue
 * @param {string} [e.pages]    "9128-9143" is written as 9128--9143
 * @param {string} [e.publisher]
 */
export function buildBibTeXEntry(e) {
  const type = bibtexEntryType(e.venue);
  const fields = [
    ["title", escapeBibText(e.title)],
    ["author", (e.authors || []).map(escapeBibText).filter(Boolean).join(" and ")],
    ["year", String(e.year || "").replace(/\D/g, "").slice(0, 4)],
    [VENUE_FIELD[type], escapeBibText(e.venue)],
    ["volume", escapeBibText(e.volume)],
    ["number", escapeBibText(e.number)],
    ["pages", escapeBibText(e.pages).replace(/\s*[-–—]+\s*/g, "--")],
    ["publisher", escapeBibText(e.publisher)],
    ["doi", String(e.doi || "").trim()],
    ["url", String(e.url || "").trim()],
    ["keywords", (e.tags || []).map(escapeBibText).filter(Boolean).join(", ")],
    ["abstract", escapeBibText(e.abstract)],
  ].filter(([, v]) => v);
  const body = fields.map(([k, v]) => `  ${k}={${v}}`).join(",\n");
  return `@${type}{${sanitizeBibKey(e.key) || "key"},\n${body}\n}`;
}

// ---------------------------------------------------------------------------
// Author check for Copy BibTeX: Scholar's BibTeX export sometimes drops a trailing author or cuts a surname to
// one letter ("L"). When the result has a DOI, the content script asks Crossref for the author list (on click
// only) and these pure helpers decide whether to offer "Fix authors" and write the corrected field.
// ---------------------------------------------------------------------------

/** Value of the author field in one BibTeX entry (braced or quoted, nested braces kept), or null. */
export function getBibAuthorField(bib) {
  const s = String(bib || "");
  const m = /(^|[,{\s])author\s*=\s*/i.exec(s);
  if (!m) return null;
  let i = m.index + m[0].length;
  const open = s[i];
  if (open === '"') {
    let depth = 0;
    for (let j = i + 1; j < s.length; j++) {
      const c = s[j];
      if (c === "{") depth++;
      else if (c === "}") depth--;
      else if (c === '"' && depth === 0 && s[j - 1] !== "\\") return { value: s.slice(i + 1, j), start: i, end: j + 1 };
    }
    return null;
  }
  if (open !== "{") return null;
  let depth = 0;
  for (let j = i; j < s.length; j++) {
    const c = s[j];
    if (c === "{" && s[j - 1] !== "\\") depth++;
    else if (c === "}" && s[j - 1] !== "\\") {
      depth--;
      if (depth === 0) return { value: s.slice(i + 1, j), start: i, end: j + 1 };
    }
  }
  return null;
}

/** Split a BibTeX author field on " and " outside braces. "others" (BibTeX's "et al.") is dropped. */
export function splitBibAuthors(field) {
  const s = String(field || "");
  const out = [];
  let depth = 0;
  let cur = "";
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "{") depth++;
    else if (c === "}") depth = Math.max(0, depth - 1);
    if (depth === 0 && /\s/.test(c) && /^\s+and\s/i.test(s.slice(i))) {
      out.push(cur);
      cur = "";
      i += s.slice(i).match(/^\s+and/i)[0].length - 1;
      continue;
    }
    cur += c;
  }
  out.push(cur);
  return out.map((a) => a.replace(/\s+/g, " ").trim()).filter((a) => a && a.toLowerCase() !== "others");
}

/** Lower case, accents and LaTeX accent commands removed, letters only: "{\"O}zt{\"u}rk" and "Öztürk" both give "ozturk". */
export function normalizeSurname(s) {
  return String(s || "")
    .replace(/\\([a-zA-Z]+)\s*/g, (_, cmd) => (/^(ss|o|O|l|L|ae|AE|oe|OE|aa|AA|i|j)$/.test(cmd) ? cmd : ""))
    .replace(/\\./g, "")
    .replace(/[øØ]/g, "o").replace(/[łŁ]/g, "l").replace(/ß/g, "ss").replace(/[æÆ]/g, "ae").replace(/[œŒ]/g, "oe")
    .replace(/[đĐ]/g, "d").replace(/ı/g, "i")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
}

/** Family name of one BibTeX author: "Last, First" or "First von Last" (last word). */
export function bibAuthorSurname(name) {
  const s = String(name || "").trim();
  if (!s) return "";
  // A wholly braced name ("{World Health Organization}") is one corporate name.
  if (/^\{.*\}$/.test(s) && !s.includes(",")) return s.slice(1, -1).trim();
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    else if (c === "," && depth === 0) return s.slice(0, i).trim();
  }
  const words = s.split(/\s+(?![^{]*\})/);
  return words[words.length - 1] || "";
}

/** Family name of one Crossref author record ({given, family} or a group {name}). */
function crossrefSurname(a) {
  return String(a?.family || a?.name || "").trim();
}

function surnamesMatch(bibNorm, crNorm) {
  if (!bibNorm || !crNorm) return false;
  if (bibNorm === crNorm) return true;
  // Compound surnames: Scholar may keep one part ("Marquez" for "Garcia Marquez", "Smith" for "Smith-Jones").
  return bibNorm.length >= 3 && (crNorm.startsWith(bibNorm) || crNorm.endsWith(bibNorm));
}

/**
 * Compare the authors in a BibTeX entry with Crossref's author list for the same DOI.
 * Flags the entry when Crossref has more authors, or when a BibTeX surname is cut to a single letter where
 * Crossref has the full name. Returns flagged: false when Crossref has no authors, when the lists agree, or when
 * they share too few surnames to be the same paper (a wrong DOI must never rewrite the authors).
 *
 * @param {string} bib  one BibTeX entry
 * @param {Array<{given?:string,family?:string,name?:string}>} crossrefAuthors  message.author from api.crossref.org/works/{doi}
 * @returns {{flagged:boolean, reason:string, bibCount:number, crossrefCount:number, missing:number, truncated:string[]}}
 */
export function checkBibAuthorsAgainstCrossref(bib, crossrefAuthors) {
  const field = getBibAuthorField(bib);
  const bibNames = splitBibAuthors(field ? field.value : "");
  const cr = (Array.isArray(crossrefAuthors) ? crossrefAuthors : []).filter((a) => normalizeSurname(crossrefSurname(a)));
  const result = { flagged: false, reason: "ok", bibCount: bibNames.length, crossrefCount: cr.length, missing: 0, truncated: [] };
  if (!cr.length) return { ...result, reason: "no-crossref-authors" };
  const crNorm = cr.map((a) => normalizeSurname(crossrefSurname(a)));
  const bibNorm = bibNames.map((n) => normalizeSurname(bibAuthorSurname(n)));

  // Same paper? Count the BibTeX surnames (two letters or more) that Crossref also lists.
  const full = bibNorm.filter((n) => n.length >= 2);
  const shared = full.filter((n) => crNorm.some((c) => surnamesMatch(n, c))).length;
  if (full.length && shared < Math.ceil(full.length / 2)) return { ...result, reason: "different-authors" };
  if (!full.length && bibNorm.length && !bibNorm.every((n, i) => crNorm[i] && crNorm[i][0] === n[0])) {
    return { ...result, reason: "different-authors" };
  }

  // Surnames cut to one letter: matched by position, and only when Crossref's name there starts with that letter.
  bibNorm.forEach((n, i) => {
    if (n.length === 1 && crNorm[i] && crNorm[i].length > 1 && crNorm[i][0] === n) result.truncated.push(bibNames[i]);
  });
  result.missing = Math.max(0, cr.length - bibNames.length);
  if (result.missing > 0 || result.truncated.length) {
    result.flagged = true;
    result.reason = result.missing > 0 && result.truncated.length ? "missing-and-truncated" : result.missing > 0 ? "missing" : "truncated";
  }
  return result;
}

/** Crossref's author list as a BibTeX author field: "Family, Given and ...", group names braced. */
export function formatCrossrefAuthors(crossrefAuthors) {
  return (Array.isArray(crossrefAuthors) ? crossrefAuthors : [])
    .map((a) => {
      const family = escapeBibText(a?.family || "");
      const given = escapeBibText(a?.given || "");
      if (family) return given ? `${family}, ${given}` : family;
      const name = escapeBibText(a?.name || "");
      return name ? `{${name}}` : "";
    })
    .filter(Boolean)
    .join(" and ");
}

/** The entry with its author field replaced by Crossref's list (added after the key line if it had none). */
export function fixBibAuthors(bib, crossrefAuthors) {
  const s = String(bib || "");
  const authors = formatCrossrefAuthors(crossrefAuthors);
  if (!authors) return s;
  const field = getBibAuthorField(s);
  if (field) return `${s.slice(0, field.start)}{${authors}}${s.slice(field.end)}`;
  const firstComma = s.indexOf(",");
  if (firstComma < 0) return s;
  return `${s.slice(0, firstComma + 1)}\n  author={${authors}},${s.slice(firstComma + 1)}`;
}
