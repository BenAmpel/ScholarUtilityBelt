/**
 * Shared, pure helpers for the library exports. library.js owns the Scholar-specific lookups
 * (venue extraction, quality badges, collection names) and passes them in, so everything here
 * is testable in Node without a DOM or chrome.* APIs.
 *
 * An "export record" is a flat, normalized view of one saved paper. Every builder in
 * src/common/*-export.js, latex-pack.js and reference-list.js takes records, never raw saved papers.
 */

/**
 * "A Smith, B Jones - Venue, 2019 - Publisher" -> ["A Smith", "B Jones"]. Drops Scholar's "…" truncation marker.
 * The author block ends at the first spaced dash, so hyphenated names ("Jean-Paul Sartre") stay whole.
 */
export function parseAuthors(authorsVenue) {
  const raw = String(authorsVenue || "").split(/\s+[-\u2013\u2014]\s+/)[0] || "";
  return raw
    .split(/\s*,\s*|\s+and\s+/i)
    .map((a) => a.replace(/[…]|\.\.\./g, "").trim())
    .filter(Boolean);
}

export function lastName(author) {
  const parts = String(author || "").trim().split(/\s+/);
  return parts.length && parts[parts.length - 1] ? parts[parts.length - 1] : "Unknown";
}

function str(v) {
  return v == null ? "" : String(v).trim();
}

/** Stored highlights that have a quote, as { quote, page, note }. */
export function highlightsOf(paper) {
  const list = paper && paper.pdfNotes && Array.isArray(paper.pdfNotes.highlights) ? paper.pdfNotes.highlights : [];
  return list
    .map((h) => ({ quote: str(h && h.quote), page: str(h && h.page), note: str(h && h.note) }))
    .filter((h) => h.quote);
}

/** DOI without a resolver prefix, or "" when it does not look like a DOI. */
export function cleanDoi(doi) {
  const d = str(doi).replace(/^(?:https?:\/\/(?:dx\.)?doi\.org\/|doi:\s*)/i, "");
  return /^10\.\d{4,9}\/\S+$/.test(d) ? d : "";
}

/**
 * @param {object} paper saved paper as stored by the extension
 * @param {object} ctx
 * @param {Map<string,{name:string}>|Map<string,string>} [ctx.collections] id -> collection (or name)
 * @param {(paper:object)=>string} [ctx.venueOf] venue when the paper has none of its own
 * @param {(venue:string)=>string[]} [ctx.badgesOf] quality badge labels for a venue
 */
export function toRecord(paper, ctx = {}) {
  const p = paper || {};
  const venue = str(p.venue) || str(ctx.venueOf ? ctx.venueOf(p) : "");
  const colls = [];
  for (const id of Array.isArray(p.collections) ? p.collections : []) {
    const c = ctx.collections && ctx.collections.get(id);
    const name = str(c && typeof c === "object" ? c.name : c);
    if (name && !colls.includes(name)) colls.push(name);
  }
  let badges;
  try { badges = ctx.badgesOf && venue ? ctx.badgesOf(venue) || [] : []; } catch (_) { badges = []; }
  const year = str(p.year).replace(/\D/g, "").slice(0, 4);
  return {
    key: str(p.key),
    title: str(p.title).replace(/\s+/g, " "),
    authors: parseAuthors(p.authorsVenue),
    year,
    venue,
    doi: cleanDoi(p.doi),
    url: str(p.url),
    abstract: str(p.abstract),
    tags: (Array.isArray(p.tags) ? p.tags : []).map(str).filter(Boolean),
    notes: typeof p.notes === "string" ? p.notes.trim() : "",
    summary: str(p.pdfNotes && p.pdfNotes.summary),
    collections: colls,
    highlights: highlightsOf(p),
    badges: (Array.isArray(badges) ? badges : []).map(str).filter(Boolean),
    citekey: str(p.citekey),
  };
}

/** Papers of one collection, or all of them when collectionId is empty. Accepts the stored map or an array. */
export function selectPapers(saved, collectionId = "") {
  const list = Array.isArray(saved) ? saved : Object.values(saved || {});
  return list.filter((p) => p && (!collectionId || (Array.isArray(p.collections) && p.collections.includes(collectionId))));
}

/** Stable order for output: first author, year, title, then storage key. */
export function sortRecords(records) {
  const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  return [...records].sort((a, b) =>
    cmp(lastName(a.authors[0] || "~").toLowerCase(), lastName(b.authors[0] || "~").toLowerCase()) ||
    cmp(a.year, b.year) ||
    cmp(a.title.toLowerCase(), b.title.toLowerCase()) ||
    cmp(a.key, b.key));
}

/** ASCII lower-case slug, at most `max` characters, never empty. Cuts at a word boundary when it can. */
export function slugify(text, max = 60) {
  let s = String(text || "")
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (s.length > max) {
    const cut = s.slice(0, max);
    const at = cut.lastIndexOf("-");
    s = (at > max * 0.5 ? cut.slice(0, at) : cut).replace(/-+$/g, "");
  }
  return s || "untitled";
}

/** YYYY-MM-DD in UTC. */
export function isoDate(now = new Date()) {
  return new Date(now).toISOString().slice(0, 10);
}

export function escapeHtml(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

const NAME_PARTICLES = new Set(["van", "von", "de", "der", "den", "di", "da", "del", "della", "du", "dos", "das", "la", "le", "bin", "ibn", "al", "el", "ter", "ten"]);

/**
 * Splits a Scholar-style author ("A Smith", "AB van der Berg", "Smith") into family and given parts.
 * Scholar lists initials first and the family name last. Lower-case particles before the last word stay
 * with the family name. A single word is treated as a family name. This is a heuristic: names written
 * "Family, Given" are handled, but Scholar rarely produces them.
 */
export function splitName(author) {
  const raw = String(author || "").replace(/\s+/g, " ").trim();
  if (!raw) return { family: "", given: "" };
  if (raw.includes(",")) {
    const [family, ...rest] = raw.split(",");
    return { family: family.trim(), given: rest.join(" ").trim() };
  }
  const words = raw.split(" ");
  if (words.length === 1) return { family: words[0], given: "" };
  let start = words.length - 1;
  while (start > 1 && NAME_PARTICLES.has(words[start - 1].toLowerCase())) start--;
  return { family: words.slice(start).join(" "), given: words.slice(0, start).join(" ") };
}
