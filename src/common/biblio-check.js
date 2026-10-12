// Bibliography checker: pure helpers. No DOM, no chrome.* APIs, no network
// unless the caller passes a fetch function (see crossrefLookup).
//
// What this module does: splits a pasted reference list (numbered, APA-like
// or BibTeX) into entries, pulls out DOIs, guesses the venue, and compares
// the result with data the extension already ships (retraction Bloom filter,
// venue quality indexes). It reports what those datasets contain and nothing
// else. It has no list of "predatory" venues and makes no such claim.

import { isPreprintVenue, qualityBadgesForVenue } from "./quality.js";

export const MAX_INPUT_CHARS = 2_000_000;
export const MAX_ENTRIES = 3000;

// ---------------------------------------------------------------------------
// DOI extraction
// ---------------------------------------------------------------------------

const DOI_RE = /10\.\d{4,9}\/[^\s"'<>]+/gi;

function stripTrailingDoiJunk(raw) {
  let s = String(raw || "");
  for (;;) {
    const before = s;
    s = s.replace(/[.,;:'"\]}>*_]+$/, "");
    // A closing parenthesis belongs to the DOI only if there is a matching "(".
    if (s.endsWith(")")) {
      const open = (s.match(/\(/g) || []).length;
      const close = (s.match(/\)/g) || []).length;
      if (close > open) s = s.slice(0, -1);
    }
    if (s === before) break;
  }
  return s;
}

/** All distinct DOIs in `text`, lower-cased, in order of appearance. */
export function extractDois(text) {
  const out = [];
  const seen = new Set();
  const src = String(text || "");
  for (const m of src.matchAll(DOI_RE)) {
    let d = stripTrailingDoiJunk(m[0]);
    try {
      if (/%[0-9a-f]{2}/i.test(d)) d = decodeURIComponent(d);
    } catch (_) {
      // keep the raw text
    }
    d = d.toLowerCase();
    if (!/^10\.\d{4,9}\/\S+$/.test(d)) continue;
    if (!seen.has(d)) {
      seen.add(d);
      out.push(d);
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// BibTeX parsing
// ---------------------------------------------------------------------------

function findClosingBrace(text, openIdx) {
  const open = text[openIdx];
  const close = open === "(" ? ")" : "}";
  let depth = 0;
  for (let i = openIdx; i < text.length; i++) {
    const ch = text[i];
    if (ch === "\\") { i++; continue; }
    if (ch === open) depth++;
    else if (ch === close) {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}

function cleanBibValue(v) {
  return String(v || "")
    .replace(/[{}]/g, "")
    .replace(/\\&/g, "&")
    .replace(/\\([#%_$])/g, "$1")
    .replace(/~/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseBibFields(body) {
  const fields = {};
  let i = 0;
  const n = body.length;
  while (i < n) {
    // advance to the next field name
    const search = /([A-Za-z][A-Za-z0-9_:-]*)\s*=\s*/g;
    search.lastIndex = i;
    const hit = search.exec(body);
    if (!hit) break;
    const name = hit[1].toLowerCase();
    let j = hit.index + hit[0].length;
    let value;
    if (body[j] === "{") {
      const end = findClosingBrace(body, j);
      if (end < 0) { value = body.slice(j + 1); j = n; }
      else { value = body.slice(j + 1, end); j = end + 1; }
    } else if (body[j] === '"') {
      let k = j + 1;
      while (k < n && !(body[k] === '"' && body[k - 1] !== "\\")) k++;
      value = body.slice(j + 1, k);
      j = k + 1;
    } else {
      let k = j;
      while (k < n && body[k] !== ",") k++;
      value = body.slice(j, k);
      j = k;
    }
    // "a # b" concatenation is rare; keep the text as is.
    fields[name] = cleanBibValue(value);
    i = j;
  }
  return fields;
}

export function looksLikeBibtex(text) {
  return /^\s*@[A-Za-z]+\s*[{(]/m.test(String(text || ""));
}

/** Parse BibTeX source into entries. Ignores @comment, @string, @preamble. */
export function parseBibtex(text) {
  const src = String(text || "");
  const entries = [];
  const re = /@([A-Za-z]+)\s*([{(])/g;
  let m;
  while ((m = re.exec(src))) {
    const type = m[1].toLowerCase();
    const openIdx = m.index + m[0].length - 1;
    const closeIdx = findClosingBrace(src, openIdx);
    const end = closeIdx < 0 ? src.length : closeIdx;
    re.lastIndex = closeIdx < 0 ? src.length : closeIdx + 1;
    if (type === "comment" || type === "string" || type === "preamble") continue;
    const body = src.slice(openIdx + 1, end);
    const comma = body.indexOf(",");
    const key = (comma < 0 ? body : body.slice(0, comma)).trim();
    const fields = comma < 0 ? {} : parseBibFields(body.slice(comma + 1));
    const raw = src.slice(m.index, closeIdx < 0 ? src.length : closeIdx + 1).trim();
    entries.push({ format: "bibtex", raw, key, type, fields });
  }
  return entries;
}

// ---------------------------------------------------------------------------
// Plain-text splitting
// ---------------------------------------------------------------------------

const NUMBER_MARKER = /^\s*(?:\[\d{1,4}\]|\(\d{1,4}\)|\d{1,4}[.)])\s+\S/;
const NUMBER_PREFIX = /^\s*(?:\[\d{1,4}\]|\(\d{1,4}\)|\d{1,4}[.)])\s+/;
// "Surname, A." or "Surname, Given" at the start of a line.
const AUTHOR_START = /^[\p{Lu}][\p{L}'’-]+(?:\s[\p{L}'’-]+)?,\s+(?:\p{Lu}\.|\p{Lu}\p{L}+)/u;
const YEAR_ANYWHERE = /\b(?:19|20)\d{2}[a-z]?\b/;

function joinLines(lines) {
  return lines.map((l) => l.trim()).filter(Boolean).join(" ").replace(/\s+/g, " ").trim();
}

/**
 * Split pasted plain text into reference strings. Tolerant, not exact:
 *  1. if at least two lines start with a number marker ([1], 1., (1)), split there;
 *  2. else if the text has blank-line separated blocks, one block per entry;
 *  3. else one entry per line, joining a wrapped line to the previous entry
 *     until that entry contains a year.
 */
export function splitReferences(text) {
  const src = String(text || "").replace(/\r\n?/g, "\n").replace(/\u00a0/g, " ");
  const lines = src.split("\n");
  const numbered = lines.filter((l) => NUMBER_MARKER.test(l)).length;
  let chunks = [];
  if (numbered >= 2) {
    let cur = null;
    for (const line of lines) {
      if (!line.trim()) continue;
      if (NUMBER_MARKER.test(line)) {
        if (cur) chunks.push(cur);
        cur = [line.replace(NUMBER_PREFIX, "")];
      } else if (cur) cur.push(line);
      // text before the first marker is dropped (a heading such as "References")
    }
    if (cur) chunks.push(cur);
    chunks = chunks.map(joinLines);
  } else {
    const blocks = src.split(/\n\s*\n/).map((b) => b.split("\n").filter((l) => l.trim())).filter((b) => b.length);
    if (blocks.length >= 2) {
      chunks = blocks.map(joinLines);
    } else {
      let cur = [];
      const flush = () => { if (cur.length) chunks.push(joinLines(cur)); cur = []; };
      for (const line of lines) {
        if (!line.trim()) continue;
        const startsNew = !/^\s/.test(line) && AUTHOR_START.test(line.trim());
        const curHasYear = cur.length > 0 && YEAR_ANYWHERE.test(cur.join(" "));
        if (cur.length === 0) cur.push(line);
        else if (startsNew && curHasYear) { flush(); cur.push(line); }
        else cur.push(line);
      }
      flush();
    }
  }
  return chunks.map((c) => c.trim()).filter((c) => c.length >= 8);
}

// ---------------------------------------------------------------------------
// Field extraction from a plain reference
// ---------------------------------------------------------------------------

function stripMarkup(s) {
  return String(s || "").replace(/[*_]{1,2}/g, "").replace(/<\/?[a-z][^>]*>/gi, "");
}

function splitSentences(s) {
  // Split at ". " before an upper-case letter, digit or opening quote, but not
  // after a single initial ("J. R.") or common abbreviations.
  const parts = [];
  let last = 0;
  const re = /\.\s+(?=[\p{Lu}0-9"“(])/gu;
  let m;
  while ((m = re.exec(s))) {
    const before = s.slice(last, m.index);
    const rawWord = before.split(/\s+/).pop() || "";
    const lastWord = rawWord.replace(/^\(/, "");
    const beforeLast = before.slice(0, before.length - rawWord.length).trimEnd();
    const afterInitial = /[,.]$/.test(beforeLast) || /(?:^|\s)In$/.test(beforeLast);
    if ((/^\p{Lu}$/u.test(lastWord) && afterInitial) || /^(?:vol|no|pp|ed|eds|al|st|dr|jr|sr)$/i.test(lastWord)) continue;
    parts.push(before.trim());
    last = m.index + m[0].length;
  }
  const tail = s.slice(last).trim();
  if (tail) parts.push(tail);
  return parts.filter(Boolean);
}

function cleanVenue(v) {
  let s = stripMarkup(v).trim();
  s = s.replace(/^in\s*[:.]?\s+/i, "");
  // Editors then book title: "A. Editor (Ed.), Book title (pp. 1-2)".
  const ed = s.match(/\(\s*eds?\.?\s*\)\s*,?\s*(.+)$/i);
  if (ed) s = ed[1];
  s = s.replace(/\s*\(\s*pp?\.?\s*[\d–-]+.*$/i, "");
  // Cut at volume, issue, pages or a bare number that follows a comma.
  s = s.replace(/\s*[,;:]\s*(?:vol\.?|volume|no\.?|issue|pp?\.?|article|art\.)\s*\d.*$/i, "");
  s = s.replace(/\s*[,;]\s*\d.*$/, "");
  s = s.replace(/\s+\d+\s*\(\d+\).*$/, "");
  s = s.replace(/\s+\d{1,4}\s*[:;]\s*\d.*$/, "");
  s = s.replace(/^["“”'\s]+|["“”'\s,.;:]+$/g, "").trim();
  if (s.length < 3 || !/\p{L}{3}/u.test(s)) return "";
  if (s.length > 200) return "";
  return s;
}

/**
 * Best-effort extraction of {title, year, venue} from one plain-text reference.
 * Returns empty strings for anything that cannot be read with some confidence.
 */
export function parsePlainReference(raw) {
  let text = stripMarkup(String(raw || "")).replace(/\s+/g, " ").trim();
  text = text.replace(NUMBER_PREFIX, "");
  const stripped = text
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/\bdoi\s*:?\s*10\.\S+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  let year;
  let title = "";
  let venue = "";

  // IEEE-like: authors, "Title," Venue, vol. 3, pp. 1-2, 2020.
  const quoted = stripped.match(/["“]([^"”]{6,}?)[,.]?["”]\s*,?\s*(.*)$/);
  const apa = stripped.match(/\(\s*((?:19|20)\d{2})[a-z]?(?:\s*,\s*[A-Za-z]+\.?(?:\s+\d+)?)?\s*\)\s*\.?\s*(.+)$/);

  if (quoted && !apa) {
    title = quoted[1].trim();
    venue = cleanVenue(quoted[2].replace(/^(?:in\s+)/i, ""));
    const y = stripped.match(/\b((?:19|20)\d{2})[a-z]?\b/g);
    year = y ? y[y.length - 1].slice(0, 4) : "";
  } else if (apa) {
    year = apa[1];
    const sents = splitSentences(apa[2]);
    title = (sents[0] || "").replace(/[.]+$/, "");
    if (sents.length >= 2) venue = cleanVenue(sents[1]);
    // "Title. In Book" where the venue sentence was swallowed by the title.
    if (!venue && sents.length === 1) venue = "";
  } else {
    const sents = splitSentences(stripped);
    const yIdx = sents.findIndex((s, i) => i >= 2 && /^(?:19|20)\d{2}\b/.test(s));
    const inlineIdx = sents.findIndex((s, i) => i >= 2 && /^.{3,}?\s(?:19|20)\d{2}[a-z]?\s*[;:,.]/.test(s));
    if (yIdx >= 2) {
      title = sents[yIdx - 2];
      venue = cleanVenue(sents[yIdx - 1]);
      year = sents[yIdx].slice(0, 4);
    } else if (inlineIdx >= 2) {
      const m = sents[inlineIdx].match(/^(.{3,}?)\s((?:19|20)\d{2})[a-z]?\s*[;:,.]/);
      title = sents[inlineIdx - 1];
      venue = cleanVenue(m ? m[1] : "");
      year = m ? m[2] : "";
    } else {
      const y = stripped.match(/\b((?:19|20)\d{2})[a-z]?\b/);
      year = y ? y[1] : "";
    }
  }
  return { title: title.trim(), year, venue };
}

/** Turn a raw plain string or a parsed BibTeX entry into a common entry shape. */
export function toEntry(item, index) {
  if (item && item.format === "bibtex") {
    const f = item.fields || {};
    const venue = f.journal || f.journaltitle || f.booktitle || "";
    const doiField = extractDois(f.doi || "").concat(extractDois(f.url || ""));
    const doi = (doiField[0] || extractDois(item.raw)[0] || "");
    const year = (f.year || f.date || "").match(/(?:19|20)\d{2}/)?.[0] || "";
    const archive = `${f.archiveprefix || ""} ${f.eprinttype || ""} ${f.eprint ? "eprint" : ""}`.trim();
    return {
      index,
      format: "bibtex",
      raw: item.raw,
      key: item.key || "",
      type: item.type || "",
      title: f.title || "",
      year,
      venue,
      doi,
      eprintHint: archive,
      venueNote: venue ? "" : (item.type === "book" || item.type === "misc" || item.type === "techreport" ? `no journal or booktitle field (entry type @${item.type})` : "no journal or booktitle field")
    };
  }
  const raw = String(item || "");
  const parsed = parsePlainReference(raw);
  return {
    index,
    format: "text",
    raw,
    key: "",
    type: "",
    title: parsed.title,
    year: parsed.year,
    venue: parsed.venue,
    doi: extractDois(raw)[0] || "",
    eprintHint: "",
    venueNote: parsed.venue ? "" : "could not read a venue from this reference"
  };
}

/** Split any supported input into entries (BibTeX is detected automatically). */
export function parseReferenceList(text) {
  const src = String(text || "").slice(0, MAX_INPUT_CHARS);
  const items = looksLikeBibtex(src) ? parseBibtex(src) : splitReferences(src);
  return items.slice(0, MAX_ENTRIES).map((it, i) => toEntry(it, i + 1));
}

// ---------------------------------------------------------------------------
// Preprints and duplicates
// ---------------------------------------------------------------------------

// DOI prefixes registered to preprint servers. From recall, not from a dataset
// shipped in this extension: 10.48550 arXiv, 10.1101 bioRxiv and medRxiv,
// 10.2139 SSRN, 10.31219 OSF Preprints, 10.20944 Preprints.org,
// 10.31234 PsyArXiv, 10.31235 SocArXiv. Text patterns in quality.js cover the rest.
const PREPRINT_DOI_PREFIXES = [
  ["10.48550/", "arXiv"],
  ["10.1101/", "bioRxiv or medRxiv"],
  ["10.2139/", "SSRN"],
  ["10.31219/", "OSF Preprints"],
  ["10.20944/", "Preprints.org"],
  ["10.31234/", "PsyArXiv"],
  ["10.31235/", "SocArXiv"]
];

/** Returns a server label, or "" when nothing indicates a preprint server. */
export function detectPreprint(entry) {
  const doi = String(entry?.doi || "").toLowerCase();
  for (const [prefix, label] of PREPRINT_DOI_PREFIXES) {
    if (doi.startsWith(prefix)) return label;
  }
  if (/arxiv/i.test(entry?.eprintHint || "")) return "arXiv";
  const venue = String(entry?.venue || "");
  if (venue && isPreprintVenue(venue)) {
    const b = qualityBadgesForVenue(venue, emptyIndex()).find((x) => x.kind === "preprint");
    return b ? b.text : "preprint server";
  }
  const raw = String(entry?.raw || "");
  if (/arxiv\.org|\barxiv\b|ssrn\.com|\bssrn\b|biorxiv|medrxiv|researchsquare|osf\.io\/preprints|preprints\.org/i.test(raw)) {
    const b = qualityBadgesForVenue(raw, emptyIndex()).find((x) => x.kind === "preprint");
    return b ? b.text : "preprint server";
  }
  return "";
}

function emptyIndex() {
  return {
    ft50: new Set(), utd24: new Set(), era: new Set(), abdc: new Map(), vhb: new Map(),
    fnege: new Map(), quartiles: new Map(), core: new Map(), ccf: new Map(), jcr: new Map(),
    impact: new Map(), norwegian: new Map(), abs: new Map(), h5: new Map()
  };
}

function titleKey(title) {
  const k = String(title || "").toLowerCase().normalize("NFKD").replace(/[̀-ͯ]/g, "").replace(/[^a-z0-9]+/g, " ").trim();
  return k.length >= 15 ? k : "";
}

/** Mark entries that repeat an earlier DOI or title. Returns Map(index -> {of, reason}). */
export function findDuplicates(entries) {
  const byDoi = new Map();
  const byTitle = new Map();
  const dups = new Map();
  for (const e of entries) {
    const d = (e.doi || "").toLowerCase();
    const t = titleKey(e.title);
    if (d && byDoi.has(d)) { dups.set(e.index, { of: byDoi.get(d), reason: "same DOI" }); continue; }
    if (t && byTitle.has(t)) { dups.set(e.index, { of: byTitle.get(t), reason: "same title" }); }
    if (d && !byDoi.has(d)) byDoi.set(d, e.index);
    if (t && !byTitle.has(t)) byTitle.set(t, e.index);
  }
  return dups;
}

// ---------------------------------------------------------------------------
// Local checks
// ---------------------------------------------------------------------------

const TIER_KINDS = new Set(["ft50", "utd24", "abdc", "abs", "vhb", "fnege", "quartile", "jcr", "core", "ccf", "era", "norwegian", "preprint", "findings"]);

/** What the shipped retraction filter is, in words, from its own metadata. */
export function describeRetractionFilter(bloom) {
  if (!bloom || !bloom.m || !bloom.k) {
    return { available: false, text: "The retraction filter could not be loaded, so no retraction check was run." };
  }
  const n = Number(bloom.count) || 0;
  const fp = n > 0 ? Math.pow(1 - Math.exp(-(bloom.k * n) / bloom.m), bloom.k) : null;
  const parts = [];
  parts.push(`Retraction check uses a filter built from ${bloom.source || "a Retraction Watch export"}${bloom.built ? ` on ${bloom.built}` : ""}${n ? `, covering ${n.toLocaleString("en-US")} DOIs` : ""}.`);
  parts.push("Retractions recorded after that date are not in it.");
  if (fp != null) parts.push(`A positive match can be a false alarm (about ${(fp * 100).toFixed(1)} in 100 DOIs, by calculation from the filter size), so confirm it at Crossref or Retraction Watch.`);
  parts.push("A negative result is not proof that a paper has not been retracted.");
  return { available: true, built: bloom.built || "", count: n, falsePositiveRate: fp, source: bloom.source || "", text: parts.join(" ") };
}

/**
 * Run the local checks.
 * ctx: { qIndex, bloom, bloomHasDoi } where qIndex comes from compileQualityIndex.
 */
export function checkReferences(entries, ctx = {}) {
  const { qIndex, bloom, bloomHasDoi } = ctx;
  const dups = findDuplicates(entries);
  const items = entries.map((e) => {
    const flags = [];
    let retraction = "no-doi";
    if (e.doi) {
      if (!bloom || typeof bloomHasDoi !== "function") retraction = "filter-unavailable";
      else retraction = bloomHasDoi(e.doi, bloom) ? "flagged" : "not-flagged";
    }
    let badges = [];
    let venueStatus = "unparsed";
    if (e.venue) {
      venueStatus = "parsed";
      if (qIndex) {
        try { badges = qualityBadgesForVenue(e.venue, qIndex); } catch (_) { badges = []; }
      }
    }
    const preprint = detectPreprint(e);
    const dup = dups.get(e.index) || null;
    if (!e.doi) flags.push("no DOI");
    if (retraction === "flagged") flags.push("matches retraction filter");
    if (preprint) flags.push(`preprint server (${preprint})`);
    if (dup) flags.push(`possible duplicate of entry ${dup.of} (${dup.reason})`);
    if (venueStatus === "unparsed") flags.push("venue not read");
    const tierBadges = badges.filter((b) => TIER_KINDS.has(b.kind)).map((b) => b.text);
    return {
      ...e,
      retraction,
      venueStatus,
      badges: badges.map((b) => ({ kind: b.kind, text: b.text, system: b.metadata?.system || "" })),
      tierBadges,
      listed: tierBadges.length > 0,
      preprint,
      duplicateOf: dup ? dup.of : null,
      duplicateReason: dup ? dup.reason : "",
      flags,
      crossref: null
    };
  });
  return { items, summary: summarize(items), filter: describeRetractionFilter(bloom) };
}

export function summarize(items) {
  const total = items.length;
  const tierMix = new Map();
  let withDoi = 0, flagged = 0, preprints = 0, dupCount = 0, venueParsed = 0, listed = 0, crossrefMissing = 0, crossrefRetracted = 0;
  const attention = [];
  for (const it of items) {
    if (it.doi) withDoi++;
    if (it.retraction === "flagged") flagged++;
    if (it.preprint) preprints++;
    if (it.duplicateOf != null) dupCount++;
    if (it.venueStatus === "parsed") venueParsed++;
    if (it.listed) listed++;
    for (const t of new Set(it.tierBadges)) tierMix.set(t, (tierMix.get(t) || 0) + 1);
    const reasons = [];
    if (it.retraction === "flagged") reasons.push("matches the retraction filter");
    if (it.crossref?.status === "ok" && it.crossref.retractionLike) { crossrefRetracted++; reasons.push(`Crossref lists an update of type ${it.crossref.updateTypes.join(", ")}`); }
    if (it.crossref?.status === "not-found") { crossrefMissing++; reasons.push("DOI not found at Crossref"); }
    if (it.duplicateOf != null) reasons.push(`possible duplicate of entry ${it.duplicateOf}`);
    if (reasons.length) attention.push({ index: it.index, reasons });
  }
  return {
    total,
    withDoi,
    withoutDoi: total - withDoi,
    retractionFlagged: flagged,
    preprints,
    duplicates: dupCount,
    venueParsed,
    venueUnparsed: total - venueParsed,
    listedInAnyIndex: listed,
    crossrefNotFound: crossrefMissing,
    crossrefUpdatesRetraction: crossrefRetracted,
    tierMix: [...tierMix.entries()].map(([label, count]) => ({ label, count })).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label)),
    attention
  };
}

// ---------------------------------------------------------------------------
// Optional Crossref lookup (only called after an explicit click)
// ---------------------------------------------------------------------------

/**
 * Look one DOI up at Crossref. `fetchFn` is injected so tests can mock it.
 * Field names ("updated-by", "type") are from recall of the public Crossref
 * REST API; the result reports update types as given and does not interpret more.
 */
export async function crossrefLookup(doi, fetchFn) {
  const url = `https://api.crossref.org/works/${encodeURIComponent(doi)}`;
  let res;
  try {
    res = await fetchFn(url, { headers: { Accept: "application/json" } });
  } catch (e) {
    return { status: "error", message: String(e?.message || e) };
  }
  if (res.status === 404) return { status: "not-found" };
  if (res.status === 429 || res.status === 503) return { status: "rate-limited" };
  if (!res.ok) return { status: "error", message: `HTTP ${res.status}` };
  let data;
  try { data = await res.json(); } catch (_) { return { status: "error", message: "unreadable response" }; }
  const msg = data?.message || {};
  const updates = Array.isArray(msg["updated-by"]) ? msg["updated-by"] : [];
  const updateTypes = [...new Set(updates.map((u) => String(u?.type || "update")))];
  return {
    status: "ok",
    title: Array.isArray(msg.title) ? String(msg.title[0] || "") : "",
    container: Array.isArray(msg["container-title"]) ? String(msg["container-title"][0] || "") : "",
    updateTypes,
    retractionLike: updateTypes.some((t) => /retract|withdraw/i.test(t))
  };
}

/**
 * Check many DOIs one at a time with a pause between requests, so the load on
 * Crossref stays low. Stops early on a rate-limit answer. Returns Map(doi -> result).
 */
export async function crossrefLookupAll(dois, opts = {}) {
  const { fetchFn, delayMs = 1000, sleep = (ms) => new Promise((r) => setTimeout(r, ms)), shouldCancel = () => false, onProgress = () => {}, max = 200 } = opts;
  const unique = [...new Set(dois.map((d) => String(d).toLowerCase()))].slice(0, max);
  const out = new Map();
  let stopped = "";
  for (let i = 0; i < unique.length; i++) {
    if (shouldCancel()) { stopped = "cancelled"; break; }
    const r = await crossrefLookup(unique[i], fetchFn);
    out.set(unique[i], r);
    onProgress(i + 1, unique.length, unique[i], r);
    if (r.status === "rate-limited") { stopped = "rate-limited"; break; }
    if (i < unique.length - 1) await sleep(delayMs);
  }
  return { results: out, stopped, requested: unique.length, skippedOverCap: Math.max(0, new Set(dois.map((d) => String(d).toLowerCase())).size - unique.length) };
}

/** Attach Crossref results to checked items (in place on a copy) and refresh the summary. */
export function applyCrossref(report, results) {
  const items = report.items.map((it) => {
    const r = it.doi ? results.get(it.doi.toLowerCase()) : null;
    return r ? { ...it, crossref: r } : it;
  });
  return { ...report, items, summary: summarize(items) };
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

function csvCell(v) {
  let s = v == null ? "" : String(v);
  // Stop spreadsheet programs from running text that starts like a formula.
  if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const RETRACTION_TEXT = {
  "flagged": "matches retraction filter (confirm at Crossref or Retraction Watch)",
  "not-flagged": "no match in filter (not proof of no retraction)",
  "no-doi": "not checked (no DOI)",
  "filter-unavailable": "not checked (filter not loaded)"
};

export function retractionText(status) {
  return RETRACTION_TEXT[status] || status;
}

export function resultsToCsv(report) {
  const head = ["entry", "reference", "doi", "retraction_filter", "venue_read", "venue_quality_badges", "preprint_server", "duplicate_of", "crossref", "flags"];
  const rows = [head.join(",")];
  for (const it of report.items) {
    const cr = it.crossref
      ? (it.crossref.status === "ok"
        ? `found${it.crossref.updateTypes.length ? `; updates: ${it.crossref.updateTypes.join(", ")}` : ""}`
        : it.crossref.status)
      : "";
    rows.push([
      it.index,
      it.raw,
      it.doi || "",
      retractionText(it.retraction),
      it.venue || (it.venueNote ? `(${it.venueNote})` : ""),
      it.tierBadges.join("; "),
      it.preprint || "",
      it.duplicateOf != null ? `${it.duplicateOf} (${it.duplicateReason})` : "",
      cr,
      it.flags.join("; ")
    ].map(csvCell).join(","));
  }
  return rows.join("\r\n") + "\r\n";
}
