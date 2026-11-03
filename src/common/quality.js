// Venue quality index helpers.
// Everything here is local-only; no network calls.

// Big annual CS/ML conferences (NeurIPS, ICML-style "Nth Annual Conference on X")
// spell the ordinal out in their official proceedings title instead of using a
// digit ("The Fortieth Annual Conference on..." not "The 40th Annual..."), and
// it changes every year. Matched as a whole leading word (or "tens ones" pair,
// since non-letters were already turned into spaces above) so it only strips
// the ordinal itself, the same as the digit-ordinal rule below strips "30th".
const ORDINAL_ONES = "first|second|third|fourth|fifth|sixth|seventh|eighth|ninth";
const ORDINAL_TEENS = "tenth|eleventh|twelfth|thirteenth|fourteenth|fifteenth|sixteenth|seventeenth|eighteenth|nineteenth";
const ORDINAL_TENS = "twentieth|thirtieth|fortieth|fiftieth|sixtieth|seventieth|eightieth|ninetieth";
const ORDINAL_TENS_WORD = "twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety";
const LEADING_WORD_ORDINAL_ANNUAL = new RegExp(
  `^(?:${ORDINAL_ONES}|${ORDINAL_TEENS}|${ORDINAL_TENS}|(?:${ORDINAL_TENS_WORD}) (?:${ORDINAL_ONES}))\\s+annual\\s+`
);

// The same venue names come up again and again (in several bundled lists and on every result), so each normalized
// form is remembered for the page's lifetime. Pure function of its input, so a cached answer is always the same.
const NORMALIZED_VENUE_CACHE = new Map();
const NORMALIZED_VENUE_CACHE_MAX = 250000;
export function normalizeVenueName(s) {
  const key = String(s || "");
  const hit = NORMALIZED_VENUE_CACHE.get(key);
  if (hit !== undefined) return hit;
  const t = normalizeVenueNameUncached(key);
  if (NORMALIZED_VENUE_CACHE.size < NORMALIZED_VENUE_CACHE_MAX) NORMALIZED_VENUE_CACHE.set(key, t);
  return t;
}
function normalizeVenueNameUncached(s) {
  let t = String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  // Strip "Proceedings of the" / "Proceedings of" so "Proceedings of the 30th ACM SIGKDD Conference..." matches
  t = t.replace(/^proceedings of the\s+/, "").replace(/^proceedings of\s+/, "").trim();
  // Strip leading "The" so "The China Quarterly" matches "China Quarterly" (e.g. SJR index)
  t = t.replace(/^the\s+/, "").trim();
  // Strip leading ordinal (e.g. "30th ", "1st ") so "30th ACM SIGKDD Conference..." matches "ACM ... Conference..."
  t = t.replace(/^\d+(st|nd|rd|th)\s+/, "").trim();
  // Strip a leading spelled-out ordinal + "annual" (e.g. "Fortieth Annual ", "Thirty Ninth Annual ")
  // so "The Fortieth Annual Conference on Neural Information Processing Systems..." (NeurIPS's own
  // official proceedings title, which spells the ordinal out and changes every year) matches
  // "Conference on Neural Information Processing Systems".
  t = t.replace(LEADING_WORD_ORDINAL_ANNUAL, "").trim();
  // Strip leading year so "2019 IEEE International Conference..." matches "IEEE International Conference..."
  t = t.replace(/^(19|20)\d{2}\s+/, "").trim();
  // Strip trailing page/date fragment ", N-N" or ", N N" so "Conference (AMCIS), 1-10" matches
  t = t.replace(/\s+\d+\s*-\s*\d+$/, "").trim();
  // Strip trailing number pairs (pages, then volume/issue) so "IEEE Internet of Things Journal 7 (9), 9128 - 9143" → "ieee internet of things journal"
  while (/^\S.*\s+\d+\s+\d+$/.test(t)) {
    t = t.replace(/\s+\d+\s+\d+$/, "").trim();
  }
  // Strip trailing single number (e.g. volume "59" in "Hawaii ... 59 516 525" after pages stripped)
  t = t.replace(/\s+\d+$/, "").trim();
  // Strip trailing parenthetical abbreviation (TOIS), (ISR), etc. so "ACM Transactions on Information Systems (TOIS) 27 (2), 1-19" → "acm transactions on information systems"
  const parentheticalAbbrevs = /\s+(?:tois|isr|isj|jmis|misq|ejis|jais|jsis|dss|kais|tkdd|tocs|tods|tis)$/i;
  t = t.replace(parentheticalAbbrevs, "").trim();
  return t;
}

export function extractVenueFromAuthorsVenue(authorsVenue) {
  // Typical Scholar search: "A Author, B Author - Journal Name, 2019 - Publisher"
  // Sometimes: "A Author - 2019 - ..."
  // Author profile pages: "Journal Name 41 (1), 236-265, 2024" or "Journal Name, 2019"
  // Multi-version papers: might have different formatting
  const s = String(authorsVenue || "").trim();
  
  if (!s) return "";
  
  // Remove "Cited by X" text if present
  let cleanText = s.replace(/\s*Cited by \d+\s*/gi, "").trim();
  // Remove "All X versions" text if present
  cleanText = cleanText.replace(/\s*All \d+ versions?\s*/gi, "").trim();
  
  const parts = cleanText.split(" - ").map((p) => p.trim()).filter(Boolean);
  
  // If we have " - " separator, venue is after the first separator
  if (parts.length >= 2) {
    const venueSeg = parts[1];
    // Try to extract venue - handle patterns like:
    // "Journal Name, Year"
    // "Journal Name 41 (1), 236-265, Year"
    // "Journal Name · Publisher"
    const m = venueSeg.match(/^(.+?),\s*(19\d{2}|20\d{2})\b/);
    if (m) {
      let venue = m[1].trim();
      // Remove page numbers if present (e.g., "236-265")
      venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
      // Remove volume/issue numbers - these prevent matching with quality indices
      // Patterns: "41 (1)", "41(1)", "Vol. 41", "Volume 41", "V41", "No. 1", etc. The \b keeps the V of "ICCV 2021",
      // "ECCV 2020" and "WACV 2021" (they read as "ICC", "ECC", "WAC" and took those conferences' ranks).
      venue = venue.replace(/\s*\b(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\b(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, ""); // "41 (1)" or "41(1)"
      venue = venue.replace(/\s*\(\d+\)/g, ""); // "(1)" standalone
      // Remove common publisher suffixes like "· Publisher Name"
      venue = venue.replace(/\s*·\s*[^·]+$/, "").trim();
      return venue;
    }
    // If no year pattern, still try to clean up the venue segment
    let venue = venueSeg.trim();
    // Remove publisher info if present (usually after " · ")
    venue = venue.split(" · ")[0].trim();
    // Remove trailing page numbers
    venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
    // Remove volume/issue numbers
    venue = venue.replace(/\s*\b(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*\b(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, "");
    venue = venue.replace(/\s*\(\d+\)/g, "");
    return venue.trim();
  }
  
  // If no " - " separator, try to extract venue directly
  // Pattern: "Journal Name, Year" or "Journal Name 41 (1), 236-265, Year" or "Journal Name · Publisher"
  if (parts.length === 1) {
    const part = parts[0];
    // Try "Journal Name, Year" or "Journal Name Volume (Issue), Pages, Year" pattern first
    const m = part.match(/^(.+?),\s*(19\d{2}|20\d{2})\b/);
    if (m) {
      let venue = m[1].trim();
      // Remove page numbers if present
      venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
      // Remove volume/issue numbers
      venue = venue.replace(/\s*\b(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\b(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, "");
      venue = venue.replace(/\s*\(\d+\)/g, "");
      // Remove publisher suffixes
      venue = venue.replace(/\s*·\s*[^·]+$/, "").trim();
      return venue;
    }
    // Try "Journal Name · Publisher" pattern
    const publisherMatch = part.match(/^(.+?)\s*·\s*/);
    if (publisherMatch) {
      let venue = publisherMatch[1].trim();
      // Remove page numbers if present
      venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
      // Remove volume/issue numbers
      venue = venue.replace(/\s*\b(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\b(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, "");
      venue = venue.replace(/\s*\(\d+\)/g, "");
      return venue.trim();
    }
    // If no patterns match, try to clean up anyway
    let venue = part.trim();
    // Remove trailing page numbers
    venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
    // Remove volume/issue numbers
    venue = venue.replace(/\s*\b(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*\b(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, "");
    venue = venue.replace(/\s*\(\d+\)/g, "");
    return venue.trim();
  }
  
  return "";
}

function parseLines(s) {
  return String(s || "")
    .split(/\r?\n/g)
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith("#"));
}

function addSynonymsToSet(set, name) {
  for (const syn of String(name || "").split("|")) {
    const n = normalizeVenueName(syn);
    if (n) set.add(n);
  }
}

function addSynonymsToMap(map, name, value) {
  for (const syn of String(name || "").split("|")) {
    const n = normalizeVenueName(syn);
    if (n) map.set(n, String(value || "").trim());
  }
}

// Lists write some journals with a subtitle that Scholar leaves off: ABS has "MIS Quarterly: Management Information
// Systems", Scholar shows "MIS Quarterly". The part before the colon is the name to match on.
function subtitleHead(rawName) {
  const i = String(rawName || "").indexOf(":");
  if (i <= 0) return "";
  const head = normalizeVenueName(String(rawName).slice(0, i));
  return head.includes(" ") || head.length >= 8 ? head : "";
}

export function normalizeVhbRank(raw) {
  let r = String(raw || "").trim().toUpperCase();
  if (!r) return "";
  r = r.replace(/\*/g, "+");
  const m = r.match(/A\+?|[BCDE]/);
  return m ? m[0] : "";
}

export function compileQualityIndex(settings, extra = {}) {
  const ft50 = new Set();
  const utd24 = new Set();
  const abdc = new Map(); // venue -> A*/A/B/C/D
  const vhb = new Map(); // venue -> A+/A/B/C/D/E
  const fnege = new Map(); // venue -> 1*/1/2/3/4
  const quartiles = new Map(); // venue -> Q1/Q2/Q3/Q4
  const core = new Map(); // venue -> A*/A/B/C or similar (includes ICORE)
  const ccf = new Map(); // venue -> A/B/C (CCF rankings)
  const jcr = new Map(); // venue -> { jifQ, jciQ, aisQ, fiveYJifQ, ... }
  const impact = new Map(); // venue -> impact factor (numeric)
  const abs = new Map(); // venue -> 4*/4/3/2/1 (ABS 2024 AJG)

  // map -> (subtitle head -> value, or CONFLICT when two subtitled entries with that head disagree). Added as exact
  // keys at the end, and only where no list entry already has that name.
  const subtitleAliases = new Map();
  const CONFLICT = Symbol("conflict");
  const noteAlias = (map, name, value) => {
    for (const syn of String(name || "").split("|")) {
      const head = subtitleHead(syn);
      if (!head) continue;
      if (!subtitleAliases.has(map)) subtitleAliases.set(map, new Map());
      const pending = subtitleAliases.get(map);
      const v = String(value || "").trim();
      pending.set(head, pending.has(head) && pending.get(head) !== v ? CONFLICT : v);
    }
  };

  const VALID_ABDC = /^A\*?$|^[BCD]$/i;
  const VALID_ABS = /^4\*?$|^[1234]$/i;
  const VALID_VHB = /^A\+?$|^[BCDE]$/i;
  const VALID_FNEGE = /^1\*?$|^[234]$/i;
  const VALID_QUARTILE = /^Q[1-4]$/i;
  const VALID_CORE = /^A\*?$|^[ABC]$/i;
  const VALID_CCF = /^[ABC]$/i;

  for (const line of parseLines(settings.qualityFt50List || "")) addSynonymsToSet(ft50, line);
  for (const line of parseLines(settings.qualityUtd24List || "")) addSynonymsToSet(utd24, line);
  for (const line of parseLines(settings.qualityAbdcRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim();
    if (!name || !rank || !VALID_ABDC.test(rank)) continue;
    addSynonymsToMap(abdc, name, rank);
    noteAlias(abdc, name, rank);
  }
  const extraVhb = extra?.vhbIndex;
  if (extraVhb && typeof extraVhb === "object") {
    const entries = extraVhb instanceof Map ? extraVhb.entries() : Object.entries(extraVhb);
    for (const [name, rank] of entries) {
      const n = normalizeVenueName(name);
      const r = normalizeVhbRank(rank);
      if (n && r && VALID_VHB.test(r)) vhb.set(n, r);
    }
  }

  for (const line of parseLines(settings.qualityVhbRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rankRaw = line.slice(idx + 1).trim();
    const rank = normalizeVhbRank(rankRaw);
    if (!name || !rank || !VALID_VHB.test(rank)) continue;
    addSynonymsToMap(vhb, name, rank);
    noteAlias(vhb, name, rank);
  }

  for (const line of parseLines(settings.qualityFnegeRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim().replace(/\s+/g, "");
    if (!name || !rank || !VALID_FNEGE.test(rank)) continue;
    addSynonymsToMap(fnege, name, rank);
    noteAlias(fnege, name, rank);
  }

  // Large-scale quartiles can be supplied as a prebuilt index (already normalized keys).
  // We load these first, then apply user overrides from settings.qualityQuartiles below.
  // This keeps user-specified values authoritative.
  const extraQuartiles = extra?.quartilesIndex || null;
  if (extraQuartiles && typeof extraQuartiles === "object") {
    for (const [name, q] of Object.entries(extraQuartiles)) {
      const n = normalizeVenueName(name);
      const v = String(q || "").trim();
      if (n && v && VALID_QUARTILE.test(v)) quartiles.set(n, v);
    }
  }

  // MDPI journals are in SJR as "Applied Sciences (Switzerland)", while Scholar writes "Applied Sciences", which is
  // also the name of a different, smaller journal. Where the two quartiles differ the bare name is ambiguous: no
  // quartile, and no near match to either journal's other badges.
  const ambiguousTitles = new Set();
  for (const key of [...quartiles.keys()]) {
    if (!key.endsWith(" switzerland")) continue;
    const bare = key.slice(0, -" switzerland".length);
    if (quartiles.has(bare) && quartiles.get(bare) !== quartiles.get(key)) {
      quartiles.delete(bare);
      ambiguousTitles.add(bare);
    }
  }

  const extraJcr = extra?.jcrIndex || null;
  if (extraJcr && typeof extraJcr === "object") {
    for (const [name, obj] of Object.entries(extraJcr)) {
      const n = normalizeVenueName(name);
      if (!n) continue;
      if (!obj || typeof obj !== "object") continue;
      jcr.set(n, obj);
    }
  }

  const extraImpact = extra?.impactIndex || null;
  if (extraImpact && typeof extraImpact === "object") {
    const entries = extraImpact instanceof Map ? extraImpact.entries() : Object.entries(extraImpact);
    for (const [name, value] of entries) {
      const n = normalizeVenueName(name);
      const num = typeof value === "number" ? value : parseFloat(String(value || "").replace(/[^0-9.]/g, ""));
      if (n && Number.isFinite(num) && num > 0) impact.set(n, num);
    }
  }

  for (const line of parseLines(settings.qualityQuartiles || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const q = line.slice(idx + 1).trim();
    if (!name || !q || !VALID_QUARTILE.test(q)) continue;
    addSynonymsToMap(quartiles, name, q);
    noteAlias(quartiles, name, q);
  }

  for (const line of parseLines(settings.qualityCoreRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim();
    if (!name || !rank || !VALID_CORE.test(rank)) continue;
    addSynonymsToMap(core, name, rank);
    noteAlias(core, name, rank);
  }

  for (const line of parseLines(settings.qualityCcfRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim();
    if (!name || !rank || !VALID_CCF.test(rank)) continue;
    addSynonymsToMap(ccf, name, rank);
    noteAlias(ccf, name, rank);
  }

  // ERA 2023, Norwegian Register, ABS 2024: supplied via extra (loaded from src/data/ in content script)
  const era = extra?.eraSet instanceof Set ? extra.eraSet : new Set();
  const norwegian = extra?.norwegianMap instanceof Map ? extra.norwegianMap : new Map();
  const extraAbs = extra?.absIndex;
  if (extraAbs && typeof extraAbs === "object") {
    const entries = extraAbs instanceof Map ? extraAbs.entries() : Object.entries(extraAbs);
    for (const [name, rank] of entries) {
      const n = normalizeVenueName(name);
      const v = String(rank || "").trim();
      if (n && v && VALID_ABS.test(v)) {
        abs.set(n, v);
        noteAlias(abs, name, v);
      }
    }
  }

  // H5-index (Google Scholar 5-year h-index for venues) from extra.h5Index { normalizedName: number }
  const h5 = new Map();
  const extraH5 = extra?.h5Index;
  if (extraH5 && typeof extraH5 === "object") {
    for (const [name, num] of Object.entries(extraH5)) {
      const n = normalizeVenueName(name);
      const val = typeof num === "number" && num > 0 ? num : 0;
      if (n && val > 0) h5.set(n, val);
    }
  }

  for (const [map, pending] of subtitleAliases) {
    for (const [head, value] of pending) {
      if (value !== CONFLICT && !map.has(head)) map.set(head, value);
    }
  }

  // ERA is a set of titles; the same Map shape lets it share findBestMatchEntry's near-match rules.
  const eraMap = new Map();
  for (const entry of era) eraMap.set(entry, "listed");

  return { ft50, utd24, abdc, vhb, fnege, quartiles, core, ccf, jcr, impact, era, eraMap, norwegian, abs, h5, ambiguousTitles };
}


// Minimum key length when venue "starts with key" so short abbreviations (e.g. "ais") don't match journals like "AIS Transactions on Replication Research"
const MIN_KEY_LENGTH_FOR_PREFIX_MATCH = 12;

/**
 * findBestMatch, returning the [listKey, value] pair so a badge can say which list entry it came from.
 * Two kinds of near match, because Scholar truncates long venue names and lists add subtitles:
 *  - forward: list entries that extend the venue ("mis quarterly" -> "mis quarterly management information systems").
 *    When several entries extend it and their values differ, the venue is ambiguous ("journal of neuroscience" ->
 *    "...nursing" 52 or "...psychology and economics" 31) and no badge beats a wrong one.
 *  - reverse: the venue extends a list entry (a venue line with extra words); the longest such entry wins.
 * `known` (the SJR title index) holds full journal names. When the venue is one of them, an entry that is another of
 * them is a different journal ("acta orthopaedica belgica" is not "acta orthopaedica"), so it is never a match;
 * nor is a shorter entry inside it (reverse), since a complete title with more words is a different journal.
 * A forward match needs a venue name specific enough to be a truncation: one generic word ("computer", or "ieee"
 * from "Proceedings of the IEEE") would match whatever list entry starts with it.
 */
// Words that make a venue line a different, lesser venue than the list entry it starts with: "ACM CCS Workshops",
// "... Companion", "... Doctoral Consortium". A reverse match is never allowed to drop them.
const COMPANION_WORDS = new Set(["workshop", "workshops", "companion", "consortium", "doctoral", "abstracts", "demo",
  "demos", "demonstrations", "poster", "posters", "tutorial", "tutorials", "symposium", "student", "industry", "letters",
  "supplement"]);

// What a reverse match may leave over after the list entry. Allowed: numbers, "special issue", an acronym or short
// code ("journal of marketing research jmr"), or the acronym and year a conference line repeats before a main-program
// track ("... neurips 2026 evaluations and datasets track"). Anything else is another title: "management science and
// engineering" is not "management science", "acm ccs workshops" is not ACM CCS.
function reverseLeftoverOk(leftover) {
  const words = leftover.split(" ").filter(Boolean);
  if (!words.length || words.some((w) => COMPANION_WORDS.has(w))) return false;
  if (words.every((w) => /^\d+$/.test(w))) return true;
  if (words[0] === "special" && words[1] === "issue") return true;
  if (/^[a-z]{2,10}\d{0,2} (?:19|20)\d{2}\b/.test(leftover)) return true;
  return words.length === 1 && words[0].length <= 8 && !["and", "of", "on", "for", "in", "the"].includes(words[0]);
}

// A forward match (the entry starts with the venue) and a reverse match (the venue starts with the entry) both need the
// same first word, so a lookup only has to scan the entries that share the venue's first word instead of the whole list
// (20,000-30,000 names for SJR, ERA, Norwegian and the impact factors). Built once per map on first use; buckets keep the
// map's order, so ties resolve exactly as a full scan would. Values are read from the map, and a map whose size changed
// is indexed again.
const FIRST_WORD_INDEX = new WeakMap();
function firstWord(s) {
  const i = s.indexOf(" ");
  return i < 0 ? s : s.slice(0, i);
}
function keysSharingFirstWord(map, venue) {
  let index = FIRST_WORD_INDEX.get(map);
  if (!index || index.size !== map.size) {
    const buckets = new Map();
    for (const key of map.keys()) {
      const w = firstWord(key);
      const bucket = buckets.get(w);
      if (bucket) bucket.push(key);
      else buckets.set(w, [key]);
    }
    index = { size: map.size, buckets };
    FIRST_WORD_INDEX.set(map, index);
  }
  return index.buckets.get(firstWord(venue)) || [];
}

function findBestMatchEntry(normalizedVenue, map, known = null, { truncated = false, noReverse = false } = {}) {
  // A venue Scholar cut short ("Information Systems …") is not that exact title; only longer entries can be it.
  if (!truncated && map.has(normalizedVenue)) return [normalizedVenue, map.get(normalizedVenue)];
  const venueIsKnownJournal = !truncated && !!known?.has?.(normalizedVenue);
  // A forward match needs at least two words: one word ("sustainability", "communications") is a whole title of its
  // own far more often than the cut-off start of a longer one.
  const venueIsSpecific = normalizedVenue.includes(" ");
  let forward = null;
  let forwardAgrees = true;
  let forwardCount = 0;
  let reverse = null;
  for (const key of keysSharingFirstWord(map, normalizedVenue)) {
    if (!map.has(key)) continue;
    const value = map.get(key);
    if (venueIsKnownJournal && known.has(key)) continue;
    if (key.startsWith(normalizedVenue + " ")) {
      if (!venueIsSpecific) continue;
      forwardCount += 1;
      if (!forward) forward = [key, value];
      else if (String(forward[1]) !== String(value)) forwardAgrees = false;
      else if (key.length < forward[0].length) forward = [key, value];
    } else if (!truncated && !noReverse && !venueIsKnownJournal && normalizedVenue.startsWith(key + " ")) {
      // Require key to be multi-word or long enough so "ais" doesn't match "ais transactions on replication research" (journal)
      if ((key.length >= MIN_KEY_LENGTH_FOR_PREFIX_MATCH || key.includes(" ")) && (!reverse || key.length > reverse[0].length)
        && reverseLeftoverOk(normalizedVenue.slice(key.length + 1))) {
        reverse = [key, value];
      }
    }
  }
  if (forward && forwardAgrees) return [forward[0], forward[1], forwardCount];
  return reverse;
}

/**
 * Badge for the user's own venue list, or null. `custom` is { name, map } where `map` is
 * normalised venue to label. Exact match first, then the same prefix match the built-in
 * lists use. Kept out of compileQualityIndex so editing the list never invalidates its cache.
 */
// Scholar marks a cut-off venue with "…" (or "..."). One at the start means the name's head is missing.
const ELLIPSIS = /…|\.\.\./;
function isTruncatedVenue(venue) {
  return ELLIPSIS.test(String(venue || ""));
}

function cutNameIsAmbiguous(v, known) {
  if (!known?.keys) return false;
  let n = 0;
  for (const key of keysSharingFirstWord(known, v)) if (key === v || key.startsWith(v + " ")) if (++n > 1) return true;
  return false;
}

export function customBadgeForVenue(venue, custom) {
  if (!custom || !custom.map || !custom.map.size) return null;
  const v = normalizeVenueName(venue);
  if (!v) return null;
  // Exact, or a list entry that the venue is the cut-off start of; never a shorter entry inside a longer venue
  // ("Journal of Marketing" on the list is not "Journal of Marketing Research"). A near match says so on hover.
  const entry = findBestMatchEntry(v, custom.map, null, { truncated: isTruncatedVenue(venue), noReverse: true });
  if (!entry) return null;
  const metadata = { system: `Your list: ${custom.name || "My list"}`, listName: custom.name || "My list",
    venueShown: String(venue || "").replace(/\s+/g, " ").trim(), venueMatched: entry[0] };
  if (entry[0] !== v) Object.assign(metadata, { nearMatch: entry[0], nearMatchCount: entry[2] || 1, venueRead: v });
  return { kind: "custom", text: String(entry[1]), metadata };
}

// Pre-print server identifiers; display label used for badge. Checked against raw venue string (case-insensitive).
const PREPRINT_PATTERNS = [
  { pattern: /ssrn\.com|paper\.ssrn|\.ssrn\b|\bssrn\b/i, label: "SSRN" },
  { pattern: /arxiv\.org|\barxiv\b/i, label: "arXiv" },
  { pattern: /biorxiv\.org|\bbiorxiv\b/i, label: "bioRxiv" },
  { pattern: /medrxiv\.org|\bmedrxiv\b/i, label: "medRxiv" },
  { pattern: /research\s*square|researchsquare\.com/i, label: "Research Square" },
  { pattern: /socarxiv\.org|\bsocarxiv\b/i, label: "SocArXiv" },
  { pattern: /psyarxiv\.org|\bpsyarxiv\b/i, label: "PsyArXiv" },
  { pattern: /chemrxiv\.org|\bchemrxiv\b/i, label: "ChemRxiv" },
  { pattern: /edarxiv\.org|\bedarxiv\b/i, label: "EdArXiv" },
  { pattern: /osf\.io\/preprints|osf\s*preprints/i, label: "OSF Preprints" },
  { pattern: /preprints\.org|\bpreprints\.org\b/i, label: "Preprints.org" },
  { pattern: /zenodo\.org/i, label: "Zenodo" },
];

function getPreprintBadge(venue) {
  const s = String(venue || "").toLowerCase();
  for (const { pattern, label } of PREPRINT_PATTERNS) {
    if (pattern.test(s)) return { kind: "preprint", text: label, metadata: { source: label, system: "Pre-print server" } };
  }
  return null;
}

/** True if venue is a preprint server (arXiv, SSRN, etc.). Used for version grouping. */
export function isPreprintVenue(venue) {
  const s = String(venue || "").toLowerCase();
  return PREPRINT_PATTERNS.some(({ pattern }) => pattern.test(s));
}

// The ACL, EMNLP, NAACL and EACL "Findings" volumes are a real, peer-reviewed, ACL
// Anthology-indexed companion track for papers that cleared review but didn't get a
// main-conference presentation slot -- not a preprint, but also not the main-track
// bar the CORE/CCF rank on that conference's name describes. No ranking body scores
// Findings itself, so this is informational only, not a rank claim (same spirit as
// the preprint badge above).
const FINDINGS_LEAD = /^\s*findings of (?:the )?/i;
const FINDINGS_ABBR = /^(?:acl|emnlp|naacl|eacl)\b/i;
// Google Scholar truncates long venue strings to a fixed length with "…", and it
// cuts wherever that budget runs out -- for one paper "Conference on Empirical
// Methods in Natural Language Processing" survives intact, for another ("Findings
// of...", which eats extra characters on "Findings of the ") it's chopped down to
// "Natural Language …" before "Processing" ever appears. So the spelled-out names
// are matched by prefix, the same tolerance findBestMatch uses elsewhere in this
// file, instead of requiring the full phrase.
const FINDINGS_FULL_NAMES = [
  "association for computational linguistics",
  "conference on empirical methods in natural language processing",
  "north american chapter of the association for computational linguistics",
  "european chapter of the association for computational linguistics"
];

function getFindingsBadge(venue) {
  const s = String(venue || "");
  const m = s.match(FINDINGS_LEAD);
  if (!m) return null;
  const rest = normalizeVenueName(s.slice(m[0].length));
  if (!rest) return null;
  const isKnown = FINDINGS_ABBR.test(rest)
    || FINDINGS_FULL_NAMES.some((full) => full.startsWith(rest) || rest.startsWith(full + " ") || rest === full);
  if (!isKnown) return null;
  return { kind: "findings", text: "ACL Findings", metadata: {
    system: "ACL Anthology Findings volume: peer-reviewed and archival, but a companion track, not the main conference program."
  } };
}

export function qualityBadgesForVenue(venue, qIndex) {
  const v = normalizeVenueName(venue);
  if (!v) return [];

  const badges = [];
  let impactBadge = null;
  // Badge kind -> the list entry a prefix (not exact) match landed on, shown on hover so a wrong match is visible.
  const nearMatch = {};
  // "Information Systems …" is the start of a longer name, not the journal Information Systems: no exact matches,
  // only list entries that begin with it. "… Science" has lost its head, so no list badge at all.
  const truncated = isTruncatedVenue(venue);
  // A cut name that several journals start with ("Information Systems …": Information Systems Journal, ... Research,
  // ... Frontiers) could be any of them, even when one list holds only one of them. Then no list badge.
  const headCut = /^\s*(?:…|\.\.\.)/.test(String(venue || "")) || (truncated && cutNameIsAmbiguous(v, qIndex.quartiles))
    || !!qIndex.ambiguousTitles?.has?.(v);
  const exact = (map) => (truncated ? undefined : map?.get?.(v));
  const venueIsKnownTitle = !truncated && !!qIndex.quartiles?.has?.(v);
  const fuzzy = (kind, map) => {
    if (headCut) return null;
    const entry = findBestMatchEntry(v, map, qIndex.quartiles, { truncated });
    if (!entry) return null;
    if (entry[0] !== v) nearMatch[kind] = { key: entry[0], count: entry[2] || 1 };
    return entry[1];
  };

  // Pre-print badge first so it's clearly visible
  const preprint = getPreprintBadge(venue);
  if (preprint) badges.push(preprint);

  const findings = getFindingsBadge(venue);
  if (findings) badges.push(findings);

  // Try exact match first, then fuzzy match for ABDC
  let abdc = exact(qIndex.abdc);
  if (!abdc) {
    abdc = fuzzy("abdc", qIndex.abdc);
  }
  const VALID_ABDC = /^A\*?$|^[BCD]$/i;
  if (abdc && VALID_ABDC.test(String(abdc).trim())) {
    const r = String(abdc).trim().toUpperCase();
    badges.push({ kind: "abdc", text: `ABDC ${r}`, metadata: { rank: r, system: "ABDC Journal Quality List 2025" } });
  }

  let vhb = exact(qIndex.vhb);
  if (!vhb && qIndex.vhb) vhb = fuzzy("vhb", qIndex.vhb);
  const VALID_VHB = /^A\+?$|^[BCDE]$/i;
  if (vhb && VALID_VHB.test(String(vhb).trim())) {
    const r = String(vhb).trim().toUpperCase().replace("*", "+");
    badges.push({ kind: "vhb", text: `VHB ${r}`, metadata: { rank: r, system: "VHB JOURQUAL 2024" } });
  }

  let fnege = exact(qIndex.fnege);
  if (!fnege && qIndex.fnege) fnege = fuzzy("fnege", qIndex.fnege);
  const VALID_FNEGE = /^1\*?$|^[234]$/i;
  if (fnege && VALID_FNEGE.test(String(fnege).trim())) {
    const r = String(fnege).trim().replace(/\s+/g, "");
    badges.push({ kind: "fnege", text: `FNEGE ${r}`, metadata: { rank: r, system: "FNEGE 2025 (via Harzing JQL 72)" } });
  }

  let abs = exact(qIndex.abs);
  if (!abs && qIndex.abs) abs = fuzzy("abs", qIndex.abs);
  const VALID_ABS = /^4\*?$|^[1234]$/i;
  if (abs && VALID_ABS.test(String(abs).trim())) {
    const r = String(abs).trim();
    badges.push({ kind: "abs", text: `ABS ${r}`, metadata: { rank: r, system: "ABS Academic Journal Guide 2024" } });
  }

  const VALID_QUARTILE = /^Q[1-4]$/i;
  const jcr = exact(qIndex.jcr) || null;
  if (jcr) {
    if (jcr.jifQ && VALID_QUARTILE.test(String(jcr.jifQ))) badges.push({ kind: "jcr", text: `JIF ${String(jcr.jifQ).toUpperCase()}`, metadata: { quartile: jcr.jifQ, system: "JCR Journal Impact Factor", jif: jcr.jif, jcrData: jcr } });
    if (jcr.jciQ && VALID_QUARTILE.test(String(jcr.jciQ))) badges.push({ kind: "jcr", text: `JCI ${String(jcr.jciQ).toUpperCase()}`, metadata: { quartile: jcr.jciQ, system: "JCR Journal Citation Indicator", jci: jcr.jci, jcrData: jcr } });
    if (jcr.aisQ && VALID_QUARTILE.test(String(jcr.aisQ))) badges.push({ kind: "jcr", text: `AIS ${String(jcr.aisQ).toUpperCase()}`, metadata: { quartile: jcr.aisQ, system: "JCR Article Influence Score", ais: jcr.ais, jcrData: jcr } });
    if (jcr.fiveYJifQ && VALID_QUARTILE.test(String(jcr.fiveYJifQ)))
      badges.push({ kind: "jcr", text: `5Y ${String(jcr.fiveYJifQ).toUpperCase()}`, metadata: { quartile: jcr.fiveYJifQ, system: "JCR 5-Year Impact Factor", fiveYJif: jcr.fiveYJif, jcrData: jcr } });
  }

  let impact = exact(qIndex.impact);
  if (impact == null && qIndex.impact) impact = fuzzy("if", qIndex.impact);
  const impactNum = typeof impact === "number" ? impact : parseFloat(String(impact || "").replace(/[^0-9.]/g, ""));
  if (Number.isFinite(impactNum) && impactNum > 0) {
    const display = impactNum.toFixed(1);
    impactBadge = { kind: "if", text: `IF ${display}`, metadata: { impact: impactNum, system: "Journal Impact Factor (2024)" } };
  }

  // Resolve CORE and CCF first so we can treat venue as conference when present.
  // ICIS first: the bare acronym "icis" is also another, C-ranked conference in the CORE portal list.
  // TREOs ("Technology, Research, Education, and Opinion") are ICIS's own separate,
  // non-archival 1-2 page extended-abstract track -- not the peer-reviewed main
  // proceedings the CORE A* rank below describes, e.g. "ICIS 2025 TREOS, 98". Workshops and the
  // doctoral consortium are not the main proceedings either.
  const companionTrack = /\b(?:treos?|workshops?|doctoral|consortium)\b/.test(v);
  let core = null;
  // The list entry an alias rule matched, so the popover can say which entry it was ("icis 2024" -> the ICIS entry).
  let coreAliasKey = null;
  const coreAlias = (key) => { const r = qIndex.core.get(key); if (r) coreAliasKey = key; return r; };
  if ((v.startsWith("icis ") || v === "icis") && !companionTrack) {
    core = coreAlias("international conference on information systems");
  }
  if (!core) core = exact(qIndex.core);
  // "42nd International Conference on Information Systems (ICIS), 1-8" → ICIS (avoid matching ISD/other "International Conference on Information Systems *")
  if (!core && v.startsWith("international conference on information systems") && (v === "international conference on information systems" || v.includes(" icis"))) {
    core = coreAlias("international conference on information systems");
  }
  if (!core && v.startsWith("americas") && v.includes("conference on information systems") && !companionTrack) {
    core = coreAlias("americas conference on information systems");
  }
  if (!core && v.startsWith("hawaii international conference on system sciences") && !companionTrack) {
    core = coreAlias("hawaii international conference on system sciences");
  }
  if (!core) {
    core = fuzzy("core", qIndex.core);
  }
  let ccf = exact(qIndex.ccf) || (qIndex.ccf ? fuzzy("ccf", qIndex.ccf) : null);
  const exactQuartile = exact(qIndex.quartiles);
  // Known journal (exact quartile match): don't show CORE/CCF. CORE/CCF are for conferences; a journal can falsely match when a conference name starts with the journal name (e.g. "Information Systems Research" matching "Information Systems Research Seminar in Scandinavia").
  if (exactQuartile) {
    core = null;
    ccf = null;
  }
  const isConference = !!(core || ccf);

  // Quartiles (SJR) are for journals. Skip quartiles when venue is a known conference to avoid false Q1.
  // Don't apply quartile prefix match when venue looks like a workshop (e.g. "IEEE Security and Privacy Workshops" must not match journal "IEEE Security and Privacy").
  const looksLikeWorkshop = /\bworkshops?\b/.test(v);
  if (exactQuartile || !isConference) {
    const q = exactQuartile || (looksLikeWorkshop ? null : fuzzy("quartile", qIndex.quartiles));
    if (q && VALID_QUARTILE.test(String(q))) badges.push({ kind: "quartile", text: String(q).toUpperCase(), metadata: { quartile: q, system: "SCImago Journal Rank 2024, best quartile across the journal's subject categories" } });
  }

  // For sets (ft50, utd24), check exact match and prefix matches
  // FT50 and UTD24: exact match, or a list entry that starts with the venue (a truncated Scholar venue line such as
  // "Production and Operations" for "Production and Operations Management"). Never the reverse: "Journal of Marketing
  // Analytics" is not "Journal of Marketing". A full SCImago title is a journal of its own, so it matches only exactly
  // ("Information Systems", Elsevier, is not "Information Systems Research"), and a forward match needs a venue
  // specific enough to be a truncation, as in findBestMatchEntry ("management" alone is not "Management Science").
  const topListMatch = (set) => {
    if (headCut) return null;
    if (!truncated && set.has(v)) return v;
    if (venueIsKnownTitle) return null;
    if (!v.includes(" ")) return null;
    for (const entry of set) if (entry.startsWith(v + " ")) return entry;
    return null;
  };
  const ft50Entry = topListMatch(qIndex.ft50);
  if (ft50Entry) {
    if (ft50Entry !== v) nearMatch.ft50 = { key: ft50Entry, count: 1 };
    badges.push({ kind: "ft50", text: "FT50", metadata: { system: "Financial Times 50 Top Research Journals" } });
  }
  const utd24Entry = topListMatch(qIndex.utd24);
  if (utd24Entry) {
    if (utd24Entry !== v) nearMatch.utd24 = { key: utd24Entry, count: 1 };
    badges.push({ kind: "utd24", text: "UTD24", metadata: { system: "UT Dallas Top 100 Business School Research Rankings" } });
  }

  const VALID_CORE = /^A\*?$|^[ABC]$/i;
  const VALID_CCF = /^[ABC]$/i;
  if (core && VALID_CORE.test(String(core).trim())) badges.push({ kind: "core", text: `CORE ${String(core).trim().toUpperCase()}`, metadata: { rank: String(core).trim().toUpperCase(), system: "CORE Conference Rankings (ICORE2026; the older CORE portal rank where ICORE2026 has none)" } });
  if (ccf && VALID_CCF.test(String(ccf).trim())) badges.push({ kind: "ccf", text: `CCF ${String(ccf).trim().toUpperCase()}`, metadata: { rank: String(ccf).trim().toUpperCase(), system: "China Computer Federation Conference Rankings" } });

  // ERA's ~26,000 titles include many short generic ones ("Findings", "Analysis"); findBestMatchEntry's guards keep
  // "Findings of the Conference on Empirical Methods..." (ACL's Findings track) off the unrelated journal "Findings".
  if ((!truncated && qIndex.era?.has?.(v)) || (qIndex.eraMap && fuzzy("era", qIndex.eraMap))) {
    badges.push({ kind: "era", text: "ERA 2023", metadata: { system: "Excellence in Research for Australia 2023" } });
  }

  let norLevel = exact(qIndex.norwegian);
  if (norLevel == null && qIndex.norwegian) norLevel = fuzzy("norwegian", qIndex.norwegian);
  if (norLevel === "1" || norLevel === "2") badges.push({ kind: "norwegian", text: `Level ${norLevel}`, metadata: { level: norLevel, system: "Norwegian Register for Scientific Journals" } });

  let h5Val = exact(qIndex.h5);
  if (h5Val == null && qIndex.h5) h5Val = fuzzy("h5", qIndex.h5);
  if (typeof h5Val === "number" && h5Val > 0) {
    badges.push({ kind: "h5", text: `h5: ${h5Val}`, metadata: { h5: h5Val, system: "Google Scholar Metrics 2025, 5-year h-index" } });
  }

  if (impactBadge) badges.push(impactBadge);
  const venueShown = String(venue || "").replace(/\s+/g, " ").trim();
  for (const b of badges) {
    // What Scholar showed and which list entry it matched, for the popover's "Scholar shows" / "Matched in the list".
    if (b.metadata && BADGE_SOURCE_BY_KIND[b.kind]) {
      b.metadata.venueShown = venueShown;
      b.metadata.venueMatched = nearMatch[b.kind]?.key || (b.kind === "core" && coreAliasKey) || v;
    }
    if (nearMatch[b.kind] && b.metadata) {
      // Vetted: the venue is a full SCImago title, so findBestMatchEntry has already ruled out every other known
      // journal; what is left is the same title with a subtitle or acronym ("journal of social issues jsi").
      Object.assign(b.metadata, { nearMatch: nearMatch[b.kind].key, nearMatchCount: nearMatch[b.kind].count, venueRead: v, nearMatchVetted: venueIsKnownTitle });
    }
  }
  return badges;
}

// One row per bundled list: the Options table "Where the built-in badges come from" and the badge popover both read
// this, so the list name and edition a user sees in the two places cannot disagree. `edition` is the edition of the
// bundled file; the SJR year is read from the file's own meta where it is known (options table, imported files).
export const BADGE_SOURCES = [
  { kind: "quartile", badge: "Q1–Q4", name: "SCImago Journal Rank", list: "SCImago Journal Rank (SJR)", edition: "2024", file: "scimago_2024_quartiles.json", url: "https://www.scimagojr.com/journalrank.php" },
  { kind: "abdc", badge: "ABDC", name: "ABDC Journal Quality List 2025", list: "ABDC Journal Quality List", edition: "2025", file: "abdc2025.csv", key: "abdc2025" },
  { kind: "abs", badge: "ABS", name: "Chartered ABS Academic Journal Guide 2024", list: "Chartered ABS Academic Journal Guide", edition: "2024", file: "abs2024.csv", key: "abs2024" },
  { kind: "vhb", badge: "VHB", name: "VHB JOURQUAL 2024", list: "VHB JOURQUAL (VHB Rating)", edition: "2024", file: "vhb2024.csv", key: "vhb2024" },
  { kind: "fnege", badge: "FNEGE", name: "FNEGE 2025, from the Harzing Journal Quality List (72nd edition)", list: "FNEGE journal ranking", edition: "2025, as given in the Harzing Journal Quality List, 72nd edition", file: "fnege2025.csv", key: "fnege2025" },
  { kind: "ft50", badge: "FT50", name: "Financial Times 50 research journals", list: "Financial Times 50 research journals (FT50)", edition: "No edition year; the current FT list", file: "ft50.txt", key: "ft50" },
  { kind: "utd24", badge: "UTD24", name: "UT Dallas 24 journals", list: "UT Dallas 24 journals (UTD24)", edition: "No edition year; the current UT Dallas list", file: "utd24.txt", key: "utd24" },
  { kind: "core", badge: "CORE", name: "CORE/ICORE 2026 conference ranking", list: "CORE conference ranking", edition: "ICORE2026, or the older CORE portal rank where ICORE2026 has none", file: "core_icore2026.csv", key: "core_icore2026" },
  { kind: "ccf", badge: "CCF", name: "China Computer Federation recommended venues", list: "China Computer Federation (CCF) recommended venues", edition: "No edition year in the bundled file", file: "ccf_ranks.csv" },
  { kind: "era", badge: "ERA 2023", name: "Excellence in Research for Australia 2023 journal list", list: "Excellence in Research for Australia (ERA) journal list", edition: "2023", file: "era2023.txt" },
  { kind: "norwegian", badge: "Level 1/2", name: "Norwegian Register for Scientific Journals", list: "Norwegian Register for Scientific Journals", edition: "No edition year in the bundled file", file: "norwegian_compact.json" },
  { kind: "h5", badge: "h5", name: "Google Scholar Metrics 5-year h-index", list: "Google Scholar Metrics, 5-year h-index", edition: "2025", file: "venue_h5_index.json" },
  { kind: "if", badge: "IF", name: "Journal Impact Factor 2024", list: "Journal Impact Factor", edition: "2024", file: "journal_impact_2024.csv" },
  { kind: "jcr", list: "Clarivate Journal Citation Reports (JCR), your imported file" },
  { kind: "custom", list: "Your list" },
];
const BADGE_SOURCE_BY_KIND = Object.fromEntries(BADGE_SOURCES.map((s) => [s.kind, s]));

const QUARTILE_NOT_APPLICABLE = "Not applicable. This badge is a rank on the list, not a quartile.";

/**
 * The four facts a badge popover states: which list, which edition, the subject category behind a quartile, and the
 * venue Scholar showed against the list entry it matched. `meta` holds the stored file meta for imported indexes
 * ({ quartilesMeta, jcrMeta }), so an imported SJR or JCR file reports its own year. Returns null for badges that are
 * not from a ranking list (pre-print server, ACL Findings).
 */
export function explainBadge(badge, meta = {}) {
  const src = BADGE_SOURCE_BY_KIND[badge?.kind];
  if (!src) return null;
  const m = badge.metadata || {};
  let list = src.list;
  let edition = src.edition || "";
  let category = QUARTILE_NOT_APPLICABLE;
  if (badge.kind === "quartile") {
    const qm = meta.quartilesMeta;
    if (qm?.year) edition = String(qm.year);
    category = "Not recorded. SJR ranks a journal in each of its subject categories; this badge is the best of those quartiles.";
  } else if (badge.kind === "jcr") {
    const jm = meta.jcrMeta;
    edition = jm?.year ? String(jm.year) : "Not recorded in your imported file";
    const cat = m.jcrData?.category;
    category = cat ? String(cat) : "Not recorded in your imported file";
  } else if (badge.kind === "custom") {
    list = `Your list: ${m.listName || String(m.system || "").replace(/^Your list:\s*/, "") || "My list"}`;
    edition = "Your own list, imported in Options";
  }
  const shown = m.venueShown || m.venueRead || "";
  const matched = m.nearMatch || m.venueMatched || "";
  const same = normalizeVenueName(shown) === normalizeVenueName(matched);
  return {
    list,
    edition,
    category,
    venueShown: shown || "Not available",
    venueMatched: matched ? (same ? `${matched} (exact match)` : matched) : "Not available",
  };
}

export const BADGE_EXPLANATION_LABELS = [
  ["list", "List"],
  ["edition", "Edition"],
  ["category", "Subject category"],
  ["venueShown", "Scholar shows"],
  ["venueMatched", "Matched in the list"],
];

/** The popover's source block as a <dl>, built with textContent only (venue strings come from the page). */
export function renderBadgeExplanation(doc, badge, meta = {}) {
  const info = explainBadge(badge, meta);
  if (!info || !doc?.createElement) return null;
  const dl = doc.createElement("dl");
  dl.className = "su-badge-source";
  for (const [field, label] of BADGE_EXPLANATION_LABELS) {
    const dt = doc.createElement("dt");
    dt.textContent = label;
    const dd = doc.createElement("dd");
    dd.textContent = info[field];
    dd.setAttribute("data-field", field);
    dl.appendChild(dt);
    dl.appendChild(dd);
  }
  return dl;
}
