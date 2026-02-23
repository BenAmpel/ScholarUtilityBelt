// Venue quality index helpers.
// Everything here is local-only; no network calls.

export function normalizeVenueName(s) {
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
      // Patterns: "41 (1)", "41(1)", "Vol. 41", "Volume 41", "V41", "No. 1", etc.
      venue = venue.replace(/\s*(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
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
    venue = venue.replace(/\s*(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
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
      venue = venue.replace(/\s*(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
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
      venue = venue.replace(/\s*(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
      venue = venue.replace(/\s*\d+\s*\([^)]*\)/g, "");
      venue = venue.replace(/\s*\(\d+\)/g, "");
      return venue.trim();
    }
    // If no patterns match, try to clean up anyway
    let venue = part.trim();
    // Remove trailing page numbers
    venue = venue.replace(/,\s*\d+[-–]\d+$/, "");
    // Remove volume/issue numbers
    venue = venue.replace(/\s*(?:Vol\.?|Volume|V\.?)\s*\d+/gi, "");
    venue = venue.replace(/\s*(?:No\.?|Number|Issue|Iss\.?)\s*\d+/gi, "");
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
  const quartiles = new Map(); // venue -> Q1/Q2/Q3/Q4
  const core = new Map(); // venue -> A*/A/B/C or similar (includes ICORE)
  const ccf = new Map(); // venue -> A/B/C (CCF rankings)
  const jcr = new Map(); // venue -> { jifQ, jciQ, aisQ, fiveYJifQ, ... }
  const impact = new Map(); // venue -> impact factor (numeric)
  const abs = new Map(); // venue -> 4*/4/3/2/1 (ABS 2024 AJG)

  const VALID_ABDC = /^A\*?$|^[BCD]$/i;
  const VALID_ABS = /^4\*?$|^[1234]$/i;
  const VALID_VHB = /^A\+?$|^[BCDE]$/i;
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
  }

  for (const line of parseLines(settings.qualityCoreRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim();
    if (!name || !rank || !VALID_CORE.test(rank)) continue;
    addSynonymsToMap(core, name, rank);
  }

  for (const line of parseLines(settings.qualityCcfRanks || "")) {
    const idx = line.lastIndexOf(",");
    if (idx <= 0 || idx >= line.length - 1) continue;
    const name = line.slice(0, idx).trim();
    const rank = line.slice(idx + 1).trim();
    if (!name || !rank || !VALID_CCF.test(rank)) continue;
    addSynonymsToMap(ccf, name, rank);
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
      if (n && v && VALID_ABS.test(v)) abs.set(n, v);
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

  return { ft50, utd24, abdc, vhb, quartiles, core, ccf, jcr, impact, era, norwegian, abs, h5 };
}


// Minimum key length when venue "starts with key" so short abbreviations (e.g. "ais") don't match journals like "AIS Transactions on Replication Research"
const MIN_KEY_LENGTH_FOR_PREFIX_MATCH = 12;

function findBestMatch(normalizedVenue, map) {
  // Try prefix matching: check if any key starts with the normalized venue
  // This handles cases like "mis quarterly" matching "mis quarterly management information systems"
  for (const [key, value] of map.entries()) {
    // Check if key starts with normalized venue (with space after)
    if (key.startsWith(normalizedVenue + " ") || key === normalizedVenue) {
      return value;
    }
    // Also try reverse: if the normalized venue starts with the key
    if (normalizedVenue.startsWith(key + " ") || normalizedVenue === key) {
      // Require key to be multi-word or long enough so "ais" doesn't match "ais transactions on replication research" (journal)
      if (key.length >= MIN_KEY_LENGTH_FOR_PREFIX_MATCH || key.includes(" "))
        return value;
    }
  }
  
  return null;
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

export function qualityBadgesForVenue(venue, qIndex) {
  const v = normalizeVenueName(venue);
  if (!v) return [];

  const badges = [];
  let impactBadge = null;

  // Pre-print badge first so it's clearly visible
  const preprint = getPreprintBadge(venue);
  if (preprint) badges.push(preprint);

  // Try exact match first, then fuzzy match for ABDC
  let abdc = qIndex.abdc.get(v);
  if (!abdc) {
    abdc = findBestMatch(v, qIndex.abdc);
  }
  const VALID_ABDC = /^A\*?$|^[BCD]$/i;
  if (abdc && VALID_ABDC.test(String(abdc).trim())) {
    const r = String(abdc).trim().toUpperCase();
    badges.push({ kind: "abdc", text: `ABDC ${r}`, metadata: { rank: r, system: "ABDC Journal Quality List" } });
  }

  let vhb = qIndex.vhb?.get?.(v);
  if (!vhb && qIndex.vhb) vhb = findBestMatch(v, qIndex.vhb);
  const VALID_VHB = /^A\+?$|^[BCDE]$/i;
  if (vhb && VALID_VHB.test(String(vhb).trim())) {
    const r = String(vhb).trim().toUpperCase().replace("*", "+");
    badges.push({ kind: "vhb", text: `VHB ${r}`, metadata: { rank: r, system: "VHB JOURQUAL 2024" } });
  }

  let abs = qIndex.abs?.get?.(v);
  if (!abs && qIndex.abs) abs = findBestMatch(v, qIndex.abs);
  const VALID_ABS = /^4\*?$|^[1234]$/i;
  if (abs && VALID_ABS.test(String(abs).trim())) {
    const r = String(abs).trim();
    badges.push({ kind: "abs", text: `ABS ${r}`, metadata: { rank: r, system: "ABS Academic Journal Guide 2024" } });
  }

  const VALID_QUARTILE = /^Q[1-4]$/i;
  const jcr = qIndex.jcr?.get?.(v) || null;
  if (jcr) {
    if (jcr.jifQ && VALID_QUARTILE.test(String(jcr.jifQ))) badges.push({ kind: "jcr", text: `JIF ${String(jcr.jifQ).toUpperCase()}`, metadata: { quartile: jcr.jifQ, system: "JCR Journal Impact Factor", jif: jcr.jif, jcrData: jcr } });
    if (jcr.jciQ && VALID_QUARTILE.test(String(jcr.jciQ))) badges.push({ kind: "jcr", text: `JCI ${String(jcr.jciQ).toUpperCase()}`, metadata: { quartile: jcr.jciQ, system: "JCR Journal Citation Indicator", jci: jcr.jci, jcrData: jcr } });
    if (jcr.aisQ && VALID_QUARTILE.test(String(jcr.aisQ))) badges.push({ kind: "jcr", text: `AIS ${String(jcr.aisQ).toUpperCase()}`, metadata: { quartile: jcr.aisQ, system: "JCR Article Influence Score", ais: jcr.ais, jcrData: jcr } });
    if (jcr.fiveYJifQ && VALID_QUARTILE.test(String(jcr.fiveYJifQ)))
      badges.push({ kind: "jcr", text: `5Y ${String(jcr.fiveYJifQ).toUpperCase()}`, metadata: { quartile: jcr.fiveYJifQ, system: "JCR 5-Year Impact Factor", fiveYJif: jcr.fiveYJif, jcrData: jcr } });
  }

  let impact = qIndex.impact?.get?.(v);
  if (impact == null && qIndex.impact) impact = findBestMatch(v, qIndex.impact);
  const impactNum = typeof impact === "number" ? impact : parseFloat(String(impact || "").replace(/[^0-9.]/g, ""));
  if (Number.isFinite(impactNum) && impactNum > 0) {
    const display = impactNum.toFixed(1);
    impactBadge = { kind: "if", text: `IF ${display}`, metadata: { impact: impactNum, system: "Journal Impact Factor (2024)" } };
  }

  // Resolve CORE and CCF first so we can treat venue as conference when present
  let core = qIndex.core.get(v);
  if (!core && (v.startsWith("icis ") || v === "icis")) {
    core = qIndex.core.get("international conference on information systems");
  }
  // "42nd International Conference on Information Systems (ICIS), 1-8" → ICIS (avoid matching ISD/other "International Conference on Information Systems *")
  if (!core && v.startsWith("international conference on information systems") && (v === "international conference on information systems" || v.includes(" icis"))) {
    core = qIndex.core.get("international conference on information systems");
  }
  if (!core && v.startsWith("americas") && v.includes("conference on information systems")) {
    core = qIndex.core.get("americas conference on information systems");
  }
  if (!core && v.startsWith("hawaii international conference on system sciences")) {
    core = qIndex.core.get("hawaii international conference on system sciences");
  }
  if (!core) {
    core = findBestMatch(v, qIndex.core);
  }
  let ccf = qIndex.ccf?.get?.(v) || (qIndex.ccf ? findBestMatch(v, qIndex.ccf) : null);
  const exactQuartile = qIndex.quartiles.get(v);
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
    const q = exactQuartile || (looksLikeWorkshop ? null : findBestMatch(v, qIndex.quartiles));
    if (q && VALID_QUARTILE.test(String(q))) badges.push({ kind: "quartile", text: String(q).toUpperCase(), metadata: { quartile: q, system: "SCImago Journal Rank (SJR)" } });
  }

  // For sets (ft50, utd24), check exact match and prefix matches
  // Only allow: exact match, or entry starts with venue (for abbreviations like "MISQ" matching "MIS Quarterly")
  // Do NOT allow venue starts with entry (prevents "Journal of Marketing Analytics" matching "Journal of Marketing")
  if (qIndex.ft50.has(v)) {
    badges.push({ kind: "ft50", text: "FT50", metadata: { system: "Financial Times 50 Top Research Journals" } });
  } else {
    // Check if any entry in the set starts with the normalized venue (for abbreviations)
    // Require minimum length to avoid false matches with very short venue names
    const MIN_PREFIX_LEN = 5;
    if (v.length >= MIN_PREFIX_LEN) {
      for (const entry of qIndex.ft50) {
        // Exact match OR entry is longer and starts with venue + space (abbreviation matching)
        // Do NOT check v.startsWith(entry) - that would cause false positives
        if (entry === v || (entry.length > v.length && entry.startsWith(v + " "))) {
          badges.push({ kind: "ft50", text: "FT50", metadata: { system: "Financial Times 50 Top Research Journals" } });
          break;
        }
      }
    }
  }

  if (qIndex.utd24.has(v)) {
    badges.push({ kind: "utd24", text: "UTD24", metadata: { system: "UT Dallas Top 100 Business School Research Rankings" } });
  } else {
    // Check if any entry in the set starts with the normalized venue (for abbreviations)
    // Require minimum length to avoid false matches with very short venue names
    const MIN_PREFIX_LEN = 5;
    if (v.length >= MIN_PREFIX_LEN) {
      for (const entry of qIndex.utd24) {
        // Exact match OR entry is longer and starts with venue + space (abbreviation matching)
        // Do NOT check v.startsWith(entry) - that would cause false positives
        if (entry === v || (entry.length > v.length && entry.startsWith(v + " "))) {
          badges.push({ kind: "utd24", text: "UTD24", metadata: { system: "UT Dallas Top 100 Business School Research Rankings" } });
          break;
        }
      }
    }
  }

  const VALID_CORE = /^A\*?$|^[ABC]$/i;
  const VALID_CCF = /^[ABC]$/i;
  if (core && VALID_CORE.test(String(core).trim())) badges.push({ kind: "core", text: `CORE ${String(core).trim().toUpperCase()}`, metadata: { rank: String(core).trim().toUpperCase(), system: "CORE/ICORE Conference Rankings" } });
  if (ccf && VALID_CCF.test(String(ccf).trim())) badges.push({ kind: "ccf", text: `CCF ${String(ccf).trim().toUpperCase()}`, metadata: { rank: String(ccf).trim().toUpperCase(), system: "China Computer Federation Conference Rankings" } });

  if (qIndex.era?.has?.(v)) {
    badges.push({ kind: "era", text: "ERA 2023", metadata: { system: "Excellence in Research for Australia 2023" } });
  } else if (qIndex.era) {
    for (const entry of qIndex.era) {
      if (entry.startsWith(v + " ") || entry === v || v.startsWith(entry + " ") || v === entry) {
        badges.push({ kind: "era", text: "ERA 2023", metadata: { system: "Excellence in Research for Australia 2023" } });
        break;
      }
    }
  }

  let norLevel = qIndex.norwegian?.get?.(v);
  if (norLevel == null && qIndex.norwegian) norLevel = findBestMatch(v, qIndex.norwegian);
  if (norLevel === "1" || norLevel === "2") badges.push({ kind: "norwegian", text: `Level ${norLevel}`, metadata: { level: norLevel, system: "Norwegian Register for Scientific Journals" } });

  let h5Val = qIndex.h5?.get?.(v);
  if (h5Val == null && qIndex.h5) h5Val = findBestMatch(v, qIndex.h5);
  if (typeof h5Val === "number" && h5Val > 0) {
    badges.push({ kind: "h5", text: `h5: ${h5Val}`, metadata: { h5: h5Val, system: "Google Scholar 5-year h-index" } });
  }

  if (impactBadge) badges.push(impactBadge);
  return badges;
}

/** Numeric venue tier weight in [0, 1] for Contribution Signal Score. Higher = stronger venue. */
const VENUE_WEIGHT_MAP = {
  ft50: 1,
  utd24: 1,
  abdc: { "a*": 0.95, "a": 0.85, "b": 0.6, "c": 0.4, "d": 0.25 },
  vhb: { "a+": 0.95, "a": 0.85, "b": 0.6, "c": 0.4, "d": 0.25, "e": 0.15 },
  abs: { "4*": 0.95, "4": 0.8, "3": 0.55, "2": 0.35, "1": 0.2 },
  jcr: { "q1": 0.75, "q2": 0.5, "q3": 0.35, "q4": 0.2 },
  quartile: { "q1": 0.75, "q2": 0.5, "q3": 0.35, "q4": 0.2 },
  core: { "a*": 0.9, "a": 0.8, "b": 0.6, "c": 0.4 },
  ccf: { "a": 0.85, "b": 0.6, "c": 0.4 },
  era: 0.75,
  norwegian: { "1": 0.8, "2": 0.6 },
  preprint: 0.15
};

export function venueWeightForVenue(venue, qIndex) {
  const badges = qualityBadgesForVenue(venue, qIndex);
  if (!badges.length) return 0.2; // baseline for unlisted venues
  let w = 0.2;
  for (const b of badges) {
    if (b.kind === "ft50" || b.kind === "utd24") w = Math.max(w, VENUE_WEIGHT_MAP[b.kind]);
    else if (b.kind === "preprint") w = Math.max(w, VENUE_WEIGHT_MAP.preprint);
    else if (b.kind === "abdc" && b.metadata?.rank) {
      const r = String(b.metadata.rank).toLowerCase().replace(/\s/g, "");
      w = Math.max(w, VENUE_WEIGHT_MAP.abdc[r] ?? 0.4);
    } else if (b.kind === "vhb" && b.metadata?.rank) {
      const r = String(b.metadata.rank).toLowerCase().replace(/\s/g, "");
      w = Math.max(w, VENUE_WEIGHT_MAP.vhb[r] ?? 0.4);
    } else if (b.kind === "abs" && b.metadata?.rank) {
      const r = String(b.metadata.rank).toLowerCase().replace(/\s/g, "");
      w = Math.max(w, VENUE_WEIGHT_MAP.abs[r] ?? 0.4);
    } else if (b.kind === "quartile" && b.metadata?.quartile) {
      const q = String(b.metadata.quartile).toLowerCase();
      w = Math.max(w, VENUE_WEIGHT_MAP.quartile[q] ?? 0.35);
    } else if (b.kind === "jcr" && b.metadata?.quartile) {
      const q = String(b.metadata.quartile).toLowerCase();
      w = Math.max(w, VENUE_WEIGHT_MAP.jcr[q] ?? 0.35);
    } else if (b.kind === "core" && b.metadata?.rank) {
      const r = String(b.metadata.rank).toLowerCase().replace(/\s/g, "");
      w = Math.max(w, VENUE_WEIGHT_MAP.core[r] ?? 0.5);
    } else if (b.kind === "ccf" && b.metadata?.rank) {
      const r = String(b.metadata.rank).toLowerCase();
      w = Math.max(w, VENUE_WEIGHT_MAP.ccf[r] ?? 0.5);
    } else if (b.kind === "era") w = Math.max(w, VENUE_WEIGHT_MAP.era);
    else if (b.kind === "norwegian" && b.metadata?.level) {
      const l = String(b.metadata.level);
      w = Math.max(w, VENUE_WEIGHT_MAP.norwegian[l] ?? 0.6);
    }
  }
  return w;
}
