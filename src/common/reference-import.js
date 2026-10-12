/**
 * Parsing for reference-manager and database exports (RIS, BibTeX and PubMed
 * MEDLINE/.nbib) into plain
 * records, plus the PRISMA-S search-log helpers. Pure functions: no DOM, no
 * storage, no network. The review workspace turns records into project papers.
 */

export const MAX_RECORDS = 20000;
export const MAX_FILE_BYTES = 25 * 1024 * 1024;

/** "ris", "bibtex", "medline" or "" from the file's own content (the file name is only a hint). */
export function detectReferenceFormat(text, fileName = "") {
  const head = String(text ?? "").replace(/^\uFEFF/, "").slice(0, 4000);
  if (/^\s*TY {2}- /m.test(head)) return "ris";
  if (/^\s*@\w+\s*[{(]/m.test(head)) return "bibtex";
  // PubMed's MEDLINE format: "PMID- 12345" (the tag is padded to four characters; some tools write "PMID - ").
  if (/^\s*PMID ?- /m.test(head)) return "medline";
  if (/\.ris$/i.test(fileName)) return "ris";
  if (/\.bib(tex)?$/i.test(fileName)) return "bibtex";
  if (/\.nbib$/i.test(fileName)) return "medline";
  return "";
}

const SPACE = /\s+/g;
const clean = (s) => String(s ?? "").replace(SPACE, " ").trim();

function normalizeDoi(raw) {
  const m = String(raw ?? "").match(/10\.\d{4,9}\/[^\s"<>{}]+/i);
  return m ? m[0].replace(/[.,;)\]]+$/, "").toLowerCase() : "";
}

function yearOf(raw) {
  const m = String(raw ?? "").match(/\b(1[5-9]\d{2}|20\d{2})\b/);
  return m ? m[1] : "";
}

/** "Béchard, Pierre" becomes "P Béchard", the style the workspace already uses; other forms are kept. */
function formatAuthor(raw) {
  const a = clean(raw);
  if (!a) return "";
  if (a.includes(",")) {
    const [last, first] = a.split(",", 2).map(clean);
    const initials = first.split(/[\s.-]+/).filter(Boolean).map((w) => w[0].toUpperCase()).join("");
    return initials ? `${initials} ${last}` : last;
  }
  return a;
}

// ---------------------------------------------------------------- RIS

const RIS_LINE = /^([A-Z][A-Z0-9]) {2}- ?(.*)$/;
const RIS_VENUE_ORDER = ["JF", "JO", "T2", "BT", "J2", "JA"];

/** Parses RIS text. Returns { records, skipped } where skipped counts entries with no title. */
export function parseRis(text) {
  const lines = String(text ?? "").replace(/^\uFEFF/, "").split(/\r\n|\n|\r/);
  const entries = [];
  let cur = null;
  let lastTag = null;
  for (const line of lines) {
    const m = line.match(RIS_LINE);
    if (m) {
      const [, tag, value] = m;
      if (tag === "TY") { cur = []; entries.push(cur); }
      if (tag === "ER") { cur = null; lastTag = null; continue; }
      if (!cur) continue;
      cur.push([tag, value]);
      lastTag = tag;
    } else if (cur && lastTag && line.trim()) {
      // Some exporters wrap long values onto the next line.
      const last = cur[cur.length - 1];
      last[1] = `${last[1]} ${line.trim()}`;
    }
  }
  const records = [];
  let skipped = 0;
  for (const fields of entries) {
    const get = (...tags) => {
      for (const t of tags) {
        const hit = fields.find(([tag, v]) => tag === t && clean(v));
        if (hit) return clean(hit[1]);
      }
      return "";
    };
    const title = get("TI", "T1");
    if (!title) { skipped++; continue; }
    const venue = RIS_VENUE_ORDER.map((t) => get(t)).find(Boolean) || "";
    const urls = fields.filter(([t]) => t === "UR").map(([, v]) => clean(v)).filter(Boolean);
    const doi = normalizeDoi(get("DO", "DI")) || normalizeDoi(urls.find((u) => /doi\.org\//i.test(u)) || "");
    records.push({
      title,
      authors: fields.filter(([t]) => t === "AU" || t === "A1").map(([, v]) => formatAuthor(v)).filter(Boolean).join(", "),
      year: yearOf(get("PY", "Y1", "DA")),
      venue,
      doi,
      url: urls.find((u) => /^https?:/i.test(u) && !/doi\.org\//i.test(u)) || urls.find((u) => /^https?:/i.test(u)) || "",
      abstract: get("AB", "N2"),
      keywords: fields.filter(([t]) => t === "KW").map(([, v]) => clean(v)).filter(Boolean),
      sourceDb: get("DB", "DP"),
    });
  }
  return { records, skipped };
}

// ------------------------------------------------------------ MEDLINE

// "TAG - value": the tag is 2 to 4 characters padded with spaces to four, then "- ".
const MEDLINE_LINE = /^([A-Z][A-Z0-9]{1,3}) *-(?: (.*)|)$/;

/** "Smith JA" becomes "JA Smith", matching formatAuthor's output; a full "Last, First" goes through formatAuthor. */
function formatMedlineAuthor(raw) {
  const a = clean(raw);
  if (a.includes(",")) return formatAuthor(a);
  const m = a.match(/^(.+?)\s+([A-Z]{1,5})$/);
  return m ? `${m[2]} ${m[1]}` : a;
}

/**
 * Parses PubMed MEDLINE text (.nbib or "MEDLINE" format). Records are separated
 * by blank lines (a new PMID line also starts one), values wrap onto lines
 * indented by six spaces, and tags repeat. Returns { records, skipped }, where
 * skipped counts records with no title. Text that has no tag lines at all
 * (stray lines between records) is ignored rather than counted.
 */
export function parseMedline(text) {
  const lines = String(text ?? "").replace(/^\uFEFF/, "").split(/\r\n|\n|\r/);
  const entries = [];
  let cur = null;
  for (const line of lines) {
    if (!line.trim()) { cur = null; continue; }
    const m = line.match(MEDLINE_LINE);
    if (m) {
      const tag = m[1];
      if (!cur || (tag === "PMID" && cur.some(([t]) => t === "PMID"))) { cur = []; entries.push(cur); }
      cur.push([tag, m[2] ?? ""]);
    } else if (cur && /^\s/.test(line)) {
      const last = cur[cur.length - 1];
      last[1] = `${last[1]} ${line.trim()}`;
    }
  }
  const records = [];
  let skipped = 0;
  for (const fields of entries) {
    const all = (...tags) => fields.filter(([t]) => tags.includes(t)).map(([, v]) => clean(v)).filter(Boolean);
    // Tags are tried in the order given (JT before TA), not in file order.
    const first = (...tags) => tags.map((t) => all(t)[0]).find(Boolean) || "";
    const title = first("TI", "BTI");
    if (!title) { skipped++; continue; }
    const pmid = (first("PMID").match(/\d+/) || [""])[0];
    const full = all("FAU");
    const authors = (full.length ? full : all("AU")).map(formatMedlineAuthor).concat(all("CN")).filter(Boolean);
    // AID and LID carry "10.xxxx/yyyy [doi]" next to PII and other identifiers; only the [doi] one is a DOI.
    const doiIds = all("AID", "LID").filter((v) => /\[doi\]\s*$/i.test(v)).map(normalizeDoi);
    const doiFromSo = normalizeDoi((first("SO").match(/doi:\s*(\S+)/i) || [])[1]);
    const keywords = [...new Set(all("MH", "OT").map((k) => k.replace(/^\*+/, "")))];
    records.push({
      title,
      authors: authors.join(", "),
      year: yearOf(first("DP")) || yearOf(first("DEP").replace(/^(\d{4})(\d\d)(\d\d)$/, "$1 $2 $3")) || yearOf(first("EDAT")),
      venue: first("JT", "TA"),
      doi: doiIds.find(Boolean) || doiFromSo,
      url: pmid ? `https://pubmed.ncbi.nlm.nih.gov/${pmid}/` : "",
      abstract: first("AB"),
      keywords,
      sourceDb: "PubMed",
      pmid,
    });
  }
  return { records, skipped };
}

// ------------------------------------------------------------- BibTeX

const LATEX_ACCENTS = /\\['"`^~=.uvHckrb]\s*\{?([A-Za-z])\}?/g;
const LATEX_COMMANDS = { "\\&": "&", "\\%": "%", "\\_": "_", "\\#": "#", "\\$": "$", "{\\ss}": "ss", "\\ss": "ss", "{\\o}": "o", "{\\O}": "O", "{\\ae}": "ae", "{\\aa}": "a", "{\\l}": "l", "{\\L}": "L" };

function cleanLatex(s) {
  let out = String(s ?? "");
  for (const [k, v] of Object.entries(LATEX_COMMANDS)) out = out.split(k).join(v);
  out = out.replace(LATEX_ACCENTS, "$1").replace(/\\[a-zA-Z]+\s*/g, "").replace(/[{}]/g, "").replace(/~/g, " ");
  return clean(out);
}

/** Reads a braced or quoted value starting at `i`; returns [rawValue, nextIndex]. */
function readBibValue(src, i) {
  const ch = src[i];
  if (ch === "{") {
    let depth = 0;
    let j = i;
    for (; j < src.length; j++) {
      if (src[j] === "{") depth++;
      else if (src[j] === "}" && --depth === 0) break;
    }
    return [src.slice(i + 1, j), j + 1];
  }
  if (ch === '"') {
    let j = i + 1;
    let depth = 0;
    for (; j < src.length; j++) {
      if (src[j] === "{") depth++;
      else if (src[j] === "}") depth--;
      else if (src[j] === '"' && depth <= 0 && src[j - 1] !== "\\") break;
    }
    return [src.slice(i + 1, j), j + 1];
  }
  let j = i;
  while (j < src.length && !/[,}\s]/.test(src[j])) j++;
  return [src.slice(i, j), j];
}

/** Parses BibTeX text. Returns { records, skipped }. Concatenation with # and @string macros are not expanded. */
export function parseBibtex(text) {
  const src = String(text ?? "").replace(/^\uFEFF/, "");
  const records = [];
  let skipped = 0;
  const startRe = /@(\w+)\s*([{(])/g;
  let m;
  while ((m = startRe.exec(src))) {
    const type = m[1].toLowerCase();
    let i = startRe.lastIndex;
    if (type === "comment" || type === "string" || type === "preamble") {
      // Skip the whole braced block so its contents are not mistaken for entries.
      const [, next] = readBibValue(src, i - 1);
      startRe.lastIndex = next;
      continue;
    }
    // Citation key runs to the first comma.
    const comma = src.indexOf(",", i);
    if (comma === -1) break;
    i = comma + 1;
    const fields = {};
    for (;;) {
      while (i < src.length && /[\s,]/.test(src[i])) i++;
      if (i >= src.length || src[i] === "}" || src[i] === ")") { i++; break; }
      const eq = src.indexOf("=", i);
      if (eq === -1) break;
      const name = src.slice(i, eq).trim().toLowerCase();
      i = eq + 1;
      while (i < src.length && /\s/.test(src[i])) i++;
      const [value, next] = readBibValue(src, i);
      i = next;
      if (name && !(name in fields)) fields[name] = value;
    }
    startRe.lastIndex = Math.max(i, startRe.lastIndex);
    const title = cleanLatex(fields.title);
    if (!title) { skipped++; continue; }
    const authors = String(fields.author ?? "").split(/\s+and\s+/i).map((a) => formatAuthor(cleanLatex(a))).filter(Boolean);
    records.push({
      title,
      authors: authors.join(", "),
      year: yearOf(fields.year || fields.date),
      venue: cleanLatex(fields.journal || fields.journaltitle || fields.booktitle || fields.series || fields.howpublished || fields.publisher || ""),
      doi: normalizeDoi(fields.doi) || normalizeDoi(fields.url),
      url: /^https?:/i.test(clean(fields.url)) && !/doi\.org\//i.test(fields.url) ? clean(fields.url) : "",
      abstract: cleanLatex(fields.abstract),
      keywords: cleanLatex(fields.keywords || fields.keyword || "").split(/\s*[;,]\s*/).filter(Boolean),
      sourceDb: "",
    });
  }
  return { records, skipped };
}

/**
 * @returns {{ format: string, records: object[], skipped: number, truncated: boolean, error: string }}
 */
export function parseReferences(text, fileName = "") {
  const format = detectReferenceFormat(text, fileName);
  if (!format) {
    return { format: "", records: [], skipped: 0, truncated: false, error: "Not a RIS, BibTeX or PubMed MEDLINE (.nbib) file. Export from your database as RIS, BibTeX or, from PubMed, as a citation file (.nbib) and try again." };
  }
  const parsed = format === "ris" ? parseRis(text) : format === "medline" ? parseMedline(text) : parseBibtex(text);
  let { records } = parsed;
  const truncated = records.length > MAX_RECORDS;
  if (truncated) records = records.slice(0, MAX_RECORDS);
  const error = records.length ? "" : "No records with a title were found in that file.";
  return { format, records, skipped: parsed.skipped, truncated, error };
}

// ------------------------------------------------------- search log

export const SEARCH_LOG_COLUMNS = ["Database", "Date searched", "Search string", "File", "Format", "Records in file", "New records", "Duplicates"];

/** Small stable hash so imported papers get deterministic ids. */
export function hashId(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) { h = (h << 5) - h + str.charCodeAt(i); h |= 0; }
  return Math.abs(h).toString(36);
}

export function searchLogRows(log) {
  return (Array.isArray(log) ? log : []).map((e) => [
    e.database || "", e.searchedOn || "", e.query || "", e.fileName || "", e.format || "",
    e.recordsInFile ?? 0, e.added ?? 0, e.duplicates ?? 0,
  ]);
}

export function searchLogToCsv(log, csvEscape) {
  const esc = csvEscape || ((v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  });
  return [SEARCH_LOG_COLUMNS, ...searchLogRows(log)].map((r) => r.map(esc).join(",")).join("\n");
}

/** Markdown table for the report; empty string when nothing was imported. */
export function searchLogToMarkdown(log) {
  const rows = searchLogRows(log);
  if (!rows.length) return "";
  const cell = (v) => String(v ?? "").replace(/\|/g, "\\|").replace(/\s+/g, " ");
  return [
    `| ${SEARCH_LOG_COLUMNS.join(" | ")} |`,
    `| ${SEARCH_LOG_COLUMNS.map(() => "---").join(" | ")} |`,
    ...rows.map((r) => `| ${r.map(cell).join(" | ")} |`),
  ].join("\n");
}

export function totalRecordsFromSearchLog(log) {
  return (Array.isArray(log) ? log : []).reduce((n, e) => n + (Number(e.recordsInFile) || 0), 0);
}
