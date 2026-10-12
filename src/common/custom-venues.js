/**
 * Your own venue list: a CSV of journal or conference names with a tier label
 * (for example a department's "A / B / C" list). Pure parsing and lookup, no
 * storage and no network. The list lives in chrome.storage.local only.
 */
import { normalizeVenueName } from "./quality.js";

export const MAX_ROWS = 5000;
export const MAX_LABEL_LENGTH = 16;
export const MAX_VENUE_LENGTH = 200;
export const MAX_LIST_NAME_LENGTH = 40;

const VENUE_HEADERS = new Set(["venue", "journal", "conference", "name", "title", "source", "publication"]);
const LABEL_HEADERS = new Set(["rank", "tier", "label", "level", "grade", "rating", "category", "list"]);

function detectDelimiter(headLine) {
  const counts = [",", ";", "\t"].map((d) => [d, headLine.split(d).length - 1]);
  counts.sort((a, b) => b[1] - a[1]);
  return counts[0][1] > 0 ? counts[0][0] : ",";
}

/** Splits CSV text into rows of cells, honouring quoted cells, doubled quotes and CRLF. */
function splitCsv(text, delimiter) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') { cell += '"'; i++; } else inQuotes = false;
      } else cell += ch;
    } else if (ch === '"') inQuotes = true;
    else if (ch === delimiter) { row.push(cell); cell = ""; }
    else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      row.push(cell); cell = "";
      if (row.some((c) => c.trim() !== "")) rows.push(row);
      row = [];
    } else cell += ch;
  }
  row.push(cell);
  if (row.some((c) => c.trim() !== "")) rows.push(row);
  return rows;
}

/**
 * @returns {{ entries: Array<[string, string]>, skipped: number, truncated: boolean, error: string }}
 * `entries` is [venue, label] pairs. `skipped` counts rows with a missing venue or label.
 */
export function parseCustomVenueCsv(text) {
  const clean = String(text ?? "").replace(/^\uFEFF/, "");
  if (!clean.trim()) return { entries: [], skipped: 0, truncated: false, error: "The file is empty." };
  const delimiter = detectDelimiter(clean.split(/\r\n|\n|\r/, 1)[0]);
  const rows = splitCsv(clean, delimiter);
  if (!rows.length) return { entries: [], skipped: 0, truncated: false, error: "The file is empty." };

  const head = rows[0].map((c) => c.trim().toLowerCase());
  let venueCol = head.findIndex((h) => VENUE_HEADERS.has(h));
  let labelCol = head.findIndex((h) => LABEL_HEADERS.has(h));
  const hasHeader = venueCol !== -1 && labelCol !== -1;
  if (!hasHeader) { venueCol = 0; labelCol = 1; }

  const dataRows = hasHeader ? rows.slice(1) : rows;
  const entries = [];
  const seen = new Set();
  let skipped = 0;
  let truncated = false;
  for (const r of dataRows) {
    const venue = String(r[venueCol] ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_VENUE_LENGTH);
    const label = String(r[labelCol] ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_LABEL_LENGTH);
    if (!venue || !label || !normalizeVenueName(venue)) { skipped++; continue; }
    const norm = normalizeVenueName(venue);
    if (seen.has(norm)) { skipped++; continue; }
    if (entries.length >= MAX_ROWS) { truncated = true; break; }
    seen.add(norm);
    entries.push([venue, label]);
  }
  if (!entries.length) {
    return {
      entries,
      skipped,
      truncated,
      error: hasHeader
        ? "No usable rows. Each row needs a venue and a tier label."
        : "No usable rows. Use two columns: venue name, then tier label (a header row with venue and tier is also accepted).",
    };
  }
  return { entries, skipped, truncated, error: "" };
}

/** Normalised venue to label. Longer keys win a prefix match, as in the built-in lists. */
export function compileCustomVenueIndex(entries) {
  const map = new Map();
  for (const [venue, label] of entries || []) {
    const key = normalizeVenueName(venue);
    if (key && label && !map.has(key)) map.set(key, String(label));
  }
  return map;
}

/** Cleans a user-typed list name for display in the badge tooltip. */
export function cleanListName(name) {
  const n = String(name ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_LIST_NAME_LENGTH);
  return n || "My list";
}
