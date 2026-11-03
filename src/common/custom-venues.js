/**
 * Your own venue list: a CSV of journal or conference names with a tier label
 * (for example a department's "A / B / C" list). Parsing, lookup and the "Add to list"
 * control on a result; no network. The list lives in chrome.storage.local only, and the
 * caller passes in the storage functions.
 */
import { isPreprintVenue, normalizeVenueName } from "./quality.js";

export const MAX_ROWS = 5000;
export const MAX_LABEL_LENGTH = 16;
export const MAX_VENUE_LENGTH = 200;
export const MAX_LIST_NAME_LENGTH = 40;
export const DEFAULT_LIST_NAME = "My list";

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
  return n || DEFAULT_LIST_NAME;
}

// ── Adding one venue from a result ──────────────────────────────────────────

const ELLIPSIS = /…|\.\.\./;
// "books.google.com", "search.proquest.com": Scholar shows the host when it has no venue.
const HOST_ONLY = /^[^\s]+\.[a-z]{2,}$/i;

/**
 * True when a venue string as Scholar shows it is worth offering to add: not empty, not cut
 * off with "…" (the full name is unknown), not a preprint server, a bare year or a host name.
 */
export function isAddableVenue(venue) {
  const raw = String(venue ?? "").replace(/\s+/g, " ").trim();
  if (raw.length < 3 || raw.length > MAX_VENUE_LENGTH) return false;
  if (ELLIPSIS.test(raw) || HOST_ONLY.test(raw) || isPreprintVenue(raw)) return false;
  const norm = normalizeVenueName(raw);
  return /[a-z]{2}/.test(norm);
}

/**
 * Whether a search result gets the chip: badges are on, nothing matched any list (built-in or
 * the user's own), "Your own list" badges are not switched off, and the venue string is usable.
 * Author profile rows are left alone.
 */
export function shouldOfferAddToList({ venue, rawBadges, settings, isAuthorProfile }) {
  if (isAuthorProfile || !settings?.showQualityBadges) return false;
  if (Array.isArray(rawBadges) && rawBadges.length) return false;
  const kinds = settings.qualityBadgeKinds;
  if (kinds && typeof kinds === "object" && kinds.custom === false) return false;
  return isAddableVenue(venue);
}

/** The tier labels already in a list, in first-seen order (the choices offered when adding). */
export function customListLabels(list) {
  const seen = new Set();
  for (const e of list?.entries || []) if (e && e[1]) seen.add(String(e[1]));
  return [...seen];
}

/**
 * Returns a new list with [venue, label] appended; the input is not changed. With no list,
 * starts one called "My list". A venue already on the list keeps its entry (added: false).
 * @returns {{ list: object|null, added: boolean, error: string }}
 */
export function appendVenueToList(list, venue, label, now = new Date()) {
  const v = String(venue ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_VENUE_LENGTH);
  const l = String(label ?? "").replace(/\s+/g, " ").trim().slice(0, MAX_LABEL_LENGTH);
  const norm = normalizeVenueName(v);
  if (!norm) return { list, added: false, error: "This result has no venue name to add." };
  if (!l) return { list, added: false, error: "Enter a tier label." };
  const base = list && Array.isArray(list.entries)
    ? list
    : { name: DEFAULT_LIST_NAME, uploadedAt: now.toISOString(), entries: [] };
  if (base.entries.some((e) => normalizeVenueName(e[0]) === norm)) return { list: base, added: false, error: "" };
  if (base.entries.length >= MAX_ROWS) return { list: base, added: false, error: "Your list is full (5,000 venues)." };
  return { list: { ...base, entries: [...base.entries, [v, l]] }, added: true, error: "" };
}

/**
 * Reads the stored list, appends the venue and saves it, through the same get/set pair the
 * options page uses. The content script redraws badges from the storage change event.
 */
export async function addVenueToStoredList({ getList, setList }, venue, label) {
  const result = appendVenueToList(await getList(), venue, label);
  if (result.added) await setList(result.list);
  return result;
}

let addFormSeq = 0;

/**
 * The quiet "Add to list" chip for a result whose venue matched no list, with a small inline
 * form to pick the tier. `onAdd(label)` saves and resolves to appendVenueToList's result.
 * `doc` is passed in so this can be built without a page.
 */
export function buildAddToListControl(doc, { venue, listName, labels = [], onAdd }) {
  if (!doc?.createElement || !isAddableVenue(venue)) return null;
  const name = cleanListName(listName);
  const shown = String(venue).replace(/\s+/g, " ").trim();
  const newList = !listName;
  const id = `su-add-to-list-tiers-${++addFormSeq}`;

  const wrap = doc.createElement("span");
  wrap.className = "su-add-to-list";

  const chip = doc.createElement("button");
  chip.type = "button";
  chip.className = "su-badge su-add-to-list-btn";
  chip.textContent = "Add to list";
  chip.title = `Add "${shown}" to ${newList ? "a new list" : "your list"} "${name}" with a tier label`;
  chip.setAttribute("aria-label", `Add ${shown} to your list ${name}`);
  chip.setAttribute("aria-expanded", "false");
  wrap.appendChild(chip);

  const form = doc.createElement("span");
  form.className = "su-add-to-list-form";
  form.setAttribute("role", "group");
  form.setAttribute("aria-label", `Add ${shown} to ${name}`);
  form.hidden = true;

  const field = doc.createElement("label");
  field.className = "su-add-to-list-label";
  field.textContent = newList ? `Tier (new list "${name}")` : `Tier in "${name}"`;
  const input = doc.createElement("input");
  input.type = "text";
  input.className = "su-add-to-list-input";
  input.maxLength = MAX_LABEL_LENGTH;
  input.autocomplete = "off";
  input.placeholder = "e.g. A";
  input.value = labels[0] || "";
  if (labels.length) {
    input.setAttribute("list", id);
    const dl = doc.createElement("datalist");
    dl.id = id;
    for (const l of labels) {
      const opt = doc.createElement("option");
      opt.value = l;
      dl.appendChild(opt);
    }
    form.appendChild(dl);
  }
  field.appendChild(input);
  form.appendChild(field);

  const save = doc.createElement("button");
  save.type = "button";
  save.className = "su-add-to-list-save";
  save.textContent = "Add";
  const cancel = doc.createElement("button");
  cancel.type = "button";
  cancel.className = "su-add-to-list-cancel";
  cancel.textContent = "Cancel";
  const status = doc.createElement("span");
  status.className = "su-add-to-list-status";
  status.setAttribute("role", "status");
  form.appendChild(save);
  form.appendChild(cancel);
  form.appendChild(status);
  wrap.appendChild(form);

  const stop = (e) => { e?.preventDefault?.(); e?.stopPropagation?.(); };
  const open = (yes) => {
    form.hidden = !yes;
    chip.hidden = yes;
    chip.setAttribute("aria-expanded", yes ? "true" : "false");
    status.textContent = "";
    if (yes) input.focus?.(); else chip.focus?.();
  };
  const submit = async () => {
    if (save.disabled) return;
    save.disabled = true;
    try {
      const r = await onAdd(input.value);
      if (r?.error) status.textContent = r.error;
      // Added (or already there): the storage change redraws this row with the new badge.
      else status.textContent = `Added to "${name}".`;
    } catch {
      status.textContent = "Couldn't save. Reload the page and try again.";
    } finally {
      save.disabled = false;
    }
  };
  chip.addEventListener("click", (e) => { stop(e); open(true); });
  cancel.addEventListener("click", (e) => { stop(e); open(false); });
  save.addEventListener("click", (e) => { stop(e); submit(); });
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { stop(e); submit(); }
    else if (e.key === "Escape") { stop(e); open(false); }
  });
  return wrap;
}
