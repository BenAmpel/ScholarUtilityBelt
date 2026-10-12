/**
 * Librarian batch report: match typed researcher names to OpenAlex authors, let the user confirm each
 * match, then report OpenAlex numbers for the confirmed ones. Google Scholar is never fetched here.
 *
 * Everything that talks to the network takes a `fetchFn`, so tests use a mock and nothing in this file
 * can call out on its own.
 *
 * WHAT THIS RELIES ON FROM MEMORY OF THE OPENALEX API (UNVERIFIED, no live calls were made):
 *  - GET /authors?search=<text> returns { results: [author...] } ranked by relevance.
 *  - GET /authors?filter=orcid:https://orcid.org/<id> returns the author with that ORCID.
 *  - An author has: id, display_name, orcid, works_count, cited_by_count,
 *    summary_stats { h_index, i10_index, "2yr_mean_citedness" }, last_known_institutions [ { display_name } ]
 *    (older records: last_known_institution { display_name }), topics [ { display_name, count } ]
 *    (older records: x_concepts [ { display_name, score } ]), counts_by_year [ { year, works_count, cited_by_count } ].
 *  - GET /works?filter=author.id:<id>&group_by=primary_location.source.id returns
 *    { group_by: [ { key, key_display_name, count } ] }, one group per venue (journal, repository, ...).
 *  - 429 means slow down and 5xx is transient; both are retried with backoff here.
 * Any field that is missing is shown as empty, never invented.
 */

export const MAX_NAMES = 50;
export const MAX_LINE_CHARS = 200;
export const REQUEST_GAP_MS = 1000;
export const REQUEST_TIMEOUT_MS = 8000;
export const MAX_RETRIES = 3;
export const MAX_CANDIDATES = 3;
const API = "https://api.openalex.org";
const MAILTO = "scholar-extension@local"; // the same placeholder the other OpenAlex calls use

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

/** ISO 7064 mod 11-2 check on a 16 character ORCID (digits, last may be X). */
export function orcidChecksumOk(orcid) {
  const s = String(orcid || "").replace(/-/g, "").toUpperCase();
  if (!/^\d{15}[\dX]$/.test(s)) return false;
  let total = 0;
  for (let i = 0; i < 15; i++) total = (total + Number(s[i])) * 2;
  const result = (12 - (total % 11)) % 11;
  return s[15] === (result === 10 ? "X" : String(result));
}

const ORCID_RE = /(?:https?:\/\/orcid\.org\/)?(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])/;

/** Normalised ORCID ("0000-0002-1825-0097") found in text, or "" (also "" when the checksum fails). */
export function findOrcid(text) {
  const m = String(text || "").match(ORCID_RE);
  if (!m) return "";
  const id = m[1].toUpperCase();
  return orcidChecksumOk(id) ? id : "";
}

/**
 * One researcher per line: "Name", "Name | institution", an ORCID alone, or "Name | ORCID".
 * Blank lines are skipped. At most MAX_NAMES entries are kept; `truncated` counts the rest.
 */
export function parseNameList(text) {
  const entries = [];
  let truncated = 0;
  for (const rawLine of String(text || "").split(/\r?\n/)) {
    const line = rawLine.trim().slice(0, MAX_LINE_CHARS);
    if (!line) continue;
    if (entries.length >= MAX_NAMES) { truncated++; continue; }
    const parts = line.split("|").map((p) => p.trim());
    let orcid = "";
    let badOrcid = false;
    const kept = [];
    for (const p of parts) {
      const found = findOrcid(p);
      if (found) orcid = found;
      else if (ORCID_RE.test(p)) badOrcid = true; // looks like an ORCID but the checksum fails
      else kept.push(p);
    }
    const name = (kept[0] || "").replace(/\s+/g, " ");
    const institution = kept.slice(1).join(" ").replace(/\s+/g, " ").trim();
    entries.push({ index: entries.length + 1, raw: line, name, institution, orcid, badOrcid });
  }
  return { entries, truncated };
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/** Adds mailto and, when set, the user's API key. */
export function withCommonParams(url, apiKey = "") {
  const u = new URL(url);
  if (!u.searchParams.has("mailto")) u.searchParams.set("mailto", MAILTO);
  if (apiKey && !u.searchParams.has("api_key")) u.searchParams.set("api_key", apiKey);
  return u.toString();
}

export function authorSearchUrl(entry, apiKey = "") {
  const u = new URL(`${API}/authors`);
  if (entry.orcid) u.searchParams.set("filter", `orcid:https://orcid.org/${entry.orcid}`);
  else u.searchParams.set("search", entry.name);
  u.searchParams.set("per-page", "5");
  return withCommonParams(u.toString(), apiKey);
}

export function authorVenuesUrl(authorId, apiKey = "") {
  const u = new URL(`${API}/works`);
  u.searchParams.set("filter", `author.id:${shortId(authorId)}`);
  u.searchParams.set("group_by", "primary_location.source.id");
  return withCommonParams(u.toString(), apiKey);
}

/** "https://openalex.org/A123" -> "A123". */
export function shortId(id) {
  const m = String(id || "").match(/A\d+/i);
  return m ? m[0].toUpperCase() : String(id || "");
}

/**
 * GET and parse JSON with a timeout and backoff. Retries on 429, 5xx, network errors and timeouts
 * (up to maxRetries more attempts, waiting 2 s, 4 s, 8 s or the server's Retry-After, at most 30 s).
 * @returns {Promise<{ok:true, data:object}|{ok:false, status:number, error:string}>}
 */
export async function getJson(url, opts = {}) {
  const { fetchFn, timeoutMs = REQUEST_TIMEOUT_MS, maxRetries = MAX_RETRIES, sleep = defaultSleep, shouldCancel = () => false } = opts;
  let last = { ok: false, status: 0, error: "no attempt" };
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (shouldCancel()) return { ok: false, status: 0, error: "cancelled" };
    let retryAfterMs = 0;
    const ctl = typeof AbortController === "function" ? new AbortController() : null;
    const timer = setTimeout(() => ctl && ctl.abort(), timeoutMs);
    try {
      const res = await fetchFn(url, { headers: { Accept: "application/json" }, signal: ctl ? ctl.signal : undefined });
      clearTimeout(timer);
      if (res.ok) {
        try {
          return { ok: true, data: await res.json() };
        } catch {
          return { ok: false, status: res.status, error: "OpenAlex sent an answer that could not be read." };
        }
      }
      last = { ok: false, status: res.status, error: res.status === 429 ? "OpenAlex asked us to slow down (429)." : `OpenAlex answered ${res.status}.` };
      if (res.status !== 429 && res.status < 500) return last;
      const ra = Number(res.headers && typeof res.headers.get === "function" ? res.headers.get("retry-after") : 0);
      if (Number.isFinite(ra) && ra > 0) retryAfterMs = Math.min(ra * 1000, 30000);
    } catch (e) {
      clearTimeout(timer);
      const aborted = e && e.name === "AbortError";
      last = { ok: false, status: 0, error: aborted ? `No answer within ${Math.round(timeoutMs / 1000)} seconds.` : "The request failed (network error)." };
    }
    if (attempt < maxRetries) await sleep(retryAfterMs || 2000 * 2 ** attempt);
  }
  return last;
}

function defaultSleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------

const num = (v) => (typeof v === "number" && Number.isFinite(v) ? v : null);

/** A small, display-ready object from an OpenAlex author record. Missing fields stay null or empty. */
export function normalizeAuthor(a) {
  if (!a || typeof a !== "object" || !a.id) return null;
  const stats = a.summary_stats && typeof a.summary_stats === "object" ? a.summary_stats : {};
  let institutions = Array.isArray(a.last_known_institutions) ? a.last_known_institutions : [];
  if (!institutions.length && a.last_known_institution) institutions = [a.last_known_institution];
  const institution = institutions.map((i) => (i && i.display_name) || "").filter(Boolean).join("; ");
  let topics = [];
  if (Array.isArray(a.topics) && a.topics.length) {
    topics = a.topics.map((t) => (t && t.display_name) || "");
  } else if (Array.isArray(a.x_concepts)) {
    topics = a.x_concepts.map((t) => (t && t.display_name) || "");
  }
  return {
    id: String(a.id),
    shortId: shortId(a.id),
    name: String(a.display_name || ""),
    orcid: a.orcid ? findOrcid(String(a.orcid)) : "",
    institution,
    worksCount: num(a.works_count),
    citedBy: num(a.cited_by_count),
    hIndex: num(stats.h_index),
    i10Index: num(stats.i10_index),
    meanCitedness2y: num(stats["2yr_mean_citedness"]),
    topics: topics.filter(Boolean).slice(0, 5),
    countsByYear: Array.isArray(a.counts_by_year)
      ? a.counts_by_year.map((c) => ({ year: num(c && c.year), works: num(c && c.works_count) })).filter((c) => c.year !== null && c.works !== null)
      : [],
  };
}

function fold(s) {
  return String(s || "").normalize("NFKD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

/**
 * Up to MAX_CANDIDATES candidates, best first, each with `match`:
 *  - "orcid": the author's ORCID equals the typed one (the only match that is pre-confirmed);
 *  - "suggested": the first of the others, only a guess;
 *  - "" for the rest.
 * Ranking: ORCID, then institution text found in the author's institution, then an exact name match
 * (accents and case ignored), then OpenAlex's own order.
 */
export function rankCandidates(entry, results) {
  const list = (Array.isArray(results) ? results : []).map(normalizeAuthor).filter(Boolean);
  const wantName = fold(entry.name);
  const wantInst = fold(entry.institution);
  const scored = list.map((c, i) => {
    const orcidHit = !!entry.orcid && c.orcid === entry.orcid;
    const instHit = !!wantInst && fold(c.institution).includes(wantInst);
    const nameHit = !!wantName && fold(c.name) === wantName;
    return { c, i, score: (orcidHit ? 100 : 0) + (instHit ? 10 : 0) + (nameHit ? 5 : 0), orcidHit };
  });
  scored.sort((a, b) => b.score - a.score || a.i - b.i);
  const top = scored.slice(0, MAX_CANDIDATES);
  let suggestedGiven = false;
  return top.map(({ c, orcidHit }) => {
    let match = "";
    if (orcidHit) match = "orcid";
    else if (!suggestedGiven) { match = "suggested"; suggestedGiven = true; }
    return { ...c, match };
  });
}

/**
 * Looks up every entry, one request per `gapMs`. Resolves to one row per entry:
 * { entry, status: "ok"|"none"|"error"|"skipped", candidates, confirmedId, error }.
 * `confirmedId` is set only for an exact ORCID match; a suggestion is never confirmed here.
 */
export async function searchAuthors(entries, opts = {}) {
  const { fetchFn, apiKey = "", gapMs = REQUEST_GAP_MS, sleep = defaultSleep, shouldCancel = () => false, onProgress = () => {}, timeoutMs, maxRetries } = opts;
  const rows = [];
  let first = true;
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if (shouldCancel()) { rows.push({ entry, status: "skipped", candidates: [], confirmedId: "", error: "" }); continue; }
    if (!entry.name && !entry.orcid) {
      rows.push({ entry, status: "error", candidates: [], confirmedId: "", error: entry.badOrcid ? "That ORCID has a wrong check digit." : "No name on this line." });
      continue;
    }
    if (!first) await sleep(gapMs);
    first = false;
    const res = await getJson(authorSearchUrl(entry, apiKey), { fetchFn, sleep, shouldCancel, timeoutMs, maxRetries });
    let row;
    if (!res.ok) {
      row = { entry, status: "error", candidates: [], confirmedId: "", error: res.error };
    } else {
      const candidates = rankCandidates(entry, res.data && res.data.results);
      const exact = candidates.find((c) => c.match === "orcid");
      row = { entry, status: candidates.length ? "ok" : "none", candidates, confirmedId: exact ? exact.id : "", error: "" };
    }
    rows.push(row);
    onProgress(i + 1, entries.length, row);
  }
  return rows;
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/** Works counted in the last five calendar years including the current one; null if the years are not listed. */
export function worksInLastFiveYears(countsByYear, now = new Date()) {
  if (!Array.isArray(countsByYear) || !countsByYear.length) return null;
  const from = now.getFullYear() - 4;
  return countsByYear.filter((c) => c.year >= from && c.year <= now.getFullYear()).reduce((s, c) => s + c.works, 0);
}

/** Top venues from a works group_by answer: up to five, largest first, the "unknown" group dropped. */
export function topVenues(groupBy, n = 5) {
  return (Array.isArray(groupBy) ? groupBy : [])
    .filter((g) => g && g.key && g.key !== "unknown" && num(g.count) !== null)
    .map((g) => ({ name: String(g.key_display_name || g.key), count: g.count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, n);
}

/**
 * Report rows for the confirmed authors only. `confirmed` is [{ entry, candidate }].
 * Fetches each author's venues (one request per gapMs). A venue failure leaves venues empty and sets
 * `venueError`; the numbers from the search are still reported.
 */
export async function buildReport(confirmed, opts = {}) {
  const { fetchFn, apiKey = "", gapMs = REQUEST_GAP_MS, sleep = defaultSleep, shouldCancel = () => false, onProgress = () => {}, now = new Date(), timeoutMs, maxRetries } = opts;
  const rows = [];
  for (let i = 0; i < confirmed.length; i++) {
    if (shouldCancel()) break;
    const { entry, candidate: c } = confirmed[i];
    if (i > 0) await sleep(gapMs);
    const res = await getJson(authorVenuesUrl(c.id, apiKey), { fetchFn, sleep, shouldCancel, timeoutMs, maxRetries });
    rows.push({
      typed: entry.raw,
      name: c.name,
      openalexId: c.id,
      orcid: c.orcid,
      institution: c.institution,
      worksCount: c.worksCount,
      citedBy: c.citedBy,
      hIndex: c.hIndex,
      meanCitedness2y: c.meanCitedness2y,
      worksLastFiveYears: worksInLastFiveYears(c.countsByYear, now),
      topVenues: res.ok ? topVenues(res.data && res.data.group_by) : [],
      venueError: res.ok ? "" : res.error,
    });
    onProgress(i + 1, confirmed.length, rows[rows.length - 1]);
  }
  return rows;
}

function csvCell(v) {
  let s = v == null ? "" : String(v);
  if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`; // keep spreadsheets from running text that starts like a formula
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function reportToCsv(rows) {
  const head = ["typed", "openalex_name", "openalex_id", "orcid", "last_known_institution", "works", "citations", "h_index", "mean_citedness_2y", "works_last_5_years", "top_venues", "venue_error"];
  const lines = [head.join(",")];
  for (const r of rows) {
    lines.push([
      r.typed, r.name, r.openalexId, r.orcid, r.institution, r.worksCount, r.citedBy, r.hIndex,
      r.meanCitedness2y === null || r.meanCitedness2y === undefined ? "" : r.meanCitedness2y.toFixed(2),
      r.worksLastFiveYears,
      r.topVenues.map((v) => `${v.name} (${v.count})`).join("; "),
      r.venueError,
    ].map(csvCell).join(","));
  }
  return lines.join("\n") + "\n";
}
