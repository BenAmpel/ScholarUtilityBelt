// Watch lists: saved queries and author names whose recent works are looked up in OpenAlex.
// Pure logic with an injected fetch function, so it can be tested without a network.
//
// What is sent to OpenAlex: the query text or author name the user saved, plus the optional API key from
// settings and the shared "mailto" parameter the other OpenAlex calls in this extension already use.
// Nothing is sent unless the user has added at least one watch item. Google Scholar is never fetched.
//
// Recalled, not verified against live docs (no live calls were made while writing this): the OpenAlex
// parameters `search`, `filter=from_publication_date:`, `filter=author.id:`, `sort=publication_date:desc`,
// `select=` and `per_page=` on /works, and `search=` on /authors. The existing code in this repo uses
// the same host, the `mailto` and `api_key` parameters, `author.id:` filters and `select=`.

export const WATCH_LIMITS = {
  maxItems: 20,
  maxResultsPerItem: 50,
  maxSeenIds: 300,
  maxValueLength: 120
};

export const DAY_MS = 24 * 60 * 60 * 1000;
export const CHECK_INTERVAL_MS = DAY_MS;
export const FIRST_LOOKBACK_DAYS = 60;
export const REPEAT_LOOKBACK_DAYS = 7;
export const OPENALEX_RATE_LIMIT_MS = 1000;
export const BACKOFF_BASE_MS = 30 * 60 * 1000;
export const CLAIM_MS = 5 * 60 * 1000;
export const OPENALEX_BASE = "https://api.openalex.org";
const MAILTO = "scholar-extension@local";

export function emptyWatchState() {
  return { items: [], errorCount: 0, backoffUntil: 0, claimUntil: 0 };
}

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function cleanValue(v) {
  return String(v ?? "")
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, WATCH_LIMITS.maxValueLength);
}

/** Repair whatever is in storage into a state the rest of this module can trust. */
export function normalizeWatchState(raw) {
  const state = emptyWatchState();
  if (!isPlainObject(raw)) return state;
  state.errorCount = Number.isFinite(raw.errorCount) ? Math.max(0, Math.floor(raw.errorCount)) : 0;
  state.backoffUntil = Number.isFinite(raw.backoffUntil) ? raw.backoffUntil : 0;
  state.claimUntil = Number.isFinite(raw.claimUntil) ? raw.claimUntil : 0;
  const seen = new Set();
  for (const it of Array.isArray(raw.items) ? raw.items : []) {
    if (!isPlainObject(it) || !it.id || seen.has(it.id)) continue;
    const type = it.type === "author" ? "author" : it.type === "query" ? "query" : null;
    const value = cleanValue(it.value);
    if (!type || !value) continue;
    seen.add(it.id);
    state.items.push({
      id: String(it.id),
      type,
      value,
      addedAt: Number.isFinite(it.addedAt) ? it.addedAt : 0,
      lastCheckedAt: Number.isFinite(it.lastCheckedAt) ? it.lastCheckedAt : 0,
      lastSeenAt: Number.isFinite(it.lastSeenAt) ? it.lastSeenAt : 0,
      lastError: typeof it.lastError === "string" ? it.lastError.slice(0, 200) : "",
      authorId: typeof it.authorId === "string" ? it.authorId : "",
      matchedName: typeof it.matchedName === "string" ? it.matchedName.slice(0, 120) : "",
      seenIds: Array.isArray(it.seenIds) ? it.seenIds.filter((x) => typeof x === "string").slice(0, WATCH_LIMITS.maxSeenIds) : [],
      unseen: Array.isArray(it.unseen) ? it.unseen.filter(isPlainObject).slice(0, WATCH_LIMITS.maxResultsPerItem) : []
    });
    if (state.items.length >= WATCH_LIMITS.maxItems) break;
  }
  return state;
}

export function addWatchItem(state, { type, value, now = Date.now() }) {
  const st = normalizeWatchState(state);
  const v = cleanValue(value);
  if (type !== "query" && type !== "author") return { ok: false, error: "Choose a search or an author." };
  if (!v) return { ok: false, error: "Enter a search or an author name." };
  if (v.length < 3) return { ok: false, error: "Enter at least 3 characters." };
  if (st.items.some((i) => i.type === type && i.value.toLowerCase() === v.toLowerCase())) {
    return { ok: false, error: "That item is already in the list." };
  }
  if (st.items.length >= WATCH_LIMITS.maxItems) {
    return { ok: false, error: `The list holds at most ${WATCH_LIMITS.maxItems} items. Remove one first.` };
  }
  const id = `w_${now.toString(36)}_${st.items.length}${Math.floor(Math.random() * 1296).toString(36)}`;
  st.items.push({
    id, type, value: v, addedAt: now, lastCheckedAt: 0, lastSeenAt: 0, lastError: "",
    authorId: "", matchedName: "", seenIds: [], unseen: []
  });
  return { ok: true, state: st };
}

export function removeWatchItem(state, id) {
  const st = normalizeWatchState(state);
  st.items = st.items.filter((i) => i.id !== id);
  return st;
}

/** Move an item's unseen works to its seen list ("I have looked at these"). */
export function markItemSeen(state, id, now = Date.now()) {
  const st = normalizeWatchState(state);
  const it = st.items.find((i) => i.id === id);
  if (it) {
    it.unseen = [];
    it.lastSeenAt = now;
  }
  return st;
}

export function totalUnseen(state) {
  return normalizeWatchState(state).items.reduce((n, i) => n + i.unseen.length, 0);
}

export function isItemDue(item, now) {
  return !item.lastCheckedAt || now - item.lastCheckedAt >= CHECK_INTERVAL_MS || item.lastCheckedAt > now;
}

/** Whether a check should run at all right now (backoff, another page's claim, nothing due). */
export function shouldRunCheck(state, now = Date.now()) {
  const st = normalizeWatchState(state);
  if (!st.items.length) return { run: false, reason: "empty" };
  if (st.backoffUntil > now) return { run: false, reason: "backoff" };
  if (st.claimUntil > now) return { run: false, reason: "busy" };
  if (!st.items.some((i) => isItemDue(i, now))) return { run: false, reason: "not due" };
  return { run: true, reason: "" };
}

function isoDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

function withCommon(params, apiKey) {
  params.set("mailto", MAILTO);
  if (apiKey) params.set("api_key", apiKey);
  return params;
}

export function buildAuthorSearchUrl(name, { apiKey = "" } = {}) {
  const p = new URLSearchParams();
  p.set("search", name);
  p.set("per_page", "5");
  p.set("select", "id,display_name,works_count");
  return `${OPENALEX_BASE}/authors?${withCommon(p, apiKey)}`;
}

export function buildWorksUrl(item, { now = Date.now(), apiKey = "" } = {}) {
  const since = item.lastCheckedAt
    ? item.lastCheckedAt - REPEAT_LOOKBACK_DAYS * DAY_MS
    : now - FIRST_LOOKBACK_DAYS * DAY_MS;
  const filters = [`from_publication_date:${isoDate(Math.max(0, since))}`];
  const p = new URLSearchParams();
  if (item.type === "author") filters.unshift(`author.id:${item.authorId}`);
  else p.set("search", item.value);
  p.set("filter", filters.join(","));
  p.set("sort", "publication_date:desc");
  p.set("per_page", String(WATCH_LIMITS.maxResultsPerItem));
  p.set("select", "id,display_name,publication_date,publication_year,doi,type,primary_location,authorships");
  return `${OPENALEX_BASE}/works?${withCommon(p, apiKey)}`;
}

/** Reduce one OpenAlex work to the fields the library page shows. Returns null for unusable records. */
export function compactWork(w) {
  if (!isPlainObject(w) || typeof w.id !== "string" || !w.id) return null;
  const title = cleanValue(w.display_name || w.title);
  if (!title) return null;
  const doiRaw = typeof w.doi === "string" ? w.doi : "";
  const doi = doiRaw.replace(/^https?:\/\/(dx\.)?doi\.org\//i, "");
  const authors = (Array.isArray(w.authorships) ? w.authorships : [])
    .map((a) => cleanValue(a?.author?.display_name))
    .filter(Boolean);
  const url = doi ? `https://doi.org/${doi}` : w.id;
  return {
    id: w.id,
    title,
    date: typeof w.publication_date === "string" ? w.publication_date.slice(0, 10) : "",
    year: Number.isFinite(Number(w.publication_year)) ? Number(w.publication_year) : null,
    doi,
    url: /^https?:\/\//i.test(url) ? url : "",
    venue: cleanValue(w.primary_location?.source?.display_name),
    authors: authors.slice(0, 3),
    authorCount: authors.length,
    type: typeof w.type === "string" ? w.type.slice(0, 40) : ""
  };
}

export function parseWorksResponse(body, cap = WATCH_LIMITS.maxResultsPerItem) {
  const results = Array.isArray(body?.results) ? body.results : [];
  const out = [];
  for (const w of results) {
    const c = compactWork(w);
    if (c) out.push(c);
    if (out.length >= cap) break;
  }
  return out;
}

/** Pick the author to follow for a saved name. The matched name is stored so the list can show it. */
export function pickAuthor(body, name) {
  const results = Array.isArray(body?.results) ? body.results : [];
  const cands = results.filter((r) => isPlainObject(r) && typeof r.id === "string" && r.display_name);
  if (!cands.length) return null;
  const want = cleanValue(name).toLowerCase();
  const exact = cands.filter((r) => String(r.display_name).toLowerCase() === want);
  const pool = exact.length ? exact : cands.slice(0, 1);
  pool.sort((a, b) => (b.works_count || 0) - (a.works_count || 0));
  const best = pool[0];
  return { id: best.id.replace(/^https?:\/\/openalex\.org\//, ""), name: cleanValue(best.display_name), worksCount: best.works_count || 0 };
}

export function backoffDelay(errorCount) {
  const n = Math.max(1, errorCount);
  return Math.min(DAY_MS, BACKOFF_BASE_MS * 2 ** (n - 1));
}

function describeFailure(res) {
  if (res?.status === 429) return "OpenAlex asked us to slow down (429).";
  if (res?.status >= 500) return `OpenAlex had a server problem (${res.status}).`;
  if (res?.status) return `OpenAlex answered ${res.status}.`;
  return "OpenAlex could not be reached.";
}

/** Failures that justify backing off everything, as opposed to one bad item. */
function isTransient(res) {
  return !res?.status || res.status === 429 || res.status >= 500;
}

/**
 * Check every due item, one request at a time with a pause between requests.
 *
 * fetchJson(url) must resolve { ok, status, body } and never reject (wrap network errors as ok:false).
 * Stops at the first transient failure (429, 5xx, network) and sets an exponential backoff, so a flaky
 * network or a rate limit costs one request, not twenty.
 *
 * Returns { state, checked, newWorks, stopped, error }.
 */
export async function runWatchCheck(rawState, { fetchJson, now = Date.now(), apiKey = "", sleep = (ms) => new Promise((r) => setTimeout(r, ms)), rateLimitMs = OPENALEX_RATE_LIMIT_MS } = {}) {
  const state = normalizeWatchState(rawState);
  const result = { state, checked: 0, newWorks: 0, stopped: "", error: "" };
  const gate = shouldRunCheck(state, now);
  if (!gate.run) {
    result.stopped = gate.reason;
    return result;
  }
  let requests = 0;
  const call = async (url) => {
    if (requests++ > 0) await sleep(rateLimitMs);
    try {
      return (await fetchJson(url)) || { ok: false };
    } catch {
      return { ok: false };
    }
  };

  for (const item of state.items) {
    if (!isItemDue(item, now)) continue;

    if (item.type === "author" && !item.authorId) {
      const res = await call(buildAuthorSearchUrl(item.value, { apiKey }));
      if (!res.ok) {
        item.lastError = describeFailure(res);
        if (isTransient(res)) return stopOnTransient(result, state, res, now);
        item.lastCheckedAt = now;
        continue;
      }
      const pick = pickAuthor(res.body, item.value);
      if (!pick) {
        item.lastError = "No author in OpenAlex matched this name.";
        item.lastCheckedAt = now;
        continue;
      }
      item.authorId = pick.id;
      item.matchedName = `${pick.name} (${pick.worksCount} works)`;
    }

    const res = await call(buildWorksUrl(item, { now, apiKey }));
    if (!res.ok) {
      item.lastError = describeFailure(res);
      if (isTransient(res)) return stopOnTransient(result, state, res, now);
      item.lastCheckedAt = now;
      continue;
    }
    const works = parseWorksResponse(res.body);
    const known = new Set([...item.seenIds, ...item.unseen.map((w) => w.id)]);
    const fresh = works.filter((w) => !known.has(w.id));
    item.unseen = [...fresh, ...item.unseen].slice(0, WATCH_LIMITS.maxResultsPerItem);
    item.seenIds = [...fresh.map((w) => w.id), ...item.seenIds].slice(0, WATCH_LIMITS.maxSeenIds);
    item.lastCheckedAt = now;
    item.lastError = "";
    state.errorCount = 0;
    result.checked++;
    result.newWorks += fresh.length;
  }
  state.claimUntil = 0;
  return result;
}

function stopOnTransient(result, state, res, now) {
  state.errorCount += 1;
  state.backoffUntil = now + backoffDelay(state.errorCount);
  state.claimUntil = 0;
  result.stopped = "error";
  result.error = describeFailure(res);
  return result;
}

/**
 * Apply the outcome of a check onto the latest stored state. The user may have added, removed or
 * cleared items while the check ran; only items that still exist are updated, and per-item fields
 * that the user can change (unseen cleared by "mark seen") are not resurrected.
 */
export function mergeCheckResult(latestRaw, checked) {
  const latest = normalizeWatchState(latestRaw);
  const byId = new Map(checked.items.map((i) => [i.id, i]));
  for (const item of latest.items) {
    const c = byId.get(item.id);
    if (!c) continue;
    item.authorId = c.authorId || item.authorId;
    item.matchedName = c.matchedName || item.matchedName;
    item.lastCheckedAt = Math.max(item.lastCheckedAt, c.lastCheckedAt);
    item.lastError = c.lastError;
    const known = new Set(item.unseen.map((w) => w.id));
    const added = c.unseen.filter((w) => !known.has(w.id) && !item.seenIds.includes(w.id));
    item.unseen = [...added, ...item.unseen].slice(0, WATCH_LIMITS.maxResultsPerItem);
    item.seenIds = Array.from(new Set([...c.seenIds, ...item.seenIds])).slice(0, WATCH_LIMITS.maxSeenIds);
  }
  latest.errorCount = checked.errorCount;
  latest.backoffUntil = checked.backoffUntil;
  latest.claimUntil = 0;
  return latest;
}
