// Runs the watch-list check against chrome.storage.local. Called from the library page and from a Scholar
// page (content script), never from a timer: there is no alarm and no notification. At most one check per
// item per 24 hours; backoff and a short claim stop two open pages from both fetching.

import { canUseFeature, getEntitlementStatus } from "./entitlement.js";
import { getSettings, getWatchLists, setWatchLists } from "./storage.js";
import { CLAIM_MS, mergeCheckResult, normalizeWatchState, runWatchCheck, shouldRunCheck } from "./watchlist.js";

/** fetchJson for extension pages: a plain fetch with a timeout that never rejects. */
export async function pageFetchJson(url, timeoutMs = 10000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { credentials: "omit", signal: ctrl.signal });
    if (!res.ok) return { ok: false, status: res.status };
    return { ok: true, status: res.status, body: await res.json() };
  } catch {
    return { ok: false };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run a check if one is due. Returns { ran, reason?, checked?, newWorks?, error? }.
 * `fetchJson` defaults to pageFetchJson; a content script passes a function that routes through the service worker.
 */
export async function maybeRunWatchCheck({ fetchJson = pageFetchJson, now = Date.now(), gate = true } = {}) {
  const stored = normalizeWatchState(await getWatchLists());
  const first = shouldRunCheck(stored, now);
  if (!first.run) return { ran: false, reason: first.reason };

  if (gate) {
    let allowed;
    try {
      allowed = canUseFeature("watchLists", await getEntitlementStatus({ needPurchased: true }));
    } catch {
      return { ran: false, reason: "locked" };
    }
    if (!allowed) return { ran: false, reason: "locked" };
  }

  // Claim, re-reading first so a page that claimed a moment ago wins.
  const fresh = normalizeWatchState(await getWatchLists());
  if (!shouldRunCheck(fresh, now).run) return { ran: false, reason: "busy" };
  fresh.claimUntil = now + CLAIM_MS;
  await setWatchLists(fresh);

  const apiKey = await getSettings()
    .then((st) => String(st?.openalexApiKey || "").trim())
    .catch(() => "");

  let outcome;
  try {
    outcome = await runWatchCheck({ ...fresh, claimUntil: 0 }, { fetchJson, now, apiKey });
  } catch (e) {
    const latest = normalizeWatchState(await getWatchLists());
    latest.claimUntil = 0;
    await setWatchLists(latest);
    return { ran: false, reason: "error", error: String(e?.message || e) };
  }
  const latest = await getWatchLists();
  await setWatchLists(mergeCheckResult(latest, outcome.state));
  return { ran: true, checked: outcome.checked, newWorks: outcome.newWorks, stopped: outcome.stopped, error: outcome.error };
}
