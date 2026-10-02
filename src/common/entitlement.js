/**
 * Decides whether a user should be grandfathered (keep all current
 * features free forever) based on how the extension was loaded.
 *
 * `reason` is `details.reason` from chrome.runtime.onInstalled:
 *   "install"              — brand new installation, no prior data.
 *   "update"                — already installed, updating to a new version.
 *   "chrome_update"         — Chrome itself updated; treat as update (existing user).
 *   "shared_module_update"  — treat as update (existing user).
 *
 * `existingValue` is whatever is already stored under the `grandfathered`
 * key, or `undefined` if it has never been set. Once set, it must never
 * change — this guards against a listener firing more than once.
 *
 * Scope of the promise: grandfathering covers the Pro features that existed
 * when Pro launched (the "legacy" entries in PRO_FEATURES below), not
 * features added afterwards. Those are "new" and need a real purchase, trial
 * or App Pass for everyone, grandfathered or not — see canUseFeature().
 */
export function decideGrandfathered(reason, existingValue) {
  if (existingValue !== undefined) return existingValue;
  return reason !== "install";
}

/**
 * Free trial length. ExtensionPay's trial page is opened with a matching
 * period string, and the trial itself is enforced here: ExtPay only reports
 * `trialStartedAt`, it doesn't end the trial for us.
 */
export const TRIAL_DAYS = 14;
const DAY_MS = 24 * 60 * 60 * 1000;

/** ms timestamp when a trial that started at `trialStartedAt` ends, or null if there is none. */
export function trialEndsAt(trialStartedAt) {
  if (!trialStartedAt) return null;
  const started = new Date(trialStartedAt).getTime();
  return Number.isFinite(started) ? started + TRIAL_DAYS * DAY_MS : null;
}

export function isTrialActive(extpayUser, now = Date.now()) {
  const ends = trialEndsAt(extpayUser?.trialStartedAt);
  return ends !== null && now < ends;
}

/**
 * Shapes the final entitlement status shown across the extension's UI.
 *
 * `paid` means "may use the legacy Pro features" (what every existing gate
 * checks). `purchased` is stricter: a real purchase, an active trial or an
 * active App Pass — never grandfathering alone. New Pro features gate on
 * `purchased` via canUseFeature().
 *
 * `grandfathered` always wins for display (a paying-but-also-grandfathered
 * user is still just "grandfathered": they don't need to see a subscription
 * status for a purchase they didn't need to make), but `purchased` still
 * reflects what they actually bought. Only filled in when the caller passed
 * ExtPay/App Pass state (the service worker skips those lookups for
 * grandfathered users unless asked, so the common path stays offline).
 */
export function describeEntitlement({ grandfathered, extpayUser, appPass, now = Date.now() }) {
  const trial = isTrialActive(extpayUser, now);
  const appPassOk = appPass?.status === "ok";
  const purchased = !!extpayUser?.paid || appPassOk || trial;
  if (grandfathered) return { paid: true, tier: "grandfathered", purchased };
  if (extpayUser?.paid) return { paid: true, tier: extpayUser.plan?.nickname || "paid", purchased: true };
  // App Pass is a third way to be entitled, only ever consulted for users who
  // opted in (see sw.js). Anything but an explicit "ok" — including rate limits
  // and network errors — leaves the user on the free plan.
  if (appPassOk) return { paid: true, tier: "app-pass", purchased: true };
  if (trial) return { paid: true, tier: "trial", purchased: true, trialEndsAt: trialEndsAt(extpayUser.trialStartedAt) };
  return { paid: false, tier: "free", purchased: false };
}

/**
 * Every Pro feature and which promise it falls under.
 *   "legacy" — existed when Pro launched; grandfathered installs keep it free forever.
 *   "new"    — added later; everyone needs a purchase, trial or App Pass.
 * To add a Pro feature, register it here as "new" and gate it with
 * canUseFeature(), not with `entitlement.paid` (which grandfathered users satisfy).
 */
export const PRO_FEATURES = {
  compareAuthors: "legacy",
  citationLineage: "legacy",
  extendedBibliometrics: "legacy",
  narrativeCv: "legacy",
  reviewWorkspace: "legacy",
  citationGraph: "legacy",
  popReport: "legacy",
};

/** Unknown feature ids fail closed. `entitlement` must have been fetched with needPurchased for "new" features. */
export function canUseFeature(featureId, entitlement) {
  const kind = PRO_FEATURES[featureId];
  if (kind === undefined) return false;
  if (entitlement?.purchased) return true;
  return kind === "legacy" && entitlement?.tier === "grandfathered";
}

// How long a joinapppass.com answer is reused before asking again. getEntitlementStatus
// runs on every Scholar page load, so without this every page would hit their server
// (and their per-network rate limit). A valid pass is trusted for hours; "no pass" is
// rechecked soon so a just-completed activation shows up quickly; errors back off briefly.
const MINUTE = 60 * 1000;
export const APP_PASS_TTL_MS = {
  ok: 6 * 60 * MINUTE,
  no_apppass: 10 * MINUTE,
  rate_limited: MINUTE,
  unknown_error: MINUTE,
};

/** `cached` is `{ status, checkedAt }` as stored by the service worker. */
export function isAppPassCacheFresh(cached, now) {
  if (!cached || typeof cached.checkedAt !== "number") return false;
  const ttl = APP_PASS_TTL_MS[cached.status];
  if (ttl === undefined) return false;
  const age = now - cached.checkedAt;
  return age >= 0 && age < ttl;
}

/**
 * Callable from any extension context (content script via dynamic import,
 * popup, or options page) to get the current user's entitlement status.
 * Always resolves — never rejects — with { paid, tier, purchased }.
 *
 * Pass `{ needPurchased: true }` when gating a "new" Pro feature: that makes
 * the service worker look up ExtensionPay (and App Pass, if opted in) even for
 * grandfathered users, who are otherwise answered offline.
 */
export function getEntitlementStatus({ needPurchased = false } = {}) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ action: "getEntitlementStatus", needPurchased }, (response) => {
      resolve(response || { paid: false, tier: "free", purchased: false });
    });
  });
}
