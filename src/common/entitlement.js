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
 */
export function decideGrandfathered(reason, existingValue) {
  if (existingValue !== undefined) return existingValue;
  return reason !== "install";
}

/**
 * Shapes the final entitlement status shown across the extension's UI.
 * `grandfathered` always wins (a paying-but-also-grandfathered user is
 * still just "grandfathered" for display purposes — they don't need to
 * see a subscription status for a purchase they didn't need to make).
 */
export function describeEntitlement({ grandfathered, extpayUser }) {
  if (grandfathered) return { paid: true, tier: "grandfathered" };
  if (extpayUser?.paid) return { paid: true, tier: extpayUser.plan?.nickname || "paid" };
  return { paid: false, tier: "free" };
}

/**
 * Callable from any extension context (content script via dynamic import,
 * popup, or options page) to get the current user's entitlement status.
 * Always resolves — never rejects — with { paid, tier }.
 */
export function getEntitlementStatus() {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ action: "getEntitlementStatus" }, (response) => {
      resolve(response || { paid: false, tier: "free" });
    });
  });
}
