// A dismissible "From the same developer" card on the extension's own pages (settings and library), never on
// Google Scholar pages. It is a plain link: the extension makes no request and records nothing about it. The only
// thing stored is that the card was dismissed, in chrome.storage.local, so it does not come back.
// ModernTex is a Mac app, so the card shows only on macOS. See docs/store-listing-cross-sell.md.

export const PROMO_URL = "https://purplelink.llc/moderntex/?utm_source=sub&utm_medium=extension&utm_campaign=promo-card";
export const DISMISS_KEY = "promoModernTexDismissed";

/** True on macOS. Prefers userAgentData, falls back to the platform string. */
export function isMacPlatform(nav) {
  if (!nav) return false;
  const uad = nav.userAgentData && nav.userAgentData.platform;
  if (typeof uad === "string" && uad) return /mac/i.test(uad);
  return /^mac/i.test(String(nav.platform || ""));
}

export function shouldShowPromo({ isMac, dismissed }) {
  return Boolean(isMac) && !dismissed;
}

/**
 * Shows #promoCard when it should be shown and wires #promoDismiss. Any failure leaves the card hidden,
 * so a storage problem never shows a promo that cannot be dismissed.
 */
export async function initPromoCard({ doc = document, storage = chrome.storage.local, nav = navigator } = {}) {
  const card = doc.getElementById("promoCard");
  const button = doc.getElementById("promoDismiss");
  const link = doc.getElementById("promoLink");
  if (!card || !button) return false;
  let dismissed;
  try {
    const stored = await storage.get(DISMISS_KEY);
    dismissed = Boolean(stored && stored[DISMISS_KEY]);
  } catch (_) {
    return false;
  }
  if (link) link.href = PROMO_URL;
  if (!shouldShowPromo({ isMac: isMacPlatform(nav), dismissed })) return false;
  button.addEventListener("click", async () => {
    card.hidden = true;
    try { await storage.set({ [DISMISS_KEY]: true }); } catch (_) { /* hidden for this visit either way */ }
  });
  card.hidden = false;
  return true;
}
