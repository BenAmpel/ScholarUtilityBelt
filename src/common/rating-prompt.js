// A "rate it" card on the extension's own pages (popup, Options, Library), never on Google Scholar pages.
// It shows only after real use: Scholar pages opened on at least MIN_ACTIVE_DAYS different days, the first of them
// at least MIN_DAYS_SINCE_FIRST days ago. The count is local: a date, a number and the choice made on the card, in
// chrome.storage.local. Nothing is sent anywhere, and the rating link is a plain link to the store's review page.
// The question is the same for everyone, with no reward and nothing locked behind it (Chrome Web Store policy).

export const REVIEW_URL = "https://chromewebstore.google.com/detail/scholar-utility-belt/omcogfcgldfmihfogbffflbocdbjockn/reviews";
export const STATE_KEY = "ratingPrompt";
export const MIN_ACTIVE_DAYS = 5;
export const MIN_DAYS_SINCE_FIRST = 7;
export const SNOOZE_DAYS = 60;
const DAY_MS = 24 * 60 * 60 * 1000;

/** The local calendar day as YYYY-MM-DD. */
export function localDay(now = Date.now()) {
  const d = new Date(now);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

/** The state after a Scholar page was used at `now`, or null when today is already counted. */
export function nextActiveState(state, now = Date.now()) {
  const s = state && typeof state === "object" ? state : {};
  const today = localDay(now);
  if (s.lastDay === today) return null;
  return {
    ...s,
    firstSeen: Number.isFinite(s.firstSeen) ? s.firstSeen : now,
    lastDay: today,
    activeDays: (Number(s.activeDays) || 0) + 1,
  };
}

/**
 * Called by the content script once a Scholar page has loaded. Two tabs opened in the same second can both count
 * the same day; that only brings the card forward by a day, so it is not locked. Never throws.
 */
export async function recordActiveDay({ storage = chrome.storage.local, now = Date.now() } = {}) {
  try {
    const stored = await storage.get(STATE_KEY);
    const next = nextActiveState(stored && stored[STATE_KEY], now);
    if (!next) return false;
    await storage.set({ [STATE_KEY]: next });
    return true;
  } catch (_) {
    return false;
  }
}

export function shouldShowRatingPrompt(state, now = Date.now()) {
  const s = state && typeof state === "object" ? state : {};
  if (s.done) return false;
  if (Number.isFinite(s.snoozedUntil) && now < s.snoozedUntil) return false;
  if ((Number(s.activeDays) || 0) < MIN_ACTIVE_DAYS) return false;
  return Number.isFinite(s.firstSeen) && now - s.firstSeen >= MIN_DAYS_SINCE_FIRST * DAY_MS;
}

/**
 * Shows #ratingCard when it should be shown and wires its three controls: #ratingLink (the store's review page),
 * #ratingLater (ask again in SNOOZE_DAYS days) and #ratingNever. Opening the review page counts as done, so the
 * card does not come back. Any storage failure leaves the card hidden.
 */
export async function initRatingPrompt({ doc = document, storage = chrome.storage.local, now = Date.now() } = {}) {
  const card = doc.getElementById("ratingCard");
  const link = doc.getElementById("ratingLink");
  const later = doc.getElementById("ratingLater");
  const never = doc.getElementById("ratingNever");
  if (!card || !link || !later || !never) return false;
  let state;
  try {
    const stored = await storage.get(STATE_KEY);
    state = (stored && stored[STATE_KEY]) || {};
  } catch (_) {
    return false;
  }
  link.href = REVIEW_URL;
  if (!shouldShowRatingPrompt(state, now)) return false;
  const close = async (patch) => {
    card.hidden = true;
    try {
      const stored = await storage.get(STATE_KEY);
      await storage.set({ [STATE_KEY]: { ...((stored && stored[STATE_KEY]) || state), ...patch } });
    } catch (_) { /* hidden for this visit either way */ }
  };
  link.addEventListener("click", () => close({ done: true, rated: now }));
  later.addEventListener("click", () => close({ snoozedUntil: now + SNOOZE_DAYS * DAY_MS }));
  never.addEventListener("click", () => close({ done: true }));
  card.hidden = false;
  return true;
}
