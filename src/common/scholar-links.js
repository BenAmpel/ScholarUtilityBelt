/**
 * Helpers for the links Scholar puts in a result row's right-hand file column
 * (.gs_or_ggsm / .gs_ggs): the [PDF]/[HTML] link and, since 2026-08-25, "Quick read".
 *
 * Quick read opens a Google-generated, query-focused summary in a dialog. It is not a
 * file, so it must never be treated as the paper's PDF. The markup was observed on
 * 2026-10-12 (anchor.gs_or_aia whose text starts with "Quick read" and whose href is a
 * signed /scholar URL); the class is Google's, so the text and path checks are fallbacks.
 */

/** True when the anchor is Scholar's Quick read link. Duck-typed so tests need no DOM. */
export function isQuickReadLink(a) {
  if (!a) return false;
  if (a.classList && typeof a.classList.contains === "function" && a.classList.contains("gs_or_aia")) {
    return true;
  }
  const label = String(a.textContent || "").replace(/\s+/g, " ").trim();
  return /^quick read\b/i.test(label);
}

/** First anchor in the file column that is a real file link (not Quick read), or null. */
export function firstFileColumnLink(anchors) {
  for (const a of Array.from(anchors || [])) {
    if (!isQuickReadLink(a)) return a;
  }
  return null;
}
