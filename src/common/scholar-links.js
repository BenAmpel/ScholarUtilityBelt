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

/**
 * Tells a normal Scholar response from Google's "we are limiting you" answers: HTTP 429, a redirect to
 * the /sorry/ page, or a CAPTCHA page (which can come back as 200 with a form). Returns "rate_limit",
 * "captcha", or null for an ordinary response. The extension only reports this to the user; it never
 * tries to solve or get around the check.
 */
export function classifyScholarResponse({ status, url, html } = {}) {
  if (/\/sorry\//i.test(String(url || ""))) return "captcha";
  const body = String(html || "").slice(0, 200000);
  if (/id=["']gs_captcha|g-recaptcha|name=["']captcha["']|unusual traffic from your computer|not a robot/i.test(body)) return "captcha";
  if (Number(status) === 429) return "rate_limit";
  return null;
}

const DOI_IN_TEXT = /10\.\d{4,9}\/[^\s"'<>]+/i;
const DOI_URL_TAILS = /\/(full|abstract|pdf|epdf|pdfdirect|meta|summary|html|fulltext|references|citedby|figures|suppl(?:ementary)?)(\/.*)?$/i;
/** A DOI cleaned of URL encoding, query strings, publisher page suffixes and trailing punctuation, or null. */
export function cleanDoi(raw) {
  if (!raw) return null;
  let d = String(raw);
  try { d = decodeURIComponent(d); } catch (_) { /* keep the raw text */ }
  const m = DOI_IN_TEXT.exec(d);
  if (!m) return null;
  d = m[0].replace(/[?#&].*$/, "").replace(DOI_URL_TAILS, "").replace(/\.pdf$/i, "");
  d = d.replace(/[.,;:)\]}>'"]+$/, "");
  return /^10\.\d{4,9}\/\S+$/.test(d) ? d : null;
}
/** The DOI a Scholar result itself shows: from its links first (publisher and doi.org URLs), then its snippet text. */
export function findResultDoi(hrefs = [], text = "") {
  for (const h of hrefs) {
    const d = cleanDoi(h);
    if (d) return d;
  }
  return cleanDoi(text);
}

/** `raw` if it is an absolute http(s) URL, else "". For href attributes built from fetched or imported data. */
export function safeHref(raw) {
  const s = String(raw ?? "").trim();
  if (!/^https?:\/\//i.test(s)) return "";
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:" ? s : "";
  } catch (_) {
    return "";
  }
}
