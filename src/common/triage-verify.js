// Structured triage card: pure helpers, no DOM, no chrome.* APIs.
//
// The on-device model (Chrome Prompt API, Gemini Nano) is asked to pull three
// fields out of a Google Scholar snippet: study design, sample, and setting.
// A small model can invent plausible details, so nothing it returns is shown
// unless the value literally appears in the snippet text. The check is a
// substring test on normalized text with word-boundary rules, plus a separate
// guard for numbers. Anything that fails becomes NOT_STATED.
//
// What this does not do: it does not judge whether a verified value is the
// right answer to the field (the snippet may say "n = 40" about a pilot while
// the study sample is larger). It only guarantees the shown text is in the
// snippet. The card says so.

export const NOT_STATED = "not stated in snippet";
export const TRIAGE_FIELDS = ["design", "sample", "setting"];
export const MAX_VALUE_LENGTH = 120;
export const MAX_SNIPPET_CHARS = 2000;

// Values a model returns when it has nothing; treated as "not stated".
const EMPTY_VALUES = new Set([
  "", "n/a", "na", "none", "null", "undefined", "unknown", "unclear", "not stated", "not specified",
  "not mentioned", "not reported", "not available", "not stated in snippet", "no information", "-",
]);

/**
 * Comparison form of a string. NFKC folds width and ligature variants (full-width digits,
 * "ﬁ"), then: lowercase, soft hyphens and zero-width characters removed, every dash and
 * minus variant becomes "-", curly quotes become straight, whitespace collapses, spaces
 * around "=", "<", ">", the plus-minus sign and "%" are dropped so "n = 120" matches
 * "N=120", and thousands separators between digits are removed so "1,200" matches "1200".
 * Diacritics are kept on purpose: a model that rewrites "Zürich" as "Zurich" has changed
 * the text, and we do not accept that as literal.
 */
export function normalizeForMatch(input) {
  let s = String(input == null ? "" : input);
  s = s.normalize("NFKC");
  s = s.replace(/[\u00AD\u200B-\u200D\u2060\uFEFF]/g, "");
  s = s.toLowerCase();
  s = s.replace(/[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]/g, "-");
  s = s.replace(/[\u2018\u2019\u201A\u201B\u2032]/g, "'").replace(/[\u201C\u201D\u201E\u201F\u2033]/g, '"');
  s = s.replace(/\s+/g, " ").trim();
  s = s.replace(/\s*([=<>±%])\s*/g, "$1");
  s = s.replace(/(\d),(?=\d{3}(?!\d))/g, "$1");
  return s;
}

// Scripts written without spaces between words (Han, kana, Thai...) get no boundary check,
// because a phrase there is normally flanked by more letters of the same sentence.
const UNSPACED_SCRIPT = /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Thai}\p{Script=Lao}\p{Script=Khmer}\p{Script=Myanmar}]/u;
function isWordChar(ch) {
  return !!ch && /[\p{L}\p{N}]/u.test(ch) && !UNSPACED_SCRIPT.test(ch);
}

/**
 * True when `needle` occurs in `haystack` (both already normalized) at word boundaries:
 * if the needle starts or ends with a letter or digit, the characters just outside the
 * match must not be letters or digits. This stops "120" matching inside "1120" and
 * "rct" matching inside "arctic".
 */
function containsAtBoundaries(haystack, needle) {
  if (!needle) return false;
  const first = needle[0];
  const last = needle[needle.length - 1];
  let from = 0;
  for (;;) {
    const at = haystack.indexOf(needle, from);
    if (at === -1) return false;
    const before = at > 0 ? haystack[at - 1] : "";
    const after = haystack[at + needle.length] || "";
    const okBefore = !isWordChar(first) || !isWordChar(before);
    const okAfter = !isWordChar(last) || !isWordChar(after);
    // A decimal point or minus sign glued to a digit also changes a number: "5" is not in "0.5".
    const numBefore = /\d/.test(first) && /[.,]/.test(before) && /\d/.test(haystack[at - 2] || "");
    const numAfter = /\d/.test(last) && /[.,]/.test(after) && /\d/.test(haystack[at + needle.length + 1] || "");
    if (okBefore && okAfter && !numBefore && !numAfter) return true;
    from = at + 1;
  }
}

/** Number tokens in normalized text: integers and decimals, as written. */
function numberTokens(normalized) {
  return normalized.match(/\d+(?:\.\d+)?/g) || [];
}

/**
 * Decide whether `value` may be shown for a snippet.
 * Returns { ok: boolean, value: string, reason?: string }. `value` is the trimmed text to
 * display when ok. Reasons: "empty", "too_long", "not_in_snippet", "number_not_in_snippet".
 */
export function verifyField(value, snippet) {
  if (typeof value !== "string") return { ok: false, value: "", reason: "empty" };
  let shown = "";
  for (const ch of value) shown += ch.charCodeAt(0) < 32 || ch.charCodeAt(0) === 127 ? " " : ch;
  shown = shown.replace(/\s+/g, " ").trim();
  const norm = normalizeForMatch(shown).replace(/[.;:,\s]+$/g, "");
  if (EMPTY_VALUES.has(norm) || norm.length < 2) return { ok: false, value: "", reason: "empty" };
  if (shown.length > MAX_VALUE_LENGTH) return { ok: false, value: "", reason: "too_long" };
  const hay = normalizeForMatch(snippet);
  if (!containsAtBoundaries(hay, norm)) return { ok: false, value: "", reason: "not_in_snippet" };
  // Belt and braces: every number in the value must be a whole number token in the snippet.
  const haySet = new Set(numberTokens(hay));
  for (const n of numberTokens(norm)) {
    if (!haySet.has(n)) return { ok: false, value: "", reason: "number_not_in_snippet" };
  }
  return { ok: true, value: shown };
}

/**
 * Verify all three fields. `parsed` is the object from parseTriageResponse (or any object).
 * Result: { design, sample, setting }, each { text, verified }. text is the verified value or NOT_STATED.
 */
export function verifyTriage(parsed, snippet) {
  const out = {};
  for (const f of TRIAGE_FIELDS) {
    const r = verifyField(parsed ? parsed[f] : undefined, snippet);
    out[f] = r.ok ? { text: r.value, verified: true } : { text: NOT_STATED, verified: false };
  }
  return out;
}

/**
 * Pull a JSON object out of model text. Handles code fences and prose around the object.
 * Returns an object holding only the three known fields, each a string or null; or null if
 * nothing usable was found. Extra keys are dropped, non-string values become null.
 */
export function parseTriageResponse(text) {
  if (typeof text !== "string") return null;
  const candidates = [];
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) candidates.push(fenced[1]);
  candidates.push(text);
  for (const c of candidates) {
    const start = c.indexOf("{");
    const end = c.lastIndexOf("}");
    if (start === -1 || end <= start) continue;
    let obj;
    try {
      obj = JSON.parse(c.slice(start, end + 1));
    } catch {
      continue;
    }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) continue;
    const out = {};
    let any = false;
    for (const f of TRIAGE_FIELDS) {
      out[f] = typeof obj[f] === "string" ? obj[f] : null;
      if (out[f] !== null) any = true;
    }
    if (any) return out;
  }
  return null;
}

export function buildTriagePrompt(snippet) {
  const s = String(snippet || "");
  const body = s.length > MAX_SNIPPET_CHARS ? s.slice(0, MAX_SNIPPET_CHARS) : s;
  return [
    "Read the snippet below from a search result. Return JSON with exactly these keys: design, sample, setting.",
    "Each value must be a short phrase copied word for word from the snippet, or null if the snippet does not state it.",
    "design: the study design or method. sample: the participants, data or sample size. setting: where or in what context the study took place.",
    "Do not paraphrase, infer or add words. Return only the JSON object.",
    "",
    "Snippet:",
    body,
  ].join("\n");
}

/** Small stable hash (FNV-1a, 32 bit) so a cached result is dropped when the snippet text changes. */
export function snippetHash(text) {
  const s = String(text || "");
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(36);
}

export const TRIAGE_CACHE_PREFIX = "suTriage:";

export function triageCacheKey(paperKey) {
  return TRIAGE_CACHE_PREFIX + String(paperKey || "");
}

/** Cached entries are { h, result }. Returns the result only if the snippet is unchanged. */
export function readTriageCache(entry, snippet) {
  if (!entry || typeof entry !== "object" || entry.h !== snippetHash(snippet)) return null;
  const r = entry.result;
  if (!r || typeof r !== "object") return null;
  for (const f of TRIAGE_FIELDS) {
    if (!r[f] || typeof r[f].text !== "string" || typeof r[f].verified !== "boolean") return null;
  }
  return r;
}

export function makeTriageCacheEntry(snippet, result) {
  return { h: snippetHash(snippet), result };
}
