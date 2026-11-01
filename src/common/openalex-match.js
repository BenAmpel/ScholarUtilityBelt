// Matching a Scholar paper to an OpenAlex work by title, and the FWCI summary built from the matched works.
// Pure functions: content.js imports them (dist/common/openalex-match.js) and tests/openalex-match.test.mjs tests them.

export function normalizeTitleForMatch(title) {
  return String(title || "")
    .toLowerCase()
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function titleSimilarity(a, b) {
  const aTokens = new Set(normalizeTitleForMatch(a).split(" ").filter(Boolean));
  const bTokens = new Set(normalizeTitleForMatch(b).split(" ").filter(Boolean));
  if (!aTokens.size || !bTokens.size) return 0;
  let inter = 0;
  for (const t of aTokens) if (bTokens.has(t)) inter += 1;
  const union = new Set([...aTokens, ...bTokens]).size;
  return union ? inter / union : 0;
}

// The title has to identify the paper on its own; year and author only choose between works that pass.
// Before, any work with a matching year and author (0.15 + 0.15) passed the 0.2 cut-off with no title words in
// common, and its citation count, percentile, open-access status and venue were shown as the profile paper's.
export const MIN_TITLE_SIMILARITY = 0.8;
// Scholar shortens long titles; a cut title that is the start of the work's title, with enough words, still counts.
const MIN_PREFIX_WORDS = 6;

export function titlesMatch(scholarTitle, workTitle) {
  const a = normalizeTitleForMatch(scholarTitle);
  const b = normalizeTitleForMatch(workTitle);
  if (!a || !b) return false;
  if (a === b) return true;
  const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];
  if (longer.startsWith(shorter + " ") && shorter.split(" ").length >= MIN_PREFIX_WORDS) return true;
  return titleSimilarity(a, b) >= MIN_TITLE_SIMILARITY;
}

export function scoreOpenAlexCandidate(work, title, year, authorName) {
  if (!work) return 0;
  const workTitle = work.display_name || work.title || "";
  if (!titlesMatch(title, workTitle)) return 0;
  let score = titleSimilarity(title, workTitle);
  const wYear = Number(work.publication_year) || null;
  if (year && wYear && Math.abs(wYear - year) <= 1) score += 0.15;
  if (authorName && Array.isArray(work.authorships)) {
    const found = work.authorships.some((a) =>
      String(a?.author?.display_name || "").toLowerCase().includes(String(authorName).toLowerCase())
    );
    if (found) score += 0.15;
  }
  return score;
}

// The best candidate whose title matches, or null.
export function pickOpenAlexWork(results, title, year, authorName) {
  let best = null;
  let bestScore = 0;
  for (const cand of Array.isArray(results) ? results : []) {
    const score = scoreOpenAlexCandidate(cand, title, year, authorName);
    if (score > bestScore) {
      bestScore = score;
      best = cand;
    }
  }
  return best;
}

const round2 = (x) => Math.round(x * 100) / 100;

// FWCI is OpenAlex's own per-work Field-Weighted Citation Impact (citations / expected citations for works of the
// same field, type and year; 1.0 = world average). It used to be mean(citation percentile) / 50, which tops out at
// 2.0 and put every median paper at exactly 1.0. The top-10% share still comes from the percentiles.
export function summarizeFwci(works, minArticles = 3) {
  const fwci = [];
  const percentiles = [];
  for (const w of Array.isArray(works) ? works : []) {
    if (Number.isFinite(w?.fwci)) fwci.push(w.fwci);
    if (Number.isFinite(w?.percentile)) percentiles.push(w.percentile);
  }
  const out = { fwci: null, fwciMedian: null, fwciArticles: fwci.length, top10Share: null };
  if (fwci.length >= minArticles) {
    out.fwci = round2(fwci.reduce((a, b) => a + b, 0) / fwci.length);
    const sorted = fwci.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    out.fwciMedian = round2(sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2);
  }
  if (percentiles.length >= minArticles) {
    out.top10Share = Math.round((percentiles.filter((p) => p >= 90).length / percentiles.length) * 100);
  }
  return out;
}
