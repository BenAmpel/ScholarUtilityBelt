/**
 * Reading order: re-rank the results on a page by how many of the user's own
 * terms appear in each title and snippet. Pure logic only; the DOM work lives in
 * src/content/signals-ui.js. Nothing is hidden, nothing leaves the browser, and
 * ties keep Scholar's original order.
 *
 * Scoring (documented so the per-row "why this order" text can match it):
 *   include / method / population term: 3 points if it is in the title,
 *     plus 1 point per occurrence in the snippet, up to 3.
 *   exclude term: -4 points if it is in the title, -2 if it is only in the snippet.
 */

export const TITLE_POINTS = 3;
export const SNIPPET_POINTS_PER_HIT = 1;
export const SNIPPET_HIT_CAP = 3;
export const EXCLUDE_TITLE_PENALTY = 4;
export const EXCLUDE_SNIPPET_PENALTY = 2;
export const MAX_TERMS_PER_GROUP = 12;

/**
 * Split a typed list into terms. Commas, semicolons and line breaks separate
 * terms; double quotes keep a phrase together. Duplicates (case-insensitive) and
 * empty terms are dropped.
 */
export function parseTerms(raw) {
  const out = [];
  const seen = new Set();
  const s = String(raw == null ? "" : raw);
  const re = /"([^"]+)"|([^,;\n"]+)/g;
  let m;
  while ((m = re.exec(s)) !== null) {
    const term = (m[1] || m[2] || "").replace(/\s+/g, " ").trim();
    if (term.length < 2) continue;
    const key = term.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(term);
    if (out.length >= MAX_TERMS_PER_GROUP) break;
  }
  return out;
}

function escapeRe(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Case-insensitive matcher for one term. Matches on word boundaries. A trailing
 * * makes it a prefix match ("random*" matches "randomized").
 */
export function termRegex(term, flags = "gi") {
  let t = String(term || "").trim();
  const prefix = t.endsWith("*");
  if (prefix) t = t.slice(0, -1);
  if (!t) return null;
  const body = escapeRe(t).replace(/\s+/g, "\\s+");
  const left = /^\w/.test(t) ? "\\b" : "";
  const right = !prefix && /\w$/.test(t) ? "\\b" : "";
  return new RegExp(`${left}${body}${right}`, flags);
}

function countMatches(text, re) {
  if (!re || !text) return 0;
  re.lastIndex = 0;
  let n = 0;
  while (re.exec(text) !== null) {
    n++;
    if (re.lastIndex === 0) break;
  }
  return n;
}

/**
 * Score one row.
 * @param {{title?: string, snippet?: string}} row
 * @param {{include?: string[], exclude?: string[], method?: string[], population?: string[]}} spec
 * @returns {{score: number, matches: {term: string, group: string, inTitle: boolean, snippetHits: number}[], why: string}}
 */
export function scoreRow(row, spec) {
  const title = String((row && row.title) || "");
  const snippet = String((row && row.snippet) || "");
  const matches = [];
  let score = 0;
  const groups = [
    ["include", spec.include],
    ["method", spec.method],
    ["population", spec.population],
    ["exclude", spec.exclude],
  ];
  for (const [group, terms] of groups) {
    for (const term of terms || []) {
      const re = termRegex(term);
      if (!re) continue;
      const inTitle = countMatches(title, re) > 0;
      const snippetHits = countMatches(snippet, re);
      if (!inTitle && !snippetHits) continue;
      matches.push({ term, group, inTitle, snippetHits });
      if (group === "exclude") {
        score -= inTitle ? EXCLUDE_TITLE_PENALTY : EXCLUDE_SNIPPET_PENALTY;
      } else {
        score += (inTitle ? TITLE_POINTS : 0) + Math.min(snippetHits, SNIPPET_HIT_CAP) * SNIPPET_POINTS_PER_HIT;
      }
    }
  }
  return { score, matches, why: explain(score, matches) };
}

function explain(score, matches) {
  if (!matches.length) return "No terms matched in the title or snippet, so this result keeps its place among the unmatched ones.";
  const parts = matches.map((m) => {
    const where = [m.inTitle ? "title" : "", m.snippetHits ? `snippet x${m.snippetHits}` : ""].filter(Boolean).join(", ");
    const label = m.group === "exclude" ? "excluded term" : m.group === "include" ? "term" : `${m.group} term`;
    return `${label} "${m.term}" in ${where}`;
  });
  return `Score ${score}: ${parts.join("; ")}.`;
}

/**
 * Order rows by score, highest first, ties in original order.
 * @param {{title?: string, snippet?: string}[]} rows in Scholar's original order
 * @returns {{order: number[], scores: ReturnType<typeof scoreRow>[]}} `order` is a permutation of 0..rows.length-1
 */
export function rankRows(rows, spec) {
  const list = Array.isArray(rows) ? rows : [];
  const scores = list.map((r) => scoreRow(r, spec || {}));
  const order = list.map((_, i) => i);
  order.sort((a, b) => scores[b].score - scores[a].score || a - b);
  return { order, scores };
}

/** True when the spec has at least one term, so there is something to rank by. */
export function hasTerms(spec) {
  return !!spec && ["include", "exclude", "method", "population"].some((g) => Array.isArray(spec[g]) && spec[g].length);
}
