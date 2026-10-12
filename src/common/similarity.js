// Library similarity and near-duplicate detection. Pure functions: TF-IDF over titles and abstracts,
// cosine similarity, no embedding model and no network.
//
// Heavy work is written as generators that yield between chunks, so the library page can run them
// with setTimeout / requestIdleCallback and stay responsive on a few thousand papers.

export const STOP_WORDS = new Set(
  (
    "a about above after again against all also am an and any are as at be because been before being below between both but by " +
    "can could did do does doing down during each few for from further had has have having he her here hers him his how i if in " +
    "into is it its itself just me more most my no nor not now of off on once only or other our ours out over own same she should " +
    "so some such than that the their theirs them then there these they this those through to too under until up very was we were " +
    "what when where which while who whom why will with would you your yours via using use used based new study analysis approach " +
    "paper results result method methods towards toward et al however thus within without among across may might many much one two"
  ).split(/\s+/)
);

const MARKER_RE = /^(i{1,3}|iv|v|vi{0,3}|ix|x|\d{1,2})$/;

function foldAccents(s) {
  return String(s || "").normalize("NFKD").replace(/[̀-ͯ]/g, "");
}

function rawTokens(text) {
  return foldAccents(text)
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}

function stem(t) {
  if (t.length > 4 && t.endsWith("ies")) return t.slice(0, -3) + "y";
  if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss") && !t.endsWith("us") && !t.endsWith("is")) return t.slice(0, -1);
  return t;
}

/** Lowercased, accent-folded, stop-word-free, lightly stemmed tokens. Numbers of 3+ digits and words of 2+ letters are kept. */
export function tokenize(text) {
  const out = [];
  for (const raw of rawTokens(text)) {
    if (STOP_WORDS.has(raw)) continue;
    if (raw.length < 2) continue;
    if (/^\d+$/.test(raw) && raw.length < 3) continue;
    out.push(stem(raw));
  }
  return out;
}

/** Part numbers and roman numerals in a title ("Part II", "2"), used to tell a series apart. */
export function seriesMarkers(title) {
  const toks = rawTokens(title);
  const set = new Set();
  for (let i = 0; i < toks.length; i++) {
    if (MARKER_RE.test(toks[i]) && (i > 0 && /^(part|volume|vol|chapter|study|experiment|paper|phase)$/.test(toks[i - 1]))) set.add(toks[i]);
    else if (MARKER_RE.test(toks[i]) && i === toks.length - 1 && toks.length > 2) set.add(toks[i]);
  }
  return set;
}

function sameSet(a, b) {
  if (a.size !== b.size) return false;
  for (const v of a) if (!b.has(v)) return false;
  return true;
}

function paperText(p) {
  return [p?.abstract, p?.snippet].filter(Boolean).join(" ");
}

function termFreq(tokens) {
  const tf = new Map();
  for (const t of tokens) tf.set(t, (tf.get(t) || 0) + 1);
  return tf;
}

function normalizeVec(vec) {
  let sum = 0;
  for (const w of vec.values()) sum += w * w;
  const norm = Math.sqrt(sum);
  if (norm > 0) for (const [k, w] of vec) vec.set(k, w / norm);
  return vec;
}

/** Cosine of two L2-normalized sparse vectors (Maps). */
export function cosine(a, b) {
  if (!a || !b) return 0;
  const [small, big] = a.size <= b.size ? [a, b] : [b, a];
  let dot = 0;
  for (const [k, w] of small) {
    const v = big.get(k);
    if (v) dot += w * v;
  }
  return dot;
}

/** Last names from a Scholar authorsVenue line, e.g. "A Smith, B Jones - Journal, 2020 - Publisher". */
export function authorSurnames(authorsVenue) {
  const first = String(authorsVenue || "").split(/\s+-\s+/)[0] || "";
  const out = [];
  for (const part of first.split(/\s*,\s*|\s+and\s+|\s*&\s*/i)) {
    const cleaned = foldAccents(part).replace(/[^A-Za-z\s'.-]/g, "").trim();
    if (!cleaned || /^(et al\.?|\.\.\.)$/i.test(cleaned)) continue;
    const words = cleaned.split(/\s+/).filter(Boolean);
    const last = words[words.length - 1].toLowerCase().replace(/[.']/g, "");
    if (last.length >= 2) out.push(last);
  }
  return out;
}

function yearOf(p) {
  const n = Number(p?.year);
  return Number.isFinite(n) && n > 1000 ? n : null;
}

// ---------------------------------------------------------------------------
// Chunked execution
// ---------------------------------------------------------------------------

/** Default yield: requestIdleCallback when present, else setTimeout(0). */
export function defaultYield() {
  return new Promise((resolve) => {
    if (typeof globalThis.requestIdleCallback === "function") globalThis.requestIdleCallback(() => resolve(), { timeout: 100 });
    else setTimeout(resolve, 0);
  });
}

/** Drive a generator to completion, awaiting yieldFn() at each `yield`. Returns the generator's return value. */
export async function runChunked(gen, yieldFn = defaultYield, onProgress) {
  let step = gen.next();
  while (!step.done) {
    if (onProgress && step.value) onProgress(step.value);
    await yieldFn();
    step = gen.next();
  }
  return step.value;
}

/** Run a generator synchronously (tests, small libraries). */
export function runSync(gen) {
  let step = gen.next();
  while (!step.done) step = gen.next();
  return step.value;
}

// ---------------------------------------------------------------------------
// Index
// ---------------------------------------------------------------------------

/**
 * Build the TF-IDF index. `titleVecs` use title tokens only (for duplicates);
 * `fullVecs` weight the title double and add abstract or snippet (for "similar to this paper").
 */
export function* buildIndexSteps(papers, { chunkSize = 200 } = {}) {
  const list = Array.isArray(papers) ? papers : Object.values(papers || {});
  const n = list.length;
  const keys = new Array(n);
  const titleTokens = new Array(n);
  const fullTokens = new Array(n);
  for (let i = 0; i < n; i++) {
    const p = list[i] || {};
    keys[i] = p.key;
    const tt = tokenize(p.title);
    titleTokens[i] = tt;
    fullTokens[i] = [...tt, ...tt, ...tokenize(paperText(p))];
    if ((i + 1) % chunkSize === 0) yield { phase: "tokenize", done: i + 1, total: n };
  }

  const dfTitle = new Map();
  const dfFull = new Map();
  for (let i = 0; i < n; i++) {
    for (const t of new Set(titleTokens[i])) dfTitle.set(t, (dfTitle.get(t) || 0) + 1);
    for (const t of new Set(fullTokens[i])) dfFull.set(t, (dfFull.get(t) || 0) + 1);
  }
  const idf = (df) => Math.log((n + 1) / (df + 1)) + 1;

  const titleVecs = new Array(n);
  const fullVecs = new Array(n);
  const titlePostings = new Map();
  const fullPostings = new Map();
  for (let i = 0; i < n; i++) {
    const tv = new Map();
    for (const [t, c] of termFreq(titleTokens[i])) tv.set(t, (1 + Math.log(c)) * idf(dfTitle.get(t)));
    titleVecs[i] = normalizeVec(tv);
    for (const [t, w] of titleVecs[i]) {
      if (!titlePostings.has(t)) titlePostings.set(t, []);
      titlePostings.get(t).push([i, w]);
    }
    const fv = new Map();
    for (const [t, c] of termFreq(fullTokens[i])) fv.set(t, (1 + Math.log(c)) * idf(dfFull.get(t)));
    fullVecs[i] = normalizeVec(fv);
    for (const [t, w] of fullVecs[i]) {
      if (!fullPostings.has(t)) fullPostings.set(t, []);
      fullPostings.get(t).push([i, w]);
    }
    if ((i + 1) % chunkSize === 0) yield { phase: "vectors", done: i + 1, total: n };
  }

  const keyIndex = new Map();
  keys.forEach((k, i) => {
    if (k !== undefined && !keyIndex.has(k)) keyIndex.set(k, i);
  });
  return { n, papers: list, keys, keyIndex, titleVecs, fullVecs, titlePostings, fullPostings };
}

export function buildIndex(papers, opts) {
  return runSync(buildIndexSteps(papers, opts));
}

/** Papers most similar to the paper with `key`, best first. Uses the inverted index, so it is fast. */
export function similarTo(index, key, { limit = 10, minScore = 0.12 } = {}) {
  const i = index?.keyIndex?.get(key);
  if (i === undefined) return [];
  const scores = new Map();
  for (const [term, w] of index.fullVecs[i]) {
    const posting = index.fullPostings.get(term);
    if (!posting) continue;
    for (const [j, wj] of posting) {
      if (j === i) continue;
      scores.set(j, (scores.get(j) || 0) + w * wj);
    }
  }
  return Array.from(scores, ([j, score]) => ({ key: index.keys[j], paper: index.papers[j], score }))
    .filter((r) => r.score >= minScore)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

// ---------------------------------------------------------------------------
// Near-duplicates
// ---------------------------------------------------------------------------

export const DUPLICATE_RULES = {
  strongTitle: 0.9,
  likelyTitle: 0.6,
  likelyAuthors: 0.5,
  strongAuthors: 0.3,
  maxYearGap: 3,
  maxPostingSize: 60
};

/**
 * Decide whether two papers are probably the same work. Returns null, or
 * { score, titleSim, authorOverlap, yearGap, reasons[] }.
 * Typical hit: a preprint and the published version (title nearly equal or lightly reworded,
 * same authors, years within a few).
 */
export function comparePapers(a, b, titleSim) {
  const ya = yearOf(a);
  const yb = yearOf(b);
  const yearGap = ya !== null && yb !== null ? Math.abs(ya - yb) : null;
  if (yearGap !== null && yearGap > DUPLICATE_RULES.maxYearGap) return null;

  const ma = seriesMarkers(a.title);
  const mb = seriesMarkers(b.title);
  if ((ma.size || mb.size) && !sameSet(ma, mb)) return null;

  const sa = new Set(authorSurnames(a.authorsVenue));
  const sb = new Set(authorSurnames(b.authorsVenue));
  let overlap = null;
  let shared = [];
  if (sa.size && sb.size) {
    shared = [...sa].filter((x) => sb.has(x));
    overlap = shared.length / Math.min(sa.size, sb.size);
  }
  const firstA = authorSurnames(a.authorsVenue)[0];
  const firstB = authorSurnames(b.authorsVenue)[0];
  const firstMatch = !!firstA && firstA === firstB;

  const strong = titleSim >= DUPLICATE_RULES.strongTitle && (overlap === null || overlap >= DUPLICATE_RULES.strongAuthors || firstMatch);
  const likely = titleSim >= DUPLICATE_RULES.likelyTitle && overlap !== null && overlap >= DUPLICATE_RULES.likelyAuthors;
  if (!strong && !likely) return null;

  const reasons = [`title match ${Math.round(titleSim * 100)}%`];
  if (overlap !== null) reasons.push(`shared author surnames: ${shared.slice(0, 4).join(", ") || "none"}`);
  else reasons.push("author names not available for one of the two");
  if (ya !== null && yb !== null) reasons.push(ya === yb ? `both ${ya}` : `years ${ya} and ${yb}`);
  const score = 0.7 * titleSim + 0.2 * (overlap ?? 0.5) + 0.1 * (yearGap === null ? 0.5 : 1 - yearGap / (DUPLICATE_RULES.maxYearGap + 1));
  return { score, titleSim, authorOverlap: overlap, yearGap, reasons };
}

/**
 * Title match in 0..1: the larger of the TF-IDF cosine and 0.85 times the share of the shorter title's
 * words found in the longer one. The second term catches a preprint whose title gained or lost a subtitle.
 * It is capped below the "strong" threshold, so it only counts together with author overlap.
 */
export function titleMatch(vecA, vecB) {
  const cos = cosine(vecA, vecB);
  const [small, big] = vecA.size <= vecB.size ? [vecA, vecB] : [vecB, vecA];
  if (small.size < 2) return cos;
  let shared = 0;
  for (const k of small.keys()) if (big.has(k)) shared++;
  return Math.max(cos, 0.85 * (shared / small.size));
}

/**
 * Find candidate pairs through the title inverted index (only papers that share a title term are compared),
 * then apply comparePapers. Yields every `chunkSize` papers.
 */
export function* findDuplicatePairsSteps(index, { chunkSize = 150, limit = 500 } = {}) {
  const pairs = [];
  const n = index.n;
  const maxPosting = Math.max(DUPLICATE_RULES.maxPostingSize, Math.floor(n * 0.05));
  for (let i = 0; i < n; i++) {
    const vecI = index.titleVecs[i];
    if (vecI.size) {
      const candidates = new Set();
      for (const term of vecI.keys()) {
        const posting = index.titlePostings.get(term);
        if (!posting || posting.length > maxPosting) continue;
        for (const [j] of posting) if (j > i) candidates.add(j);
      }
      for (const j of candidates) {
        const sim = titleMatch(vecI, index.titleVecs[j]);
        if (sim < DUPLICATE_RULES.likelyTitle) continue;
        const verdict = comparePapers(index.papers[i], index.papers[j], sim);
        if (verdict) pairs.push({ a: index.keys[i], b: index.keys[j], ...verdict });
      }
    }
    if ((i + 1) % chunkSize === 0) yield { phase: "pairs", done: i + 1, total: n };
  }
  pairs.sort((x, y) => y.score - x.score);
  return pairs.slice(0, limit);
}

export function findDuplicatePairs(index, opts) {
  return runSync(findDuplicatePairsSteps(index, opts));
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/** Cheap fingerprint of the library contents that matter here: keys, titles, authors, years, update stamps. */
export function librarySignature(papers) {
  const list = Array.isArray(papers) ? papers : Object.values(papers || {});
  let h = 2166136261;
  const mix = (str) => {
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    h ^= 124;
    h = Math.imul(h, 16777619);
  };
  for (const p of list) {
    mix(String(p?.key ?? ""));
    mix(String(p?.title ?? ""));
    mix(String(p?.authorsVenue ?? ""));
    mix(String(p?.year ?? ""));
    mix(String(p?.updatedAt ?? ""));
    mix(String((p?.abstract || p?.snippet || "").length));
  }
  return `${list.length}:${(h >>> 0).toString(36)}`;
}

/**
 * Cache for the index and the duplicate pairs. Both are rebuilt only when librarySignature changes.
 * Concurrent callers share one in-flight computation.
 */
export function createSimilarityCache({ yieldFn = defaultYield, chunkSize } = {}) {
  let sig = null;
  let indexPromise = null;
  let pairsPromise = null;
  const stats = { indexBuilds: 0, pairBuilds: 0 };

  function reset(next) {
    sig = next;
    indexPromise = null;
    pairsPromise = null;
  }

  async function getIndex(papers, onProgress) {
    const s = librarySignature(papers);
    if (s !== sig) reset(s);
    if (!indexPromise) {
      stats.indexBuilds++;
      indexPromise = runChunked(buildIndexSteps(papers, { chunkSize }), yieldFn, onProgress);
    }
    return indexPromise;
  }

  async function getPairs(papers, onProgress) {
    const index = await getIndex(papers, onProgress);
    if (!pairsPromise) {
      stats.pairBuilds++;
      pairsPromise = runChunked(findDuplicatePairsSteps(index, { chunkSize }), yieldFn, onProgress);
    }
    return pairsPromise;
  }

  return { getIndex, getPairs, invalidate: () => reset(null), stats };
}
