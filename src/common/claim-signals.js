/**
 * Claim-evidence chips: small, hedged flags computed from the text of a Google
 * Scholar result snippet. Pure and local: no network, no AI.
 *
 * Design rule: it is better to miss than to over-flag. Every rule below only
 * fires on a narrow pattern and backs off when the snippet shows a design or
 * wording that would make the flag unfair (a randomized trial next to a causal
 * verb, a p-value next to "significant", a qualitative study with few
 * participants). Every tooltip says the judgement comes from the snippet only,
 * and no text anywhere says that a paper lacks something.
 *
 * The word lists are written from general knowledge of how methods are
 * reported, not from a published standard. They are heuristics.
 */

export const DEFAULT_MIN_N = 30;
export const MAX_CHIPS = 3;
// Snippets shorter than this carry too little text for any rule to be fair.
const MIN_TEXT_LENGTH = 60;
// The "no method words" rule needs a longer snippet before it may fire.
const MIN_METHOD_RULE_LENGTH = 200;

export const SNIPPET_NOTE =
  "Judged from the Scholar snippet only. The full paper may state this elsewhere.";

function normalize(raw) {
  return String(raw == null ? "" : raw)
    .replace(/[…]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function clampMinN(value) {
  const n = Math.round(Number(value));
  if (!Number.isFinite(n)) return DEFAULT_MIN_N;
  return Math.max(2, Math.min(10000, n));
}

// ---- Rule 1: causal wording with an observational design, or no design -----

// Finite third-person verb forms only. "reduced" and "caused" are skipped on
// purpose: they are usually adjectives or passives ("reduced risk").
const CAUSAL_VERB_RE = /\b(causes|leads to|led to|drives|reduces|improves|prevents)\b/gi;
// "causes" and "drives" are also nouns. Skip them when the word right before
// shows a noun use ("the causes of", "hard drives").
const NOUN_BEFORE_RE =
  /\b(?:the|of|root|leading|main|common|possible|underlying|major|primary|potential|known|other|many|several|these|those|its|their|all|two|three|hard|flash|disk|usb|solid-state)\s+$/i;
const NEGATED_BEFORE_RE = /\b(?:not|never|no|n't)\s+(?:\w+\s+)?$/i;
// A question or condition earlier in the same sentence ("whether X reduces Y").
const CONDITION_BEFORE_RE = /\b(?:whether|if|how|why|unless)\b[^.;:]*$/i;
const NOUN_AFTER_RE = /^\s+of\b/i;

const OBSERVATIONAL_RE =
  /\b(cross-sectional|cross sectional|observational|correlational|retrospective|case-control|cohort|questionnaire|survey|surveys|ecological study|registry-based|secondary data|archival)\b/i;
const STRONG_DESIGN_RE =
  /\b(randomi[sz]ed|randomi[sz]ation|randomly (?:assigned|allocated)|rcts?|controlled (?:clinical )?trials?|trials?|experiments?|experimental|quasi-experiment\w*|natural experiment|instrumental variables?|difference-in-differences|regression discontinuity|mendelian|meta-analys[ie]s|systematic review|simulations?|in vitro|in vivo|knockout|mice|rats|crossover|cross-over)\b/i;
// Technical papers say "our method improves accuracy". That is a benchmark
// claim, not a causal claim about the world, so the no-design rule backs off.
const TECHNICAL_RE =
  /\b(algorithms?|architectures?|neural|deep learning|machine learning|benchmarks?|accuracy|we propose|we present|we introduce|proposed (?:method|approach|model|framework|algorithm)|our (?:method|approach|model|framework|algorithm|system))\b/i;

function findCausalVerb(text) {
  CAUSAL_VERB_RE.lastIndex = 0;
  let m;
  while ((m = CAUSAL_VERB_RE.exec(text)) !== null) {
    const before = text.slice(Math.max(0, m.index - 40), m.index);
    const after = text.slice(m.index + m[0].length, m.index + m[0].length + 6);
    if (NEGATED_BEFORE_RE.test(before) || CONDITION_BEFORE_RE.test(before)) continue;
    if (/^(causes|drives)$/i.test(m[0]) && NOUN_BEFORE_RE.test(before)) continue;
    if (/^causes$/i.test(m[0]) && NOUN_AFTER_RE.test(after)) continue;
    return m[0].toLowerCase();
  }
  return null;
}

function causalChip(text) {
  if (STRONG_DESIGN_RE.test(text)) return null;
  const verb = findCausalVerb(text);
  if (!verb) return null;
  const obs = text.match(OBSERVATIONAL_RE);
  if (obs) {
    return {
      id: "causal-observational",
      label: "Causal wording, observational design",
      tip: `The snippet says "${verb}" and also mentions a ${obs[0].toLowerCase()} design. Observational designs usually support association, so check how the full paper words the claim. ${SNIPPET_NOTE}`,
    };
  }
  if (TECHNICAL_RE.test(text)) return null;
  return {
    id: "causal-no-design",
    label: "Causal wording, design not stated",
    tip: `The snippet says "${verb}" and does not name a study design. ${SNIPPET_NOTE}`,
  };
}

// ---- Rule 2: "significant" with no statistic in the snippet ----------------

const SIG_STAT_RE =
  /\b(?:statistically significant(?:ly)?|significant(?:ly)?\s+(?:\w+\s+){0,2}?(?:higher|lower|more|less|greater|larger|smaller|better|worse|faster|slower|increase\w*|decrease\w*|improve\w*|reduc\w*|differ\w*|effects?|associat\w*|correlat\w*|relationships?|impacts?|predict\w*|positive\w*|negative\w*|changes?|outperform\w*))\b/gi;
const STAT_PRESENT_RES = [
  /\bp\s*[<>=≤≥]\s*[.\d]/i,
  /\bp[- ]?values?\b/i,
  /\bconfidence intervals?\b/i,
  /\b\d{2}\s*%\s*(?:ci|confidence)/i,
  /\bCIs?\b/,
  /\beffect sizes?\b/i,
  /\bcohen'?s\s*d\b/i,
  /\b(?:odds|hazard|risk|rate) ratios?\b/i,
  /\brelative risk\b/i,
  /\b(?:OR|HR|RR|SMD|AOR|aHR)\s*[=:]?\s*\d/,
  /[βηχ]\s*2?\s*[=(]/,
  /\b(?:beta|eta|chi)[- ]?(?:squared|square)?\s*=/i,
  /\br\s*=\s*[-−]?\s*[.\d]/i,
  /\bR2\b|R²/,
  /\b[dtF]\s*\(?\s*\d*\)?\s*=\s*[-−]?\d/,
  /\bt\(\d+\)/i,
  /\bF\(\d+/,
  /\bbayes factors?\b/i,
  /\bchi-?square\b/i,
  /\bpercentage points?\b/i,
  /\d\s*%/,
  /\bpercent\b/i,
  /\b\d+(?:\.\d+)?[- ]?(?:fold|times)\b/i,
  /\bstandard deviations?\b|\bSD\b/,
];

function significanceChip(text) {
  SIG_STAT_RE.lastIndex = 0;
  let hit = null;
  let m;
  while ((m = SIG_STAT_RE.exec(text)) !== null) {
    const before = text.slice(Math.max(0, m.index - 14), m.index);
    // "no significant difference" and "non-significant" are not claims of an effect.
    if (/\b(?:no|not|non|never|without)[-\s]+(?:\w+\s+)?$/i.test(before)) continue;
    hit = m[0];
    break;
  }
  if (!hit) return null;
  if (STAT_PRESENT_RES.some((re) => re.test(text))) return null;
  return {
    id: "significant-no-stat",
    label: "'Significant' without a statistic",
    tip: `The snippet says "${hit.toLowerCase()}" but shows no p-value, effect size, percentage or interval. ${SNIPPET_NOTE}`,
  };
}

// ---- Rule 3: a stated sample size below the threshold ----------------------

const N_EQUALS_RE = /\b[nN]\s*=\s*(\d{1,3}(?:,\d{3})+|\d+)\b/g;
const N_NOUN_RE =
  /\b(\d{1,3}(?:,\d{3})+|\d+)\s+(?:\w+\s+)?(participants|patients|subjects|respondents|students|children|adults|women|men|individuals|volunteers|employees|workers|people|samples|cases|mice|rats|teachers|clinicians|physicians|nurses|households|schools|firms|companies|adolescents|infants|veterans|users)\b/gi;
// Small samples are normal for these designs, so the rule stays quiet.
const SMALL_N_OK_RE =
  /\b(qualitative|interviews?|case study|case studies|case report|ethnograph\w*|pilot|single-case|phenomenolog\w*|grounded theory|focus groups?|in vitro|cell lines?|replicates?|n-of-1|phase (?:i|1)\b|feasibility|proof[- ]of[- ]concept|mice|rats|in vivo|knockout|animals?)\b/i;

function parseCount(raw) {
  return parseInt(String(raw).replace(/,/g, ""), 10);
}

function smallNChip(text, minN) {
  if (SMALL_N_OK_RE.test(text)) return null;
  const found = [];
  let m;
  N_EQUALS_RE.lastIndex = 0;
  while ((m = N_EQUALS_RE.exec(text)) !== null) found.push(parseCount(m[1]));
  N_NOUN_RE.lastIndex = 0;
  while ((m = N_NOUN_RE.exec(text)) !== null) {
    // "5 of 400 participants" style subgroups are skipped by taking the largest number below.
    found.push(parseCount(m[1]));
  }
  const nums = found.filter((n) => Number.isFinite(n) && n >= 2);
  if (!nums.length) return null;
  // The largest stated N is used so a subgroup count never triggers the chip.
  const n = Math.max(...nums);
  if (n >= minN) return null;
  return {
    id: "small-n",
    label: `Stated N is ${n}`,
    tip: `The snippet gives a sample of ${n}, below your threshold of ${minN}. Small samples can be appropriate for some designs. ${SNIPPET_NOTE}`,
  };
}

// ---- Rule 4: novelty and priority wording ----------------------------------

const NOVELTY_RES = [
  /\bnovel\s+(?:approach|method|framework|algorithm|model|technique|architecture|paradigm|strategy|metric|measure|tool|system|solution)\b/i,
  /\bnew state[- ]of[- ]the[- ]art\b/i,
  /\b(?:achieves?|achieving|sets?|establish\w*|outperform\w*|surpass\w*|beats?)\s+(?:the\s+)?(?:new\s+)?state[- ]of[- ]the[- ]art\b/i,
  /\bthe first (?:to|study|paper|work|attempt|time|evidence|large-scale|comprehensive|systematic)\b/i,
  /\bfor the first time\b/i,
];

function noveltyChip(text) {
  for (const re of NOVELTY_RES) {
    const m = text.match(re);
    if (m) {
      return {
        id: "novelty-wording",
        label: "Novelty wording",
        tip: `The snippet uses priority wording ("${m[0].toLowerCase()}"). Claims of being first or best are hard to judge from a snippet. ${SNIPPET_NOTE}`,
      };
    }
  }
  return null;
}

// ---- Rule 5: no method vocabulary at all -----------------------------------

const METHOD_RE =
  /\b(methods?|methodology|approach(?:es)?|using|used|data|datasets?|participants|samples?|surveys?|experiments?|experimental|trials?|regressions?|models?|simulations?|analys[ie]s|analy[sz]ed|interviews?|algorithms?|framework|measured|measurement|empirical|qualitative|quantitative|cohort|questionnaire|observations?|we (?:\w+ed|use|conduct\w*|analy\w+|survey\w*|interview\w*|measure\w*|test\w*|compar\w+|develop\w*|propose|present|introduce|train\w*|implement\w*|collect\w*|recruit\w*|examin\w+|estimat\w+|evaluat\w+|apply|review\w*))\b/i;
// Essays, reviews and conceptual pieces legitimately have no method words.
const NON_EMPIRICAL_RE =
  /\b(discuss\w*|argue\w*|conceptual|essay|commentary|perspective|viewpoint|editorial|overview|introduction|chapter|book|histor\w*|theor\w+|review|survey of|reflect\w*|outline\w*|explore\w*|critique|philosoph\w*|policy)\b/i;

function noMethodChip(text) {
  if (text.length < MIN_METHOD_RULE_LENGTH) return null;
  if (METHOD_RE.test(text) || NON_EMPIRICAL_RE.test(text)) return null;
  return {
    id: "no-method-words",
    label: "No method words in snippet",
    tip: `No method vocabulary (data, sample, survey, experiment, model, and similar) appears in the snippet. Snippets are short and often stop before the methods. ${SNIPPET_NOTE}`,
  };
}

/**
 * Analyse one snippet.
 * @param {string} rawText snippet or abstract text
 * @param {{minN?: number, maxChips?: number}} [opts]
 * @returns {{id: string, label: string, tip: string}[]} at most `maxChips` chips (default 3)
 */
export function analyzeClaims(rawText, opts = {}) {
  const text = normalize(rawText);
  if (text.length < MIN_TEXT_LENGTH) return [];
  const minN = clampMinN(opts.minN == null ? DEFAULT_MIN_N : opts.minN);
  const max = Math.max(0, Math.min(MAX_CHIPS, Math.round(Number(opts.maxChips ?? MAX_CHIPS)) || 0));
  const chips = [
    causalChip(text),
    significanceChip(text),
    smallNChip(text, minN),
    noveltyChip(text),
    noMethodChip(text),
  ].filter(Boolean);
  return chips.slice(0, max);
}
