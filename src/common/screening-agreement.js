/**
 * Screening agreement between two reviewers, computed from the per-reviewer
 * votes already stored in a review project (project.decisions[paperId].votes).
 *
 * Pure functions, no DOM and no storage.
 *
 * How votes are treated (this is also what the UI and the generated methods
 * paragraph say):
 *  - A reviewer "screened" a paper only if they have a vote of include, exclude
 *    or maybe. A missing vote, a vote of "unscreened" or an unknown status
 *    means the paper is not screened by that reviewer.
 *  - Agreement and kappa are computed only over papers BOTH chosen reviewers
 *    screened. Papers one or both have not screened are counted separately and
 *    left out of the statistics, so they do not count as agreement or
 *    disagreement.
 *  - "maybe" mode "separate" (default): maybe is its own category, so
 *    include/maybe is a disagreement and kappa uses three categories.
 *    "maybe" mode "include": maybe is treated as include (a common rule for
 *    title and abstract screening); "exclude" likewise treats maybe as exclude.
 *    Kappa then uses two categories.
 *  - Cohen's kappa = (po - pe) / (1 - pe), po the observed agreement and pe the
 *    agreement expected from each reviewer's category totals. When pe is 1
 *    (both reviewers used one single category for every compared paper) kappa is
 *    0/0, so it is reported as null (undefined) rather than 1 or 0. It is also
 *    null when no paper was screened by both.
 *  - The kappa labels follow Landis and Koch (1977), quoted from memory: below 0
 *    poor, 0 to 0.20 slight, 0.21 to 0.40 fair, 0.41 to 0.60 moderate, 0.61 to
 *    0.80 substantial, above 0.80 almost perfect. They are a convention, not a
 *    threshold the field agrees on.
 */

export const SCREEN_STATUSES = ["include", "exclude", "maybe"];
export const MAYBE_MODES = ["separate", "include", "exclude"];
export const MAX_RESOLUTIONS = 5000;

/** The screening status of one reviewer on one decision entry, or "unscreened". */
export function reviewerStatus(decision, reviewerId) {
  const votes = Array.isArray(decision?.votes) ? decision.votes : [];
  const vote = votes.find((v) => v && v.reviewerId === reviewerId);
  const status = vote?.status;
  return SCREEN_STATUSES.includes(status) ? status : "unscreened";
}

function mapStatus(status, maybeMode) {
  if (status === "maybe" && maybeMode === "include") return "include";
  if (status === "maybe" && maybeMode === "exclude") return "exclude";
  return status;
}

export function kappaLabel(kappa) {
  if (typeof kappa !== "number" || !Number.isFinite(kappa)) return "";
  if (kappa < 0) return "poor";
  if (kappa <= 0.2) return "slight";
  if (kappa <= 0.4) return "fair";
  if (kappa <= 0.6) return "moderate";
  if (kappa <= 0.8) return "substantial";
  return "almost perfect";
}

/**
 * @param {{papers?: object, decisions?: object}} project
 * @param {string} reviewerA id
 * @param {string} reviewerB id
 * @param {{maybeMode?: "separate"|"include"|"exclude"}} [opts]
 */
export function computeAgreement(project, reviewerA, reviewerB, opts = {}) {
  const maybeMode = MAYBE_MODES.includes(opts.maybeMode) ? opts.maybeMode : "separate";
  const categories = maybeMode === "separate" ? ["include", "exclude", "maybe"] : ["include", "exclude"];
  const papers = project?.papers && typeof project.papers === "object" ? project.papers : {};
  const decisions = project?.decisions && typeof project.decisions === "object" ? project.decisions : {};
  const matrix = {};
  categories.forEach((r) => { matrix[r] = {}; categories.forEach((c) => { matrix[r][c] = 0; }); });
  let compared = 0;
  let agreed = 0;
  let onlyA = 0;
  let onlyB = 0;
  let neither = 0;
  const conflicts = [];
  if (!reviewerA || !reviewerB || reviewerA === reviewerB) {
    return emptyResult(categories, maybeMode, Object.keys(papers).length, matrix);
  }
  for (const paperId of Object.keys(papers)) {
    const rawA = reviewerStatus(decisions[paperId], reviewerA);
    const rawB = reviewerStatus(decisions[paperId], reviewerB);
    const a = mapStatus(rawA, maybeMode);
    const b = mapStatus(rawB, maybeMode);
    if (a === "unscreened" && b === "unscreened") { neither += 1; continue; }
    if (a === "unscreened") { onlyB += 1; continue; }
    if (b === "unscreened") { onlyA += 1; continue; }
    compared += 1;
    matrix[a][b] += 1;
    if (a === b) agreed += 1;
    else conflicts.push({ paperId, a: rawA, b: rawB });
  }
  const total = Object.keys(papers).length;
  const percent = compared ? (agreed / compared) * 100 : null;
  let kappa = null;
  let kappaNote = "";
  let po = null;
  let pe = null;
  if (!compared) {
    kappaNote = "No paper was screened by both reviewers.";
  } else {
    po = agreed / compared;
    pe = 0;
    for (const cat of categories) {
      let rowTotal = 0;
      let colTotal = 0;
      for (const other of categories) { rowTotal += matrix[cat][other]; colTotal += matrix[other][cat]; }
      pe += (rowTotal / compared) * (colTotal / compared);
    }
    if (Math.abs(1 - pe) < 1e-12) {
      kappaNote = "Kappa is undefined: both reviewers used a single category for every compared paper.";
    } else {
      kappa = (po - pe) / (1 - pe);
    }
  }
  return {
    reviewerA, reviewerB, maybeMode, categories, matrix,
    total, compared, agreed, disagreed: compared - agreed,
    onlyA, onlyB, neither,
    percent, po, pe, kappa, kappaNote, kappaLabel: kappaLabel(kappa),
    conflicts
  };
}

function emptyResult(categories, maybeMode, total, matrix) {
  return {
    reviewerA: "", reviewerB: "", maybeMode, categories, matrix, total,
    compared: 0, agreed: 0, disagreed: 0, onlyA: 0, onlyB: 0, neither: total,
    percent: null, po: null, pe: null, kappa: null,
    kappaNote: "Choose two different reviewers.", kappaLabel: "", conflicts: []
  };
}

/** Latest resolution for a paper, or null. `log` is the append-only resolution log. */
export function latestResolution(log, paperId) {
  const list = Array.isArray(log) ? log : [];
  for (let i = list.length - 1; i >= 0; i -= 1) if (list[i]?.paperId === paperId) return list[i];
  return null;
}

/**
 * Appends a resolution to the log and returns a new array (never edits earlier
 * entries). Returns null when the entry is not valid. Oldest entries are
 * dropped beyond MAX_RESOLUTIONS.
 */
export function appendResolution(log, entry, now = new Date()) {
  const list = Array.isArray(log) ? log : [];
  const paperId = String(entry?.paperId || "");
  const decision = String(entry?.decision || "");
  const resolvedBy = String(entry?.resolvedBy || "").trim().slice(0, 120);
  if (!paperId || !SCREEN_STATUSES.includes(decision) || !resolvedBy) return null;
  const at = now instanceof Date ? now.toISOString() : String(now);
  const next = list.concat([{
    id: `res_${at}_${list.length}`,
    paperId, resolvedBy, decision, at,
    note: String(entry?.note || "").trim().slice(0, 1000)
  }]);
  return next.length > MAX_RESOLUTIONS ? next.slice(next.length - MAX_RESOLUTIONS) : next;
}

function pct(n) { return n === null ? "not available" : `${n.toFixed(1)}%`; }
function round2(n) { return (Math.round(n * 100) / 100).toFixed(2); }

const MAYBE_TEXT = {
  separate: "A maybe vote was kept as its own category, so include against maybe counts as a disagreement and kappa uses three categories (include, exclude, maybe).",
  include: "A maybe vote was counted as include, so kappa uses two categories (include, exclude).",
  exclude: "A maybe vote was counted as exclude, so kappa uses two categories (include, exclude)."
};

/**
 * A plain methods paragraph with the numbers. `names` is {a, b}; `resolvedCount`
 * is how many of the listed conflicts have a logged resolution; `blind` says
 * whether the project was in blind screening mode.
 */
export function methodsParagraph(result, names = {}, extra = {}) {
  const nameA = names.a || "reviewer A";
  const nameB = names.b || "reviewer B";
  if (!result || !result.compared) {
    return `Screening votes were recorded by ${nameA} and ${nameB}, but no record has been screened by both yet, so agreement cannot be reported.`;
  }
  const parts = [];
  parts.push(`Screening votes were recorded by two reviewers (${nameA} and ${nameB})${extra.blind ? " with blind screening switched on" : ""}.`);
  const left = result.onlyA + result.onlyB + result.neither;
  parts.push(`Agreement was calculated on the ${result.compared} of ${result.total} records that both reviewers had screened${left ? `; ${left} record${left === 1 ? " was" : "s were"} left out because at least one reviewer had not screened ${left === 1 ? "it" : "them"}` : ""}.`);
  parts.push(MAYBE_TEXT[result.maybeMode] || MAYBE_TEXT.separate);
  let k;
  if (result.kappa === null) k = `Cohen's kappa could not be calculated (${result.kappaNote.replace(/\.$/, "").toLowerCase()})`;
  else k = `Cohen's kappa was ${round2(result.kappa)}, which Landis and Koch's labels call ${result.kappaLabel}`;
  parts.push(`The reviewers agreed on ${result.agreed} of ${result.compared} records (${pct(result.percent)}). ${k}.`);
  if (result.disagreed) {
    const resolved = Number(extra.resolvedCount) || 0;
    parts.push(`${result.disagreed} disagreement${result.disagreed === 1 ? "" : "s"} ${result.disagreed === 1 ? "was" : "were"} listed for resolution${resolved ? `; ${resolved} ${resolved === 1 ? "has" : "have"} a recorded resolution in the project's log` : "; none has a recorded resolution yet"}.`);
  }
  return parts.join(" ");
}

function csvCell(value) {
  const s = value == null ? "" : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** CSV of the resolution log. `titleFor(paperId)` and `nameFor(reviewerId)` are optional lookups. */
export function resolutionsToCsv(log, titleFor = () => "") {
  const rows = [["Paper id", "Title", "Resolved by", "Decision", "When", "Note"]];
  (Array.isArray(log) ? log : []).forEach((r) => rows.push([r.paperId, titleFor(r.paperId) || "", r.resolvedBy, r.decision, r.at, r.note || ""]));
  return rows.map((r) => r.map(csvCell).join(",")).join("\n");
}
