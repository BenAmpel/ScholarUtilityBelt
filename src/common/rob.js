/**
 * Risk-of-bias grid: packaged templates and the pure logic around them.
 *
 * This records a reviewer's own judgement per paper, per domain. It does not
 * apply any official tool and never works out a judgement for the user: the
 * overall judgement per paper is whatever the user sets.
 *
 * Domain names are short labels written for this grid. They are not the
 * official signalling questions or wording of RoB 2, ROBINS-I or the
 * Newcastle-Ottawa Scale. The domain lists and the star maximums below are
 * from memory (RoB 2: five domains; ROBINS-I: seven; NOS cohort: selection up
 * to 4 stars, comparability up to 2, outcome up to 3). Check them against the
 * official documents before relying on them.
 *
 * Stored per project as project.rob:
 *   {
 *     defaultTemplate: "rob2",
 *     customDomains: [{ key, label }],
 *     papers: { [paperId]: { template?, reviewers: { [reviewerId]: { [domainKey]: value } }, overall: { judgement, note } } }
 *   }
 * `value` is "low" | "some" | "high" for judgement templates and an integer
 * star count for the star template. A missing key means "not assessed".
 */

export const ROB_NOTICE = "This grid only records your own judgement. Apply the official tool (RoB 2, ROBINS-I or the Newcastle-Ottawa Scale) using its guidance; the names here are short labels, not the official wording, and nothing is judged for you.";

export const ROB_LEVELS = ["low", "some", "high"];
export const ROB_LEVEL_LABELS = { low: "Low", some: "Some concerns", high: "High" };
export const MAX_CUSTOM_DOMAINS = 30;

export const ROB_TEMPLATES = {
  rob2: {
    id: "rob2",
    label: "RoB 2 (randomized trials)",
    scale: "judgement",
    domains: [
      { key: "rob2_randomization", label: "Randomization process" },
      { key: "rob2_deviations", label: "Deviations from intended interventions" },
      { key: "rob2_missing", label: "Missing outcome data" },
      { key: "rob2_measurement", label: "Measurement of the outcome" },
      { key: "rob2_selection", label: "Selection of the reported result" }
    ]
  },
  robinsi: {
    id: "robinsi",
    label: "ROBINS-I (non-randomized studies)",
    scale: "judgement",
    domains: [
      { key: "robinsi_confounding", label: "Confounding" },
      { key: "robinsi_selection", label: "Selection of participants" },
      { key: "robinsi_classification", label: "Classification of interventions" },
      { key: "robinsi_deviations", label: "Deviations from intended interventions" },
      { key: "robinsi_missing", label: "Missing data" },
      { key: "robinsi_measurement", label: "Measurement of outcomes" },
      { key: "robinsi_reported", label: "Selection of the reported result" }
    ]
  },
  nos: {
    id: "nos",
    label: "Newcastle-Ottawa (cohort, stars)",
    scale: "stars",
    domains: [
      { key: "nos_selection", label: "Selection", maxStars: 4 },
      { key: "nos_comparability", label: "Comparability", maxStars: 2 },
      { key: "nos_outcome", label: "Outcome", maxStars: 3 }
    ]
  },
  custom: {
    id: "custom",
    label: "Custom checklist (edit the domains)",
    scale: "judgement",
    domains: []
  }
};

export const DEFAULT_CUSTOM_DOMAINS = [
  { key: "c_selection", label: "Selection of participants" },
  { key: "c_measurement", label: "Measurement" },
  { key: "c_missing", label: "Missing data" },
  { key: "c_reporting", label: "Reporting" }
];

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }

export function normalizeRob(rob) {
  const out = isObj(rob) ? rob : {};
  if (!ROB_TEMPLATES[out.defaultTemplate]) out.defaultTemplate = "rob2";
  if (!Array.isArray(out.customDomains)) out.customDomains = DEFAULT_CUSTOM_DOMAINS.map((d) => ({ ...d }));
  out.customDomains = out.customDomains
    .filter((d) => isObj(d) && typeof d.key === "string" && d.key)
    .slice(0, MAX_CUSTOM_DOMAINS)
    .map((d) => ({ key: d.key, label: String(d.label || d.key).slice(0, 80) }));
  if (!isObj(out.papers)) out.papers = {};
  return out;
}

/** The template in force for a paper, with the project's custom domains filled in for "custom". */
export function templateFor(rob, paperId) {
  const r = normalizeRob(rob);
  const id = ROB_TEMPLATES[r.papers[paperId]?.template] ? r.papers[paperId].template : r.defaultTemplate;
  const base = ROB_TEMPLATES[id];
  return id === "custom" ? { ...base, domains: r.customDomains.map((d) => ({ ...d })) } : base;
}

function paperEntry(rob, paperId) {
  const r = normalizeRob(rob);
  if (!isObj(r.papers[paperId])) r.papers[paperId] = {};
  const e = r.papers[paperId];
  if (!isObj(e.reviewers)) e.reviewers = {};
  if (!isObj(e.overall)) e.overall = { judgement: "", note: "" };
  return e;
}

/** Whether `value` is a legal answer for that domain of that template. */
export function isValidValue(template, domain, value) {
  if (template.scale === "stars") return Number.isInteger(value) && value >= 0 && value <= (domain.maxStars || 0);
  return ROB_LEVELS.includes(value);
}

/**
 * Sets (or clears, with "" / null / undefined) one reviewer's answer on one
 * domain. Returns true when stored. Unknown domains and illegal values are
 * rejected and leave the data unchanged.
 */
export function setJudgement(rob, paperId, reviewerId, domainKey, value) {
  if (!paperId || !reviewerId) return false;
  const template = templateFor(rob, paperId);
  const domain = template.domains.find((d) => d.key === domainKey);
  if (!domain) return false;
  const entry = paperEntry(rob, paperId);
  if (value === "" || value === null || value === undefined) {
    if (entry.reviewers[reviewerId]) delete entry.reviewers[reviewerId][domainKey];
    return true;
  }
  const v = template.scale === "stars" && typeof value === "string" && /^\d+$/.test(value) ? Number(value) : value;
  if (!isValidValue(template, domain, v)) return false;
  entry.reviewers[reviewerId] = isObj(entry.reviewers[reviewerId]) ? entry.reviewers[reviewerId] : {};
  entry.reviewers[reviewerId][domainKey] = v;
  return true;
}

/** The user's overall judgement for a paper. Never derived from the domains. */
export function setOverall(rob, paperId, judgement, note) {
  if (!paperId) return false;
  if (judgement != null && judgement !== "" && !ROB_LEVELS.includes(judgement)) return false;
  const entry = paperEntry(rob, paperId);
  if (judgement != null) entry.overall.judgement = judgement;
  if (note != null) entry.overall.note = String(note).slice(0, 2000);
  return true;
}

export function setPaperTemplate(rob, paperId, templateId) {
  if (!paperId) return false;
  if (templateId !== "" && !ROB_TEMPLATES[templateId]) return false;
  const entry = paperEntry(rob, paperId);
  if (templateId) entry.template = templateId; else delete entry.template;
  return true;
}

export function setDefaultTemplate(rob, templateId) {
  const r = normalizeRob(rob);
  if (!ROB_TEMPLATES[templateId]) return false;
  r.defaultTemplate = templateId;
  return true;
}

export function addCustomDomain(rob, label) {
  const r = normalizeRob(rob);
  const text = String(label || "").trim().slice(0, 80);
  if (!text || r.customDomains.length >= MAX_CUSTOM_DOMAINS) return null;
  if (r.customDomains.some((d) => d.label.toLowerCase() === text.toLowerCase())) return null;
  let n = r.customDomains.length + 1;
  let key = `c_${n}`;
  while (r.customDomains.some((d) => d.key === key)) { n += 1; key = `c_${n}`; }
  const domain = { key, label: text };
  r.customDomains.push(domain);
  return domain;
}

export function renameCustomDomain(rob, key, label) {
  const r = normalizeRob(rob);
  const d = r.customDomains.find((x) => x.key === key);
  const text = String(label || "").trim().slice(0, 80);
  if (!d || !text) return false;
  d.label = text;
  return true;
}

export function removeCustomDomain(rob, key) {
  const r = normalizeRob(rob);
  const before = r.customDomains.length;
  r.customDomains = r.customDomains.filter((d) => d.key !== key);
  return r.customDomains.length < before;
}

/**
 * Per-reviewer tallies for one paper, plus how many domains two or more
 * reviewers both answered with different values. `reviewers` is
 * [{id, name}]. Counts only: no overall judgement is derived.
 */
export function summarizePaper(rob, paperId, reviewers) {
  const r = normalizeRob(rob);
  const template = templateFor(r, paperId);
  const entry = isObj(r.papers[paperId]) ? r.papers[paperId] : {};
  const byReviewer = [];
  for (const rev of reviewers || []) {
    const answers = isObj(entry.reviewers?.[rev.id]) ? entry.reviewers[rev.id] : {};
    const tally = { reviewerId: rev.id, name: rev.name || rev.id, assessed: 0, notAssessed: 0, low: 0, some: 0, high: 0, stars: 0, maxStars: 0 };
    for (const d of template.domains) {
      const v = answers[d.key];
      if (template.scale === "stars") {
        tally.maxStars += d.maxStars || 0;
        if (isValidValue(template, d, v)) { tally.assessed += 1; tally.stars += v; } else tally.notAssessed += 1;
      } else if (ROB_LEVELS.includes(v)) { tally.assessed += 1; tally[v] += 1; } else tally.notAssessed += 1;
    }
    byReviewer.push(tally);
  }
  let differing = 0;
  for (const d of template.domains) {
    const vals = (reviewers || []).map((rev) => entry.reviewers?.[rev.id]?.[d.key]).filter((v) => v !== undefined && v !== null && v !== "");
    if (vals.length >= 2 && new Set(vals).size > 1) differing += 1;
  }
  return { template: template.id, byReviewer, differing, overall: { judgement: entry.overall?.judgement || "", note: entry.overall?.note || "" } };
}

/** Display text for one stored answer: the level label, the star count, or "" when not assessed. */
export function robValueText(template, v) {
  if (template.scale === "stars") return Number.isInteger(v) ? String(v) : "";
  return ROB_LEVEL_LABELS[v] || "";
}

function csvCell(value) {
  const s = value == null ? "" : String(value);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function mdCell(value) {
  return String(value == null ? "" : value).replace(/\\/g, "\\\\").replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
}

function exportRows(rob, paperIds, papers, reviewers) {
  const r = normalizeRob(rob);
  const rows = [];
  for (const id of paperIds) {
    const template = templateFor(r, id);
    const entry = isObj(r.papers[id]) ? r.papers[id] : {};
    for (const rev of reviewers) {
      const answers = isObj(entry.reviewers?.[rev.id]) ? entry.reviewers[rev.id] : {};
      rows.push({
        id, title: papers?.[id]?.title || "", year: papers?.[id]?.year || "",
        template, reviewer: rev.name || rev.id,
        cells: template.domains.map((d) => ({ label: d.label, text: robValueText(template, answers[d.key]) })),
        overall: ROB_LEVEL_LABELS[entry.overall?.judgement] || "",
        note: entry.overall?.note || ""
      });
    }
  }
  return rows;
}

/** CSV, one row per paper and reviewer. Domain columns are the union across templates in use. */
export function robToCsv({ rob, paperIds, papers, reviewers }) {
  const rows = exportRows(rob, paperIds || [], papers, reviewers || []);
  const labels = [];
  rows.forEach((row) => row.cells.forEach((c) => { if (!labels.includes(c.label)) labels.push(c.label); }));
  const header = ["Title", "Year", "Template", "Reviewer", ...labels, "Overall (set by you)", "Overall note"];
  const lines = [header];
  rows.forEach((row) => {
    lines.push([row.title, row.year, row.template.label, row.reviewer, ...labels.map((l) => row.cells.find((c) => c.label === l)?.text ?? ""), row.overall, row.note]);
  });
  return lines.map((l) => l.map(csvCell).join(",")).join("\n");
}

/** Markdown: the notice, then one table per template in use. */
export function robToMarkdown({ rob, paperIds, papers, reviewers, title }) {
  const rows = exportRows(rob, paperIds || [], papers, reviewers || []);
  const out = [`# ${title || "Risk of bias"}`, "", `_${ROB_NOTICE}_`, ""];
  if (!rows.length) { out.push("No papers or reviewers to report."); return out.join("\n"); }
  const byTemplate = new Map();
  rows.forEach((row) => { if (!byTemplate.has(row.template.id)) byTemplate.set(row.template.id, []); byTemplate.get(row.template.id).push(row); });
  for (const [, group] of byTemplate) {
    const t = group[0].template;
    out.push(`## ${t.label}`, "");
    const header = ["Paper", "Reviewer", ...t.domains.map((d) => (t.scale === "stars" ? `${d.label} (max ${d.maxStars})` : d.label)), "Overall (set by you)", "Note"];
    out.push(`| ${header.map(mdCell).join(" | ")} |`, `| ${header.map(() => "---").join(" | ")} |`);
    group.forEach((row) => {
      const paper = `${row.title || "Untitled"}${row.year ? ` (${row.year})` : ""}`;
      out.push(`| ${[paper, row.reviewer, ...row.cells.map((c) => c.text), row.overall, row.note].map(mdCell).join(" | ")} |`);
    });
    out.push("");
  }
  return out.join("\n");
}
