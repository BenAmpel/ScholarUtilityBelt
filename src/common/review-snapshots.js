/**
 * Review snapshots and the decision change log.
 *
 * A snapshot is a named, dated deep copy of the review work in a project:
 * screening decisions (votes and overrides), extraction fields and values,
 * quality checklist and answers, tags, notes, PICO boxes, highlights, the
 * reviewer list, the risk-of-bias grid, evidence-matrix columns and the
 * screening resolution log. It does NOT include the papers themselves, the
 * search log, duplicates or update checks, so restoring never removes records.
 * Papers added after a snapshot keep their place but lose their decisions,
 * extraction and notes when you restore (they go back to the snapshot's state).
 *
 * Both lists are capped, oldest dropped first, because they live in
 * chrome.storage.local together with the project:
 *   project.snapshots   [{ id, name, createdAt, kind: "manual"|"auto", counts, data }]
 *   project.changeLog   [{ id, at, paperId, reviewerId, old, new, source }]   (append-only)
 */

export const MAX_SNAPSHOTS = 20;
export const MAX_LOG_ENTRIES = 5000;
export const SNAPSHOT_KEYS = [
  "decisions", "extractionFields", "extraction", "quality", "qualityChecklist", "tags", "notes",
  "pico", "highlights", "reviewers", "rob", "matrixColumns", "agreementResolutions"
];
export const OVERRIDE_REVIEWER = "(override)";

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}
function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }

function effectiveStatus(decision, reviewerId) {
  if (reviewerId === OVERRIDE_REVIEWER) return decision?.overrideStatus || "none";
  const votes = Array.isArray(decision?.votes) ? decision.votes : [];
  const v = votes.find((x) => x && x.reviewerId === reviewerId);
  return v?.status || "unscreened";
}

export function summarizeCounts(project) {
  const decisions = isObj(project?.decisions) ? project.decisions : {};
  let votes = 0;
  for (const d of Object.values(decisions)) if (Array.isArray(d?.votes)) votes += d.votes.length;
  return {
    papers: isObj(project?.papers) ? Object.keys(project.papers).length : 0,
    decisions: Object.keys(decisions).length,
    votes,
    extracted: isObj(project?.extraction) ? Object.keys(project.extraction).length : 0
  };
}

/** Builds a snapshot object (not yet stored). Returns null for a missing project. */
export function takeSnapshot(project, name, opts = {}) {
  if (!isObj(project)) return null;
  const now = opts.now instanceof Date ? opts.now : new Date();
  const kind = opts.kind === "auto" ? "auto" : "manual";
  const data = {};
  for (const key of SNAPSHOT_KEYS) if (project[key] !== undefined) data[key] = clone(project[key]);
  const fallback = `${kind === "auto" ? "Automatic" : "Snapshot"} ${now.toISOString().slice(0, 16).replace("T", " ")}`;
  return {
    id: `snap_${now.getTime().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
    name: String(name || "").trim().slice(0, 80) || fallback,
    createdAt: now.toISOString(),
    kind,
    counts: summarizeCounts(project),
    data
  };
}

/** Returns a new list with the snapshot appended and the oldest dropped beyond the cap. */
export function addSnapshot(list, snapshot, max = MAX_SNAPSHOTS) {
  const base = Array.isArray(list) ? list : [];
  if (!isObj(snapshot) || !snapshot.id) return base.slice();
  const next = base.concat([snapshot]);
  return next.length > max ? next.slice(next.length - max) : next;
}

export function deleteSnapshot(list, id) {
  return (Array.isArray(list) ? list : []).filter((s) => s?.id !== id);
}

export function approxBytes(snapshot) {
  try { return JSON.stringify(snapshot).length; } catch { return 0; }
}

/**
 * Decision changes between two decision maps: one entry per paper and reviewer
 * whose vote status differs, plus (override) entries when a resolution
 * override differs. Used to log a restore.
 */
export function diffDecisions(before, after) {
  const a = isObj(before) ? before : {};
  const b = isObj(after) ? after : {};
  const changes = [];
  const paperIds = new Set([...Object.keys(a), ...Object.keys(b)]);
  for (const paperId of paperIds) {
    const ids = new Set([OVERRIDE_REVIEWER]);
    [a[paperId], b[paperId]].forEach((d) => (Array.isArray(d?.votes) ? d.votes : []).forEach((v) => { if (v?.reviewerId) ids.add(v.reviewerId); }));
    for (const reviewerId of ids) {
      const oldStatus = effectiveStatus(a[paperId], reviewerId);
      const newStatus = effectiveStatus(b[paperId], reviewerId);
      if (oldStatus !== newStatus) changes.push({ paperId, reviewerId, old: oldStatus, new: newStatus });
    }
  }
  return changes;
}

/**
 * Replaces the snapshot's keys on `project` with deep copies from the snapshot
 * (keys the snapshot does not have are reset to empty so the result matches
 * the snapshot, not a mix). Mutates and returns the decision changes it made,
 * for the log. Returns null when the snapshot is unusable.
 */
export function restoreSnapshot(project, snapshot) {
  if (!isObj(project) || !isObj(snapshot) || !isObj(snapshot.data)) return null;
  const before = clone(project.decisions) || {};
  for (const key of SNAPSHOT_KEYS) {
    if (snapshot.data[key] !== undefined) project[key] = clone(snapshot.data[key]);
    else if (key === "decisions" || key === "extraction" || key === "quality" || key === "tags" || key === "notes" || key === "pico" || key === "highlights") project[key] = {};
    else if (key === "agreementResolutions" || key === "matrixColumns") project[key] = [];
    else if (key === "rob") delete project[key];
  }
  return diffDecisions(before, project.decisions);
}

/**
 * Appends entries to a change log (returns a new array; earlier entries are never
 * edited). Each input: {paperId, reviewerId, old, new, source?}. Entries whose
 * old and new are equal are skipped. Oldest dropped beyond the cap.
 */
export function appendLog(log, entries, now = new Date(), max = MAX_LOG_ENTRIES) {
  const base = Array.isArray(log) ? log : [];
  const at = now instanceof Date ? now.toISOString() : String(now);
  const added = [];
  (Array.isArray(entries) ? entries : []).forEach((e, i) => {
    if (!e || !e.paperId || !e.reviewerId || e.old === e.new) return;
    added.push({
      id: `chg_${at}_${base.length + i}`,
      at, paperId: String(e.paperId), reviewerId: String(e.reviewerId),
      old: String(e.old ?? ""), new: String(e.new ?? ""), source: String(e.source || "screening")
    });
  });
  if (!added.length) return base;
  const next = base.concat(added);
  return next.length > max ? next.slice(next.length - max) : next;
}

function csvCell(value) {
  let s = value == null ? "" : String(value);
  // Stop spreadsheet programs from running text that starts like a formula (numbers keep their sign).
  if (typeof value !== "number" && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** CSV of the change log. `titleFor(paperId)` and `nameFor(reviewerId)` are optional lookups. */
export function logToCsv(log, titleFor = () => "", nameFor = (id) => id) {
  const rows = [["When", "Paper id", "Title", "Reviewer", "Old", "New", "Source"]];
  (Array.isArray(log) ? log : []).forEach((e) => rows.push([e.at, e.paperId, titleFor(e.paperId) || "", nameFor(e.reviewerId) || e.reviewerId, e.old, e.new, e.source || ""]));
  return rows.map((r) => r.map(csvCell).join(",")).join("\n");
}
