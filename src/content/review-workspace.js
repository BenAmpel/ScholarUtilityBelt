/**
 * Systematic-review workspace — lazy-loaded from content.js only when the
 * user opens the "Review workspace" button next to the search-results
 * filter bar (dynamic import, see openReviewOverlay in content.js). This
 * is one of the largest, least-frequently-used features in the extension
 * (multi-reviewer screening, dedup, extraction, PRISMA export), so moving
 * it out of the always-loaded content.js bundle is worth the size.
 *
 * All state/helpers shared with content.js are passed in via `deps`
 * rather than imported, since content.js is a classic script, not a
 * module. Review-project persistence (getReviewProjects/setReviewProjects/
 * normalizeReviewProject/createReviewProject) and its storage key/default
 * constants moved in here too, since they're used exclusively by this
 * feature — no other call sites existed in content.js.
 */
export function createReviewWorkspace(deps) {
  const {
    escapeHtml,
    csvEscape,
    extractDOIFromResult,
    extractPaperFromResult,
    fetchOpenAlexWorkById,
    fetchOpenAlexWorkForPaper,
    getBestPdfUrl,
    getCachedAuthorCitationCount,
    getCachedAuthorPaper,
    getCitationCountFromResult,
    getScholarSearchQuery,
    getSnippetText,
    hashString,
    normalizeDoi,
    normalizeTitleForMatch,
    scanResults,
    downloadBlob,
    getStorageMap,
    setStorageMap,
    CLS_BTN_SEC,
  } = deps;

  const REVIEW_PROJECTS_KEY = "reviewProjects";
  const REVIEW_DEFAULT_QUALITY_CHECKLIST = [
    { key: "randomization", label: "Randomization" },
    { key: "blinding", label: "Blinding" },
    { key: "allocation", label: "Allocation concealment" },
    { key: "attrition", label: "Attrition handling" },
    { key: "reporting", label: "Selective reporting" }
  ];
  const REVIEW_DEFAULT_REVIEWERS = [{ id: "you", name: "You" }];

  async function getReviewProjects() {
    if (!chrome?.storage?.local?.get) return {};
    return getStorageMap(REVIEW_PROJECTS_KEY);
  }
  async function setReviewProjects(map) {
    if (!chrome?.storage?.local?.set) return;
    await setStorageMap(REVIEW_PROJECTS_KEY, map);
  }
  function normalizeReviewProject(project) {
    if (!project || typeof project !== "object") return null;
    project.papers = project.papers || {};
    project.fingerprints = project.fingerprints || {};
    project.decisions = project.decisions || {};
    project.pico = project.pico || {};
    project.tags = project.tags || {};
    project.notes = project.notes || {};
    project.extractionFields = Array.isArray(project.extractionFields) && project.extractionFields.length
      ? project.extractionFields
      : ["Design", "Sample", "Outcome"];
    project.extraction = project.extraction || {};
    project.dedupeCount = Number(project.dedupeCount) || 0;
    project.duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
    project.highlights = project.highlights || {};
    project.quality = project.quality || {};
    project.qualityChecklist = Array.isArray(project.qualityChecklist) && project.qualityChecklist.length
      ? project.qualityChecklist
      : REVIEW_DEFAULT_QUALITY_CHECKLIST.map((c) => ({ ...c }));
    project.reviewers = Array.isArray(project.reviewers) && project.reviewers.length
      ? project.reviewers
      : REVIEW_DEFAULT_REVIEWERS.map((r) => ({ ...r }));
    if (!project.activeReviewerId || !project.reviewers.some((r) => r.id === project.activeReviewerId)) {
      project.activeReviewerId = project.reviewers[0]?.id || "you";
    }
    project.blindMode = !!project.blindMode;
    project.updates = Array.isArray(project.updates) ? project.updates : [];
    project.lastUpdateCheck = project.lastUpdateCheck || null;
    return project;
  }
  function createReviewProject(name, query) {
    const id = `rev_${Date.now().toString(36)}`;
    return {
      id,
      name: name || "New review",
      query: query || "",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      papers: {},
      fingerprints: {},
      decisions: {},
      pico: {},
      tags: {},
      notes: {},
      extractionFields: ["Design", "Sample", "Outcome"],
      extraction: {},
      dedupeCount: 0,
      duplicates: [],
      highlights: {},
      quality: {},
      qualityChecklist: REVIEW_DEFAULT_QUALITY_CHECKLIST.map((c) => ({ ...c })),
      reviewers: REVIEW_DEFAULT_REVIEWERS.map((r) => ({ ...r })),
      activeReviewerId: "you",
      blindMode: false,
      lastUpdateCheck: null,
      updates: []
    };
  }
  async function ensureReviewState() {
    if (window.suReviewState) return window.suReviewState;
    const projects = await getReviewProjects();
    const ids = Object.keys(projects);
    let dirty = false;
    for (const id of ids) {
      const normalized = normalizeReviewProject(projects[id]);
      if (normalized) {
        projects[id] = normalized;
        dirty = true;
      }
    }
    let activeId = ids[0] || null;
    if (!activeId) {
      const q = getScholarSearchQuery();
      const project = createReviewProject("New review", q || "");
      projects[project.id] = project;
      activeId = project.id;
      dirty = true;
    }
    if (dirty) {
      await setReviewProjects(projects);
    }
    window.suReviewState = { projects, activeId, tab: "overview" };
    return window.suReviewState;
  }
  function getActiveReviewProject() {
    const state = window.suReviewState;
    if (!state) return null;
    return state.projects?.[state.activeId] || null;
  }
  async function saveReviewProject(project) {
    if (!project || !project.id) return;
    const state = await ensureReviewState();
    normalizeReviewProject(project);
    project.updatedAt = new Date().toISOString();
    state.projects[project.id] = project;
    await setReviewProjects(state.projects);
  }
  function extractReviewPaperFromResult(row, isAuthorProfile) {
    const paper = isAuthorProfile ? getCachedAuthorPaper(row) : extractPaperFromResult(row);
    if (!paper || !paper.title) return null;
    const snippetText = getSnippetText(row);
    const authorsPart = (paper.authorsVenue || "").split(/\s*[-–—]\s*/)[0]?.trim() || "";
    const citations = isAuthorProfile ? getCachedAuthorCitationCount(row) : getCitationCountFromResult(row);
    const doi = normalizeDoi(extractDOIFromResult(row));
    const pdfInfo = getBestPdfUrl(row);
    return {
      id: paper.key,
      title: paper.title || "",
      year: paper.year || "",
      authors: authorsPart,
      venue: paper.venue || "",
      url: paper.url || "",
      citedByUrl: paper.citedByUrl || "",
      pdfUrl: pdfInfo?.url || "",
      pdfLabel: pdfInfo?.label || "",
      doi: doi || "",
      citations: citations || 0,
      abstract: (snippetText || "").trim()
    };
  }
  function paperFingerprint(p) {
    const doi = normalizeDoi(p?.doi || "");
    if (doi) return `doi:${doi}`;
    const titleKey = normalizeTitleForMatch(p?.title || "");
    return titleKey ? `title:${titleKey}` : `key:${p?.id || ""}`;
  }
  function addPapersToProject(project, papers) {
    if (!project || !Array.isArray(papers)) return { added: 0, duplicates: 0 };
    let added = 0;
    let duplicates = 0;
    for (const p of papers) {
      if (!p || !p.title) continue;
      const fp = paperFingerprint(p);
      if (project.fingerprints?.[fp]) {
        duplicates += 1;
        project.duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
        project.duplicates.push({
          fingerprint: fp,
          existingId: project.fingerprints[fp],
          paper: { ...p },
          addedAt: new Date().toISOString()
        });
        continue;
      }
      project.papers[p.id] = p;
      project.fingerprints[fp] = p.id;
      added += 1;
    }
    project.dedupeCount = (project.dedupeCount || 0) + duplicates;
    return { added, duplicates };
  }
  function getActiveReviewer(project) {
    normalizeReviewProject(project);
    const reviewers = project.reviewers || [];
    let active = reviewers.find((r) => r.id === project.activeReviewerId) || reviewers[0];
    if (!active) {
      active = { ...REVIEW_DEFAULT_REVIEWERS[0] };
      reviewers.push(active);
      project.activeReviewerId = active.id;
    }
    return active;
  }
  function normalizeDecisionEntry(decision) {
    if (!decision || typeof decision !== "object") return { status: "unscreened", reason: "", votes: [] };
    decision.status = decision.status || "unscreened";
    decision.reason = decision.reason || "";
    decision.overrideStatus = decision.overrideStatus || "";
    decision.votes = Array.isArray(decision.votes) ? decision.votes : [];
    return decision;
  }
  function getConsensusStatus(votes) {
    if (!Array.isArray(votes) || votes.length === 0) return "unscreened";
    const counts = { include: 0, exclude: 0, maybe: 0 };
    votes.forEach((v) => {
      if (v?.status === "include") counts.include += 1;
      else if (v?.status === "exclude") counts.exclude += 1;
      else if (v?.status === "maybe") counts.maybe += 1;
    });
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (entries[0][1] === 0) return "unscreened";
    if (entries[1][1] === entries[0][1]) return "conflict";
    return entries[0][0];
  }
  function getDecisionStatus(project, id, opts = {}) {
    const decision = normalizeDecisionEntry(project.decisions?.[id]);
    const reviewerId = opts.reviewerId;
    const blind = !!opts.blind;
    if (blind && reviewerId) {
      const vote = decision.votes.find((v) => v.reviewerId === reviewerId);
      return vote?.status || "unscreened";
    }
    if (decision.overrideStatus) return decision.overrideStatus;
    if (decision.votes && decision.votes.length) {
      return getConsensusStatus(decision.votes);
    }
    return decision.status || "unscreened";
  }
  function getReviewerVote(decision, reviewerId) {
    if (!decision || !reviewerId) return null;
    const votes = Array.isArray(decision.votes) ? decision.votes : [];
    return votes.find((v) => v.reviewerId === reviewerId) || null;
  }
  function upsertReviewerVote(project, id, reviewerId, nextStatus, nextReason) {
    if (!project || !id || !reviewerId) return;
    project.decisions = project.decisions || {};
    const decision = normalizeDecisionEntry(project.decisions[id]);
    const votes = decision.votes;
    const existing = votes.find((v) => v.reviewerId === reviewerId);
    const now = new Date().toISOString();
    if (nextStatus && decision.overrideStatus) {
      decision.overrideStatus = "";
    }
    if (existing) {
      if (nextStatus) existing.status = nextStatus;
      if (nextReason != null) existing.reason = nextReason;
      existing.updatedAt = now;
    } else {
      votes.push({
        reviewerId,
        status: nextStatus || "unscreened",
        reason: nextReason || "",
        createdAt: now,
        updatedAt: now
      });
    }
    decision.status = getConsensusStatus(votes);
    project.decisions[id] = decision;
  }
  function computeReviewStats(project) {
    const ids = Object.keys(project.papers || {});
    let included = 0;
    let excluded = 0;
    let maybe = 0;
    let unscreened = 0;
    let conflicts = 0;
    for (const id of ids) {
      const status = getDecisionStatus(project, id, { blind: false });
      if (status === "include") included += 1;
      else if (status === "exclude") excluded += 1;
      else if (status === "maybe") maybe += 1;
      else if (status === "conflict") conflicts += 1;
      else unscreened += 1;
    }
    return {
      total: ids.length,
      included,
      excluded,
      maybe,
      unscreened,
      conflicts,
      duplicates: project.dedupeCount || 0
    };
  }
  function computeReviewerStats(project) {
    const reviewers = Array.isArray(project.reviewers) ? project.reviewers : [];
    const stats = {};
    reviewers.forEach((r) => {
      stats[r.id] = { name: r.name, total: 0, include: 0, exclude: 0, maybe: 0 };
    });
    const decisions = project.decisions || {};
    for (const [id, decisionRaw] of Object.entries(decisions)) {
      const decision = normalizeDecisionEntry(decisionRaw);
      const votes = Array.isArray(decision.votes) ? decision.votes : [];
      votes.forEach((v) => {
        const entry = stats[v.reviewerId] || (stats[v.reviewerId] = { name: v.reviewerId, total: 0, include: 0, exclude: 0, maybe: 0 });
        entry.total += 1;
        if (v.status === "include") entry.include += 1;
        else if (v.status === "exclude") entry.exclude += 1;
        else if (v.status === "maybe") entry.maybe += 1;
      });
    }
    return Object.entries(stats).map(([id, s]) => ({ id, ...s }));
  }
  function getConflictPapers(project) {
    const out = [];
    const papers = project.papers || {};
    for (const id of Object.keys(papers)) {
      const decision = normalizeDecisionEntry(project.decisions?.[id]);
      const votes = Array.isArray(decision.votes) ? decision.votes : [];
      if (votes.length < 2) continue;
      const consensus = getConsensusStatus(votes);
      if (consensus !== "conflict") continue;
      out.push({ id, paper: papers[id], votes });
    }
    return out;
  }
  function buildTermWeights(project) {
    const papers = project.papers || {};
    const pos = new Map();
    const neg = new Map();
    const tokenize = (txt) => String(txt || "").toLowerCase().replace(/[^a-z0-9\\s]/g, " ").split(/\\s+/).filter((t) => t.length >= 3);
    Object.keys(papers).forEach((id) => {
      const status = getDecisionStatus(project, id, { blind: false });
      if (status !== "include" && status !== "exclude") return;
      const text = `${papers[id]?.title || ""} ${papers[id]?.abstract || ""}`;
      const tokens = tokenize(text);
      const target = status === "include" ? pos : neg;
      tokens.forEach((t) => target.set(t, (target.get(t) || 0) + 1));
    });
    const weights = new Map();
    const allTokens = new Set([...pos.keys(), ...neg.keys()]);
    allTokens.forEach((t) => {
      const a = (pos.get(t) || 0) + 1;
      const b = (neg.get(t) || 0) + 1;
      weights.set(t, Math.log(a / b));
    });
    return weights;
  }
  function scorePaperForActiveLearning(paper, weights) {
    if (!paper) return 0;
    if (!weights || weights.size === 0) return 0;
    const tokens = String(`${paper.title || ""} ${paper.abstract || ""}`)
      .toLowerCase()
      .replace(/[^a-z0-9\\s]/g, " ")
      .split(/\\s+/)
      .filter((t) => t.length >= 3);
    let score = 0;
    tokens.forEach((t) => { score += weights.get(t) || 0; });
    return Math.round(score * 10) / 10;
  }
  function normalizeExtractionField(field, idx) {
    if (!field) return { key: `field_${idx}`, label: `Field ${idx + 1}`, type: "text", options: [] };
    if (typeof field === "string") {
      const key = field.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || `field_${idx}`;
      return { key, label: field, type: "text", options: [] };
    }
    const key = String(field.key || field.label || `field_${idx}`).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || `field_${idx}`;
    return {
      key,
      label: field.label || field.key || `Field ${idx + 1}`,
      type: field.type || "text",
      options: Array.isArray(field.options) ? field.options : []
    };
  }
  function getExtractionFields(project) {
    const fields = Array.isArray(project.extractionFields) ? project.extractionFields : [];
    return fields.map((f, idx) => normalizeExtractionField(f, idx));
  }
  function renderPrismaDiagram(stats) {
    const total = stats.total;
    const dupes = stats.duplicates;
    const screened = total - stats.unscreened;
    const included = stats.included;
    return `
      <svg class="su-review-prisma" viewBox="0 0 520 240" role="img" aria-label="PRISMA diagram">
        <rect x="20" y="20" width="200" height="50" rx="8"></rect>
        <text x="120" y="48" text-anchor="middle">Records: ${total}</text>
        <rect x="300" y="20" width="200" height="50" rx="8"></rect>
        <text x="400" y="48" text-anchor="middle">Duplicates: ${dupes}</text>
        <rect x="20" y="110" width="200" height="50" rx="8"></rect>
        <text x="120" y="138" text-anchor="middle">Screened: ${screened}</text>
        <rect x="300" y="110" width="200" height="50" rx="8"></rect>
        <text x="400" y="138" text-anchor="middle">Excluded: ${stats.excluded}</text>
        <rect x="160" y="190" width="200" height="50" rx="8"></rect>
        <text x="260" y="218" text-anchor="middle">Included: ${included}</text>
        <line x1="120" y1="70" x2="120" y2="110"></line>
        <line x1="220" y1="135" x2="300" y2="135"></line>
        <line x1="220" y1="135" x2="260" y2="190"></line>
      </svg>
    `;
  }
  function exportReviewDecisionsCsv(project) {
    if (!project) return;
    const rows = [["Title", "Year", "Decision", "Consensus", "Reviewer", "Reason", "Tags", "URL"]];
    for (const [id, paper] of Object.entries(project.papers || {})) {
      const decision = normalizeDecisionEntry(project.decisions?.[id]);
      const consensus = getDecisionStatus(project, id, { blind: false });
      const tags = Array.isArray(project.tags?.[id]) ? project.tags[id].join("; ") : "";
      const votes = Array.isArray(decision.votes) && decision.votes.length ? decision.votes : [{ reviewerId: "", status: decision.status, reason: decision.reason }];
      votes.forEach((v) => {
        const reviewerName = (project.reviewers || []).find((r) => r.id === v.reviewerId)?.name || v.reviewerId || "";
        rows.push([
          paper.title || "",
          paper.year || "",
          v.status || "",
          consensus,
          reviewerName,
          v.reason || "",
          tags,
          paper.url || ""
        ]);
      });
    }
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-decisions.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }
  function exportReviewExtractionCsv(project) {
    if (!project) return;
    const fields = getExtractionFields(project);
    const headers = ["Title", "Year", ...fields.map((f) => f.label)];
    const rows = [headers];
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    includedIds.forEach((id) => {
      const paper = project.papers[id];
      const data = project.extraction[id] || {};
      rows.push([
        paper?.title || "",
        paper?.year || "",
        ...fields.map((f) => data[f.key] || "")
      ]);
    });
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-extraction.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }
  function exportReviewQualityCsv(project) {
    if (!project) return;
    const checklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
    const headers = ["Title", "Year", "Risk", ...checklist.map((c) => c.label), "Notes"];
    const rows = [headers];
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    includedIds.forEach((id) => {
      const paper = project.papers[id];
      const q = project.quality[id] || {};
      const checks = q.checks || {};
      rows.push([
        paper?.title || "",
        paper?.year || "",
        q.risk || "",
        ...checklist.map((c) => (checks[c.key] ? "Yes" : "")),
        q.notes || ""
      ]);
    });
    const csv = rows.map((r) => r.map(csvEscape).join(",")).join("\n");
    downloadBlob(`${project.name}-quality.csv`.replace(/\s+/g, "-").toLowerCase(), "text/csv;charset=utf-8", csv);
  }
  function exportReviewBibTeX(project) {
    if (!project) return;
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const entries = includedIds.map((id) => {
      const p = project.papers[id];
      const key = `${(project.name || "review").replace(/\\W+/g, "")}${p.year || ""}${hashString(p.title || "")}`.slice(0, 32);
      const authors = p.authors ? p.authors.split(/\\s*[,;]\\s*|\\s+and\\s+/i).join(" and ") : "";
      return `@article{${key},\\n  title={${p.title || ""}},\\n  author={${authors}},\\n  year={${p.year || ""}},\\n  doi={${p.doi || ""}},\\n  url={${p.url || ""}}\\n}`;
    }).join("\\n\\n");
    downloadBlob(`${project.name}-included.bib`.replace(/\\s+/g, "-").toLowerCase(), "text/plain;charset=utf-8", entries || "");
  }
  function exportReviewReport(project, stats) {
    if (!project) return;
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const lines = [
      `# Systematic Review: ${project.name}`,
      "",
      `Query: ${project.query || "—"}`,
      `Generated: ${new Date().toLocaleString()}`,
      "",
      `Total records: ${stats.total}`,
      `Included: ${stats.included}`,
      `Excluded: ${stats.excluded}`,
      `Maybe: ${stats.maybe}`,
      `Conflicts: ${stats.conflicts}`,
      "",
      "## Included papers",
      ...includedIds.map((id) => `- ${project.papers[id]?.title || "Untitled"} (${project.papers[id]?.year || "—"})`)
    ];
    downloadBlob(`${project.name}-report.md`.replace(/\\s+/g, "-").toLowerCase(), "text/markdown;charset=utf-8", lines.join("\\n"));
  }
  function exportPrismaSvg(stats, name) {
    const svg = renderPrismaDiagram(stats);
    const filename = `${(name || "review")}-prisma.svg`.replace(/\\s+/g, "-").toLowerCase();
    downloadBlob(filename, "image/svg+xml;charset=utf-8", svg);
  }
  async function checkReviewUpdates(project) {
    if (!project) return;
    const lastCheck = project.lastUpdateCheck ? new Date(project.lastUpdateCheck) : new Date(0);
    const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
    const seeds = includedIds.slice(0, 8);
    const newUpdates = [];
    for (const id of seeds) {
      const paper = project.papers[id];
      if (!paper?.title) continue;
      const work = await fetchOpenAlexWorkForPaper(paper, "");
      const related = Array.isArray(work?.relatedWorks) ? work.relatedWorks.slice(0, 10) : [];
      for (const rid of related) {
        const w = await fetchOpenAlexWorkById(rid);
        if (!w || !w.year) continue;
        const dateStr = w?.id ? null : null;
        const pubYear = Number(w.year) || 0;
        if (pubYear && pubYear >= lastCheck.getFullYear()) {
          newUpdates.push({ id: w.openalexId || rid, title: w.title, year: w.year, url: w.url || "" });
        }
      }
    }
    const existing = Array.isArray(project.updates) ? project.updates : [];
    const existingIds = new Set(existing.map((u) => u.id));
    project.updates = [...newUpdates.filter((u) => !existingIds.has(u.id)), ...existing].slice(0, 50);
    project.lastUpdateCheck = new Date().toISOString();
  }
  function renderReviewOverlay(project) {
    const overlay = ensureReviewOverlay();
    const content = overlay.querySelector("#su-review-content");
    const state = window.suReviewState;
    if (!content || !state || !project) return;
    normalizeReviewProject(project);
    const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
    if (reviewerSelect) {
      reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
      reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
    }
    const blindToggleTop = overlay.querySelector("#su-review-blind-toggle-top");
    if (blindToggleTop) blindToggleTop.checked = !!project.blindMode;
    overlay.querySelectorAll(".su-review-tab").forEach((btn) => {
      const isActive = btn.dataset.reviewTab === (state.tab || "overview");
      btn.classList.toggle("su-review-tab-active", isActive);
    });
    const reviewer = getActiveReviewer(project);
    const reviewerId = reviewer?.id || "you";
    const stats = computeReviewStats(project);
    const tab = state.tab || "overview";
    const weights = buildTermWeights(project);
    const decisionFilter = overlay.querySelector("#su-review-filter-status")?.value || "all";
    const searchFilter = String(overlay.querySelector("#su-review-filter-search")?.value || "").toLowerCase().trim();
    const tagFilterRaw = String(overlay.querySelector("#su-review-filter-tag")?.value || "").toLowerCase().trim();
    const tagFilters = tagFilterRaw ? tagFilterRaw.split(",").map((t) => t.trim()).filter(Boolean) : [];
    const conflictOnly = !!overlay.querySelector("#su-review-filter-conflict")?.checked;
    const sortMode = overlay.querySelector("#su-review-filter-sort")?.value || "score";
    const picoFilter = {
      p: String(overlay.querySelector("#su-review-filter-p")?.value || "").toLowerCase().trim(),
      i: String(overlay.querySelector("#su-review-filter-i")?.value || "").toLowerCase().trim(),
      c: String(overlay.querySelector("#su-review-filter-c")?.value || "").toLowerCase().trim(),
      o: String(overlay.querySelector("#su-review-filter-o")?.value || "").toLowerCase().trim()
    };
    if (tab === "overview") {
      const screened = stats.total - stats.unscreened;
      const pct = stats.total ? Math.round((screened / stats.total) * 100) : 0;
      const tagCounts = new Map();
      Object.values(project.tags || {}).forEach((list) => {
        (list || []).forEach((t) => {
          const key = String(t || "").trim();
          if (!key) return;
          tagCounts.set(key, (tagCounts.get(key) || 0) + 1);
        });
      });
      const topTags = Array.from(tagCounts.entries()).sort((a, b) => b[1] - a[1]).slice(0, 8);
      const reviewerStats = computeReviewerStats(project);
      const sortedWeights = Array.from(weights.entries()).sort((a, b) => b[1] - a[1]);
      const topPos = sortedWeights.filter(([, w]) => w > 0).slice(0, 6);
      const topNeg = sortedWeights.filter(([, w]) => w < 0).slice(0, 6);
      content.innerHTML = `
        <div class="su-review-stats">
          <div><strong>${stats.total}</strong> records</div>
          <div><strong>${stats.included}</strong> included</div>
          <div><strong>${stats.excluded}</strong> excluded</div>
          <div><strong>${stats.maybe}</strong> maybe</div>
          <div><strong>${stats.unscreened}</strong> unscreened</div>
          <div><strong>${stats.duplicates}</strong> duplicates</div>
          <div><strong>${stats.conflicts}</strong> conflicts</div>
        </div>
        <div class="su-review-progress">
          <div class="su-review-progress-bar"><span style="width:${pct}%"></span></div>
          <div class="su-review-progress-meta">${pct}% screened (${screened}/${stats.total || 0})</div>
        </div>
        <div class="su-review-overview-grid">
          <div class="su-review-card">
            <div class="su-review-card-title">Team activity</div>
            ${reviewerStats.length ? reviewerStats.map((r) => `
              <div class="su-review-card-row">
                <span>${escapeHtml(r.name || r.id)}</span>
                <span>${r.total} screens · ${r.include} include</span>
              </div>
            `).join("") : '<div class="su-review-muted">No reviewers yet.</div>'}
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Top tags</div>
            <div class="su-review-tag-cloud">
              ${topTags.length ? topTags.map(([tag, count]) => `<span class="su-review-tag-chip">${escapeHtml(tag)} <em>${count}</em></span>`).join("") : '<span class="su-review-muted">No tags yet.</span>'}
            </div>
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Active learning signals</div>
            <div class="su-review-signal-grid">
              <div>
                <div class="su-review-signal-label">Positive terms</div>
                ${topPos.length ? topPos.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>+${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more includes.</div>'}
              </div>
              <div>
                <div class="su-review-signal-label">Negative terms</div>
                ${topNeg.length ? topNeg.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more excludes.</div>'}
              </div>
            </div>
          </div>
        </div>
        <div class="su-review-help">
          <p>Workflow: import results → screen (include/exclude) → extract data → assess quality → export.</p>
          <p>Active learning uses your include/exclude decisions to prioritize similar papers.</p>
        </div>
      `;
      return;
    }
    if (tab === "screening") {
      const ids = Object.keys(project.papers || {});
      let rows = ids.map((id) => {
        const p = project.papers[id];
        const decision = normalizeDecisionEntry(project.decisions[id]);
        const status = getDecisionStatus(project, id, { blind: project.blindMode, reviewerId });
        const consensus = getDecisionStatus(project, id, { blind: false });
        const score = scorePaperForActiveLearning(p, weights);
        const vote = getReviewerVote(decision, reviewerId);
        const tags = Array.isArray(project.tags?.[id]) ? project.tags[id] : [];
        return { id, p, decision, status, consensus, score, vote, tags };
      });
      if (decisionFilter !== "all") {
        if (decisionFilter === "conflict") rows = rows.filter((r) => r.consensus === "conflict");
        else rows = rows.filter((r) => r.status === decisionFilter);
      }
      if (conflictOnly) rows = rows.filter((r) => r.consensus === "conflict");
      if (searchFilter) rows = rows.filter((r) => (r.p.title || "").toLowerCase().includes(searchFilter));
      if (tagFilters.length) rows = rows.filter((r) => {
        const tagSet = new Set((r.tags || []).map((t) => String(t || "").toLowerCase()));
        return tagFilters.some((t) => tagSet.has(t));
      });
      rows = rows.filter((r) => {
        const pico = project.pico?.[r.id] || {};
        const match = (field, val) => !val || String(field || "").toLowerCase().includes(val);
        return match(pico.population, picoFilter.p) && match(pico.intervention, picoFilter.i) && match(pico.comparator, picoFilter.c) && match(pico.outcome, picoFilter.o);
      });
      if (sortMode === "recent") {
        rows.sort((a, b) => (Number(b.p.year) || 0) - (Number(a.p.year) || 0));
      } else if (sortMode === "citations") {
        rows.sort((a, b) => (Number(b.p.citations) || 0) - (Number(a.p.citations) || 0));
      } else if (sortMode === "title") {
        rows.sort((a, b) => String(a.p.title || "").localeCompare(String(b.p.title || "")));
      } else {
        rows.sort((a, b) => b.score - a.score);
      }
      const limited = rows.slice(0, 80);
      content.innerHTML = `
        <div class="su-review-filters">
          <select id="su-review-filter-status">
            <option value="all">All</option>
            <option value="unscreened">Unscreened</option>
            <option value="include">Include</option>
            <option value="exclude">Exclude</option>
            <option value="maybe">Maybe</option>
            <option value="conflict">Conflict</option>
          </select>
          <select id="su-review-filter-sort">
            <option value="score">Sort: active learning</option>
            <option value="recent">Sort: recent</option>
            <option value="citations">Sort: citations</option>
            <option value="title">Sort: title</option>
          </select>
          <input id="su-review-filter-search" class="su-review-input" placeholder="Search title" />
          <input id="su-review-filter-tag" class="su-review-input" placeholder="Tags (comma-separated)" />
          <label class="su-review-filter-check"><input type="checkbox" id="su-review-filter-conflict" ${conflictOnly ? "checked" : ""} /> Conflicts</label>
          <input id="su-review-filter-p" class="su-review-input" placeholder="P: population" />
          <input id="su-review-filter-i" class="su-review-input" placeholder="I: intervention" />
          <input id="su-review-filter-c" class="su-review-input" placeholder="C: comparator" />
          <input id="su-review-filter-o" class="su-review-input" placeholder="O: outcome" />
        </div>
        <div class="su-review-actions">
          <button type="button" class="su-graph-btn" data-review-autoscreen="1">Auto-screen suggestions</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-decisions="1">Export decisions CSV</button>
        </div>
        <div class="su-review-table">
          ${limited.map((r) => {
            const decision = r.decision || {};
            const pico = project.pico[r.id] || {};
            const vote = r.vote || {};
            const suggestion = r.score >= 1.2 ? "include" : (r.score <= -1.2 ? "exclude" : "");
            const statusClass = `su-review-status-${r.status}`;
            return `
              <details class="su-review-row" data-review-id="${escapeHtml(r.id)}">
                <summary>
                  <span class="su-review-title">
                    ${r.p.url ? `<a href="${escapeHtml(r.p.url)}" target="_blank" rel="noopener">${escapeHtml(r.p.title || "Untitled")}</a>` : escapeHtml(r.p.title || "Untitled")}
                  </span>
                  <span class="su-review-meta">${r.p.year || "—"} · ${r.p.citations || 0} cites · score ${r.score}</span>
                  <span class="su-review-badges">
                    <span class="su-review-status ${statusClass}">${r.status}</span>
                    ${suggestion ? `<span class="su-review-suggest su-review-suggest-${suggestion}">AI: ${suggestion}</span>` : ""}
                    ${r.tags.length ? r.tags.map((t) => `<span class="su-review-tag-chip">${escapeHtml(t)}</span>`).join("") : ""}
                  </span>
                  <select class="su-review-decision" data-review-decision>
                    <option value="unscreened" ${r.status === "unscreened" ? "selected" : ""}>Unscreened</option>
                    <option value="include" ${r.status === "include" ? "selected" : ""}>Include</option>
                    <option value="exclude" ${r.status === "exclude" ? "selected" : ""}>Exclude</option>
                    <option value="maybe" ${r.status === "maybe" ? "selected" : ""}>Maybe</option>
                  </select>
                </summary>
                <div class="su-review-row-body">
                  <label>Reason <input type="text" class="su-review-input" data-review-reason value="${escapeHtml(vote?.reason || decision.reason || "")}"></label>
                  <div class="su-review-pico">
                    <input class="su-review-input" placeholder="Population" data-review-pico="population" value="${escapeHtml(pico.population || "")}" />
                    <input class="su-review-input" placeholder="Intervention" data-review-pico="intervention" value="${escapeHtml(pico.intervention || "")}" />
                    <input class="su-review-input" placeholder="Comparator" data-review-pico="comparator" value="${escapeHtml(pico.comparator || "")}" />
                    <input class="su-review-input" placeholder="Outcome" data-review-pico="outcome" value="${escapeHtml(pico.outcome || "")}" />
                  </div>
                  <div class="su-review-tags">
                    <div class="su-review-tag-list">
                      ${(r.tags || []).map((t, idx) => `<span class="su-review-tag-chip">${escapeHtml(t)} <button type="button" data-review-remove-tag="${idx}">×</button></span>`).join("")}
                    </div>
                    <div class="su-review-tag-input-row">
                      <input class="su-review-input su-review-tag-input" placeholder="Add tag" data-review-tag-input value="" />
                      <button type="button" class="${CLS_BTN_SEC}" data-review-add-tag="1">Add tag</button>
                    </div>
                  </div>
                  <label>Notes <textarea class="su-review-input su-review-notes" data-review-notes>${escapeHtml(project.notes[r.id] || "")}</textarea></label>
                  <div class="su-review-highlights">
                    <div class="su-review-highlights-header">
                      <span>PDF highlights</span>
                      ${r.p.pdfUrl ? `<a class="su-review-pdf-link" href="${escapeHtml(r.p.pdfUrl)}" target="_blank" rel="noopener">Open PDF</a>` : ""}
                    </div>
                    <div class="su-review-highlights-list">
                      ${(project.highlights?.[r.id] || []).map((h, hIdx) => `
                        <div class="su-review-highlight-item">
                          <div class="su-review-highlight-quote">“${escapeHtml(h.quote || "")}”</div>
                          <div class="su-review-highlight-meta">p.${escapeHtml(h.page || "")} · ${escapeHtml(h.note || "")}</div>
                          <button type="button" class="su-review-highlight-remove" data-review-remove-highlight="${hIdx}">Remove</button>
                        </div>
                      `).join("")}
                    </div>
                    <div class="su-review-highlight-form">
                      <input class="su-review-input su-review-highlight-page" placeholder="Page" data-review-highlight-page />
                      <input class="su-review-input su-review-highlight-quote" placeholder="Quote" data-review-highlight-quote />
                      <input class="su-review-input su-review-highlight-note" placeholder="Note" data-review-highlight-note />
                      <button type="button" class="su-graph-btn" data-review-add-highlight="1">Add highlight</button>
                    </div>
                  </div>
                  ${project.blindMode ? "" : `
                    <div class="su-review-votes">
                      <div class="su-review-votes-title">Votes</div>
                      ${(decision.votes || []).map((v) => {
                        const reviewerName = (project.reviewers || []).find((r2) => r2.id === v.reviewerId)?.name || v.reviewerId;
                        return `<div class="su-review-vote-item">${escapeHtml(reviewerName)}: ${escapeHtml(v.status || "")}</div>`;
                      }).join("") || '<div class="su-review-muted">No votes yet.</div>'}
                    </div>
                  `}
                </div>
              </details>
            `;
          }).join("")}
        </div>
      `;
      const statusSel = content.querySelector("#su-review-filter-status");
      if (statusSel) statusSel.value = decisionFilter;
      const sortSel = content.querySelector("#su-review-filter-sort");
      if (sortSel) sortSel.value = sortMode;
      const searchInput = content.querySelector("#su-review-filter-search");
      if (searchInput) searchInput.value = searchFilter;
      const tagInput = content.querySelector("#su-review-filter-tag");
      if (tagInput) tagInput.value = tagFilterRaw;
      const pInput = content.querySelector("#su-review-filter-p");
      const iInput = content.querySelector("#su-review-filter-i");
      const cInput = content.querySelector("#su-review-filter-c");
      const oInput = content.querySelector("#su-review-filter-o");
      if (pInput) pInput.value = picoFilter.p;
      if (iInput) iInput.value = picoFilter.i;
      if (cInput) cInput.value = picoFilter.c;
      if (oInput) oInput.value = picoFilter.o;
      const conflictCheck = content.querySelector("#su-review-filter-conflict");
      if (conflictCheck) conflictCheck.checked = conflictOnly;
      const inputs = content.querySelectorAll(".su-review-filters .su-review-input");
      inputs.forEach((input) => {
        input.addEventListener("input", () => renderReviewOverlay(project));
      });
      if (statusSel) statusSel.addEventListener("change", () => renderReviewOverlay(project));
      if (sortSel) sortSel.addEventListener("change", () => renderReviewOverlay(project));
      if (conflictCheck) conflictCheck.addEventListener("change", () => renderReviewOverlay(project));
      return;
    }
    if (tab === "dedupe") {
      const duplicates = Array.isArray(project.duplicates) ? project.duplicates : [];
      content.innerHTML = `
        <div class="su-review-help">
          <p>${duplicates.length} duplicate records detected via DOI/title fingerprinting.</p>
        </div>
        <div class="su-review-duplicates">
          ${duplicates.length ? duplicates.map((d, idx) => `
            <div class="su-review-duplicate-item" data-review-duplicate-index="${idx}">
              <div class="su-review-duplicate-title">${escapeHtml(d.paper?.title || "Untitled")}</div>
              <div class="su-review-duplicate-meta">Matches ${escapeHtml(d.fingerprint || "")}</div>
              <div class="su-review-duplicate-actions">
                <button type="button" class="${CLS_BTN_SEC}" data-review-add-duplicate="${idx}">Add anyway</button>
                <button type="button" class="su-graph-btn" data-review-dismiss-duplicate="${idx}">Dismiss</button>
              </div>
            </div>
          `).join("") : '<div class="su-review-muted">No duplicates detected.</div>'}
        </div>
      `;
      return;
    }
    if (tab === "extraction") {
      const fields = getExtractionFields(project);
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      content.innerHTML = `
        <div class="su-review-extract-controls">
          <input id="su-review-field-name" class="su-review-input" placeholder="Add field (e.g., Sample Size)" />
          <select id="su-review-field-type" class="su-review-input">
            <option value="text">Text</option>
            <option value="number">Number</option>
            <option value="select">Select</option>
          </select>
          <input id="su-review-field-options" class="su-review-input" placeholder="Options (comma-separated)" />
          <button type="button" class="su-graph-btn" data-review-add-field="1">Add field</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-extraction="1">Export extraction CSV</button>
        </div>
        <div class="su-review-extract-table">
          <table>
            <thead>
              <tr>
                <th>Paper</th>
                ${fields.map((f) => `<th>${escapeHtml(f.label)}</th>`).join("")}
              </tr>
            </thead>
            <tbody>
              ${includedIds.map((id) => {
                const p = project.papers[id];
                const data = project.extraction[id] || {};
                return `<tr data-review-id="${escapeHtml(id)}">
                  <td class="su-review-extract-title">${escapeHtml(p?.title || "Untitled")}</td>
                  ${fields.map((f) => {
                    const value = data[f.key] || "";
                    if (f.type === "select") {
                      return `<td><select class="su-review-input" data-review-extract="${escapeHtml(f.key)}">
                        <option value=""></option>
                        ${f.options.map((opt) => `<option value="${escapeHtml(opt)}" ${String(opt) === String(value) ? "selected" : ""}>${escapeHtml(opt)}</option>`).join("")}
                      </select></td>`;
                    }
                    const inputType = f.type === "number" ? "number" : "text";
                    return `<td><input class="su-review-input" type="${inputType}" data-review-extract="${escapeHtml(f.key)}" value="${escapeHtml(value)}" /></td>`;
                  }).join("")}
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
      `;
      return;
    }
    if (tab === "quality") {
      const checklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      content.innerHTML = `
        <div class="su-review-quality-controls">
          <input id="su-review-quality-field" class="su-review-input" placeholder="Add checklist item" />
          <button type="button" class="su-graph-btn" data-review-add-quality="1">Add item</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-quality="1">Export quality CSV</button>
        </div>
        <div class="su-review-quality-table">
          <table>
            <thead>
              <tr>
                <th>Paper</th>
                <th>Risk</th>
                ${checklist.map((c) => `<th>${escapeHtml(c.label)}</th>`).join("")}
                <th>Notes</th>
              </tr>
            </thead>
            <tbody>
              ${includedIds.map((id) => {
                const p = project.papers[id];
                const q = project.quality[id] || {};
                const checks = q.checks || {};
                return `<tr data-review-id="${escapeHtml(id)}">
                  <td class="su-review-extract-title">${escapeHtml(p?.title || "Untitled")}</td>
                  <td>
                    <select class="su-review-input" data-review-quality-risk>
                      <option value=""></option>
                      <option value="low" ${q.risk === "low" ? "selected" : ""}>Low</option>
                      <option value="some" ${q.risk === "some" ? "selected" : ""}>Some</option>
                      <option value="high" ${q.risk === "high" ? "selected" : ""}>High</option>
                    </select>
                  </td>
                  ${checklist.map((c) => `<td><input type="checkbox" data-review-quality-check="${escapeHtml(c.key)}" ${checks[c.key] ? "checked" : ""} /></td>`).join("")}
                  <td><input class="su-review-input" data-review-quality-notes value="${escapeHtml(q.notes || "")}" /></td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
        </div>
      `;
      return;
    }
    if (tab === "prisma") {
      content.innerHTML = `
        <div class="su-review-prisma-wrap">
          ${renderPrismaDiagram(stats)}
        </div>
        <div class="su-review-actions">
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-prisma="1">Download PRISMA SVG</button>
        </div>
      `;
      return;
    }
    if (tab === "updates") {
      const updates = Array.isArray(project.updates) ? project.updates : [];
      content.innerHTML = `
        <div class="su-review-updates">
          <div class="su-review-updates-meta">Last check: ${project.lastUpdateCheck ? new Date(project.lastUpdateCheck).toLocaleString() : "Never"}</div>
          <button type="button" class="su-graph-btn" data-review-check-updates="1">Check updates</button>
          <div class="su-review-updates-list">
            ${updates.length ? updates.map((u) => `<div class="su-review-update-item">${escapeHtml(u.title || "Untitled")} <span>${u.year || ""}</span></div>`).join("") : '<div class="su-graph-empty">No updates yet.</div>'}
          </div>
        </div>
      `;
      return;
    }
    if (tab === "insights") {
      const screened = stats.total - stats.unscreened;
      const includeRate = screened ? stats.included / screened : 0;
      const projected = Math.round(includeRate * stats.unscreened);
      const scenarios = [25, 50, 100].map((n) => ({ n, expected: Math.round(includeRate * n) }));
      const sortedWeights = Array.from(weights.entries()).sort((a, b) => b[1] - a[1]);
      const topPos = sortedWeights.filter(([, w]) => w > 0).slice(0, 10);
      const topNeg = sortedWeights.filter(([, w]) => w < 0).slice(0, 10);
      content.innerHTML = `
        <div class="su-review-card">
          <div class="su-review-card-title">Screening simulation</div>
          <div class="su-review-card-row">Current include rate: <strong>${Math.round(includeRate * 100)}%</strong></div>
          <div class="su-review-card-row">Projected includes remaining: <strong>${projected}</strong></div>
          <div class="su-review-sim-grid">
            ${scenarios.map((s) => `<div class="su-review-sim-item">Next ${s.n} screens → ~${s.expected} includes</div>`).join("")}
          </div>
        </div>
        <div class="su-review-signal-grid">
          <div class="su-review-card">
            <div class="su-review-card-title">Include signals</div>
            ${topPos.length ? topPos.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>+${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more include/exclude decisions.</div>'}
          </div>
          <div class="su-review-card">
            <div class="su-review-card-title">Exclude signals</div>
            ${topNeg.length ? topNeg.map(([t, w]) => `<div class="su-review-signal-item">${escapeHtml(t)} <span>${w.toFixed(2)}</span></div>`).join("") : '<div class="su-review-muted">Need more include/exclude decisions.</div>'}
          </div>
        </div>
      `;
      return;
    }
    if (tab === "team") {
      const reviewerStats = computeReviewerStats(project);
      const conflicts = getConflictPapers(project);
      content.innerHTML = `
        <div class="su-review-team-controls">
          <input id="su-review-reviewer-name" class="su-review-input" placeholder="Add reviewer name" />
          <button type="button" class="su-graph-btn" data-review-add-reviewer="1">Add reviewer</button>
          <label class="su-review-filter-check"><input type="checkbox" id="su-review-blind-toggle" ${project.blindMode ? "checked" : ""} /> Blind screening</label>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-decisions="1">Export team CSV</button>
        </div>
        <div class="su-review-team-list">
          ${reviewerStats.length ? reviewerStats.map((r) => `
            <div class="su-review-team-item">
              <strong>${escapeHtml(r.name || r.id)}</strong>
              <span>${r.total} screens · ${r.include} include · ${r.exclude} exclude</span>
            </div>
          `).join("") : '<div class="su-review-muted">No reviewers yet.</div>'}
        </div>
        <div class="su-review-conflicts">
          <div class="su-review-card-title">Conflicts</div>
          ${conflicts.length ? conflicts.map((c) => `
            <div class="su-review-conflict-item" data-review-id="${escapeHtml(c.id)}">
              <div class="su-review-conflict-title">${escapeHtml(c.paper?.title || "Untitled")}</div>
              <div class="su-review-conflict-votes">
                ${c.votes.map((v) => {
                  const name = (project.reviewers || []).find((r) => r.id === v.reviewerId)?.name || v.reviewerId;
                  return `${escapeHtml(name)}: ${escapeHtml(v.status || "")}`;
                }).join(" · ")}
              </div>
              <select class="su-review-input" data-review-conflict-resolve>
                <option value="">Resolve…</option>
                <option value="include">Include</option>
                <option value="exclude">Exclude</option>
                <option value="maybe">Maybe</option>
              </select>
            </div>
          `).join("") : '<div class="su-review-muted">No conflicts detected.</div>'}
        </div>
      `;
      return;
    }
    if (tab === "report") {
      const includedIds = Object.keys(project.papers || {}).filter((id) => getDecisionStatus(project, id, { blind: false }) === "include");
      const lines = [
        `# Systematic Review: ${project.name}`,
        "",
        `Query: ${project.query || "—"}`,
        `Generated: ${new Date().toLocaleString()}`,
        "",
        `Total records: ${stats.total}`,
        `Included: ${stats.included}`,
        `Excluded: ${stats.excluded}`,
        `Maybe: ${stats.maybe}`,
        `Conflicts: ${stats.conflicts}`,
        "",
        "## Included papers",
        ...includedIds.map((id) => `- ${project.papers[id]?.title || "Untitled"} (${project.papers[id]?.year || "—"})`)
      ];
      const reportText = lines.join("\n");
      content.innerHTML = `
        <textarea class="su-review-report" readonly></textarea>
        <div class="su-review-actions">
          <button type="button" class="su-graph-btn" data-review-copy-report="1">Copy report</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-report="1">Download report</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export-bib="1">Export BibTeX</button>
        </div>
      `;
      const reviewReportEl = content.querySelector(".su-review-report");
      if (reviewReportEl) reviewReportEl.value = reportText;
      return;
    }
  }
  function ensureReviewOverlay() {
    let overlay = document.getElementById("su-review-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "su-review-overlay";
    overlay.className = "su-review-overlay";
    overlay.innerHTML = `
      <div class="su-review-backdrop" data-review-close="1"></div>
      <div class="su-review-panel">
        <div class="su-review-header">
          <div>
            <div class="su-review-title">Systematic Review Workspace</div>
            <div class="su-review-subtitle">Local screening, dedupe, extraction, quality, PRISMA, and updates</div>
          </div>
          <button type="button" class="${CLS_BTN_SEC}" data-review-close="1">Close</button>
        </div>
        <div class="su-review-project-bar">
          <select id="su-review-project-select"></select>
          <select id="su-review-reviewer-select" title="Active reviewer"></select>
          <button type="button" class="su-graph-btn" data-review-new="1">New</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-rename="1">Rename</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-add-page="1">Add page results</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-export="1">Export JSON</button>
          <button type="button" class="${CLS_BTN_SEC}" data-review-import="1">Import JSON</button>
          <label class="su-review-blind-toggle"><input type="checkbox" id="su-review-blind-toggle-top" /> Blind</label>
          <input type="file" id="su-review-import-file" accept="application/json" style="display:none" />
        </div>
        <div class="su-review-tabs">
          <button type="button" class="su-review-tab" data-review-tab="overview">Overview</button>
          <button type="button" class="su-review-tab" data-review-tab="screening">Screening</button>
          <button type="button" class="su-review-tab" data-review-tab="dedupe">Dedupe</button>
          <button type="button" class="su-review-tab" data-review-tab="extraction">Extraction</button>
          <button type="button" class="su-review-tab" data-review-tab="quality">Quality</button>
          <button type="button" class="su-review-tab" data-review-tab="prisma">PRISMA</button>
          <button type="button" class="su-review-tab" data-review-tab="updates">Updates</button>
          <button type="button" class="su-review-tab" data-review-tab="insights">Insights</button>
          <button type="button" class="su-review-tab" data-review-tab="report">Report</button>
          <button type="button" class="su-review-tab" data-review-tab="team">Team</button>
        </div>
        <div id="su-review-content" class="su-review-content"></div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", async (e) => {
      const close = e.target.closest("[data-review-close]");
      if (close) {
        overlay.classList.remove("su-visible");
        return;
      }
      const tab = e.target.closest("[data-review-tab]");
      if (tab) {
        const state = await ensureReviewState();
        state.tab = String(tab.dataset.reviewTab || "overview");
        renderReviewOverlay(getActiveReviewProject());
        return;
      }
      const newBtn = e.target.closest("[data-review-new]");
      if (newBtn) {
        const state = await ensureReviewState();
        const name = prompt("Project name?", "New review");
        if (!name) return;
        const project = createReviewProject(name, getScholarSearchQuery() || "");
        state.projects[project.id] = project;
        state.activeId = project.id;
        await setReviewProjects(state.projects);
        renderReviewOverlay(project);
        return;
      }
      const renameBtn = e.target.closest("[data-review-rename]");
      if (renameBtn) {
        const project = getActiveReviewProject();
        if (!project) return;
        const name = prompt("Rename project", project.name);
        if (!name) return;
        project.name = name;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addPage = e.target.closest("[data-review-add-page]");
      if (addPage) {
        const project = getActiveReviewProject();
        if (!project) return;
        const { results, isAuthorProfile } = scanResults();
        const papers = results.map((row) => extractReviewPaperFromResult(row, isAuthorProfile)).filter(Boolean);
        addPapersToProject(project, papers);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const exportBtn = e.target.closest("[data-review-export]");
      if (exportBtn) {
        const project = getActiveReviewProject();
        if (!project) return;
        const json = JSON.stringify(project, null, 2);
        downloadBlob(`${project.name.replace(/[^a-z0-9]+/gi, "-").toLowerCase()}-review.json`, "application/json", json);
        return;
      }
      const importBtn = e.target.closest("[data-review-import]");
      if (importBtn) {
        const fileInput = overlay.querySelector("#su-review-import-file");
        if (fileInput) fileInput.click();
        return;
      }
      const addField = e.target.closest("[data-review-add-field]");
      if (addField) {
        const project = getActiveReviewProject();
        const fieldInput = overlay.querySelector("#su-review-field-name");
        const value = String(fieldInput?.value || "").trim();
        if (!project || !value) return;
        project.extractionFields = Array.isArray(project.extractionFields) ? project.extractionFields : [];
        const typeSelect = overlay.querySelector("#su-review-field-type");
        const optionsInput = overlay.querySelector("#su-review-field-options");
        const type = String(typeSelect?.value || "text");
        const options = type === "select"
          ? String(optionsInput?.value || "").split(",").map((s) => s.trim()).filter(Boolean)
          : [];
        project.extractionFields.push({ label: value, type, options });
        if (fieldInput) fieldInput.value = "";
        if (optionsInput) optionsInput.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const checkUpdates = e.target.closest("[data-review-check-updates]");
      if (checkUpdates) {
        const project = getActiveReviewProject();
        if (!project) return;
        await checkReviewUpdates(project);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const copyReport = e.target.closest("[data-review-copy-report]");
      if (copyReport) {
        const textarea = overlay.querySelector(".su-review-report");
        if (textarea) {
          textarea.select();
          document.execCommand("copy");
        }
        return;
      }
      const exportReport = e.target.closest("[data-review-export-report]");
      if (exportReport) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewReport(project, computeReviewStats(project));
        return;
      }
      const exportBib = e.target.closest("[data-review-export-bib]");
      if (exportBib) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewBibTeX(project);
        return;
      }
      const exportDecisions = e.target.closest("[data-review-export-decisions]");
      if (exportDecisions) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewDecisionsCsv(project);
        return;
      }
      const exportExtraction = e.target.closest("[data-review-export-extraction]");
      if (exportExtraction) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewExtractionCsv(project);
        return;
      }
      const exportQuality = e.target.closest("[data-review-export-quality]");
      if (exportQuality) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportReviewQualityCsv(project);
        return;
      }
      const exportPrisma = e.target.closest("[data-review-export-prisma]");
      if (exportPrisma) {
        const project = getActiveReviewProject();
        if (!project) return;
        exportPrismaSvg(computeReviewStats(project), project.name);
        return;
      }
      const autoScreen = e.target.closest("[data-review-autoscreen]");
      if (autoScreen) {
        const project = getActiveReviewProject();
        if (!project) return;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        const weights = buildTermWeights(project);
        const candidates = [];
        for (const id of Object.keys(project.papers || {})) {
          const status = getDecisionStatus(project, id, { blind: project.blindMode, reviewerId });
          if (status !== "unscreened") continue;
          const score = scorePaperForActiveLearning(project.papers[id], weights);
          const suggestion = score >= 1.2 ? "include" : (score <= -1.2 ? "exclude" : "");
          if (!suggestion) continue;
          candidates.push({ id, suggestion });
        }
        const toApply = candidates.slice(0, 30);
        if (!toApply.length) return;
        if (!window.confirm(`Auto-screen ${toApply.length} papers based on active-learning suggestions?`)) return;
        toApply.forEach((c) => {
          upsertReviewerVote(project, c.id, reviewerId, c.suggestion, "");
        });
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addReviewer = e.target.closest("[data-review-add-reviewer]");
      if (addReviewer) {
        const project = getActiveReviewProject();
        if (!project) return;
        const input = overlay.querySelector("#su-review-reviewer-name");
        const name = String(input?.value || "").trim();
        if (!name) return;
        const id = `rev_${Date.now().toString(36)}`;
        project.reviewers = Array.isArray(project.reviewers) ? project.reviewers : [];
        project.reviewers.push({ id, name });
        project.activeReviewerId = id;
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addTag = e.target.closest("[data-review-add-tag]");
      if (addTag) {
        const row = addTag.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const input = row.querySelector("[data-review-tag-input]");
        const value = String(input?.value || "").trim();
        if (!value) return;
        project.tags[id] = Array.isArray(project.tags[id]) ? project.tags[id] : [];
        if (!project.tags[id].includes(value)) project.tags[id].push(value);
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const removeTagBtn = e.target.closest("[data-review-remove-tag]");
      if (removeTagBtn) {
        const row = removeTagBtn.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const idx = parseInt(removeTagBtn.getAttribute("data-review-remove-tag") || "-1", 10);
        if (!Array.isArray(project.tags[id])) return;
        project.tags[id].splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addHighlight = e.target.closest("[data-review-add-highlight]");
      if (addHighlight) {
        const row = addHighlight.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const pageInput = row.querySelector("[data-review-highlight-page]");
        const quoteInput = row.querySelector("[data-review-highlight-quote]");
        const noteInput = row.querySelector("[data-review-highlight-note]");
        const quote = String(quoteInput?.value || "").trim();
        if (!quote) return;
        const page = String(pageInput?.value || "").trim();
        const note = String(noteInput?.value || "").trim();
        project.highlights[id] = Array.isArray(project.highlights[id]) ? project.highlights[id] : [];
        project.highlights[id].push({ quote, page, note, createdAt: new Date().toISOString() });
        if (pageInput) pageInput.value = "";
        if (quoteInput) quoteInput.value = "";
        if (noteInput) noteInput.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const removeHighlight = e.target.closest("[data-review-remove-highlight]");
      if (removeHighlight) {
        const row = removeHighlight.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const idx = parseInt(removeHighlight.getAttribute("data-review-remove-highlight") || "-1", 10);
        if (!Array.isArray(project.highlights[id])) return;
        project.highlights[id].splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addDuplicate = e.target.closest("[data-review-add-duplicate]");
      if (addDuplicate) {
        const project = getActiveReviewProject();
        if (!project) return;
        const idx = parseInt(addDuplicate.getAttribute("data-review-add-duplicate") || "-1", 10);
        const dup = Array.isArray(project.duplicates) ? project.duplicates[idx] : null;
        if (!dup || !dup.paper) return;
        const newId = `${dup.paper.id || dup.paper.title || "dup"}_${Date.now().toString(36)}`;
        project.papers[newId] = { ...dup.paper, id: newId, duplicateOf: dup.existingId || "" };
        project.duplicates.splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const dismissDuplicate = e.target.closest("[data-review-dismiss-duplicate]");
      if (dismissDuplicate) {
        const project = getActiveReviewProject();
        if (!project) return;
        const idx = parseInt(dismissDuplicate.getAttribute("data-review-dismiss-duplicate") || "-1", 10);
        if (Array.isArray(project.duplicates)) project.duplicates.splice(idx, 1);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const addQuality = e.target.closest("[data-review-add-quality]");
      if (addQuality) {
        const project = getActiveReviewProject();
        if (!project) return;
        const input = overlay.querySelector("#su-review-quality-field");
        const label = String(input?.value || "").trim();
        if (!label) return;
        const key = label.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
        project.qualityChecklist = Array.isArray(project.qualityChecklist) ? project.qualityChecklist : [];
        project.qualityChecklist.push({ key, label });
        if (input) input.value = "";
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
    });
    overlay.addEventListener("change", async (e) => {
      const select = e.target.closest("#su-review-project-select");
      if (select) {
        const state = await ensureReviewState();
        state.activeId = select.value;
        const project = getActiveReviewProject();
        const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
        if (reviewerSelect && project) {
          reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
          reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
        }
        const blindToggle = overlay.querySelector("#su-review-blind-toggle-top");
        if (blindToggle) blindToggle.checked = !!project?.blindMode;
        renderReviewOverlay(project);
        return;
      }
      const reviewerSelect = e.target.closest("#su-review-reviewer-select");
      if (reviewerSelect) {
        const project = getActiveReviewProject();
        if (!project) return;
        project.activeReviewerId = reviewerSelect.value;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const blindToggle = e.target.closest("#su-review-blind-toggle-top, #su-review-blind-toggle");
      if (blindToggle) {
        const project = getActiveReviewProject();
        if (!project) return;
        project.blindMode = !!blindToggle.checked;
        const topToggle = overlay.querySelector("#su-review-blind-toggle-top");
        if (topToggle && topToggle !== blindToggle) topToggle.checked = !!project.blindMode;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const fileInput = e.target.closest("#su-review-import-file");
      if (fileInput && fileInput.files?.length) {
        const file = fileInput.files[0];
        const text = await file.text();
        try {
          const project = JSON.parse(text);
          if (project?.id) {
            normalizeReviewProject(project);
            const state = await ensureReviewState();
            state.projects[project.id] = project;
            state.activeId = project.id;
            await setReviewProjects(state.projects);
            renderReviewOverlay(project);
          }
        } catch {}
        fileInput.value = "";
        return;
      }
      const decision = e.target.closest("[data-review-decision]");
      if (decision) {
        const row = decision.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        upsertReviewerVote(project, id, reviewerId, decision.value, null);
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const conflictResolve = e.target.closest("[data-review-conflict-resolve]");
      if (conflictResolve) {
        const row = conflictResolve.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const decision = normalizeDecisionEntry(project.decisions[id]);
        decision.overrideStatus = conflictResolve.value || "";
        project.decisions[id] = decision;
        await saveReviewProject(project);
        renderReviewOverlay(project);
        return;
      }
      const extractInput = e.target.closest("[data-review-extract]");
      if (extractInput) {
        const row = extractInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = extractInput.dataset.reviewExtract;
        project.extraction[id] = project.extraction[id] || {};
        project.extraction[id][key] = extractInput.value;
        await saveReviewProject(project);
        return;
      }
      const qualityRisk = e.target.closest("[data-review-quality-risk]");
      if (qualityRisk) {
        const row = qualityRisk.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.quality[id] = project.quality[id] || {};
        project.quality[id].risk = qualityRisk.value;
        await saveReviewProject(project);
        return;
      }
      const qualityCheck = e.target.closest("[data-review-quality-check]");
      if (qualityCheck) {
        const row = qualityCheck.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = qualityCheck.getAttribute("data-review-quality-check");
        project.quality[id] = project.quality[id] || {};
        project.quality[id].checks = project.quality[id].checks || {};
        project.quality[id].checks[key] = !!qualityCheck.checked;
        await saveReviewProject(project);
        return;
      }
    });
    overlay.addEventListener("input", async (e) => {
      const reason = e.target.closest("[data-review-reason]");
      if (reason) {
        const row = reason.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const reviewer = getActiveReviewer(project);
        const reviewerId = reviewer?.id || "you";
        upsertReviewerVote(project, id, reviewerId, null, reason.value);
        await saveReviewProject(project);
        return;
      }
      const picoInput = e.target.closest("[data-review-pico]");
      if (picoInput) {
        const row = picoInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        const key = picoInput.dataset.reviewPico;
        project.pico[id] = project.pico[id] || {};
        project.pico[id][key] = picoInput.value;
        await saveReviewProject(project);
        return;
      }
      const notesInput = e.target.closest("[data-review-notes]");
      if (notesInput) {
        const row = notesInput.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.notes[id] = notesInput.value;
        await saveReviewProject(project);
        return;
      }
      const qualityNotes = e.target.closest("[data-review-quality-notes]");
      if (qualityNotes) {
        const row = qualityNotes.closest("[data-review-id]");
        const project = getActiveReviewProject();
        if (!row || !project) return;
        const id = row.dataset.reviewId;
        project.quality[id] = project.quality[id] || {};
        project.quality[id].notes = qualityNotes.value;
        await saveReviewProject(project);
        return;
      }
    });
    return overlay;
  }
  async function openReviewOverlay() {
    await ensureReviewState();
    const overlay = ensureReviewOverlay();
    const state = window.suReviewState;
    const select = overlay.querySelector("#su-review-project-select");
    if (select) {
      select.innerHTML = Object.values(state.projects).map((p) => `<option value="${escapeHtml(p.id)}">${escapeHtml(p.name)}</option>`).join("");
      select.value = state.activeId;
    }
    const project = getActiveReviewProject();
    const reviewerSelect = overlay.querySelector("#su-review-reviewer-select");
    if (reviewerSelect && project) {
      reviewerSelect.innerHTML = (project.reviewers || []).map((r) => `<option value="${escapeHtml(r.id)}">${escapeHtml(r.name || r.id)}</option>`).join("");
      reviewerSelect.value = project.activeReviewerId || (project.reviewers?.[0]?.id || "you");
    }
    const blindToggle = overlay.querySelector("#su-review-blind-toggle-top");
    if (blindToggle) blindToggle.checked = !!project?.blindMode;
    overlay.classList.add("su-visible");
    renderReviewOverlay(getActiveReviewProject());
  }

  return { open: openReviewOverlay };
}
