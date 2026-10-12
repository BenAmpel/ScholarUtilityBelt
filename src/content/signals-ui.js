/**
 * DOM side of three Pro features on Google Scholar result pages:
 *   claimEvidenceChips  small hedged chips under a snippet (src/common/claim-signals.js)
 *   reproSignals        "signals found" chips for code, data, preregistration (src/common/repro-signals.js)
 *   readingOrder        toolbar control that re-orders results by the user's own terms (src/common/reading-order.js)
 *
 * Everything here is local: it reads the rows already on the page and never
 * makes a request. Styling is by class only (content.css), no inline styles.
 * Loaded lazily by content.js through importModuleWithRetry.
 */
import { analyzeClaims, DEFAULT_MIN_N } from "../common/claim-signals.js";
import { detectReproSignals, reproChips, SIGNALS_NOTE } from "../common/repro-signals.js";
import { parseTerms, rankRows, hasTerms, termRegex } from "../common/reading-order.js";

const CLAIM_CLASS = "su-claim-chips";
const REPRO_CLASS = "su-repro-signals";
const WHY_CLASS = "su-ro-why";
const MARK_CLASS = "su-ro-mark";
const IDX_ATTR = "data-su-ro-idx";
// Our own and other extension badges are not part of Scholar's row content.
const OWN_UI_SELECTOR = `.${CLAIM_CLASS}, .${REPRO_CLASS}, .su-external, .su-code-badge, .su-btnbar, .su-artifact-badge`;

export const SIGNAL_FEATURE_IDS = ["claimEvidenceChips", "reproSignals", "readingOrder"];

// ---- Per-row chips ---------------------------------------------------------

function snippetOf(row) {
  const el = row.querySelector(".gs_rs");
  return el ? (el.textContent || "").trim() : "";
}

function collectLinks(row) {
  const links = [];
  for (const a of row.querySelectorAll("a[href]")) {
    if (a.closest(OWN_UI_SELECTOR)) continue;
    const href = a.getAttribute("href") || "";
    if (!/^https?:/i.test(href)) continue;
    let role = "other";
    if (a.closest(".gs_rt")) role = "title";
    else if (a.closest(".gs_or_ggsm, .gs_ggs, .gs_ggsd")) role = "file";
    links.push({ href, role });
  }
  return links;
}

function makeChip(chip, extraClass) {
  const el = document.createElement(chip.url ? "a" : "span");
  el.className = `su-signal-chip ${extraClass}`;
  el.textContent = chip.label;
  el.title = chip.tip;
  el.setAttribute("aria-label", `${chip.label}. ${chip.tip}`);
  if (chip.url) {
    el.href = chip.url;
    el.target = "_blank";
    el.rel = "noopener noreferrer";
  } else {
    el.tabIndex = 0;
  }
  return el;
}

function insertAfterSnippet(row, node) {
  const snippet = row.querySelector(".gs_rs");
  if (snippet && snippet.parentNode) {
    // Keep chips in a stable order if both groups are present: claim chips first, then repro.
    const anchor = node.classList.contains(REPRO_CLASS) ? row.querySelector(`.${CLAIM_CLASS}`) || snippet : snippet;
    anchor.after(node);
    return;
  }
  (row.querySelector(".gs_ri") || row).appendChild(node);
}

export function clearRowSignals(row) {
  row.querySelectorAll(`.${CLAIM_CLASS}, .${REPRO_CLASS}`).forEach((el) => el.remove());
}

/**
 * Add or refresh the claim and reproducibility chips on one search-result row.
 * `allowed` says which Pro features the user may use; `settings` says which are switched on.
 */
export function renderRowSignals(row, { settings = {}, allowed = {}, ownDoi = "" } = {}) {
  clearRowSignals(row);
  if (!row || !row.querySelector) return;
  const snippet = snippetOf(row);
  if (settings.showClaimEvidenceChips !== false && allowed.claimEvidenceChips && snippet) {
    const chips = analyzeClaims(snippet, { minN: settings.claimChipMinN ?? DEFAULT_MIN_N });
    if (chips.length) {
      const wrap = document.createElement("div");
      wrap.className = CLAIM_CLASS;
      wrap.setAttribute("role", "group");
      wrap.setAttribute("aria-label", "Claim-evidence chips, judged from the snippet only");
      for (const c of chips) wrap.appendChild(makeChip(c, "su-claim-chip"));
      insertAfterSnippet(row, wrap);
    }
  }
  if (settings.showReproSignals !== false && allowed.reproSignals) {
    const chips = reproChips(detectReproSignals({ text: snippet, links: collectLinks(row), ownDoi }));
    if (chips.length) {
      const wrap = document.createElement("div");
      wrap.className = REPRO_CLASS;
      wrap.setAttribute("role", "group");
      wrap.setAttribute("aria-label", "Signals found");
      wrap.title = SIGNALS_NOTE;
      const lead = document.createElement("span");
      lead.className = "su-repro-lead";
      lead.textContent = "Signals found:";
      wrap.appendChild(lead);
      for (const c of chips) wrap.appendChild(makeChip(c, "su-repro-chip"));
      insertAfterSnippet(row, wrap);
    }
  }
}

// ---- Reading order ---------------------------------------------------------

function resultsContainer() {
  return document.querySelector("#gs_res_ccl_mid");
}

function topLevelRows(container) {
  return Array.from(container.children).filter((el) => el.classList && el.classList.contains("gs_r"));
}

/** Give every row an index for Scholar's original order. Rows that appear later get the next free numbers. */
function stampOriginalOrder(rows) {
  let max = -1;
  for (const r of rows) {
    const v = parseInt(r.getAttribute(IDX_ATTR), 10);
    if (Number.isFinite(v) && v > max) max = v;
  }
  for (const r of rows) {
    if (!Number.isFinite(parseInt(r.getAttribute(IDX_ATTR), 10))) r.setAttribute(IDX_ATTR, String(++max));
  }
}

function byOriginalIndex(rows) {
  return rows.slice().sort((a, b) => parseInt(a.getAttribute(IDX_ATTR), 10) - parseInt(b.getAttribute(IDX_ATTR), 10));
}

function placeRows(container, rows) {
  // Rows are placed before whatever followed the last row, so non-result children keep their place.
  const all = topLevelRows(container);
  const last = all[all.length - 1];
  const anchor = last ? last.nextSibling : null;
  for (const r of rows) container.insertBefore(r, anchor);
}

// NodeFilter constants (SHOW_TEXT = 4, FILTER_ACCEPT = 1, FILTER_REJECT = 2), inlined so the module needs no extra global.
const SHOW_TEXT = 4;
const FILTER_ACCEPT = 1;
const FILTER_REJECT = 2;

function textNodesIn(root) {
  const out = [];
  const walker = document.createTreeWalker(root, SHOW_TEXT, {
    acceptNode(node) {
      if (!node.nodeValue || !node.nodeValue.trim()) return FILTER_REJECT;
      const p = node.parentElement;
      if (p && p.closest(`.${MARK_CLASS}, .su-external, .su-btnbar`)) return FILTER_REJECT;
      return FILTER_ACCEPT;
    },
  });
  let n;
  while ((n = walker.nextNode())) out.push(n);
  return out;
}

function markTerms(root, terms) {
  if (!root || !terms.length) return;
  const matchers = terms.map((t) => ({ ...t, re: termRegex(t.term, "gi") })).filter((t) => t.re);
  for (const node of textNodesIn(root)) {
    const value = node.nodeValue;
    const ranges = [];
    for (const m of matchers) {
      m.re.lastIndex = 0;
      let hit;
      while ((hit = m.re.exec(value)) !== null) {
        if (!hit[0]) { m.re.lastIndex++; continue; }
        ranges.push({ start: hit.index, end: hit.index + hit[0].length, exclude: m.exclude });
      }
    }
    if (!ranges.length) continue;
    ranges.sort((a, b) => a.start - b.start || b.end - a.end);
    const frag = document.createDocumentFragment();
    let pos = 0;
    for (const r of ranges) {
      if (r.start < pos) continue; // overlaps an earlier match
      if (r.start > pos) frag.appendChild(document.createTextNode(value.slice(pos, r.start)));
      const mark = document.createElement("mark");
      mark.className = r.exclude ? `${MARK_CLASS} su-ro-mark-ex` : MARK_CLASS;
      mark.textContent = value.slice(r.start, r.end);
      frag.appendChild(mark);
      pos = r.end;
    }
    if (pos < value.length) frag.appendChild(document.createTextNode(value.slice(pos)));
    node.parentNode.replaceChild(frag, node);
  }
}

function clearRowOrderMarks(row) {
  row.querySelectorAll(`.${WHY_CLASS}`).forEach((el) => el.remove());
  const parents = new Set();
  row.querySelectorAll(`mark.${MARK_CLASS}`).forEach((mark) => {
    const parent = mark.parentNode;
    parent.replaceChild(document.createTextNode(mark.textContent), mark);
    parents.add(parent);
  });
  parents.forEach((p) => p.normalize());
}

function addWhy(row, position, total, why) {
  const el = document.createElement("span");
  el.className = WHY_CLASS;
  el.tabIndex = 0;
  el.textContent = `Order ${position} of ${total}`;
  el.title = `Why this order. ${why} Reading order is local and only moves results; none are hidden.`;
  el.setAttribute("aria-label", el.title);
  const snippet = row.querySelector(".gs_rs");
  if (snippet) snippet.after(el);
  else (row.querySelector(".gs_ri") || row).appendChild(el);
}

/**
 * Re-order the results by the given terms. Returns {ok, count, matched} or {ok:false, reason}.
 * Original order is stamped on the rows first, so restoreOriginalOrder() can put it back exactly.
 */
export function applyReadingOrder(spec) {
  const container = resultsContainer();
  if (!container) return { ok: false, reason: "No results list found on this page." };
  if (!hasTerms(spec)) return { ok: false, reason: "Type at least one term first." };
  const rows = topLevelRows(container);
  if (!rows.length) return { ok: false, reason: "No results found on this page." };
  stampOriginalOrder(rows);
  const original = byOriginalIndex(rows);
  const data = original.map((r) => ({
    title: (r.querySelector(".gs_rt")?.textContent || "").trim(),
    snippet: snippetOf(r),
  }));
  // Remove highlights and tooltips from a previous run before marking again.
  original.forEach(clearRowOrderMarks);
  const { order, scores } = rankRows(data, spec);
  placeRows(container, order.map((i) => original[i]));
  const markList = [
    ...(spec.include || []).map((term) => ({ term, exclude: false })),
    ...(spec.method || []).map((term) => ({ term, exclude: false })),
    ...(spec.population || []).map((term) => ({ term, exclude: false })),
    ...(spec.exclude || []).map((term) => ({ term, exclude: true })),
  ];
  let matched = 0;
  order.forEach((i, pos) => {
    const row = original[i];
    if (scores[i].matches.length) matched++;
    markTerms(row.querySelector(".gs_rt"), markList);
    markTerms(row.querySelector(".gs_rs"), markList);
    addWhy(row, pos + 1, original.length, scores[i].why);
  });
  return { ok: true, count: original.length, matched };
}

/** Put the results back in Scholar's order and remove highlights and tooltips. */
export function restoreOriginalOrder() {
  const container = resultsContainer();
  if (!container) return { ok: false, reason: "No results list found on this page." };
  const rows = topLevelRows(container);
  rows.forEach(clearRowOrderMarks);
  const stamped = rows.filter((r) => Number.isFinite(parseInt(r.getAttribute(IDX_ATTR), 10)));
  if (stamped.length) placeRows(container, byOriginalIndex(stamped));
  return { ok: true, count: rows.length };
}

// ---- Toolbar row -----------------------------------------------------------

const FEATURE_LABELS = {
  claimEvidenceChips: "claim-evidence chips",
  reproSignals: "reproducibility signals",
  readingOrder: "reading order",
};

function field(placeholder, title, id) {
  const input = document.createElement("input");
  input.type = "text";
  input.id = id;
  input.className = "su-filter-input su-ro-input";
  input.placeholder = placeholder;
  input.title = title;
  input.maxLength = 300;
  input.autocomplete = "off";
  input.setAttribute("aria-label", title);
  return input;
}

function button(label, title) {
  const b = document.createElement("button");
  b.type = "button";
  b.className = "su-filter-quick-btn";
  b.textContent = label;
  b.title = title;
  return b;
}

/**
 * The toolbar row: the reading-order form for entitled users, and one small hint
 * naming any switched-on Pro feature that is locked, with an Unlock button.
 * Returns null when there is nothing to show. The rest of the page never waits on it.
 *
 * @param {{settings: object, allowed: Record<string, boolean>, onUnlock: () => void,
 *          isAllowed?: () => Promise<boolean>}} deps
 *   `isAllowed` re-checks the entitlement when the user presses Re-order.
 */
export function buildSignalsToolbarRow({ settings = {}, allowed = {}, onUnlock, isAllowed }) {
  const enabled = {
    claimEvidenceChips: settings.showClaimEvidenceChips !== false,
    reproSignals: settings.showReproSignals !== false,
    readingOrder: settings.showReadingOrder !== false,
  };
  const locked = SIGNAL_FEATURE_IDS.filter((id) => enabled[id] && !allowed[id]);
  const showForm = enabled.readingOrder && allowed.readingOrder;
  if (!showForm && !locked.length) return null;

  const row = document.createElement("div");
  row.className = "su-within-results-filter-row su-filter-signals-row su-ro-row";

  if (showForm) {
    const label = document.createElement("span");
    label.className = "su-filter-label su-filter-label-text";
    label.textContent = "Reading order:";
    const include = field("Include terms", "Include terms, separated by commas. Quote a phrase to keep it together.", "su-ro-include");
    const exclude = field("Exclude terms", "Exclude terms, separated by commas. Results with these move down; none are hidden.", "su-ro-exclude");
    const extra = field("Method or population (optional)", "Optional method or population terms, separated by commas. Counted like include terms.", "su-ro-extra");
    const apply = button("Re-order", "Re-order the results on this page by how many of your terms appear in each title and snippet.");
    const reset = button("Original order", "Restore Scholar's order exactly and remove highlights.");
    const status = document.createElement("span");
    status.className = "su-ro-status";
    status.setAttribute("role", "status");
    status.setAttribute("aria-live", "polite");
    const run = async () => {
      if (isAllowed && !(await isAllowed())) {
        status.textContent = "Reading order is a Pro feature.";
        return;
      }
      const spec = {
        include: parseTerms(include.value),
        exclude: parseTerms(exclude.value),
        method: parseTerms(extra.value),
        population: [],
      };
      const res = applyReadingOrder(spec);
      status.textContent = res.ok
        ? `${res.matched} of ${res.count} results matched. Hover "Order" on a result to see why. Nothing is hidden.`
        : res.reason;
    };
    apply.addEventListener("click", run);
    for (const input of [include, exclude, extra]) {
      input.addEventListener("keydown", (e) => {
        if (e.key === "Enter") { e.preventDefault(); run(); }
      });
    }
    reset.addEventListener("click", () => {
      const res = restoreOriginalOrder();
      status.textContent = res.ok ? "Original order restored." : res.reason;
    });
    row.append(label, include, exclude, extra, apply, reset, status);
  }

  if (locked.length) {
    const hint = document.createElement("span");
    hint.className = "su-pro-hint";
    const names = locked.map((id) => FEATURE_LABELS[id]);
    hint.textContent = `Pro, locked: ${names.join(", ")}.`;
    const unlock = button("Unlock", "Start a free trial or unlock Pro features");
    unlock.addEventListener("click", () => {
      if (typeof onUnlock === "function") onUnlock();
    });
    row.append(hint, unlock);
  }
  return row;
}
