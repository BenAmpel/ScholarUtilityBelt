/**
 * Scholar's "View article" page (/citations?view_op=view_citation): the one page with a paper's full author list,
 * volume, issue, pages, publisher, abstract and per-year citations. content.js reads the page into plain rows
 * (readArticlePage there); everything here is pure, so it is unit-tested without a DOM. Nothing here asks Scholar
 * for anything: it only reads what the open page already shows.
 */
import { buildBibTeXEntry } from "./bibtex.js";
import { buildRisEntry } from "./zotero-export.js";

// Scholar's English field labels. Other interface languages translate them, so each field also has a
// structural fallback below (the description has its own id, the citations row holds the chart, the venue
// comes right after the date).
const LABELS = {
  authors: /^(authors?|inventors?)$/i,
  date: /^publication date$/i,
  venue: /^(journal|conference|source|book|book title|series)$/i,
  volume: /^volume$/i,
  issue: /^issue$/i,
  pages: /^pages$/i,
  publisher: /^publisher$/i,
  description: /^description$/i,
  citations: /^total citations$/i,
};
const DATE_RE = /^(\d{4})(?:\/\d{1,2}){0,2}$/;
const PAGE_RANGE_RE = /^[A-Za-z]?\d+\s*[-–]\s*[A-Za-z]?\d+$/;

const tidy = (v) => String(v ?? "").replace(/\s+/g, " ").trim();

/**
 * @param {object} raw from content.js: { title, url, pdfUrl, ownerName, rows: [{ label, value, isDescription,
 *   isCitations, citedByText, citedByHref }], bars: [{ year, count }] }
 * @returns the article as fields, or null when the page has no title or rows.
 */
export function articleFromPage(raw) {
  const rows = Array.isArray(raw?.rows) ? raw.rows : [];
  const title = tidy(raw?.title);
  if (!title || !rows.length) return null;
  const out = {
    title,
    url: String(raw.url || ""),
    pdfUrl: String(raw.pdfUrl || ""),
    ownerName: tidy(raw.ownerName),
    authors: [],
    authorsTruncated: false,
    date: "",
    year: null,
    venue: "",
    volume: "",
    issue: "",
    pages: "",
    publisher: "",
    abstract: "",
    citedBy: null,
    clusterId: null,
    history: [],
  };
  let dateIndex = -1;
  const used = new Set();
  rows.forEach((r, i) => {
    const label = tidy(r.label);
    const value = tidy(r.value);
    if (r.isDescription || LABELS.description.test(label)) {
      out.abstract = value;
    } else if (r.isCitations || LABELS.citations.test(label)) {
      out.citedBy = citedByNumber(r.citedByText);
      out.clusterId = citesParam(r.citedByHref);
    } else if (LABELS.authors.test(label) || (i === 0 && !LABELS.date.test(label))) {
      out.authorsTruncated = /(\.\.\.|…)$/.test(value);
      out.authors = value.replace(/(\.\.\.|…)$/, "").split(/\s*,\s*/).map((a) => a.trim()).filter(Boolean);
    } else if (LABELS.date.test(label) || (!out.date && DATE_RE.test(value))) {
      out.date = value;
      const m = DATE_RE.exec(value);
      out.year = m ? Number(m[1]) : null;
      dateIndex = i;
    } else if (LABELS.venue.test(label)) out.venue = value;
    else if (LABELS.volume.test(label)) out.volume = value;
    else if (LABELS.issue.test(label)) out.issue = value;
    else if (LABELS.pages.test(label)) out.pages = value;
    else if (LABELS.publisher.test(label)) out.publisher = value;
    else return;
    used.add(i);
  });
  // Labels in another interface language: the venue is the row after the date, and pages look like 12-34.
  if (!out.venue && dateIndex >= 0 && rows[dateIndex + 1] && !used.has(dateIndex + 1)) {
    out.venue = tidy(rows[dateIndex + 1].value);
    used.add(dateIndex + 1);
  }
  if (!out.pages) {
    const p = rows.find((r, i) => !used.has(i) && PAGE_RANGE_RE.test(tidy(r.value)));
    if (p) out.pages = tidy(p.value);
  }
  out.history = normalizeHistory(raw.bars);
  return out;
}

function citedByNumber(text) {
  const m = String(text || "").match(/\d[\d.,\u00a0\u202f ]*/);
  if (!m) return null;
  const n = parseInt(m[0].replace(/\D/g, ""), 10);
  return Number.isFinite(n) ? n : null;
}

function citesParam(href) {
  try {
    return new URL(String(href || ""), "https://scholar.google.com").searchParams.get("cites") || null;
  } catch (_) {
    return null;
  }
}

function normalizeHistory(bars) {
  const byYear = new Map();
  for (const b of Array.isArray(bars) ? bars : []) {
    const year = Number(b?.year);
    const count = Number(String(b?.count ?? "").replace(/\D/g, ""));
    if (Number.isInteger(year) && year > 1900 && Number.isFinite(count)) byYear.set(year, (byYear.get(year) || 0) + count);
  }
  if (!byYear.size) return [];
  const years = [...byYear.keys()];
  const out = [];
  // A year with no bar had no citations, so the history has no gaps.
  for (let y = Math.min(...years); y <= Math.max(...years); y++) out.push({ year: y, count: byYear.get(y) || 0 });
  return out;
}

/**
 * Plain numbers from the page's own citation chart. The current year is partial, so it is reported as "so far"
 * and kept out of the peak and the trend.
 */
export function citationStats(article, nowYear = new Date().getFullYear()) {
  const history = Array.isArray(article?.history) ? article.history : [];
  const total = Number.isFinite(article?.citedBy) ? article.citedBy : history.reduce((a, b) => a + b.count, 0);
  const firstYear = Number.isInteger(article?.year) ? article.year : history[0]?.year ?? null;
  const years = firstYear != null ? Math.max(1, nowYear - firstYear) : null;
  const perYear = years && total ? Math.round((total / years) * 10) / 10 : null;
  const complete = history.filter((h) => h.year < nowYear);
  const current = history.find((h) => h.year === nowYear);
  const peak = complete.length ? complete.reduce((best, h) => (h.count > best.count ? h : best), complete[0]) : null;
  let trend = null;
  if (complete.length >= 3) {
    // The last full year against the average of up to three years before it.
    const last = complete[complete.length - 1];
    const prior = complete.slice(-4, -1);
    const mean = prior.reduce((a, b) => a + b.count, 0) / prior.length;
    if (mean > 0) trend = { pct: Math.round(((last.count - mean) / mean) * 100), last, fromYear: prior[0].year, toYear: prior[prior.length - 1].year };
  }
  return {
    perYear,
    peak: peak && peak.count > 0 ? { year: peak.year, count: peak.count } : null,
    trend,
    soFar: current ? { year: current.year, count: current.count } : null,
  };
}

/** One short phrase per fact, for the line under Scholar's chart. */
export function citationStatsLines(stats) {
  const lines = [];
  if (stats?.perYear != null) lines.push(`${stats.perYear >= 10 ? Math.round(stats.perYear) : stats.perYear} citations a year on average`);
  if (stats?.peak) lines.push(`peak ${stats.peak.count} in ${stats.peak.year}`);
  if (stats?.trend) {
    const { pct, last, fromYear, toYear } = stats.trend;
    const span = fromYear === toYear ? `${fromYear}` : `${fromYear}–${toYear}`;
    lines.push(Math.abs(pct) < 10
      ? `${last.year} about level with the ${span} average`
      : `${last.year} ${pct > 0 ? "up" : "down"} ${Math.abs(pct)}% on the ${span} average`);
  }
  if (stats?.soFar) lines.push(`${stats.soFar.count} so far in ${stats.soFar.year}`);
  return lines;
}

function nameKey(name) {
  const tokens = String(name || "")
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .toLowerCase()
    .replace(/[^a-z\s-]/g, " ")
    .split(/\s+/)
    .filter((t) => t && !/^(jr|sr|ii|iii|iv)$/.test(t));
  if (!tokens.length) return null;
  return { family: tokens[tokens.length - 1], initial: tokens[0][0] };
}

/**
 * Where the profile's owner sits in the author list, matched on surname and first initial.
 * @returns {{ index: number, count: number, role: "sole"|"first"|"last"|"middle" }|null}
 */
export function authorPosition(authors, ownerName) {
  const owner = nameKey(ownerName);
  const list = Array.isArray(authors) ? authors : [];
  if (!owner || !list.length) return null;
  const i = list.findIndex((a) => {
    const k = nameKey(a);
    return k && k.family === owner.family && k.initial === owner.initial;
  });
  if (i < 0) return null;
  const count = list.length;
  const role = count === 1 ? "sole" : i === 0 ? "first" : i === count - 1 ? "last" : "middle";
  return { index: i + 1, count, role };
}

/** "Sagar Samtani is author 2 of 4." A truncated list (ending in "...") cannot say "last" and shows "4+". */
export function authorPositionText(pos, ownerName, truncated = false) {
  if (!pos) return "";
  const who = tidy(ownerName) || "The profile's owner";
  const of = `${pos.count}${truncated ? "+" : ""}`;
  if (pos.role === "sole" && !truncated) return `${who} is the sole author.`;
  if (pos.role === "first") return `${who} is the first of ${of} authors.`;
  if (pos.role === "last" && !truncated) return `${who} is the last of ${of} authors.`;
  return `${who} is author ${pos.index} of ${of}.`;
}

function bibKey(article) {
  const first = nameKey(article.authors[0]);
  const word = String(article.title || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().match(/[a-z]{4,}/);
  return `${first ? first.family : "paper"}${article.year || ""}${word ? word[0] : ""}`.replace(/[^A-Za-z0-9]/g, "");
}

/** BibTeX with everything the page shows: every author, volume, issue, pages, publisher, DOI and abstract. */
export function articleBibTeX(article, doi = "") {
  return buildBibTeXEntry({
    key: bibKey(article),
    title: article.title,
    authors: article.authors,
    year: article.year,
    venue: article.venue,
    volume: article.volume,
    number: article.issue,
    pages: article.pages,
    publisher: article.publisher,
    doi,
    url: article.url,
    abstract: article.abstract,
  });
}

/** RIS with the same fields, for EndNote, Zotero and Mendeley. */
export function articleRis(article, doi = "") {
  return buildRisEntry({
    title: article.title,
    authors: article.authors,
    year: article.year ? String(article.year) : "",
    venue: article.venue,
    volume: article.volume,
    issue: article.issue,
    pages: article.pages,
    publisher: article.publisher,
    doi,
    url: article.url,
    abstract: article.abstract,
    tags: [],
    notes: "",
    summary: "",
    highlights: [],
    collections: [],
  }, { includeNotes: false }) + "\r\n";
}

/** "Yunji Liang, Sagar Samtani - IEEE Internet of Things Journal, 2020": the authors-and-venue line the library reads. */
export function authorsVenueLine(article) {
  const tail = [article.venue, article.year].filter(Boolean).join(", ");
  return [article.authors.join(", "), tail].filter(Boolean).join(" - ");
}
