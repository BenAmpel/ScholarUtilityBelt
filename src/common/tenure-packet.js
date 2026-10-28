// Tenure and promotion packet: turns a snapshot of data the extension has
// already read from a Google Scholar profile page into a document model, and
// renders that model as print-friendly HTML. The same model goes to
// docx-writer.js for the .docx. Pure functions; no DOM, no network.
//
// The packet does not claim to satisfy any institution's requirements. It says
// where every number came from and that it reflects the profile as of the
// generation date.

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

export const PACKET_DISCLAIMER = "A department or institution may require its own format and its own sources. This summary does not claim to meet any institution's requirements.";

export function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function num(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).replace(/[,\s]/g, ""));
  return Number.isFinite(n) ? n : null;
}

// Scholar's own counts are whole numbers, written "5,432", "5.432" or "5 432" depending on the interface language.
function count(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const digits = String(v).replace(/\D/g, "");
  return digits ? Number(digits) : null;
}

function str(v, max = 400) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim().slice(0, max);
}

function fmt(n) {
  const v = num(n);
  if (v == null) return "not available";
  return Number.isInteger(v) ? v.toLocaleString("en-US") : String(Math.round(v * 100) / 100);
}

export function longDate(d) {
  const t = d instanceof Date && !Number.isNaN(d.getTime()) ? d : new Date();
  return `${t.getDate()} ${MONTHS[t.getMonth()]} ${t.getFullYear()}`;
}

/**
 * Read the "Cited by" box of a Scholar profile. Input is plain text pulled from
 * the page: headers like ["", "All", "Since 2021"] and rows like
 * [["Citations", "5,432", "3,100"], ["h-index", "31", "25"], ["i10-index", "70", "55"]].
 */
export function parseSidebarTable(table) {
  const out = { sinceLabel: "", citations: null, hIndex: null, i10Index: null };
  if (!table || !Array.isArray(table.rows)) return out;
  const headers = Array.isArray(table.headers) ? table.headers.map((h) => str(h, 40)) : [];
  out.sinceLabel = headers[2] || "";
  // Rows are always citations, h-index, i10-index in that order; the labels are translated ("Zitiert von", "h-Index"),
  // so a known label wins and the position is the fallback.
  const byPosition = ["citations", "hIndex", "i10Index"];
  table.rows.forEach((row, i) => {
    if (!Array.isArray(row) || row.length < 2) return;
    const label = str(row[0], 40).toLowerCase();
    const pair = { all: count(row[1]), recent: count(row[2]) };
    const key = label === "citations" ? "citations" : label === "h-index" ? "hIndex" : label === "i10-index" ? "i10Index" : byPosition[i];
    if (key && out[key] == null) out[key] = pair;
  });
  return out;
}

/**
 * Pair the profile's per-year citation chart labels with its bar values. Only
 * done when the two lists are the same length; otherwise returns null rather
 * than guessing which bar belongs to which year.
 */
export function pairCitationHistogram(hist) {
  if (!hist || !Array.isArray(hist.years) || !Array.isArray(hist.counts)) return null;
  if (hist.years.length === 0 || hist.years.length !== hist.counts.length) return null;
  const rows = [];
  for (let i = 0; i < hist.years.length; i++) {
    const y = num(hist.years[i]);
    const c = num(hist.counts[i]);
    if (y == null || y < 1900 || y > 2100 || c == null || c < 0) return null;
    rows.push({ year: y, citations: c });
  }
  return rows;
}

const QUALITY_ROWS = [
  ["ft50", "FT50", "Financial Times 50 journal list shipped with the extension"],
  ["utd24", "UTD24", "UT Dallas journal list shipped with the extension"],
  ["a", "ABDC A* or A", "ABDC Journal Quality List ranks shipped with the extension (A* and A counted together)"],
  ["vhb", "VHB JOURQUAL (any rank)", "VHB JOURQUAL 2024 list shipped with the extension"],
  ["abs4star", "ABS 4*", "ABS Academic Journal Guide 2024 list shipped with the extension"],
  ["q1", "SJR quartile Q1", "SCImago Journal Rank quartiles shipped with the extension"],
  ["q2", "SJR quartile Q2", "SCImago Journal Rank quartiles shipped with the extension"],
  ["q3", "SJR quartile Q3", "SCImago Journal Rank quartiles shipped with the extension"],
  ["q4", "SJR quartile Q4", "SCImago Journal Rank quartiles shipped with the extension"],
  ["core:A*", "CORE A*", "CORE/ICORE conference rankings shipped with the extension"],
  ["core:A", "CORE A", "CORE/ICORE conference rankings shipped with the extension"],
  ["core:B", "CORE B", "CORE/ICORE conference rankings shipped with the extension"],
  ["core:C", "CORE C", "CORE/ICORE conference rankings shipped with the extension"],
  ["era", "ERA 2023 listed", "ERA 2023 journal list shipped with the extension"]
];

function qualityCount(qc, key) {
  if (!qc) return 0;
  if (key.startsWith("core:")) return num(qc.core && qc.core[key.slice(5)]) || 0;
  return num(qc[key]) || 0;
}

/** Reduce whatever the page gave us to the fields the packet uses, with sane types and caps. */
export function normalizeSnapshot(raw) {
  const s = raw && typeof raw === "object" ? raw : {};
  const stats = s.stats && typeof s.stats === "object" ? s.stats : {};
  const papers = (Array.isArray(s.papers) ? s.papers : []).slice(0, 5000).map((p) => ({
    title: str(p && p.title, 300),
    year: (() => { const y = num(p && p.year); return y != null && y >= 1900 && y <= 2100 ? y : null; })(),
    citations: Math.max(0, num(p && p.citations) || 0),
    authorsText: str(p && p.authorsText, 300)
  }));
  const authorship = {
    solo: num(stats.soloAuthored),
    first: num(stats.firstAuthor),
    middle: num(stats.middleAuthor),
    last: num(stats.lastAuthor)
  };
  return {
    authorName: str(s.authorName, 120) || "Author name not found on the page",
    profileUrl: /^https:\/\/scholar\.google\.[a-z.]+\/citations\?/i.test(str(s.profileUrl, 500)) ? str(s.profileUrl, 500) : "",
    sidebar: parseSidebarTable(s.sidebar),
    perYear: pairCitationHistogram(s.histogram),
    papers,
    partial: !!s.partial,
    qualityEnabled: s.qualityBadgesEnabled !== false,
    stats: {
      hIndex: num(stats.hIndex),
      gIndex: num(stats.gIndex),
      mIndex: num(stats.mIndex),
      h5Index: num(stats.h5Index),
      firstYear: num(stats.firstYear),
      totalCitations: num(stats.totalCitations),
      qualityCounts: stats.qualityCounts && typeof stats.qualityCounts === "object" ? stats.qualityCounts : null
    },
    authorship
  };
}

/**
 * Build the document model. Block types match docx-writer.js plus "callout".
 * `now` is a Date; the generation date printed in the document comes from it.
 */
export function buildTenurePacket(rawSnapshot, now = new Date()) {
  const snap = normalizeSnapshot(rawSnapshot);
  const date = longDate(now);
  const n = snap.papers.length;
  const blocks = [];

  blocks.push({ type: "title", text: "Scholar profile summary" });
  blocks.push({ type: "paragraph", text: snap.authorName, bold: true });
  blocks.push({ type: "paragraph", text: `Generated ${date}` });
  if (snap.profileUrl) blocks.push({ type: "paragraph", text: `Profile: ${snap.profileUrl}` });
  blocks.push({
    type: "callout",
    text: `This summary was made by the Scholar Utility Belt browser extension from the Google Scholar profile page that was open on ${date}, and from datasets shipped with the extension. Every number reflects the profile as of that date and changes when the profile changes. Google Scholar profiles can be incomplete or contain errors, and the author should check this document against the profile before using it. ${PACKET_DISCLAIMER}`
  });
  if (snap.partial) {
    blocks.push({
      type: "callout",
      text: `Only ${fmt(n)} publication${n === 1 ? "" : "s"} had been loaded on the page when this was generated, and the profile lists more. Figures labeled as computed from loaded publications are partial. Figures labeled as reported by Google Scholar cover the whole profile.`
    });
  }

  // 1. Indices
  blocks.push({ type: "heading", level: 1, text: "Citation indices" });
  const sb = snap.sidebar;
  const since = sb.sinceLabel ? sb.sinceLabel.toLowerCase().replace(/^since\s*/, "since ") : "recent";
  const rows = [];
  const SCH = "Reported by Google Scholar (profile sidebar)";
  if (sb.citations) {
    rows.push(["Citations, all time", fmt(sb.citations.all), SCH]);
    if (sb.citations.recent != null) rows.push([`Citations, ${since}`, fmt(sb.citations.recent), SCH]);
  }
  if (sb.hIndex) {
    rows.push(["h-index, all time", fmt(sb.hIndex.all), SCH]);
    if (sb.hIndex.recent != null) rows.push([`h-index, ${since}`, fmt(sb.hIndex.recent), SCH]);
  }
  if (sb.i10Index) {
    rows.push(["i10-index, all time", fmt(sb.i10Index.all), SCH]);
    if (sb.i10Index.recent != null) rows.push([`i10-index, ${since}`, fmt(sb.i10Index.recent), SCH]);
  }
  const COMP = `Computed by the extension from the ${fmt(n)} publication${n === 1 ? "" : "s"} loaded on the page`;
  rows.push(["Publications loaded on the page", fmt(n), "Count of publication rows on the page when generated"]);
  const loadedCites = snap.papers.reduce((a, p) => a + p.citations, 0);
  if (n) rows.push(["Citations across loaded publications", fmt(loadedCites), COMP + " (sum of each row's Cited by count)"]);
  if (snap.stats.hIndex != null) rows.push(["h-index (computed)", fmt(snap.stats.hIndex), COMP]);
  if (snap.stats.gIndex != null) rows.push(["g-index (computed)", fmt(snap.stats.gIndex), COMP + "; largest g such that the top g papers have at least g squared citations in total"]);
  if (snap.stats.mIndex != null) rows.push(["m-index (computed)", fmt(snap.stats.mIndex), `${COMP}; h-index divided by years since the earliest publication year loaded${snap.stats.firstYear != null ? ` (${snap.stats.firstYear})` : ""}`]);
  if (snap.stats.h5Index != null) rows.push(["h5-index (computed)", fmt(snap.stats.h5Index), `${COMP}; h-index over publications from the last five calendar years`]);
  if (n) rows.push(["i10-index (computed)", fmt(snap.papers.filter((p) => p.citations >= 10).length), `${COMP}; publications with at least 10 citations`]);
  if (rows.length === 0) rows.push(["No index values were found on the page", "not available", ""]);
  blocks.push({ type: "table", header: ["Measure", "Value", "Where it comes from"], rows, numericCols: [1], colWidths: [3, 1.2, 5] });

  // 2. Citations per year
  blocks.push({ type: "heading", level: 1, text: "Citations per year" });
  if (snap.perYear && snap.perYear.length) {
    blocks.push({ type: "table", header: ["Year", "Citations"], rows: snap.perYear.map((r) => [String(r.year), fmt(r.citations)]), numericCols: [1], colWidths: [1, 1] });
    blocks.push({ type: "note", text: "Source: the citations-by-year chart on the Google Scholar profile page, as shown on the generation date. The current year is partial." });
  } else {
    blocks.push({ type: "paragraph", text: "Not available. The citations-by-year chart could not be read from the page, so no per-year figures are given." });
  }

  // 3. Publications by year
  blocks.push({ type: "heading", level: 1, text: "Publications by year" });
  const byYear = new Map();
  let undated = 0;
  for (const p of snap.papers) {
    if (p.year == null) undated++;
    else byYear.set(p.year, (byYear.get(p.year) || 0) + 1);
  }
  if (byYear.size) {
    const ys = [...byYear.keys()].sort((a, b) => a - b);
    blocks.push({ type: "table", header: ["Year", "Publications"], rows: ys.map((y) => [String(y), fmt(byYear.get(y))]), numericCols: [1], colWidths: [1, 1] });
  } else {
    blocks.push({ type: "paragraph", text: "Not available. No publication years were found among the loaded publications." });
  }
  blocks.push({ type: "note", text: `Source: counted by the extension from the ${fmt(n)} publication${n === 1 ? "" : "s"} loaded on the page, using the year Google Scholar shows for each.${undated ? ` ${undated} had no year and are not counted above.` : ""} Scholar lists items it treats as publications; it may include items a department would not count.` });

  // 4. Venue quality
  blocks.push({ type: "heading", level: 1, text: "Venue lists" });
  if (!snap.qualityEnabled) {
    blocks.push({ type: "paragraph", text: "Not available. Venue badges were turned off in the extension's settings when this was generated." });
  } else {
    const qc = snap.stats.qualityCounts;
    const qrows = QUALITY_ROWS.map(([key, label, src]) => [label, fmt(qualityCount(qc, key)), src]).filter((r) => r[1] !== "0");
    if (qrows.length) {
      blocks.push({ type: "table", header: ["List or rank", "Publications", "Dataset"], rows: qrows, numericCols: [1], colWidths: [3, 1.4, 5] });
    } else {
      blocks.push({ type: "paragraph", text: "None of the loaded publications matched the venue datasets shipped with the extension." });
    }
    blocks.push({ type: "note", text: "Source: the extension matches the venue name Google Scholar shows for each publication to the datasets named above. A publication can appear in more than one row. A venue that is not in these datasets is not counted anywhere; that is not a judgement of quality. Matching is by name and can miss a venue or match the wrong one. Dataset versions are those shipped with the installed extension." });
  }

  // 5. Authorship
  blocks.push({ type: "heading", level: 1, text: "Authorship position" });
  const a = snap.authorship;
  if (a.first != null || a.solo != null || a.last != null || a.middle != null) {
    const arows = [];
    if (a.first != null) arows.push(["First author", fmt(a.first)]);
    if (a.solo != null) arows.push(["Sole author", fmt(a.solo)]);
    if (a.last != null) arows.push(["Last author", fmt(a.last)]);
    if (a.middle != null) arows.push(["Other position", fmt(a.middle)]);
    blocks.push({ type: "table", header: ["Position", "Publications"], rows: arows, numericCols: [1], colWidths: [2, 1] });
    blocks.push({ type: "note", text: "Source: computed by the extension by matching the profile owner's name against the author list Google Scholar shows for each loaded publication. Author lists on Scholar can be abbreviated or cut short, so these counts can be off. Whether author order carries meaning depends on the field." });
  } else {
    blocks.push({ type: "paragraph", text: "Not available. Authorship positions were not computed on the page." });
  }

  // 6. Top cited
  blocks.push({ type: "heading", level: 1, text: "Most cited publications" });
  const top = snap.papers.slice().sort((x, y) => y.citations - x.citations).slice(0, 10);
  if (top.length) {
    blocks.push({
      type: "table",
      header: ["#", "Title and authors as listed", "Year", "Citations"],
      rows: top.map((p, i) => [String(i + 1), p.authorsText ? `${p.title}\n${p.authorsText}` : p.title, p.year == null ? "" : String(p.year), fmt(p.citations)]),
      numericCols: [3],
      colWidths: [0.5, 7, 1, 1.3]
    });
    blocks.push({ type: "note", text: "Source: the Cited by count Google Scholar shows for each loaded publication on the generation date." });
  } else {
    blocks.push({ type: "paragraph", text: "Not available. No publications were loaded on the page." });
  }

  blocks.push({ type: "heading", level: 1, text: "About this document" });
  blocks.push({ type: "paragraph", text: `Generated ${date} by Scholar Utility Belt from the Google Scholar profile page and datasets stored in the extension. No additional requests were made to Google Scholar to produce it. ${PACKET_DISCLAIMER}` });

  return { title: `Scholar profile summary - ${snap.authorName}`, authorName: snap.authorName, generatedLabel: date, blocks };
}

/** Render the model as an HTML fragment (<article class="doc">). All text is escaped. */
export function packetToHtml(packet) {
  const out = ['<article class="doc">'];
  for (const b of (packet && packet.blocks) || []) {
    if (!b || typeof b !== "object") continue;
    if (b.type === "title") out.push(`<h1>${escapeHtml(b.text)}</h1>`);
    else if (b.type === "heading") out.push(`<h${b.level === 2 ? 3 : 2}>${escapeHtml(b.text)}</h${b.level === 2 ? 3 : 2}>`);
    else if (b.type === "paragraph") out.push(`<p>${b.bold ? "<strong>" : ""}${escapeHtml(b.text)}${b.bold ? "</strong>" : ""}</p>`);
    else if (b.type === "note") out.push(`<p class="src">${escapeHtml(b.text)}</p>`);
    else if (b.type === "callout") out.push(`<div class="notice">${escapeHtml(b.text)}</div>`);
    else if (b.type === "table") {
      const numeric = new Set(b.numericCols || []);
      const head = (b.header || []).map((h) => `<th>${escapeHtml(h)}</th>`).join("");
      const body = (b.rows || []).map((r) => `<tr>${r.map((c, i) => `<td${numeric.has(i) ? ' class="num"' : ""}>${escapeHtml(c).replace(/\n/g, "<br>")}</td>`).join("")}</tr>`).join("");
      out.push(`<table>${head ? `<thead><tr>${head}</tr></thead>` : ""}<tbody>${body}</tbody></table>`);
    }
  }
  out.push("</article>");
  return out.join("\n");
}

export function packetFileName(packet, now = new Date()) {
  const slug = String((packet && packet.authorName) || "author").toLowerCase().normalize("NFKD").replace(/[̀-ͯ]/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "author";
  const d = now instanceof Date ? now : new Date();
  const p2 = (v) => String(v).padStart(2, "0");
  return `scholar-profile-summary-${slug}-${d.getFullYear()}-${p2(d.getMonth() + 1)}-${p2(d.getDate())}`;
}
