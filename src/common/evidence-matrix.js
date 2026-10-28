/**
 * Evidence matrix: papers as rows, user-defined concept columns. Each column
 * only points at data the review project already holds (an extraction field,
 * the paper's tags, its notes, or a PICO box), so the only new stored data is
 * the list of column definitions, project.matrixColumns:
 *
 *   [{ id, label, source: { type: "extraction", key }
 *                       | { type: "tags", contains?: string }   // all tags, or only tags containing the text
 *                       | { type: "notes" }
 *                       | { type: "pico", key: "population"|"intervention"|"comparator"|"outcome" } }]
 *
 * Pure functions; the workspace does the rendering and the download.
 */

export const MAX_COLUMNS = 20;
export const PICO_KEYS = ["population", "intervention", "comparator", "outcome"];
export const EMPTY_GROUP = "(empty)";

function isObj(x) { return x && typeof x === "object" && !Array.isArray(x); }

export function normalizeColumns(columns) {
  if (!Array.isArray(columns)) return [];
  const out = [];
  const seen = new Set();
  for (const c of columns) {
    if (!isObj(c) || typeof c.id !== "string" || !c.id || seen.has(c.id) || !validSource(c.source)) continue;
    seen.add(c.id);
    out.push({ id: c.id, label: String(c.label || c.id).slice(0, 80), source: cleanSource(c.source) });
    if (out.length >= MAX_COLUMNS) break;
  }
  return out;
}

function validSource(s) {
  if (!isObj(s)) return false;
  if (s.type === "extraction") return typeof s.key === "string" && !!s.key;
  if (s.type === "pico") return PICO_KEYS.includes(s.key);
  return s.type === "tags" || s.type === "notes";
}
function cleanSource(s) {
  if (s.type === "extraction") return { type: "extraction", key: s.key };
  if (s.type === "pico") return { type: "pico", key: s.key };
  if (s.type === "tags") return { type: "tags", contains: String(s.contains || "").trim().slice(0, 80) };
  return { type: "notes" };
}

/** Returns a new column list with the column added, or null when the label or source is not usable or the cap is reached. */
export function addColumn(columns, label, source, now = Date.now()) {
  const list = normalizeColumns(columns);
  const text = String(label || "").trim().slice(0, 80);
  if (!text || !validSource(source) || list.length >= MAX_COLUMNS) return null;
  let n = Number(now) || Date.now();
  let id = `mx_${n.toString(36)}`;
  while (list.some((c) => c.id === id)) { n += 1; id = `mx_${n.toString(36)}`; }
  return list.concat([{ id, label: text, source: cleanSource(source) }]);
}

export function removeColumn(columns, id) {
  return normalizeColumns(columns).filter((c) => c.id !== id);
}

/** Moves a column one step left (-1) or right (+1). */
export function moveColumn(columns, id, delta) {
  const list = normalizeColumns(columns);
  const i = list.findIndex((c) => c.id === id);
  const j = i + (delta < 0 ? -1 : 1);
  if (i < 0 || j < 0 || j >= list.length) return list;
  [list[i], list[j]] = [list[j], list[i]];
  return list;
}

function cellValues(column, paperId, data) {
  const s = column.source;
  if (s.type === "extraction") {
    const v = data.extraction?.[paperId]?.[s.key];
    const t = v == null ? "" : String(v).trim();
    return t ? [t] : [];
  }
  if (s.type === "pico") {
    const t = String(data.pico?.[paperId]?.[s.key] ?? "").trim();
    return t ? [t] : [];
  }
  if (s.type === "notes") {
    const t = String(data.notes?.[paperId] ?? "").trim();
    return t ? [t] : [];
  }
  const tags = Array.isArray(data.tags?.[paperId]) ? data.tags[paperId].map((t) => String(t ?? "").trim()).filter(Boolean) : [];
  const needle = (s.contains || "").toLowerCase();
  return needle ? tags.filter((t) => t.toLowerCase().includes(needle)) : tags;
}

/**
 * Builds the rows. `data` is {papers, extraction, tags, notes, pico}; `paperIds`
 * picks and orders the papers (for example the included ones).
 * Each row: {id, title, year, cells: [{values: string[], text: string}]}.
 */
export function buildMatrix(data, columns, paperIds) {
  const cols = normalizeColumns(columns);
  const papers = isObj(data?.papers) ? data.papers : {};
  const ids = Array.isArray(paperIds) ? paperIds : Object.keys(papers);
  const rows = ids.filter((id) => isObj(papers[id])).map((id) => ({
    id,
    title: String(papers[id].title || "Untitled"),
    year: String(papers[id].year || ""),
    cells: cols.map((c) => {
      const values = cellValues(c, id, data || {});
      return { values, text: values.join("; ") };
    })
  }));
  return { columns: cols, rows };
}

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });

/** by: "title" | "year" | a column id. Empty cells always sort last. Returns a new array. */
export function sortRows(matrix, by, dir = "asc") {
  const colIndex = matrix.columns.findIndex((c) => c.id === by);
  const sign = dir === "desc" ? -1 : 1;
  const key = (row) => (by === "title" ? row.title : by === "year" ? row.year : colIndex >= 0 ? row.cells[colIndex].text : row.title);
  return matrix.rows.map((r, i) => ({ r, i })).sort((a, b) => {
    const ka = key(a.r);
    const kb = key(b.r);
    if (!ka && !kb) return a.i - b.i;
    if (!ka) return 1;
    if (!kb) return -1;
    return sign * collator.compare(ka, kb) || a.i - b.i;
  }).map((x) => x.r);
}

/**
 * Groups rows by a column. A paper with several values in that column (several
 * tags) appears under each of them. Rows with no value go in an "(empty)" group
 * placed last. Returns [{group, rows}]. With no valid column id, one group "".
 */
export function groupRows(matrix, rows, columnId) {
  const idx = matrix.columns.findIndex((c) => c.id === columnId);
  if (idx < 0) return [{ group: "", rows }];
  const map = new Map();
  const empty = [];
  for (const row of rows) {
    const vals = row.cells[idx].values;
    if (!vals.length) { empty.push(row); continue; }
    for (const v of vals) {
      if (!map.has(v)) map.set(v, []);
      map.get(v).push(row);
    }
  }
  const groups = Array.from(map.entries()).sort((a, b) => collator.compare(a[0], b[0])).map(([group, r]) => ({ group, rows: r }));
  if (empty.length) groups.push({ group: EMPTY_GROUP, rows: empty });
  return groups;
}

function csvCell(value) {
  let s = value == null ? "" : String(value);
  // Stop spreadsheet programs from running text that starts like a formula (numbers keep their sign).
  if (typeof value !== "number" && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function mdCell(value) {
  return String(value == null ? "" : value).replace(/\\/g, "\\\\").replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
}
function htmlEscape(value) {
  return String(value == null ? "" : value).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function flatten(groups, grouped) {
  const out = [];
  groups.forEach((g) => g.rows.forEach((row) => out.push({ group: grouped ? g.group : null, row })));
  return out;
}

/** `groups` is the output of groupRows (one unnamed group when not grouping). */
export function matrixToCsv(matrix, groups, grouped = false) {
  const header = [...(grouped ? ["Group"] : []), "Title", "Year", ...matrix.columns.map((c) => c.label)];
  const lines = [header];
  flatten(groups, grouped).forEach(({ group, row }) => {
    lines.push([...(grouped ? [group] : []), row.title, row.year, ...row.cells.map((c) => c.text)]);
  });
  return lines.map((l) => l.map(csvCell).join(",")).join("\n");
}

export function matrixToMarkdown(matrix, groups, grouped = false, title = "Evidence matrix") {
  const out = [`# ${title}`, ""];
  const table = (rows) => {
    const header = ["Paper", ...matrix.columns.map((c) => c.label)];
    const lines = [`| ${header.map(mdCell).join(" | ")} |`, `| ${header.map(() => "---").join(" | ")} |`];
    rows.forEach((row) => lines.push(`| ${[`${row.title}${row.year ? ` (${row.year})` : ""}`, ...row.cells.map((c) => c.text)].map(mdCell).join(" | ")} |`));
    return lines;
  };
  if (!matrix.rows.length) { out.push("No papers to show."); return out.join("\n"); }
  if (grouped) {
    groups.forEach((g) => { out.push(`## ${g.group}`, "", ...table(g.rows), ""); });
  } else {
    out.push(...table(groups.flatMap((g) => g.rows)), "");
  }
  return out.join("\n");
}

/**
 * A standalone HTML page with a plain bordered table that Word and Google Docs
 * paste or open as a table. The inline attributes are deliberate: this is a
 * downloaded file, not an extension page, so the extension's CSP does not apply.
 */
export function matrixToHtml(matrix, groups, grouped = false, title = "Evidence matrix") {
  const colspan = matrix.columns.length + 1;
  const head = `<tr><th>Paper</th>${matrix.columns.map((c) => `<th>${htmlEscape(c.label)}</th>`).join("")}</tr>`;
  const body = [];
  groups.forEach((g) => {
    if (grouped) body.push(`<tr><td colspan="${colspan}"><b>${htmlEscape(g.group)}</b></td></tr>`);
    g.rows.forEach((row) => {
      body.push(`<tr><td>${htmlEscape(row.title)}${row.year ? ` (${htmlEscape(row.year)})` : ""}</td>${row.cells.map((c) => `<td>${htmlEscape(c.text)}</td>`).join("")}</tr>`);
    });
  });
  return `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>${htmlEscape(title)}</title></head>
<body>
<h1>${htmlEscape(title)}</h1>
<table border="1" cellspacing="0" cellpadding="4">
<thead>${head}</thead>
<tbody>
${body.join("\n")}
</tbody>
</table>
</body></html>
`;
}
