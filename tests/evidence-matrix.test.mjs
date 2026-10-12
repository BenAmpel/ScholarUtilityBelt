import { describe, it } from "node:test";
import assert from "node:assert/strict";

const M = await import("../src/common/evidence-matrix.js");
const {
  normalizeColumns, addColumn, removeColumn, moveColumn, buildMatrix, sortRows, groupRows,
  matrixToCsv, matrixToMarkdown, matrixToHtml, MAX_COLUMNS, EMPTY_GROUP,
} = M;

const data = {
  papers: { p1: { title: "Beta trial", year: 2020 }, p2: { title: "Alpha cohort", year: "2018" }, p3: { title: 'Gamma <study> | "x"', year: "" }, p4: { title: "Delta", year: "2022" } },
  extraction: { p1: { sample: "120" }, p2: { sample: "9" }, p3: { sample: "" } },
  tags: { p1: ["theme: trust", "method: survey"], p2: ["theme: trust"], p3: [], p4: ["method: interview", null, "  "] },
  notes: { p1: "Line one\nline two", p2: "  " },
  pico: { p1: { population: "adults" } },
};
let cols = [];
cols = addColumn(cols, "Sample", { type: "extraction", key: "sample" }, 1);
cols = addColumn(cols, "Theme", { type: "tags", contains: "theme" }, 2);
cols = addColumn(cols, "Notes", { type: "notes" }, 3);
cols = addColumn(cols, "Who", { type: "pico", key: "population" }, 4);
const ids = ["p1", "p2", "p3", "p4"];

describe("columns", () => {
  it("adds with unique ids, validates sources and caps the count", () => {
    assert.equal(cols.length, 4);
    assert.equal(new Set(cols.map((c) => c.id)).size, 4);
    assert.equal(addColumn(cols, "", { type: "notes" }), null);
    assert.equal(addColumn(cols, "x", { type: "extraction" }), null);
    assert.equal(addColumn(cols, "x", { type: "pico", key: "bogus" }), null);
    assert.equal(addColumn(cols, "x", "notes"), null);
    let big = [];
    for (let i = 0; i < MAX_COLUMNS + 3; i += 1) big = addColumn(big, `c${i}`, { type: "notes" }, 100) || big;
    assert.equal(big.length, MAX_COLUMNS);
  });
  it("same timestamp still gives distinct ids", () => {
    let c = addColumn([], "a", { type: "notes" }, 5);
    c = addColumn(c, "b", { type: "notes" }, 5);
    assert.notEqual(c[0].id, c[1].id);
  });
  it("normalizes garbage, removes and reorders", () => {
    assert.deepEqual(normalizeColumns("x"), []);
    assert.equal(normalizeColumns([null, { id: "a", source: { type: "zzz" } }, { id: "a", label: "A", source: { type: "notes" } }, { id: "a", source: { type: "notes" } }]).length, 1);
    assert.equal(removeColumn(cols, cols[0].id).length, 3);
    assert.equal(moveColumn(cols, cols[0].id, 1)[1].id, cols[0].id);
    assert.deepEqual(moveColumn(cols, cols[0].id, -1), cols);
  });
});

describe("buildMatrix", () => {
  const m = buildMatrix(data, cols, ids);
  it("reads extraction, tags (filtered), notes and PICO; blanks stay empty", () => {
    const p1 = m.rows[0];
    assert.equal(p1.cells[0].text, "120");
    assert.equal(p1.cells[1].text, "theme: trust");
    assert.equal(p1.cells[2].text, "Line one\nline two");
    assert.equal(p1.cells[3].text, "adults");
    const p3 = m.rows[2];
    assert.deepEqual(p3.cells.map((c) => c.text), ["", "", "", ""]);
    assert.equal(m.rows[1].cells[2].text, "", "whitespace-only note is empty");
  });
  it("skips unknown paper ids and tolerates missing data", () => {
    assert.equal(buildMatrix(data, cols, ["nope", "p1"]).rows.length, 1);
    assert.equal(buildMatrix(undefined, cols, undefined).rows.length, 0);
    assert.equal(buildMatrix({ papers: { a: { title: "A" } } }, cols).rows[0].cells.length, 4);
  });
  it("tags column with no filter lists every non-blank tag", () => {
    const all = buildMatrix(data, addColumn([], "Tags", { type: "tags" }), ids);
    assert.deepEqual(all.rows[3].cells[0].values, ["method: interview"]);
  });
});

describe("sortRows", () => {
  const m = buildMatrix(data, cols, ids);
  it("sorts by title, year, and a column with numeric awareness; empty last", () => {
    assert.deepEqual(sortRows(m, "title").map((r) => r.id), ["p2", "p1", "p4", "p3"]);
    assert.deepEqual(sortRows(m, "year").map((r) => r.id), ["p2", "p1", "p4", "p3"]);
    assert.deepEqual(sortRows(m, cols[0].id).map((r) => r.id), ["p2", "p1", "p3", "p4"]);
    assert.deepEqual(sortRows(m, cols[0].id, "desc").map((r) => r.id), ["p1", "p2", "p3", "p4"]);
  });
  it("does not mutate and falls back to title for an unknown key", () => {
    const before = m.rows.map((r) => r.id).join();
    sortRows(m, "year");
    assert.equal(m.rows.map((r) => r.id).join(), before);
    assert.equal(sortRows(m, "nope")[0].id, "p2");
  });
});

describe("groupRows", () => {
  const m = buildMatrix(data, cols, ids);
  it("groups by a column, repeats multi-value papers, puts empty last", () => {
    const tagCol = addColumn([], "Tags", { type: "tags" });
    const tm = buildMatrix(data, tagCol, ids);
    const g = groupRows(tm, tm.rows, tagCol[0].id);
    assert.deepEqual(g.map((x) => x.group), ["method: interview", "method: survey", "theme: trust", EMPTY_GROUP]);
    assert.deepEqual(g[2].rows.map((r) => r.id), ["p1", "p2"]);
    assert.deepEqual(g[3].rows.map((r) => r.id), ["p3"]);
    assert.equal(g[1].rows[0].id, "p1");
  });
  it("returns one unnamed group for an unknown column", () => {
    const g = groupRows(m, m.rows, "nope");
    assert.equal(g.length, 1);
    assert.equal(g[0].rows.length, 4);
  });
});

describe("exports", () => {
  const m = buildMatrix(data, cols, ids);
  const flat = [{ group: "", rows: sortRows(m, "title") }];
  it("CSV quotes commas, quotes and newlines and adds a Group column when grouped", () => {
    const csv = matrixToCsv(m, flat);
    assert.match(csv.split("\n")[0], /^Title,Year,Sample,Theme,Notes,Who$/);
    assert.match(csv, /"Line one\nline two"/);
    assert.match(csv, /"Gamma <study> \| ""x"""/);
    const g = groupRows(m, m.rows, cols[1].id);
    const gcsv = matrixToCsv(m, g, true);
    assert.match(gcsv.split("\n")[0], /^Group,Title/);
    assert.match(gcsv, /\(empty\)/);
  });
  it("Markdown escapes pipes and flattens newlines; supports groups", () => {
    const md = matrixToMarkdown(m, flat, false, "Matrix");
    assert.match(md, /^# Matrix/);
    assert.match(md, /Gamma <study> \\\| "x"/);
    assert.match(md, /Line one line two/);
    const gmd = matrixToMarkdown(m, groupRows(m, m.rows, cols[1].id), true);
    assert.match(gmd, /## theme: trust/);
    assert.match(matrixToMarkdown({ columns: [], rows: [] }, []), /No papers/);
  });
  it("HTML escapes everything and is a complete document with a bordered table", () => {
    const html = matrixToHtml(m, flat);
    assert.match(html, /^<!DOCTYPE html>/);
    assert.match(html, /<table border="1"/);
    assert.match(html, /Gamma &lt;study&gt; \| &quot;x&quot;/);
    assert.ok(!html.includes("<study>"));
    const g = matrixToHtml(m, groupRows(m, m.rows, cols[1].id), true);
    assert.match(g, /<td colspan="5"><b>theme: trust<\/b>/);
  });
});
