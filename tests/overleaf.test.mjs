import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { toRecord } from "../src/common/export-model.js";
import { buildOverleafMain, escapeLatex, overleafFormFields, submitToOverleaf, OVERLEAF_URL, MAX_SOURCE_BYTES } from "../src/common/overleaf.js";

const rec = (key, title, authorsVenue, year, extra = {}) => ({ ...toRecord({ key, title, authorsVenue, year, ...extra }), citekey: extra.citekey || `k${key}` });

describe("escapeLatex", () => {
  it("escapes every LaTeX special character", () => {
    assert.equal(escapeLatex("50% of R&D #1 costs $5 in a_b {x} ~y ^z \\q"),
      "50\\% of R\\&D \\#1 costs \\$5 in a\\_b \\{x\\} \\textasciitilde{}y \\textasciicircum{}z \\textbackslash{}q");
  });
  it("turns newlines and control characters into spaces and tolerates null", () => {
    assert.equal(escapeLatex("a\nb\r\nc\u0000d"), "a b c d");
    assert.equal(escapeLatex(null), "");
  });
});

describe("buildOverleafMain", () => {
  const records = [
    rec("1", "Costs & benefits of 100% effort_", "A Smith", 2020, { notes: "private \\end{document} note", citekey: "smith2020costs" }),
    rec("2", "Another paper", "B Jones, C Lee", 2019, { citekey: "jones2019another" }),
  ];
  it("embeds the bib before the document class, cites every key and uses bibtex", () => {
    const r = buildOverleafMain(records);
    assert.equal(r.ok, true);
    const s = r.source;
    assert.ok(s.indexOf("\\begin{filecontents*}{refs.bib}") < s.indexOf("\\documentclass"));
    assert.ok(s.indexOf("\\end{filecontents*}") < s.indexOf("\\documentclass"));
    assert.match(s, /@article\{jones2019another|@misc\{jones2019another|@\w+\{jones2019another/);
    assert.match(s, /\\cite\{jones2019another,smith2020costs\}/);
    assert.match(s, /\\bibliography\{refs\}/);
  });
  it("escapes special characters in titles shown in the body", () => {
    const s = buildOverleafMain(records).source;
    assert.match(s, /Costs \\& benefits of 100\\% effort\\_/);
  });
  it("leaves private notes out by default, in the body and the bib", () => {
    const r = buildOverleafMain(records);
    assert.equal(r.notesIncluded, 0);
    assert.ok(!r.source.includes("private"));
    assert.ok(!r.source.includes("Note:"));
  });
  it("includes escaped notes only when asked", () => {
    const r = buildOverleafMain(records, { includeNotes: true });
    assert.equal(r.notesIncluded, 1);
    assert.match(r.source, /\\emph\{Note:\} private \\textbackslash\{\}end\\\{document\\\} note/);
    assert.equal((r.source.match(/\\end\{document\}/g) || []).length, 1);
  });
  it("reports empty input and records without keys", () => {
    assert.deepEqual(buildOverleafMain([]).reason, "empty");
    assert.equal(buildOverleafMain([{ ...records[0], citekey: "" }]).ok, false);
    assert.equal(buildOverleafMain(null).ok, false);
  });
  it("refuses a file over the size cap", () => {
    const big = Array.from({ length: 40 }, (_, i) => rec(String(i), "Title " + i, "A Smith", 2020, { abstract: "x".repeat(20000), citekey: `big${i}` }));
    const r = buildOverleafMain(big);
    assert.equal(r.ok, false);
    assert.equal(r.reason, "too-large");
    assert.ok(r.bytes > MAX_SOURCE_BYTES);
    assert.equal(buildOverleafMain(records, { maxBytes: 50 }).reason, "too-large");
  });
  it("cannot be tricked into closing the filecontents block early", () => {
    const r = buildOverleafMain([rec("1", "Sneaky", "A B", 2020, { abstract: "\\end{filecontents*} \\input{x}", citekey: "sneaky" })]);
    assert.equal((r.source.match(/\\end\{filecontents\*\}/g) || []).length, 1);
  });
});

describe("form", () => {
  it("posts only snip, snip_name and engine to the Overleaf docs URL in a new tab", () => {
    const f = overleafFormFields("\\documentclass{article}");
    assert.equal(f.action, OVERLEAF_URL);
    assert.equal(f.target, "_blank");
    assert.equal(f.method, "post");
    assert.deepEqual(f.fields.map((x) => x[0]), ["snip", "snip_name", "engine"]);
  });
  it("submitToOverleaf builds, submits and removes a form", () => {
    const made = [];
    const doc = {
      body: { appendChild: (n) => made.push(n) },
      createElement: (tag) => ({ tag, children: [], appendChild(c) { this.children.push(c); }, submit() { this.submitted = true; }, remove() { this.removed = true; } }),
    };
    submitToOverleaf("SRC", doc);
    const form = made[0];
    assert.equal(form.action, OVERLEAF_URL);
    assert.equal(form.target, "_blank");
    assert.equal(form.submitted, true);
    assert.equal(form.removed, true);
    assert.equal(form.children.find((c) => c.name === "snip").value, "SRC");
  });
});
