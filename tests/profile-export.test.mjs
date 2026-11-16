import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  filterRowsByYear, parseYearBound, splitProfileAuthors, profileCiteKey,
  buildProfileBibTeX, buildProfileCsv, csvCell,
} from "../src/common/bibtex.js";

const row = (o = {}) => ({ title: "Deep learning for fraud", authors: "A Smith, B Jones", venue: "Journal of Fraud, 12 (3), 1-9", year: "2021", citations: 5, url: "https://scholar.google.com/x", ...o });

describe("year range filter", () => {
  const rows = [row({ year: "2018" }), row({ year: "2020" }), row({ year: "2022" }), row({ year: "" })];
  it("keeps all rows with no bounds, including rows without a year", () => assert.equal(filterRowsByYear(rows, "", "").length, 4));
  it("applies both bounds inclusively", () => assert.deepEqual(filterRowsByYear(rows, "2018", "2020").map((r) => r.year), ["2018", "2020"]));
  it("open-ended from", () => assert.deepEqual(filterRowsByYear(rows, "2020", "").map((r) => r.year), ["2020", "2022"]));
  it("open-ended to", () => assert.deepEqual(filterRowsByYear(rows, "", "2018").map((r) => r.year), ["2018"]));
  it("swaps reversed bounds", () => assert.equal(filterRowsByYear(rows, "2022", "2018").length, 3));
  it("ignores invalid bounds as open", () => { assert.equal(parseYearBound("abc"), null); assert.equal(filterRowsByYear(rows, "x", "2018").length, 1); });
});

describe("BibTeX from profile rows", () => {
  it("escapes special characters and balances braces", () => {
    const out = buildProfileBibTeX([row({ title: "R&D 50% {of} {bad", venue: "" })]);
    assert.match(out, /title=\{R\\&D 50\\% of bad\}/);
    assert.match(out, /^@misc\{smith2021rd,/);
  });
  it("uses @article for a journal venue", () => assert.match(buildProfileBibTeX([row()]), /^@article\{smith2021deep,/));
  it("makes unique keys with a, b suffixes", () => {
    const out = buildProfileBibTeX([row(), row(), row()]);
    assert.match(out, /@article\{smith2021deep,/);
    assert.match(out, /@article\{smith2021deepa,/);
    assert.match(out, /@article\{smith2021deepb,/);
  });
  it("skips leading stop words and folds accents in keys", () => {
    assert.equal(profileCiteKey(row({ authors: "J Müller", title: "The Economy of Trust" })), "muller2021economy");
  });
  it("keeps truncated author lists as shown and notes it", () => {
    assert.deepEqual(splitProfileAuthors("A Smith, B Jones, ..."), { names: ["A Smith", "B Jones"], truncated: true });
    const out = buildProfileBibTeX([row({ authors: "A Smith, B Jones, ..." })]);
    assert.match(out, /author=\{A Smith and B Jones\}/);
    assert.match(out, /note=\{Author list shortened/);
  });
  it("returns an empty string for no rows", () => assert.equal(buildProfileBibTeX([]), ""));
});

describe("CSV from profile rows", () => {
  it("starts with a BOM and a header row, CRLF line ends", () => {
    const csv = buildProfileCsv([row()]);
    assert.equal(csv.charCodeAt(0), 0xfeff);
    assert.equal(csv.slice(1).split("\r\n")[0], "Title,Authors,Venue,Year,Citations,URL");
    assert.ok(csv.endsWith("\r\n"));
  });
  it("quotes commas, quotes and newlines", () => {
    assert.equal(csvCell("a,b"), '"a,b"');
    assert.equal(csvCell('say "hi"'), '"say ""hi"""');
    assert.equal(csvCell("line1\nline2"), '"line1\nline2"');
  });
  it("neutralises formula-like text but leaves numbers", () => {
    assert.equal(csvCell("=SUM(A1)"), "'=SUM(A1)");
    assert.equal(csvCell(-3), "-3");
  });
  it("writes citations as a number", () => assert.match(buildProfileCsv([row({ citations: "x" })]), /,2021,0,/));
});
