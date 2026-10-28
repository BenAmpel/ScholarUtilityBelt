import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { scholarPageLogEntry, searchLogToCsv } from "../src/common/reference-import.js";

const now = new Date(2026, 9, 28, 9, 30);

describe("Scholar page search-log entries", () => {
  it("records the query exactly, with year limits, sorting and paging", () => {
    const e = scholarPageLogEntry({ url: "https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+%22working+memory%22&as_ylo=2019&as_yhi=2024&start=10", papersCount: 10, added: 8, duplicates: 2, now });
    assert.equal(e.database, "Google Scholar");
    assert.equal(e.query, 'sleep deprivation "working memory" [years 2019-2024] [results from 11]');
    assert.equal(e.searchedOn, "2026-10-28");
    assert.equal(e.format, "scholar-page");
    assert.deepEqual([e.recordsInFile, e.added, e.duplicates], [10, 8, 2]);
    assert.match(e.fileName, /^https:\/\/scholar\.google\.com\/scholar\?/);
  });
  it("names author profiles", () => {
    const e = scholarPageLogEntry({ url: "https://scholar.google.com/citations?user=X", isAuthorProfile: true, authorName: "Ada Lovelace", papersCount: 20, now });
    assert.equal(e.database, "Google Scholar author profile");
    assert.equal(e.query, "Author profile: Ada Lovelace");
  });
  it("tolerates a bad URL and still fits the CSV export", () => {
    const e = scholarPageLogEntry({ url: "not a url", papersCount: 3, now });
    assert.equal(e.query, "");
    const csv = searchLogToCsv([e]);
    assert.match(csv.split("\n")[1], /^Google Scholar,2026-10-28,,,scholar-page,3,0,0$/);
  });
});

import { spreadsheetSafe } from "../src/common/reference-import.js";
describe("search-log CSV and spreadsheets", () => {
  it("neutralises cells that a spreadsheet would run as formulas", () => {
    assert.equal(spreadsheetSafe('=HYPERLINK("http://x","y")'), `'=HYPERLINK("http://x","y")`);
    assert.equal(spreadsheetSafe("+1"), "'+1");
    assert.equal(spreadsheetSafe("@SUM(A1)"), "'@SUM(A1)");
    assert.equal(spreadsheetSafe("sleep deprivation"), "sleep deprivation");
    assert.equal(spreadsheetSafe(-3), "-3", "real numbers keep their sign");
  });
  it("applies to the exported search log", () => {
    const csv = searchLogToCsv([{ database: "Google Scholar", query: "=cmd|' /c calc'!A0", recordsInFile: 1, added: 1, duplicates: 0 }]);
    assert.match(csv.split("\n")[1], /^Google Scholar,,'=cmd\|' \/c calc'!A0,/);
  });
});

describe("content-script CSV exports", () => {
  it("the shared csvEscape guards formula cells and keeps numbers", async () => {
    const fs = await import("node:fs");
    const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
    const fn = src.slice(src.indexOf("function csvEscape(value) {"), src.indexOf("/** Venue tier for version ordering"));
    // eslint-disable-next-line no-new-func
    const csvEscape = new Function(`${fn}; return csvEscape;`)();
    assert.equal(csvEscape("=1+1"), "'=1+1");
    assert.equal(csvEscape('=HYPERLINK("a","b")'), `"'=HYPERLINK(""a"",""b"")"`);
    assert.equal(csvEscape(-5), "-5");
    assert.equal(csvEscape("Deep learning"), "Deep learning");
  });
});
