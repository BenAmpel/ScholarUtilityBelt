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
