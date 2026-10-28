import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { appendLog, logToCsv } from "../src/common/review-snapshots.js";
import { appendResolution, resolutionsToCsv } from "../src/common/screening-agreement.js";

// Paper titles come from Scholar and other databases, where anyone can publish a document whose title starts like a
// spreadsheet formula. Every CSV export must neutralise those cells.
const EVIL = '=HYPERLINK("http://example.invalid","x")';

describe("CSV exports neutralise formula-like text", () => {
  it("decision change log", () => {
    const log = appendLog([], [{ paperId: "p1", reviewerId: "a", old: "unscreened", new: "include" }], Date.UTC(2026, 9, 28));
    const csv = logToCsv(log, () => EVIL, () => "@reviewer");
    assert.match(csv, /"'=HYPERLINK\(""http:\/\/example\.invalid"",""x""\)"/);
    assert.match(csv, /,'@reviewer,/);
  });
  it("screening resolutions", () => {
    const log = appendResolution([], { paperId: "p1", resolvedBy: "+Dana", decision: "include", note: "-2 points" }, Date.UTC(2026, 9, 28));
    const csv = resolutionsToCsv(log, () => EVIL);
    assert.match(csv, /'\+Dana/);
    assert.match(csv, /'-2 points/);
    assert.doesNotMatch(csv, /,=HYPERLINK/);
  });
});
