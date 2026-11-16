import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { mergeCustomVenueList, describeMerge, parseCustomVenueCsv, compileCustomVenueIndex, cleanListName, MAX_ROWS, MAX_LABEL_LENGTH } =
  await import("../src/common/custom-venues.js");
const { customBadgeForVenue } = await import("../src/common/quality.js");

describe("parseCustomVenueCsv", () => {
  it("reads a headered file in any column order", () => {
    const r = parseCustomVenueCsv("Tier,Journal\nA,MIS Quarterly\nB,Decision Support Systems\n");
    assert.deepEqual(r.entries, [["MIS Quarterly", "A"], ["Decision Support Systems", "B"]]);
    assert.equal(r.error, "");
  });

  it("reads a headerless two-column file", () => {
    const r = parseCustomVenueCsv("Nature,Tier 1\nScience,Tier 1\n");
    assert.deepEqual(r.entries, [["Nature", "Tier 1"], ["Science", "Tier 1"]]);
  });

  it("handles quotes, embedded commas, doubled quotes, CRLF and a BOM", () => {
    const r = parseCustomVenueCsv('\uFEFFvenue,tier\r\n"Journal of ""Big"", Small Data",A\r\n');
    assert.deepEqual(r.entries, [['Journal of "Big", Small Data', "A"]]);
  });

  it("detects semicolon and tab delimiters", () => {
    assert.deepEqual(parseCustomVenueCsv("venue;rank\nNature;A").entries, [["Nature", "A"]]);
    assert.deepEqual(parseCustomVenueCsv("venue\ttier\nNature\tA").entries, [["Nature", "A"]]);
  });

  it("skips rows missing a venue or label and repeated venues, and counts them", () => {
    const r = parseCustomVenueCsv("venue,tier\nNature,A\n,B\nScience,\nnature,C\n");
    assert.deepEqual(r.entries, [["Nature", "A"]]);
    assert.equal(r.skipped, 3);
  });

  it("truncates long labels", () => {
    const r = parseCustomVenueCsv(`venue,tier\nNature,${"x".repeat(40)}`);
    assert.equal(r.entries[0][1].length, MAX_LABEL_LENGTH);
  });

  it("caps the number of rows", () => {
    const rows = Array.from({ length: MAX_ROWS + 5 }, (_, i) => `Journal Number ${i} Quarterly,A`).join("\n");
    const r = parseCustomVenueCsv(rows);
    assert.equal(r.entries.length, MAX_ROWS);
    assert.equal(r.truncated, true);
  });

  it("reports an error for empty or unusable input", () => {
    assert.match(parseCustomVenueCsv("").error, /empty/i);
    assert.match(parseCustomVenueCsv("   \n  ").error, /empty/i);
    assert.match(parseCustomVenueCsv("venue,tier\n,\n").error, /no usable rows/i);
    assert.match(parseCustomVenueCsv("justonecolumn\nNature").error, /no usable rows/i);
  });

  it("never throws on odd input", () => {
    for (const bad of [null, undefined, 42, '"unterminated,A', "\u0000\u0001"]) {
      assert.doesNotThrow(() => parseCustomVenueCsv(bad));
    }
  });
});

describe("custom badge lookup", () => {
  const entries = [["MIS Quarterly", "A"], ["Information Systems Research", "A"], ["Decision Support Systems", "B"]];
  const custom = { name: "Dean's list", map: compileCustomVenueIndex(entries) };

  it("matches ignoring case and punctuation", () => {
    const b = customBadgeForVenue("mis quarterly.", custom);
    assert.equal(b.kind, "custom");
    assert.equal(b.text, "A");
    assert.match(b.metadata.system, /Dean's list/);
  });

  it("matches a Scholar venue string that continues past the listed name", () => {
    assert.equal(customBadgeForVenue("Decision Support Systems 142", custom)?.text, "B");
  });

  it("returns null for unlisted venues, an empty venue, or no list", () => {
    assert.equal(customBadgeForVenue("Journal of Unrelated Things", custom), null);
    assert.equal(customBadgeForVenue("", custom), null);
    assert.equal(customBadgeForVenue("MIS Quarterly", null), null);
    assert.equal(customBadgeForVenue("MIS Quarterly", { name: "x", map: new Map() }), null);
  });
});

describe("cleanListName", () => {
  it("trims, collapses whitespace, limits length and defaults", () => {
    assert.equal(cleanListName("  Dean   list "), "Dean list");
    assert.equal(cleanListName("x".repeat(100)).length, 40);
    assert.equal(cleanListName(""), "My list");
    assert.equal(cleanListName(undefined), "My list");
  });
});

describe("mergeCustomVenueList", () => {
  const base = () => ({ name: "Dept", uploadedAt: "2026-01-01T00:00:00.000Z", entries: [["MIS Quarterly", "A"], ["Nature", "B"]] });
  const now = new Date("2026-11-16T00:00:00Z");

  it("adds new venues and keeps existing entries", () => {
    const r = mergeCustomVenueList(base(), [["Science", "A"], ["Cell", "C"]], { now });
    assert.deepEqual(r.list.entries, [["MIS Quarterly", "A"], ["Nature", "B"], ["Science", "A"], ["Cell", "C"]]);
    assert.equal(r.added, 2);
    assert.equal(r.updated, 0);
    assert.equal(r.list.name, "Dept");
  });

  it("matches duplicates by normalized name and the uploaded row wins", () => {
    const r = mergeCustomVenueList(base(), [["NATURE.", "A*"], ["mis quarterly", "A"], ["Cell", "C"]], { now });
    assert.deepEqual(r.list.entries[1], ["NATURE.", "A*"]);
    assert.equal(r.list.entries.length, 3);
    assert.equal(r.added, 1);
    assert.equal(r.updated, 1);
    assert.equal(r.unchanged, 1);
    assert.equal(describeMerge(r).startsWith("Added 1 venue, updated 1."), true);
  });

  it("does not change its inputs", () => {
    const b = base();
    mergeCustomVenueList(b, [["Nature", "Z"]], { now });
    assert.deepEqual(b, base());
  });

  it("starts a list when none exists", () => {
    const r = mergeCustomVenueList(null, [["Nature", "A"]], { name: "  Mine ", now });
    assert.equal(r.list.name, "Mine");
    assert.deepEqual(r.list.entries, [["Nature", "A"]]);
  });

  it("stops at MAX_ROWS and counts what was dropped", () => {
    const full = { name: "x", uploadedAt: "", entries: Array.from({ length: MAX_ROWS }, (_, i) => [`Journal ${i} Alpha`, "A"]) };
    const r = mergeCustomVenueList(full, [["Brand New Venue", "A"], ["Journal 3 Alpha", "B"]], { now });
    assert.equal(r.added, 0);
    assert.equal(r.dropped, 1);
    assert.equal(r.updated, 1);
    assert.equal(r.list.entries.length, MAX_ROWS);
  });

  it("replace path is unchanged: a parsed upload is stored as the whole list", () => {
    const parsed = parseCustomVenueCsv("venue,tier\nScience,A\n");
    assert.deepEqual(parsed.entries, [["Science", "A"]]);
  });
});
