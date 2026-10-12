import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { parseCustomVenueCsv, compileCustomVenueIndex, cleanListName, MAX_ROWS, MAX_LABEL_LENGTH } =
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
