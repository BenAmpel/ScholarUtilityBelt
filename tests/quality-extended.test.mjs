import { describe, it } from "node:test";
import assert from "node:assert/strict";

const quality = await import("../src/common/quality.js");
const {
  normalizeVenueName,
  compileQualityIndex,
  qualityBadgesForVenue,
  venueWeightForVenue,
  isPreprintVenue,
  extractVenueFromAuthorsVenue,
} = quality;

// ---------------------------------------------------------------------------
// venueWeightForVenue (untested in original suite)
// ---------------------------------------------------------------------------
describe("venueWeightForVenue", () => {
  it("returns higher weight for FT50 venue", () => {
    const idx = compileQualityIndex({ qualityFt50List: "Nature" });
    const weight = venueWeightForVenue("Nature", idx);
    assert.ok(weight > 0.5, `Expected weight > 0.5, got ${weight}`);
  });

  it("returns higher weight for UTD24 venue", () => {
    const idx = compileQualityIndex({ qualityUtd24List: "MIS Quarterly" });
    const weight = venueWeightForVenue("MIS Quarterly", idx);
    assert.ok(weight > 0.5, `Expected weight > 0.5, got ${weight}`);
  });

  it("returns low weight for preprint", () => {
    const idx = compileQualityIndex({});
    const weight = venueWeightForVenue("arXiv preprint", idx);
    assert.ok(weight < 0.5, `Expected weight < 0.5 for preprint, got ${weight}`);
  });

  it("returns baseline 0.2 for unknown venue", () => {
    const idx = compileQualityIndex({});
    const weight = venueWeightForVenue("Unknown Journal 999", idx);
    assert.equal(weight, 0.2);
  });

  it("returns higher weight for ABDC A* venue", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Top Journal,A*" });
    const weight = venueWeightForVenue("Top Journal", idx);
    assert.ok(weight > 0.5, `Expected weight > 0.5, got ${weight}`);
  });

  it("handles null/empty venue — returns baseline", () => {
    const idx = compileQualityIndex({});
    assert.equal(venueWeightForVenue("", idx), 0.2);
    assert.equal(venueWeightForVenue(null, idx), 0.2);
  });
});

// ---------------------------------------------------------------------------
// qualityBadgesForVenue — extended coverage
// ---------------------------------------------------------------------------
describe("qualityBadgesForVenue (extended)", () => {
  it("returns UTD24 badge", () => {
    const idx = compileQualityIndex({ qualityUtd24List: "Management Science" });
    const badges = qualityBadgesForVenue("Management Science", idx);
    const utd = badges.find((b) => b.kind === "utd24");
    assert.ok(utd, "should have UTD24 badge");
  });

  it("returns VHB badge", () => {
    const idx = compileQualityIndex({ qualityVhbRanks: "MIS Quarterly,A*" });
    const badges = qualityBadgesForVenue("MIS Quarterly", idx);
    const vhb = badges.find((b) => b.kind === "vhb");
    assert.ok(vhb, "should have VHB badge");
    assert.equal(vhb.metadata.rank, "A+");
  });

  it("returns quartile badge", () => {
    const idx = compileQualityIndex({ qualityQuartiles: "Nature,Q1" });
    const badges = qualityBadgesForVenue("Nature", idx);
    const q = badges.find((b) => b.kind === "quartile");
    assert.ok(q, "should have quartile badge");
  });

  it("returns multiple badges for well-ranked venue", () => {
    const idx = compileQualityIndex({
      qualityFt50List: "Nature",
      qualityAbdcRanks: "Nature,A*",
      qualityQuartiles: "Nature,Q1",
    });
    const badges = qualityBadgesForVenue("Nature", idx);
    assert.ok(badges.length >= 3, `Expected >= 3 badges, got ${badges.length}`);
  });

  it("detects SSRN as preprint", () => {
    const idx = compileQualityIndex({});
    const badges = qualityBadgesForVenue("Available at SSRN 3456789", idx);
    const pp = badges.find((b) => b.kind === "preprint");
    assert.ok(pp, "should detect SSRN as preprint");
  });

  it("handles venue with trailing numbers", () => {
    const idx = compileQualityIndex({
      qualityAbdcRanks: "IEEE Internet of Things Journal,A*",
    });
    const badges = qualityBadgesForVenue(
      "IEEE Internet of Things Journal 7 9 9128 9143",
      idx
    );
    const abdc = badges.find((b) => b.kind === "abdc");
    assert.ok(abdc, "should match after stripping trailing numbers");
  });
});

// ---------------------------------------------------------------------------
// normalizeVenueName — edge cases
// ---------------------------------------------------------------------------
describe("normalizeVenueName (edge cases)", () => {
  it("handles unicode characters", () => {
    const result = normalizeVenueName("Revue Française de Gestion");
    assert.ok(result.length > 0);
  });

  it("handles very long venue names", () => {
    const longName = "International Conference on " + "Very ".repeat(50) + "Important Research";
    const result = normalizeVenueName(longName);
    assert.ok(result.length > 0);
  });

  it("strips ordinals: 2nd, 3rd, 4th, 21st, 22nd, 23rd", () => {
    assert.equal(normalizeVenueName("2nd Workshop on AI"), "workshop on ai");
    assert.equal(normalizeVenueName("3rd Annual Conference"), "annual conference");
    assert.equal(normalizeVenueName("21st Symposium"), "symposium");
  });

  it("handles multiple year-like numbers", () => {
    const result = normalizeVenueName("2021 IEEE/ACM Conference 2021");
    assert.ok(!result.includes("2021"));
  });
});

// ---------------------------------------------------------------------------
// extractVenueFromAuthorsVenue — edge cases
// ---------------------------------------------------------------------------
describe("extractVenueFromAuthorsVenue (edge cases)", () => {
  it("handles multiple authors with commas", () => {
    const result = extractVenueFromAuthorsVenue(
      "A Smith, B Jones, C Lee - Journal of Computing, 2020 - springer.com"
    );
    assert.equal(result, "Journal of Computing");
  });

  it("handles venue with special characters", () => {
    const result = extractVenueFromAuthorsVenue(
      "A Author - ACM Trans. Inf. Syst., 2021 - acm.org"
    );
    assert.ok(result.includes("ACM"));
  });

  it("handles no publisher part", () => {
    const result = extractVenueFromAuthorsVenue("A Author - Science, 2020");
    assert.equal(result, "Science");
  });
});

// ---------------------------------------------------------------------------
// isPreprintVenue — edge cases
// ---------------------------------------------------------------------------
describe("isPreprintVenue (edge cases)", () => {
  it("detects Research Square", () => {
    assert.equal(isPreprintVenue("Research Square"), true);
  });

  it("detects chemRxiv", () => {
    assert.equal(isPreprintVenue("chemRxiv 2021"), true);
  });

  it("does not flag journals with 'preprint' in author metadata", () => {
    assert.equal(isPreprintVenue("Journal of Preprint Studies"), false);
  });
});

// ---------------------------------------------------------------------------
// compileQualityIndex — edge cases
// ---------------------------------------------------------------------------
describe("compileQualityIndex (edge cases)", () => {
  it("handles empty strings in all fields", () => {
    const idx = compileQualityIndex({
      qualityFt50List: "",
      qualityUtd24List: "",
      qualityAbdcRanks: "",
      qualityVhbRanks: "",
      qualityQuartiles: "",
    });
    assert.equal(idx.ft50.size, 0);
    assert.equal(idx.utd24.size, 0);
    assert.equal(idx.abdc.size, 0);
  });

  it("handles duplicate entries in lists", () => {
    const idx = compileQualityIndex({ qualityFt50List: "Nature\nNature\nnature" });
    assert.equal(idx.ft50.size, 1);
  });

  it("handles ABDC ranks with extra whitespace", () => {
    const idx = compileQualityIndex({
      qualityAbdcRanks: "  Nature  ,  A*  \n  Science , A  ",
    });
    assert.equal(idx.abdc.get("nature"), "A*");
    assert.equal(idx.abdc.get("science"), "A");
  });

  it("rejects invalid quartile values", () => {
    const idx = compileQualityIndex({
      qualityQuartiles: "BadJournal,Q5\nGoodJournal,Q1",
    });
    assert.equal(idx.quartiles.has("badjournal"), false);
    assert.equal(idx.quartiles.get("goodjournal"), "Q1");
  });
});
