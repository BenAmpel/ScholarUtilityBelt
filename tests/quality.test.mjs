import { describe, it } from "node:test";
import assert from "node:assert/strict";

// quality.js uses ES module exports but the project is commonjs.
// Dynamic import works because Node treats .js with export as ESM when loaded via import().
const quality = await import("../src/common/quality.js");
const {
  normalizeVenueName,
  extractVenueFromAuthorsVenue,
  normalizeVhbRank,
  compileQualityIndex,
  isPreprintVenue,
  qualityBadgesForVenue,
  venueWeightForVenue,
} = quality;

// ---------------------------------------------------------------------------
// normalizeVenueName
// ---------------------------------------------------------------------------
describe("normalizeVenueName", () => {
  it("lowercases and trims whitespace", () => {
    assert.equal(normalizeVenueName("  Nature  "), "nature");
  });

  it("replaces & with 'and'", () => {
    assert.equal(normalizeVenueName("Science & Engineering"), "science and engineering");
  });

  it("strips non-alphanumeric characters", () => {
    assert.equal(normalizeVenueName("J. of A.I."), "j of a i");
  });

  it("collapses multiple spaces", () => {
    assert.equal(normalizeVenueName("IEEE   Internet   Things"), "ieee internet things");
  });

  it("strips 'Proceedings of the'", () => {
    const result = normalizeVenueName("Proceedings of the 30th ACM SIGKDD Conference");
    // Also strips leading ordinal "30th"
    assert.equal(result, "acm sigkdd conference");
  });

  it("strips 'Proceedings of' (without 'the')", () => {
    const result = normalizeVenueName("Proceedings of ACM CHI");
    assert.equal(result, "acm chi");
  });

  it("strips leading 'The'", () => {
    assert.equal(normalizeVenueName("The China Quarterly"), "china quarterly");
  });

  it("strips leading ordinal", () => {
    assert.equal(normalizeVenueName("1st International Workshop"), "international workshop");
  });

  it("strips leading spelled-out ordinal + annual (NeurIPS-style proceedings titles)", () => {
    assert.equal(
      normalizeVenueName("The Fortieth Annual Conference on Neural Information Processing Systems (NeurIPS 2026), Evaluations & Datasets Track"),
      "conference on neural information processing systems neurips 2026 evaluations and datasets track"
    );
    assert.equal(
      normalizeVenueName("Thirty-Ninth Annual Conference on Neural Information Processing Systems"),
      "conference on neural information processing systems"
    );
    assert.equal(normalizeVenueName("Twenty-First Annual Conference on X"), "conference on x");
  });

  it("does not touch a digit-ordinal + annual (unchanged existing behavior)", () => {
    assert.equal(normalizeVenueName("3rd Annual Conference"), "annual conference");
  });

  it("strips leading year", () => {
    assert.equal(normalizeVenueName("2019 IEEE International Conference"), "ieee international conference");
  });

  it("strips trailing page range", () => {
    assert.equal(normalizeVenueName("Conference AMCIS 1-10"), "conference amcis");
  });

  it("strips trailing volume/issue/pages numbers", () => {
    const result = normalizeVenueName("IEEE Internet of Things Journal 7 9 9128 9143");
    assert.equal(result, "ieee internet of things journal");
  });

  it("strips trailing parenthetical abbreviation (TOIS)", () => {
    // After normalizing, parenthetical becomes lowercase token
    const result = normalizeVenueName("ACM Transactions on Information Systems (TOIS) 27 2 1 19");
    assert.equal(result, "acm transactions on information systems");
  });

  it("handles null/undefined input", () => {
    assert.equal(normalizeVenueName(null), "");
    assert.equal(normalizeVenueName(undefined), "");
    assert.equal(normalizeVenueName(""), "");
  });
});

// ---------------------------------------------------------------------------
// extractVenueFromAuthorsVenue
// ---------------------------------------------------------------------------
describe("extractVenueFromAuthorsVenue", () => {
  it("extracts venue from 'Author - Journal, Year' format", () => {
    const result = extractVenueFromAuthorsVenue("A Smith, B Jones - Nature, 2019 - publisher.com");
    assert.equal(result, "Nature");
  });

  it("extracts venue from author profile format with volume/issue", () => {
    const result = extractVenueFromAuthorsVenue("A Smith - Journal of AI 41 (1), 236-265, 2024");
    assert.equal(result, "Journal of AI");
  });

  it("handles empty/null input", () => {
    assert.equal(extractVenueFromAuthorsVenue(""), "");
    assert.equal(extractVenueFromAuthorsVenue(null), "");
    assert.equal(extractVenueFromAuthorsVenue(undefined), "");
  });

  it("strips volume/issue numbers from venue", () => {
    const result = extractVenueFromAuthorsVenue("X Author - MIS Quarterly 42 (3), 100-120, 2021 - pub");
    assert.equal(result, "MIS Quarterly");
  });

  it("handles publisher dot separator", () => {
    const result = extractVenueFromAuthorsVenue("A Author - Science · AAAS");
    assert.equal(result, "Science");
  });

  it("handles single-part input with year", () => {
    const result = extractVenueFromAuthorsVenue("Nature Communications, 2023");
    assert.equal(result, "Nature Communications");
  });
});

// ---------------------------------------------------------------------------
// normalizeVhbRank
// ---------------------------------------------------------------------------
describe("normalizeVhbRank", () => {
  it("normalizes A+ rank (star to plus)", () => {
    assert.equal(normalizeVhbRank("A*"), "A+");
  });

  it("normalizes plain letter ranks", () => {
    assert.equal(normalizeVhbRank("A"), "A");
    assert.equal(normalizeVhbRank("B"), "B");
    assert.equal(normalizeVhbRank("C"), "C");
    assert.equal(normalizeVhbRank("D"), "D");
    assert.equal(normalizeVhbRank("E"), "E");
  });

  it("handles lowercase input", () => {
    assert.equal(normalizeVhbRank("a"), "A");
    assert.equal(normalizeVhbRank("a*"), "A+");
  });

  it("handles whitespace", () => {
    assert.equal(normalizeVhbRank("  A  "), "A");
  });

  it("returns empty string for invalid input", () => {
    assert.equal(normalizeVhbRank(""), "");
    assert.equal(normalizeVhbRank(null), "");
    assert.equal(normalizeVhbRank("XYZ"), "");
  });
});

// ---------------------------------------------------------------------------
// isPreprintVenue
// ---------------------------------------------------------------------------
describe("isPreprintVenue", () => {
  it("detects arXiv", () => {
    assert.equal(isPreprintVenue("arXiv preprint arXiv:2103.00001"), true);
  });

  it("detects SSRN", () => {
    assert.equal(isPreprintVenue("Available at SSRN 1234567"), true);
  });

  it("detects bioRxiv", () => {
    assert.equal(isPreprintVenue("bioRxiv"), true);
  });

  it("detects medRxiv", () => {
    assert.equal(isPreprintVenue("medRxiv 2021.01.01"), true);
  });

  it("does not flag regular journals", () => {
    assert.equal(isPreprintVenue("Nature"), false);
    assert.equal(isPreprintVenue("MIS Quarterly"), false);
  });

  it("handles null/empty", () => {
    assert.equal(isPreprintVenue(""), false);
    assert.equal(isPreprintVenue(null), false);
  });
});

// ---------------------------------------------------------------------------
// compileQualityIndex
// ---------------------------------------------------------------------------
describe("compileQualityIndex", () => {
  it("returns an object with expected maps/sets", () => {
    const idx = compileQualityIndex({});
    assert.ok(idx.ft50 instanceof Set);
    assert.ok(idx.utd24 instanceof Set);
    assert.ok(idx.abdc instanceof Map);
    assert.ok(idx.vhb instanceof Map);
    assert.ok(idx.quartiles instanceof Map);
  });

  it("parses FT50 list entries", () => {
    const settings = { qualityFt50List: "Nature\nScience\n" };
    const idx = compileQualityIndex(settings);
    assert.ok(idx.ft50.has("nature"));
    assert.ok(idx.ft50.has("science"));
    assert.equal(idx.ft50.size, 2);
  });

  it("parses UTD24 list entries", () => {
    const settings = { qualityUtd24List: "MIS Quarterly\nManagement Science" };
    const idx = compileQualityIndex(settings);
    assert.ok(idx.utd24.has("mis quarterly"));
    assert.ok(idx.utd24.has("management science"));
  });

  it("parses ABDC CSV ranks", () => {
    const settings = { qualityAbdcRanks: "Nature,A*\nScience,A\nFoo Journal,B\n" };
    const idx = compileQualityIndex(settings);
    assert.equal(idx.abdc.get("nature"), "A*");
    assert.equal(idx.abdc.get("science"), "A");
    assert.equal(idx.abdc.get("foo journal"), "B");
  });

  it("skips invalid ABDC ranks", () => {
    const settings = { qualityAbdcRanks: "Bad Journal,Z\n" };
    const idx = compileQualityIndex(settings);
    assert.equal(idx.abdc.size, 0);
  });

  it("parses VHB CSV ranks and normalizes", () => {
    const settings = { qualityVhbRanks: "Top Journal,A*\nGood Journal,B\n" };
    const idx = compileQualityIndex(settings);
    assert.equal(idx.vhb.get("top journal"), "A+");
    assert.equal(idx.vhb.get("good journal"), "B");
  });

  it("parses quartiles", () => {
    const settings = { qualityQuartiles: "Nature,Q1\nOther,Q3\n" };
    const idx = compileQualityIndex(settings);
    assert.equal(idx.quartiles.get("nature"), "Q1");
    assert.equal(idx.quartiles.get("other"), "Q3");
  });

  it("ignores comment lines starting with #", () => {
    const settings = { qualityFt50List: "# comment\nNature\n" };
    const idx = compileQualityIndex(settings);
    assert.equal(idx.ft50.size, 1);
    assert.ok(idx.ft50.has("nature"));
  });

  it("loads extra quartiles index", () => {
    const idx = compileQualityIndex({}, { quartilesIndex: { "Nature": "Q1" } });
    assert.equal(idx.quartiles.get("nature"), "Q1");
  });

  it("user settings override extra quartiles", () => {
    const settings = { qualityQuartiles: "Nature,Q2" };
    const idx = compileQualityIndex(settings, { quartilesIndex: { "Nature": "Q1" } });
    // User override should win
    assert.equal(idx.quartiles.get("nature"), "Q2");
  });
});

// ---------------------------------------------------------------------------
// qualityBadgesForVenue (basic smoke test)
// ---------------------------------------------------------------------------
describe("qualityBadgesForVenue", () => {
  it("returns empty array for unknown venue", () => {
    const idx = compileQualityIndex({});
    const badges = qualityBadgesForVenue("Unknown Journal 12345", idx);
    assert.deepEqual(badges, []);
  });

  it("returns ABDC badge for indexed venue", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Nature,A*" });
    const badges = qualityBadgesForVenue("Nature", idx);
    const abdcBadge = badges.find((b) => b.kind === "abdc");
    assert.ok(abdcBadge);
    assert.equal(abdcBadge.metadata.rank, "A*");
  });

  it("returns preprint badge for arXiv", () => {
    const idx = compileQualityIndex({});
    const badges = qualityBadgesForVenue("arXiv preprint", idx);
    const preprintBadge = badges.find((b) => b.kind === "preprint");
    assert.ok(preprintBadge);
    assert.equal(preprintBadge.text, "arXiv");
  });

  it("returns FT50 badge", () => {
    const idx = compileQualityIndex({ qualityFt50List: "Nature" });
    const badges = qualityBadgesForVenue("Nature", idx);
    const ft50Badge = badges.find((b) => b.kind === "ft50");
    assert.ok(ft50Badge, "should have FT50 badge");
  });

  it("returns CORE A* badge for NeurIPS's official spelled-out-ordinal proceedings title", () => {
    const idx = compileQualityIndex({
      qualityCoreRanks:
        "Advances in Neural Information Processing Systems (was NIPS)|NeurIPS|Conference on Neural Information Processing Systems,A*",
    });
    const badges = qualityBadgesForVenue(
      "The Fortieth Annual Conference on Neural Information Processing Systems (NeurIPS 2026), Evaluations & Datasets Track",
      idx
    );
    const coreBadge = badges.find((b) => b.kind === "core");
    assert.ok(coreBadge, "should have a CORE badge");
    assert.equal(coreBadge.metadata.rank, "A*");
  });
});
