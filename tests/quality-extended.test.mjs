import { describe, it } from "node:test";
import assert from "node:assert/strict";

const quality = await import("../src/common/quality.js");
const {
  normalizeVenueName,
  compileQualityIndex,
  qualityBadgesForVenue,
  isPreprintVenue,
  extractVenueFromAuthorsVenue,
} = quality;

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

describe("badges say when they come from a close (prefix) match", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  const idx = compileQualityIndex({ qualityAbdcRanks: "Journal of Information Technology,A\nMIS Quarterly Management Information Systems,A*" });
  it("an exact match carries no near-match note", () => {
    const b = qualityBadgesForVenue("Journal of Information Technology", idx).find((x) => x.kind === "abdc");
    assert.ok(b);
    assert.equal(b.metadata.nearMatch, undefined);
  });
  it("a prefix match names the list entry and the venue it read", () => {
    const b = qualityBadgesForVenue("MIS Quarterly", idx).find((x) => x.kind === "abdc");
    assert.ok(b);
    assert.equal(b.text, "ABDC A*");
    assert.equal(b.metadata.nearMatch, "mis quarterly management information systems");
    assert.equal(b.metadata.venueRead, "mis quarterly");
  });
});

describe("prefix matches never guess between different venues", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  it("no h5 badge when the venue only begins other journals' names (shipped bug: J Neurosci showed J Neurosci Nursing's 52)", () => {
    const idx = compileQualityIndex({}, { h5Index: { "journal of neuroscience nursing": 52, "journal of neuroscience psychology and economics": 31 } });
    assert.equal(qualityBadgesForVenue("Journal of Neuroscience", idx).find((b) => b.kind === "h5"), undefined);
  });
  it("entries that all agree still give the badge, and say how many matched", () => {
    const idx = compileQualityIndex({ qualityQuartiles: "MIS Quarterly Executive,Q1\nMIS Quarterly Management Information Systems,Q1" });
    const b = qualityBadgesForVenue("MIS Quarterly", idx).find((x) => x.kind === "quartile");
    assert.equal(b?.text, "Q1");
    assert.equal(b.metadata.nearMatchCount, 2);
  });
  it("a list's colon subtitle makes the short name an exact match", () => {
    const idx = compileQualityIndex({}, { absIndex: new Map([["MIS Quarterly: Management Information Systems", "4*"], ["MIS Quarterly Executive", "2"]]) });
    const b = qualityBadgesForVenue("MIS Quarterly", idx).find((x) => x.kind === "abs");
    assert.equal(b?.text, "ABS 4*");
    assert.equal(b.metadata.nearMatch, undefined);
  });
  it("a journal listed under the short name itself beats a subtitle alias", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Information Policy: Studies in Something,B\nInformation Policy,A" });
    assert.equal(qualityBadgesForVenue("Information Policy", idx).find((x) => x.kind === "abdc")?.text, "ABDC A");
  });
  it("two subtitled entries with the same short name and different ranks add no alias", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Journal of Things: Series A,A\nJournal of Things: Series B,C" });
    assert.equal(idx.abdc.has("journal of things"), false);
  });
});

describe("a known journal never takes another known journal's badge", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  const sjr = { "acta orthopaedica": "Q2", "acta orthopaedica belgica": "Q4", "acoustics": "Q2", "acoustics australia": "Q3",
    "national academy of sciences india section a physical sciences": "Q3", "computer": "Q1" };
  const h5Of = (venue, h5Index) => qualityBadgesForVenue(venue, compileQualityIndex({}, { quartilesIndex: sjr, h5Index })).find((b) => b.kind === "h5")?.text;
  it("forward: Acoustics does not show Acoustics Australia's h5", () => {
    assert.equal(h5Of("Acoustics", { "acoustics australia": 31 }), undefined);
  });
  it("reverse: Acta Orthopaedica Belgica does not show Acta Orthopaedica's h5", () => {
    assert.equal(h5Of("Acta Orthopaedica Belgica", { "acta orthopaedica": 136 }), undefined);
  });
  it("reverse: a complete title does not borrow a shorter list entry (PNAS India is not PNAS)", () => {
    assert.equal(h5Of("Proceedings of the National Academy of Sciences, India Section A: Physical Sciences", { "national academy of sciences": 242 }), undefined);
  });
  it("a one-word generic venue does not prefix-match (Computer -> Computer and Telecommunications Law Review)", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Computer and Telecommunications Law Review,C" }, { quartilesIndex: sjr });
    assert.equal(qualityBadgesForVenue("Computer", idx).find((b) => b.kind === "abdc"), undefined);
  });
  it("a truncated Scholar venue still matches its one list entry", () => {
    assert.equal(h5Of("Journal of Experimental Psychology: Learning", { "journal of experimental psychology learning memory and cognition": 80 }), "h5: 80");
  });
});

describe("ERA goes through the shared near-match rules", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  const eraSet = new Set(["findings", "punishment and society the international journal of penology"]);
  const idx = compileQualityIndex({}, { eraSet, quartilesIndex: { "punishment and society": "Q1" } });
  it("ACL Findings does not take the journal Findings' ERA listing", () => {
    assert.equal(qualityBadgesForVenue("Findings of the Association for Computational Linguistics: EMNLP 2023", idx).find((b) => b.kind === "era"), undefined);
  });
  it("a full title matches its subtitled ERA entry, vetted (no warning edge)", () => {
    const b = qualityBadgesForVenue("Punishment & Society", idx).find((x) => x.kind === "era");
    assert.equal(b?.text, "ERA 2023");
    assert.equal(b.metadata.nearMatchVetted, true);
  });
});
