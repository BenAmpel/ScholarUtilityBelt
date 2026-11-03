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

describe("FT50 and UTD24 go through the same known-title and specificity rules", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  const list = "Information Systems Research\nManagement Science\nProduction and Operations Management";
  const idx = compileQualityIndex({ qualityFt50List: list, qualityUtd24List: list }, { quartilesIndex: { "information systems": "Q1" } });
  const kinds = (venue) => qualityBadgesForVenue(venue, idx).map((b) => b.kind).filter((k) => k === "ft50" || k === "utd24");
  it("the journal Information Systems (Elsevier) is not Information Systems Research", () => {
    assert.deepEqual(kinds("Information Systems"), []);
  });
  it("a one-word venue does not prefix-match (Management is not Management Science)", () => {
    assert.deepEqual(kinds("Management"), []);
  });
  it("a truncated Scholar venue still matches, with the entry shown on hover", () => {
    const b = qualityBadgesForVenue("Production and Operations", idx).find((x) => x.kind === "ft50");
    assert.equal(b?.metadata.nearMatch, "production and operations management");
    assert.deepEqual(kinds("Production and Operations"), ["ft50", "utd24"]);
  });
  it("an exact title still matches", () => {
    assert.deepEqual(kinds("Management Science"), ["ft50", "utd24"]);
  });
});

describe("venue lines that are not the list entry they start with", async () => {
  const { compileQualityIndex, qualityBadgesForVenue, extractVenueFromAuthorsVenue, customBadgeForVenue } = await import("../src/common/quality.js");
  const kinds = (venue, idx) => qualityBadgesForVenue(venue, idx).map((b) => b.text);
  const core = compileQualityIndex({
    qualityCoreRanks: "International Conference on Computer Vision|ICCV,A*\nICC,B\nACM Conference on Computer and Communications Security|CCS,A*\nHawaii International Conference on System Sciences|HICSS,A\nInternational Conference on Information Systems|ICIS,A*\nICIS,C\nDSN,C\nDSN,A",
  });
  it("keeps the V of ICCV and ECCV when it strips volume numbers", () => {
    assert.equal(extractVenueFromAuthorsVenue("J Smith - ICCV 2021"), "ICCV 2021");
    assert.equal(extractVenueFromAuthorsVenue("J Smith - Journal of X V. 41, 2021"), "Journal of X");
    assert.deepEqual(kinds("ICCV 2021", core), ["CORE A*"]);
  });
  it("a workshop, companion or doctoral consortium does not take the main conference's rank", () => {
    assert.deepEqual(kinds("ACM Conference on Computer and Communications Security Workshops", core), []);
    assert.deepEqual(kinds("Hawaii International Conference on System Sciences: Doctoral Consortium", core), []);
    assert.deepEqual(kinds("ICIS 2019 Workshop on Foo", core), []);
  });
  it("bare ICIS is the A* conference, not the other ICIS", () => {
    assert.deepEqual(kinds("ICIS", core), ["CORE A*"]);
  });
  it("the later CORE row wins for an acronym in both lists (ICORE2026 is loaded last)", () => {
    assert.deepEqual(kinds("DSN", core), ["CORE A"]);
  });
  it("a longer title does not borrow a shorter journal's badges", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Management Science,A*" });
    assert.deepEqual(kinds("Management Science and Engineering", idx), []);
    assert.deepEqual(kinds("Academy of Management Review, Special Issue", compileQualityIndex({ qualityAbdcRanks: "Academy of Management Review,A*" })), ["ABDC A*"]);
  });
  it("a cut-off venue is not the short journal with that name, and an ambiguous cut gets nothing", () => {
    const quartilesIndex = { "information systems": "Q1", "information systems research": "Q1", "information systems journal": "Q1",
      "production and operations management": "Q1" };
    const idx = compileQualityIndex({ qualityFt50List: "Information Systems Research\nProduction and Operations Management" }, { quartilesIndex });
    assert.deepEqual(kinds("Information Systems …", idx), []);
    assert.deepEqual(kinds("… Science", idx), []);
    assert.deepEqual(kinds("Production and Operations …", idx), ["Q1", "FT50"]);
    assert.deepEqual(kinds("Information Systems", idx), ["Q1"]);
  });
  it("one word is not the cut-off start of a longer title (Sustainability)", () => {
    const idx = compileQualityIndex({ qualityAbdcRanks: "Sustainability Accounting, Management and Policy Journal,A" });
    assert.deepEqual(kinds("Sustainability", idx), []);
  });
  it("MDPI's Applied Sciences does not take the smaller journal's quartile", () => {
    const idx = compileQualityIndex({}, { quartilesIndex: { "applied sciences": "Q4", "applied sciences switzerland": "Q2" } });
    assert.deepEqual(kinds("Applied Sciences", idx), []);
  });
  it("a custom list entry is not a longer venue that starts with it, and a near match says so", () => {
    const custom = { name: "Mine", map: new Map([["journal of marketing", "A*"], ["production and operations management", "Top"]]) };
    assert.equal(customBadgeForVenue("Journal of Marketing Research", custom), null);
    assert.equal(customBadgeForVenue("Journal of Marketing", custom).text, "A*");
    assert.equal(customBadgeForVenue("Production and Operations", custom).metadata.nearMatch, "production and operations management");
  });
});

describe("bundled VHB list", async () => {
  const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");
  const { readFileSync } = await import("node:fs");
  const idx = compileQualityIndex({ qualityVhbRanks: readFileSync(new URL("../src/data/vhb2024.csv", import.meta.url), "utf8") });
  const vhb = (venue) => qualityBadgesForVenue(venue, idx).find((b) => b.kind === "vhb")?.text;
  it("American Economic Review is A+ (an AER: Insights row used to overwrite it with A)", () => {
    assert.equal(vhb("American Economic Review"), "VHB A+");
    assert.equal(vhb("American Economic Review: Insights"), "VHB A");
  });
  it("bare Corporate Governance is two journals with different ratings: no badge", () => {
    assert.equal(vhb("Corporate Governance"), undefined);
  });
});

describe("first-word index for near matches", async () => {
  const { customBadgeForVenue } = await import("../src/common/quality.js");
  it("sees entries added to a list after its first lookup", () => {
    const map = new Map([["journal of finance", "A"]]);
    const custom = { name: "Mine", map };
    assert.equal(customBadgeForVenue("Production and Operations", custom), null);
    map.set("production and operations management", "Top");
    assert.equal(customBadgeForVenue("Production and Operations", custom)?.text, "Top");
  });
  it("reads the current value, not one cached in the index", () => {
    const map = new Map([["production and operations management", "B"]]);
    const custom = { name: "Mine", map };
    assert.equal(customBadgeForVenue("Production and Operations", custom)?.text, "B");
    map.set("production and operations management", "A");
    assert.equal(customBadgeForVenue("Production and Operations", custom)?.text, "A");
  });
});
