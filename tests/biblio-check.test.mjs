import { describe, it } from "node:test";
import assert from "node:assert/strict";

globalThis.atob = (b64) => Buffer.from(b64, "base64").toString("binary");
globalThis.window = {};
globalThis.chrome = { runtime: { getURL: (p) => `chrome-ext://${p}` } };

const B = await import("../src/common/biblio-check.js");
const { compileQualityIndex } = await import("../src/common/quality.js");
const { bloomHasDoi } = await import("../src/content/data-loader.js");

// A tiny bloom filter built the same way data-loader.js reads it (FNV-1a, double hashing; the second hash salt is the string "\u0001salt").
function fnv1a32(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
  return h >>> 0;
}
function makeBloom(dois, m = 4096, k = 4) {
  const bits = new Uint8Array(Math.ceil(m / 8));
  for (const doi of dois) {
    const s = doi.toLowerCase().trim();
    const h1 = fnv1a32(s);
    const h2 = (fnv1a32(s + "\u0001salt") | 1) >>> 0;
    for (let i = 0; i < k; i++) {
      const idx = ((h1 + i * h2) >>> 0) % m;
      bits[idx >> 3] |= 1 << (idx & 7);
    }
  }
  return { m, k, bits, source: "Test export", built: "2026-02-06", count: dois.length };
}

const qIndex = compileQualityIndex({
  qualityFt50List: "MIS Quarterly\nJournal of Marketing",
  qualityUtd24List: "MIS Quarterly\nInformation Systems Research",
  qualityAbdcRanks: "MIS Quarterly,A*\nJournal of Information Technology,A\nInformation Systems Research,A*",
  qualityQuartiles: "MIS Quarterly,Q1\nNature,Q1"
});

describe("extractDois", () => {
  it("finds bare, doi: and URL forms and trims punctuation", () => {
    assert.deepEqual(B.extractDois("see https://doi.org/10.1234/ABC.def-1)."), ["10.1234/abc.def-1"]);
    assert.deepEqual(B.extractDois("doi:10.5555/xyz_2; and 10.1000/182."), ["10.5555/xyz_2", "10.1000/182"]);
  });
  it("keeps a balanced closing parenthesis inside a DOI", () => {
    assert.deepEqual(B.extractDois("10.1016/S0140-6736(20)30183-5."), ["10.1016/s0140-6736(20)30183-5"]);
  });
  it("decodes percent escapes and de-duplicates", () => {
    assert.deepEqual(B.extractDois("10.1002/a%2Fb and 10.1002/a/b"), ["10.1002/a/b"]);
  });
  it("returns nothing for text without a DOI or for bad input", () => {
    assert.deepEqual(B.extractDois("version 10.5 of the thing"), []);
    assert.deepEqual(B.extractDois(null), []);
  });
});

describe("splitReferences", () => {
  it("splits numbered lists with [n] markers and wrapped lines", () => {
    const text = `References
[1] Smith, J. (2020). A study of things. Journal of Stuff, 12(3), 45-67.
    https://doi.org/10.1000/one
[2] Lee, K., & Park, M. (2019). Another study. MIS Quarterly, 43(1), 1-20.
[3] Wu, X. (2018). Third. Nature, 5, 1-2.`;
    const parts = B.splitReferences(text);
    assert.equal(parts.length, 3);
    assert.ok(parts[0].includes("10.1000/one"));
    assert.ok(parts[1].startsWith("Lee, K."));
  });
  it("splits 1. style numbering", () => {
    const parts = B.splitReferences("1. Alpha, A. (2001). Title one here. Venue One, 1, 1-2.\n2. Beta, B. (2002). Title two here. Venue Two, 2, 3-4.");
    assert.equal(parts.length, 2);
  });
  it("splits blank-line separated blocks", () => {
    const parts = B.splitReferences("Alpha, A. (2001). Title one here.\nVenue One, 1, 1-2.\n\nBeta, B. (2002). Title two here.\nVenue Two, 2, 3-4.");
    assert.equal(parts.length, 2);
    assert.ok(parts[0].includes("Venue One"));
  });
  it("splits an APA list with one entry per line and hanging indents", () => {
    const text = `Alpha, A., & Gamma, G. (2001). Title one here. Venue One, 1(2), 1-2.
Beta, B. (2002). Title two here that wraps onto a second
  line with an indent. Venue Two, 2, 3-4.
Delta, D. (2003). Title three here. Venue Three, 3, 5-6.`;
    const parts = B.splitReferences(text);
    assert.equal(parts.length, 3);
    assert.ok(parts[1].includes("indent. Venue Two"));
  });
  it("handles empty and junk input", () => {
    assert.deepEqual(B.splitReferences(""), []);
    assert.deepEqual(B.splitReferences(null), []);
    assert.deepEqual(B.splitReferences("   \n \n"), []);
  });
});

describe("parsePlainReference", () => {
  it("reads an APA reference", () => {
    const r = B.parsePlainReference("Smith, J., & Lee, K. (2020). Trust in online markets. *MIS Quarterly*, 44(2), 123-150. https://doi.org/10.25300/MISQ/2020/1");
    assert.equal(r.year, "2020");
    assert.equal(r.title, "Trust in online markets");
    assert.equal(r.venue, "MIS Quarterly");
  });
  it("reads an IEEE-style reference with a quoted title", () => {
    const r = B.parsePlainReference('A. Smith and B. Jones, "Deep nets for graphs," IEEE Trans. Neural Netw., vol. 3, no. 2, pp. 1-10, 2019.');
    assert.equal(r.title, "Deep nets for graphs");
    assert.equal(r.venue, "IEEE Trans. Neural Netw");
    assert.equal(r.year, "2019");
  });
  it("reads a Vancouver-style reference", () => {
    const r = B.parsePlainReference("Smith J, Jones K. Effect of rest on sleep. J Sleep Res. 2018;27(3):45-9.");
    assert.equal(r.venue, "J Sleep Res");
    assert.equal(r.year, "2018");
    assert.equal(r.title, "Effect of rest on sleep");
  });
  it("reads a book chapter venue after the editors", () => {
    const r = B.parsePlainReference("Doe, J. (2015). Chapter title here. In A. Editor (Ed.), Handbook of Things (pp. 10-20). Publisher.");
    assert.equal(r.venue, "Handbook of Things");
  });
  it("returns an empty venue when it cannot tell", () => {
    const r = B.parsePlainReference("Some text with no structure at all 2020");
    assert.equal(r.venue, "");
    assert.equal(r.year, "2020");
  });
});

describe("parseBibtex", () => {
  const bib = `
@comment{ignored}
@string{x = "y"}
@article{smith2020,
  author = {Smith, John and Lee, K.},
  title = {Trust in {Online} Markets},
  journal = {MIS Quarterly},
  year = 2020,
  doi = {10.25300/MISQ/2020/1}
}
@inproceedings{lee2019,
  title = "A paper, with a comma",
  booktitle = {Proceedings of the International Conference on Information Systems},
  year = {2019}
}
@book{doe2015, title={A Book}, publisher={Press}, year={2015}}
@misc{pre, title={Preprint thing}, eprint={2101.00001}, archivePrefix={arXiv}, year={2021}}
`;
  it("parses entries, fields and braces", () => {
    const e = B.parseBibtex(bib);
    assert.equal(e.length, 4);
    assert.equal(e[0].key, "smith2020");
    assert.equal(e[0].fields.title, "Trust in Online Markets");
    assert.equal(e[0].fields.year, "2020");
    assert.equal(e[1].fields.title, "A paper, with a comma");
  });
  it("tolerates an unterminated entry", () => {
    const e = B.parseBibtex("@article{k, title={Broken, journal={X}");
    assert.equal(e.length, 1);
  });
  it("builds common entries with venue and DOI", () => {
    const list = B.parseReferenceList(bib);
    assert.equal(list[0].venue, "MIS Quarterly");
    assert.equal(list[0].doi, "10.25300/misq/2020/1");
    assert.equal(list[2].venue, "");
    assert.match(list[2].venueNote, /no journal or booktitle/);
    assert.equal(B.detectPreprint(list[3]), "arXiv");
  });
});

describe("detectPreprint", () => {
  it("uses DOI prefixes, venue text and raw text", () => {
    assert.equal(B.detectPreprint({ doi: "10.48550/arxiv.2101.00001", raw: "", venue: "" }), "arXiv");
    assert.equal(B.detectPreprint({ doi: "10.2139/ssrn.123", raw: "", venue: "" }), "SSRN");
    assert.equal(B.detectPreprint({ doi: "", raw: "Smith (2020). Title. arXiv preprint arXiv:2001.00001.", venue: "arXiv preprint arXiv:2001.00001" }), "arXiv");
    assert.equal(B.detectPreprint({ doi: "10.1234/x", raw: "Smith (2020). Title. Nature.", venue: "Nature" }), "");
  });
});

describe("findDuplicates", () => {
  it("flags repeated DOIs and repeated long titles, not short or different ones", () => {
    const entries = [
      { index: 1, doi: "10.1/a", title: "Trust in online markets" },
      { index: 2, doi: "10.1/b", title: "Something else entirely different" },
      { index: 3, doi: "10.1/A".toLowerCase(), title: "Different title but same doi value" },
      { index: 4, doi: "", title: "Trust in Online Markets." },
      { index: 5, doi: "", title: "Short" },
      { index: 6, doi: "", title: "Short" }
    ];
    const d = B.findDuplicates(entries);
    assert.deepEqual(d.get(3), { of: 1, reason: "same DOI" });
    assert.deepEqual(d.get(4), { of: 1, reason: "same title" });
    assert.equal(d.has(2), false);
    assert.equal(d.has(6), false);
  });
});

describe("checkReferences", () => {
  const retractedDoi = "10.9999/retracted.1";
  const bloom = makeBloom([retractedDoi]);
  const text = `[1] Smith, J. (2020). Trust in online markets. MIS Quarterly, 44(2), 123-150. https://doi.org/10.25300/MISQ/2020/1
[2] Doe, D. (2021). A flagged paper about things. Journal of Information Technology, 36, 1-10. doi:${retractedDoi}
[3] Roe, R. (2019). Preprint of a thing. arXiv preprint arXiv:1901.00001. https://doi.org/10.48550/arXiv.1901.00001
[4] Smith, J. (2020). Trust in online markets. MIS Quarterly, 44(2), 123-150.
[5] Poe, P. (2018). A book-like reference without a clear venue 2018`;
  const entries = B.parseReferenceList(text);
  const report = B.checkReferences(entries, { qIndex, bloom, bloomHasDoi });

  it("parsed five entries", () => assert.equal(entries.length, 5));

  it("flags a DOI that is in the filter and not others", () => {
    assert.equal(report.items[1].retraction, "flagged");
    assert.equal(report.items[0].retraction, "not-flagged");
    assert.equal(report.items[3].retraction, "no-doi");
  });
  it("adds venue badges from the local indexes", () => {
    assert.ok(report.items[0].tierBadges.includes("FT50"));
    assert.ok(report.items[0].tierBadges.includes("UTD24"));
    assert.ok(report.items[0].tierBadges.includes("ABDC A*"));
    assert.ok(report.items[0].tierBadges.includes("Q1"));
    assert.equal(report.items[1].tierBadges.includes("ABDC A"), true);
  });
  it("says when the venue cannot be read", () => {
    assert.equal(report.items[4].venueStatus, "unparsed");
    assert.ok(report.items[4].flags.includes("venue not read"));
  });
  it("detects the preprint and the duplicate", () => {
    assert.equal(report.items[2].preprint, "arXiv");
    assert.equal(report.items[3].duplicateOf, 1);
  });
  it("summarises counts, tier mix and attention list", () => {
    const s = report.summary;
    assert.equal(s.total, 5);
    assert.equal(s.withDoi, 3);
    assert.equal(s.withoutDoi, 2);
    assert.equal(s.retractionFlagged, 1);
    assert.equal(s.preprints, 1);
    assert.equal(s.duplicates, 1);
    assert.equal(s.venueUnparsed >= 1, true);
    assert.equal(s.tierMix.find((t) => t.label === "FT50").count, 2);
    assert.deepEqual(s.attention.map((a) => a.index), [2, 4]);
  });
  it("describes the filter honestly, with date, count and the limits", () => {
    const t = report.filter.text;
    assert.match(t, /2026-02-06/);
    assert.match(t, /not proof/);
    assert.match(t, /false alarm/);
    assert.equal(report.filter.available, true);
  });
  it("works without a filter or index", () => {
    const r = B.checkReferences(entries, {});
    assert.equal(r.items[0].retraction, "filter-unavailable");
    assert.equal(r.items[0].tierBadges.length, 0);
    assert.equal(r.filter.available, false);
  });
  it("never mentions predatory venues", () => {
    const blob = JSON.stringify(report) + B.resultsToCsv(report);
    assert.equal(/predator/i.test(blob), false);
  });
  it("real bloomHasDoi agrees with the test filter", () => {
    assert.equal(bloomHasDoi(retractedDoi, bloom), true);
    assert.equal(bloomHasDoi("10.1/unrelated", bloom), false);
  });
});

describe("describeRetractionFilter", () => {
  it("reports unavailable for a missing filter", () => {
    assert.equal(B.describeRetractionFilter(null).available, false);
  });
  it("computes a small false-positive rate for the shipped filter parameters", () => {
    const f = B.describeRetractionFilter({ m: 720000, k: 10, count: 59905, built: "2026-02-06", source: "Crossref Retraction Watch" });
    assert.ok(f.falsePositiveRate > 0.001 && f.falsePositiveRate < 0.01);
  });
});

describe("crossref lookup (mocked fetch)", () => {
  const okRes = (body) => ({ ok: true, status: 200, json: async () => body });
  it("reads title, container and update types", async () => {
    const r = await B.crossrefLookup("10.1/a", async () => okRes({ message: { title: ["T"], "container-title": ["J"], "updated-by": [{ type: "retraction" }, { type: "correction" }] } }));
    assert.equal(r.status, "ok");
    assert.equal(r.retractionLike, true);
    assert.deepEqual(r.updateTypes, ["retraction", "correction"]);
  });
  it("handles 404, 429, other errors and thrown errors", async () => {
    assert.equal((await B.crossrefLookup("10.1/a", async () => ({ ok: false, status: 404 }))).status, "not-found");
    assert.equal((await B.crossrefLookup("10.1/a", async () => ({ ok: false, status: 429 }))).status, "rate-limited");
    assert.equal((await B.crossrefLookup("10.1/a", async () => ({ ok: false, status: 500 }))).status, "error");
    assert.equal((await B.crossrefLookup("10.1/a", async () => { throw new Error("offline"); })).status, "error");
  });
  it("pauses between requests, deduplicates and respects the cap", async () => {
    const calls = [];
    const sleeps = [];
    const out = await B.crossrefLookupAll(["10.1/a", "10.1/A", "10.1/b", "10.1/c"], {
      fetchFn: async (url) => { calls.push(url); return okRes({ message: {} }); },
      sleep: async (ms) => { sleeps.push(ms); },
      delayMs: 1000,
      max: 2
    });
    assert.equal(calls.length, 2);
    assert.deepEqual(sleeps, [1000]);
    assert.equal(out.skippedOverCap, 1);
    assert.equal(out.stopped, "");
  });
  it("stops at the first rate-limit answer", async () => {
    let n = 0;
    const out = await B.crossrefLookupAll(["10.1/a", "10.1/b", "10.1/c"], {
      fetchFn: async () => (++n === 2 ? { ok: false, status: 429 } : okRes({ message: {} })),
      sleep: async () => {}
    });
    assert.equal(n, 2);
    assert.equal(out.stopped, "rate-limited");
  });
  it("can be cancelled", async () => {
    let n = 0;
    const out = await B.crossrefLookupAll(["10.1/a", "10.1/b"], {
      fetchFn: async () => { n++; return okRes({ message: {} }); },
      sleep: async () => {},
      shouldCancel: () => n >= 1
    });
    assert.equal(out.stopped, "cancelled");
    assert.equal(n, 1);
  });
  it("applyCrossref updates the summary", async () => {
    const entries = B.parseReferenceList("[1] A, A. (2020). Title of paper one. Nature, 1, 1-2. doi:10.1000/a\n[2] B, B. (2021). Title of paper two. Nature, 1, 1-2. doi:10.1000/b");
    const base = B.checkReferences(entries, {});
    const results = new Map([["10.1000/a", { status: "not-found" }], ["10.1000/b", { status: "ok", updateTypes: ["retraction"], retractionLike: true }]]);
    const r = B.applyCrossref(base, results);
    assert.equal(r.summary.crossrefNotFound, 1);
    assert.equal(r.summary.crossrefUpdatesRetraction, 1);
    assert.equal(r.summary.attention.length, 2);
  });
});

describe("resultsToCsv", () => {
  it("quotes cells and neutralises formula starts", () => {
    const entries = B.parseReferenceList("[1] =HYPERLINK(\"x\"), A. (2020). Title, with comma here. Nature, 1, 1-2.\n[2] B, B. (2021). Second paper title here. Nature, 1, 1-2.");
    const csv = B.resultsToCsv(B.checkReferences(entries, { qIndex }));
    const lines = csv.trim().split("\r\n");
    assert.equal(lines.length, 3);
    assert.ok(lines[0].startsWith("entry,reference,doi"));
    assert.ok(lines[1].includes("\"'=HYPERLINK"));
    assert.ok(csv.includes("Title, with comma here"));
  });
});

describe("input guards", () => {
  it("handles empty text and very long text", () => {
    assert.deepEqual(B.parseReferenceList(""), []);
    const many = Array.from({ length: 4000 }, (_, i) => `[${i + 1}] Author, A. (2020). Title number ${i} for the test. Nature, 1, 1-2.`).join("\n");
    assert.equal(B.parseReferenceList(many).length, B.MAX_ENTRIES);
  });
});
