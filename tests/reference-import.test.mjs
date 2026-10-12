import { describe, it } from "node:test";
import assert from "node:assert/strict";

const {
  detectReferenceFormat, parseRis, parseBibtex, parseReferences,
  searchLogToCsv, searchLogToMarkdown, totalRecordsFromSearchLog, hashId, MAX_RECORDS,
} = await import("../src/common/reference-import.js");

const RIS = [
  "TY  - JOUR",
  "TI  - Reducing hallucination in structured outputs via",
  "      retrieval-augmented generation",
  "AU  - Béchard, Patrice",
  "AU  - Ayala, Orlando Marquez",
  "JO  - NAACL Industry Track",
  "PY  - 2024/06/01/",
  "DO  - 10.18653/V1/2024.NAACL-INDUSTRY.19",
  "UR  - https://doi.org/10.18653/v1/2024.naacl-industry.19",
  "UR  - https://aclanthology.org/2024.naacl-industry.19/",
  "AB  - We study RAG.",
  "KW  - rag",
  "KW  - llm",
  "DB  - PubMed",
  "ER  - ",
  "",
  "TY  - CONF",
  "T1  - Second paper",
  "A1  - Smith, J.",
  "T2  - Proc. of Things",
  "Y1  - 2021",
  "ER  - ",
  "TY  - JOUR",
  "AU  - No Title, Author",
  "ER  - ",
].join("\n");

const BIB = `
@comment{ignored @article{fake, title={Not real}} }
@string{acl = "ACL"}
@article{wos1,
  author = {Béchard, P. and Ayala, O. M. and {World Health Organization}},
  title = {Reducing {GPT}-4 hallucination \\& bias: a {Q\\"u}ick look},
  journal = "Journal of {Things}",
  year = 2024,
  doi = {https://doi.org/10.1000/ABC.123},
  abstract = {Line one
    line two with 50\\% effect.},
  keywords = {rag; llm, bias},
  url = {https://example.org/p}
}
@inproceedings{z2,
  title = "Second {Paper}",
  author = "Smith, John",
  booktitle = {Proceedings of X},
  date = {2021-05-02}
}
@misc{notitle, author={A B}}
`;

describe("detectReferenceFormat", () => {
  it("detects by content, not just by file name", () => {
    assert.equal(detectReferenceFormat(RIS, "export.txt"), "ris");
    assert.equal(detectReferenceFormat(BIB, "savedrecs.txt"), "bibtex");
    assert.equal(detectReferenceFormat("\uFEFFTY  - JOUR\nTI  - x"), "ris");
  });
  it("falls back to the extension and returns empty when unknown", () => {
    assert.equal(detectReferenceFormat("", "a.ris"), "ris");
    assert.equal(detectReferenceFormat("", "a.bib"), "bibtex");
    assert.equal(detectReferenceFormat("just some text", "a.txt"), "");
  });
});

describe("parseRis", () => {
  const { records, skipped } = parseRis(RIS);
  it("reads records and skips ones without a title", () => {
    assert.equal(records.length, 2);
    assert.equal(skipped, 1);
  });
  it("joins wrapped titles and formats authors as initial plus surname", () => {
    assert.equal(records[0].title, "Reducing hallucination in structured outputs via retrieval-augmented generation");
    assert.equal(records[0].authors, "P Béchard, OM Ayala");
    assert.equal(records[1].authors, "J Smith");
  });
  it("normalises year, DOI, venue, urls, keywords and source", () => {
    assert.equal(records[0].year, "2024");
    assert.equal(records[0].doi, "10.18653/v1/2024.naacl-industry.19");
    assert.equal(records[0].venue, "NAACL Industry Track");
    assert.equal(records[0].url, "https://aclanthology.org/2024.naacl-industry.19/");
    assert.deepEqual(records[0].keywords, ["rag", "llm"]);
    assert.equal(records[0].sourceDb, "PubMed");
    assert.equal(records[1].venue, "Proc. of Things");
    assert.equal(records[1].year, "2021");
  });
  it("copes with CRLF and a missing final ER", () => {
    const r = parseRis("TY  - JOUR\r\nTI  - One\r\nAU  - Doe, Jane\r\n");
    assert.equal(r.records.length, 1, "an entry with no closing ER is still captured");
    assert.equal(r.records[0].authors, "J Doe");
  });
});

describe("parseBibtex", () => {
  const { records, skipped } = parseBibtex(BIB);
  it("skips @comment and @string blocks and entries with no title", () => {
    assert.equal(records.length, 2);
    assert.equal(skipped, 1);
    assert.ok(!records.some((r) => /Not real/.test(r.title)));
  });
  it("cleans LaTeX in titles and keeps balanced text", () => {
    assert.equal(records[0].title, "Reducing GPT-4 hallucination & bias: a Quick look");
    assert.equal(records[1].title, "Second Paper");
  });
  it("splits authors on and, keeps corporate authors, formats names", () => {
    assert.equal(records[0].authors, "P Béchard, OM Ayala, World Health Organization");
    assert.equal(records[1].authors, "J Smith");
  });
  it("reads bare, quoted and braced values, DOI from a URL and the venue by type", () => {
    assert.equal(records[0].year, "2024");
    assert.equal(records[0].doi, "10.1000/abc.123");
    assert.equal(records[0].venue, "Journal of Things");
    assert.equal(records[0].url, "https://example.org/p");
    assert.equal(records[1].venue, "Proceedings of X");
    assert.equal(records[1].year, "2021");
  });
  it("collapses abstract whitespace and unescapes percent signs", () => {
    assert.equal(records[0].abstract, "Line one line two with 50% effect.");
    assert.deepEqual(records[0].keywords, ["rag", "llm", "bias"]);
  });
  it("survives truncated and malformed input", () => {
    for (const bad of ["@article{x, title = {oops", "@article", "@@@", "@article{k,}", "", null]) {
      assert.doesNotThrow(() => parseBibtex(bad));
    }
  });
});

describe("parseReferences", () => {
  it("returns an error for unknown formats and empty results", () => {
    assert.match(parseReferences("hello", "x.txt").error, /RIS, BibTeX/);
    assert.match(parseReferences("TY  - JOUR\nER  - ", "x.ris").error, /No records/);
  });
  it("returns the detected format and records", () => {
    const r = parseReferences(RIS, "export.ris");
    assert.equal(r.format, "ris");
    assert.equal(r.records.length, 2);
    assert.equal(r.error, "");
  });
  it("caps the number of records", () => {
    const big = Array.from({ length: MAX_RECORDS + 3 }, (_, i) => `TY  - JOUR\nTI  - Paper ${i}\nER  - `).join("\n");
    const r = parseReferences(big, "big.ris");
    assert.equal(r.records.length, MAX_RECORDS);
    assert.equal(r.truncated, true);
  });
});

describe("search log helpers", () => {
  const log = [
    { database: "PubMed", searchedOn: "2026-10-12", query: '"RAG" AND hallucination | x', fileName: "a.ris", format: "ris", recordsInFile: 120, added: 100, duplicates: 20 },
    { database: "Scopus", searchedOn: "2026-10-13", query: "TITLE-ABS(rag)", fileName: "b.bib", format: "bibtex", recordsInFile: 80, added: 60, duplicates: 20 },
  ];
  it("writes a CSV with quoted fields", () => {
    const csv = searchLogToCsv(log);
    const lines = csv.split("\n");
    assert.equal(lines.length, 3);
    assert.match(lines[0], /^Database,Date searched,Search string/);
    assert.match(lines[1], /"""RAG"" AND hallucination \| x"/);
  });
  it("writes a Markdown table and escapes pipes", () => {
    const md = searchLogToMarkdown(log);
    assert.match(md, /^\| Database \| Date searched/);
    assert.match(md, /hallucination \\\| x/);
    assert.equal(searchLogToMarkdown([]), "");
  });
  it("totals records and hashes deterministically", () => {
    assert.equal(totalRecordsFromSearchLog(log), 200);
    assert.equal(totalRecordsFromSearchLog(undefined), 0);
    assert.equal(hashId("abc"), hashId("abc"));
    assert.notEqual(hashId("abc"), hashId("abd"));
  });
});

// ---------------------------------------------------------- MEDLINE (.nbib)

const { parseMedline } = await import("../src/common/reference-import.js");

// Written to look like a PubMed "Save > Format: PubMed" export: padded four-character tags,
// continuation lines indented by six spaces, repeated AU/FAU/AID/MH tags.
const NBIB = [
  "PMID- 33301246",
  "OWN - NLM",
  "STAT- MEDLINE",
  "DP  - 2021 Mar 15",
  "TI  - Safety and efficacy of the BNT162b2 mRNA vaccine against SARS-CoV-2 in adults: a",
  "      systematic review of randomised trials.",
  "PG  - 1-12",
  "AB  - BACKGROUND: Trials reported high efficacy. METHODS: We searched three",
  "      databases. RESULTS: Efficacy was 95%.",
  "FAU - Polack, Fernando P",
  "AU  - Polack FP",
  "FAU - Thomas, Stephen J",
  "AU  - Thomas SJ",
  "FAU - Jean-Pierre, Marie-Claire",
  "AU  - Jean-Pierre MC",
  "AD  - Some University.",
  "LA  - eng",
  "PT  - Journal Article",
  "PT  - Systematic Review",
  "TA  - Lancet Infect Dis",
  "JT  - The Lancet. Infectious diseases",
  "AID - S1473-3099(21)00001-2 [pii]",
  "AID - 10.1016/S1473-3099(21)00001-2 [doi]",
  "LID - 10.1016/S1473-3099(21)00001-2 [doi]",
  "MH  - Adult",
  "MH  - *COVID-19 Vaccines/administration & dosage",
  "OT  - mRNA",
  "SO  - Lancet Infect Dis. 2021 Mar 15;21(3):1-12. doi: 10.1016/S1473-3099(21)00001-2.",
  "",
  "PMID- 28000001",
  "TI  - A study with no abstract and old-style author initials only",
  "AU  - Nguyen TT",
  "AU  - O'Brien K",
  "AU  - van der Berg J",
  "DP  - 1998 Dec",
  "TA  - J Clin Epidemiol",
  "AID - 12345678 [pii]",
  "SO  - J Clin Epidemiol. 1998 Dec;51(12):1-5. doi: 10.1000/ABC.99",
  "",
  "",
  "PMID- 9",
  "OWN - NLM",
  "AB  - A record that has an abstract but no title.",
].join("\n");

describe("detectReferenceFormat (MEDLINE)", () => {
  it("sniffs PMID lines with or without the padded tag, and the .nbib extension", () => {
    assert.equal(detectReferenceFormat(NBIB, "pubmed-export.txt"), "medline");
    assert.equal(detectReferenceFormat("PMID - 123\nTI  - x", "a.txt"), "medline");
    assert.equal(detectReferenceFormat("﻿PMID- 123\nTI  - x"), "medline");
    assert.equal(detectReferenceFormat("", "citation.nbib"), "medline");
    assert.equal(detectReferenceFormat(RIS, "x.nbib"), "ris", "content beats the extension");
  });
});

describe("parseMedline", () => {
  const { records, skipped } = parseMedline(NBIB);
  it("reads records and skips one with no title", () => {
    assert.equal(records.length, 2);
    assert.equal(skipped, 1);
  });
  it("joins wrapped titles and abstracts", () => {
    assert.equal(records[0].title, "Safety and efficacy of the BNT162b2 mRNA vaccine against SARS-CoV-2 in adults: a systematic review of randomised trials.");
    assert.match(records[0].abstract, /^BACKGROUND: Trials reported high efficacy\. METHODS: We searched three databases\. RESULTS/);
  });
  it("uses FAU for authors in the same style as RIS, and falls back to AU initials", () => {
    assert.equal(records[0].authors, "FP Polack, SJ Thomas, MC Jean-Pierre");
    assert.equal(records[1].authors, "TT Nguyen, K O'Brien, J van der Berg");
  });
  it("takes year, venue, DOI, keywords, pmid and source", () => {
    assert.equal(records[0].year, "2021");
    assert.equal(records[0].venue, "The Lancet. Infectious diseases");
    assert.equal(records[0].doi, "10.1016/s1473-3099(21)00001-2");
    assert.deepEqual(records[0].keywords, ["Adult", "COVID-19 Vaccines/administration & dosage", "mRNA"]);
    assert.equal(records[0].pmid, "33301246");
    assert.equal(records[0].sourceDb, "PubMed");
    assert.equal(records[0].url, "https://pubmed.ncbi.nlm.nih.gov/33301246/");
  });
  it("copes with a missing abstract, a pii-only AID, a DOI found only in SO, and TA as venue", () => {
    assert.equal(records[1].abstract, "");
    assert.equal(records[1].venue, "J Clin Epidemiol");
    assert.equal(records[1].doi, "10.1000/abc.99");
    assert.equal(records[1].year, "1998");
  });
  it("handles CRLF line endings and a missing trailing blank line", () => {
    const crlf = NBIB.replace(/\n/g, "\r\n");
    const r = parseMedline(crlf);
    assert.equal(r.records.length, 2);
    assert.equal(r.records[0].title, records[0].title);
    assert.equal(r.skipped, 1);
  });
  it("starts a new record at a PMID line even without a blank line", () => {
    const r = parseMedline("PMID- 1\nTI  - One\nPMID- 2\nTI  - Two");
    assert.deepEqual(r.records.map((x) => x.title), ["One", "Two"]);
  });
  it("tolerates malformed input without throwing", () => {
    assert.deepEqual(parseMedline(""), { records: [], skipped: 0 });
    assert.deepEqual(parseMedline("hello\nthis is not medline\n\n???"), { records: [], skipped: 0 });
    const r = parseMedline("PMID- x\nTI  - \nAU  - \n\nTI  - Orphan fields, no PMID\nDP  - n.d.\nJT  -");
    assert.equal(r.records.length, 1);
    assert.equal(r.records[0].title, "Orphan fields, no PMID");
    assert.equal(r.records[0].year, "");
    assert.equal(r.records[0].pmid, "");
    assert.equal(r.skipped, 1);
  });
  it("is reached through parseReferences and reports the format", () => {
    const p = parseReferences(NBIB, "pubmed-result.nbib");
    assert.equal(p.format, "medline");
    assert.equal(p.records.length, 2);
    assert.equal(p.error, "");
    assert.match(parseReferences("nothing useful", "x.txt").error, /MEDLINE/);
  });
  it("appears in the search log as its own format", () => {
    const csv = searchLogToCsv([{ database: "PubMed", fileName: "a.nbib", format: "medline", recordsInFile: 2, added: 2, duplicates: 0 }]);
    assert.match(csv.split("\n")[1], /^PubMed,,,a\.nbib,medline,2,2,0$/);
  });
});

describe("duplicate matching across records with and without a DOI", async () => {
  const { matchKeys, findExistingKey, registerKeys, yearsCompatible, MIN_TITLE_MATCH_CHARS } = await import("../src/common/reference-import.js");
  const h = { normalizeDoi: (s) => String(s || "").toLowerCase().trim(), normalizeTitle: (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim() };
  const longTitle = "Test record one: effect of a made-up intervention on a made-up outcome in a randomized trial";

  it("gives a paper with a DOI a DOI key and a title key, and a paper without one just the title", () => {
    assert.deepEqual(matchKeys({ doi: "10.1/x", title: longTitle }, h).map((k) => k.split(":")[0]), ["doi", "title"]);
    assert.deepEqual(matchKeys({ title: "Short" }, h).map((k) => k.split(":")[0]), ["title"]);
    assert.deepEqual(matchKeys({ doi: "10.1/x", title: "Short" }, h), ["doi:10.1/x"], "a short title is no evidence next to a DOI");
    assert.deepEqual(matchKeys({ doi: "10.1/x", pmid: "123", title: "Short" }, h), ["doi:10.1/x", "pmid:123"]);
  });
  it("matches a DOI-less record to a stored record that has a DOI, by title", () => {
    const fps = {}; const papers = {};
    const first = { id: "a", doi: "10.9999/test.0001", title: longTitle, year: "2021" };
    papers.a = first; registerKeys(fps, first, "a", h);
    const incoming = { id: "b", title: longTitle, year: "2021" };
    assert.equal(findExistingKey(fps, papers, incoming, h), `title:${h.normalizeTitle(longTitle)}`);
  });
  it("still matches by DOI when the titles differ", () => {
    const fps = {}; const papers = {};
    const first = { id: "a", doi: "10.1/x", title: "One title of reasonable length for the test", year: "2020" };
    papers.a = first; registerKeys(fps, first, "a", h);
    assert.equal(findExistingKey(fps, papers, { id: "b", doi: "10.1/x", title: "Another wording entirely" }, h), "doi:10.1/x");
  });
  it("does not merge two different papers that share a short title, or whose years are far apart", () => {
    const fps = {}; const papers = {};
    const first = { id: "a", title: "Introduction", year: "2020" };
    papers.a = first; registerKeys(fps, first, "a", h);
    assert.equal(findExistingKey(fps, papers, { id: "b", doi: "10.1/y", title: "Introduction", year: "2020" }, h), "", "short title next to a DOI is not a match");
    const long = { id: "c", title: longTitle, year: "2010" };
    papers.c = long; registerKeys(fps, long, "c", h);
    assert.equal(findExistingKey(fps, papers, { id: "d", title: longTitle, year: "2021" }, h), "", "eleven years apart is a different paper");
    assert.ok(MIN_TITLE_MATCH_CHARS >= 20);
  });
  it("treats a missing or one-year-off year as compatible", () => {
    assert.equal(yearsCompatible("2021", ""), true);
    assert.equal(yearsCompatible(undefined, 2020), true);
    assert.equal(yearsCompatible("2021", "2022"), true);
    assert.equal(yearsCompatible("2021", "2023"), false);
  });
  it("registerKeys never takes over a key another paper owns", () => {
    const fps = { "doi:10.1/x": "a" };
    registerKeys(fps, { doi: "10.1/x", title: longTitle }, "b", h);
    assert.equal(fps["doi:10.1/x"], "a");
    assert.equal(fps[`title:${h.normalizeTitle(longTitle)}`], "b");
  });
});
