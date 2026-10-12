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
    assert.match(parseReferences("hello", "x.txt").error, /RIS or BibTeX/);
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
