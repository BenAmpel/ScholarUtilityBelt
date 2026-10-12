import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { bibtexEntryType, escapeBibText, sanitizeBibKey, buildBibTeXEntry } = await import("../src/common/bibtex.js");

describe("bibtexEntryType", () => {
  it("treats journals as article", () => {
    assert.equal(bibtexEntryType("Nature"), "article");
    assert.equal(bibtexEntryType("MIS Quarterly"), "article");
  });
  it("treats conference venues as inproceedings", () => {
    assert.equal(bibtexEntryType("Proceedings of the 2024 Conference of the North American Chapter of the ACL"), "inproceedings");
    assert.equal(bibtexEntryType("Advances in Neural Information Processing Systems NeurIPS"), "inproceedings");
    assert.equal(bibtexEntryType("ICIS 2023"), "inproceedings");
  });
  it("treats preprint servers and an empty venue as misc", () => {
    assert.equal(bibtexEntryType("arXiv preprint arXiv:2502.15040"), "misc");
    assert.equal(bibtexEntryType("SSRN"), "misc");
    assert.equal(bibtexEntryType(""), "misc");
    assert.equal(bibtexEntryType(undefined), "misc");
  });
});

describe("escapeBibText", () => {
  it("escapes LaTeX specials and collapses whitespace", () => {
    assert.equal(escapeBibText("Q&A  at 5% #1"), "Q\\&A at 5\\% \\#1");
  });
  it("keeps balanced braces and drops unbalanced ones", () => {
    assert.equal(escapeBibText("The {GPT}-4 study"), "The {GPT}-4 study");
    assert.equal(escapeBibText("Broken {brace"), "Broken brace");
    assert.equal(escapeBibText("Broken } brace"), "Broken brace");
  });
  it("removes backslashes instead of passing raw LaTeX through", () => {
    assert.equal(escapeBibText("a\\b"), "ab");
  });
  it("handles null and undefined", () => {
    assert.equal(escapeBibText(null), "");
    assert.equal(escapeBibText(undefined), "");
  });
});

describe("sanitizeBibKey", () => {
  it("removes whitespace and BibTeX delimiters", () => {
    assert.equal(sanitizeBibKey("Smith 2024,{abc}"), "Smith2024abc");
  });
});

describe("buildBibTeXEntry", () => {
  const base = {
    key: "Smith2024ab",
    title: "Reducing hallucination & bias",
    authors: ["P Smith", "A Jones"],
    year: 2024,
    venue: "Nature",
    doi: "10.1/x",
    url: "https://example.org/p",
    tags: ["rag", "llm"],
    abstract: "We study X.",
  };

  it("writes a journal article with venue, keywords and abstract", () => {
    const out = buildBibTeXEntry(base);
    assert.match(out, /^@article\{Smith2024ab,/);
    assert.match(out, /title=\{Reducing hallucination \\& bias\}/);
    assert.match(out, /author=\{P Smith and A Jones\}/);
    assert.match(out, /journal=\{Nature\}/);
    assert.match(out, /keywords=\{rag, llm\}/);
    assert.match(out, /abstract=\{We study X\.\}/);
    assert.match(out, /\n\}$/);
  });

  it("uses booktitle for conferences and howpublished for preprints", () => {
    assert.match(buildBibTeXEntry({ ...base, venue: "Proceedings of ACL" }), /^@inproceedings\{[^]*booktitle=\{Proceedings of ACL\}/);
    assert.match(buildBibTeXEntry({ ...base, venue: "arXiv preprint" }), /^@misc\{[^]*howpublished=\{arXiv preprint\}/);
  });

  it("omits empty fields instead of writing doi={}", () => {
    const out = buildBibTeXEntry({ ...base, doi: "", url: "", tags: [], abstract: "", venue: "" });
    assert.doesNotMatch(out, /doi=|url=|keywords=|abstract=|journal=/);
    assert.match(out, /^@misc\{/);
  });

  it("never writes private notes", () => {
    assert.doesNotMatch(buildBibTeXEntry({ ...base, notes: "my private note" }), /private note/);
  });

  it("falls back to a valid key and keeps year to four digits", () => {
    const out = buildBibTeXEntry({ ...base, key: "", year: "2024a" });
    assert.match(out, /^@article\{key,/);
    assert.match(out, /year=\{2024\}/);
  });
});
