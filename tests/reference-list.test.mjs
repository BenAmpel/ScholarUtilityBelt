import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { toRecord, sortRecords } from "../src/common/export-model.js";
import {
  STYLES, sourceKind, formatReference, formatParts, partsToHtml, inTextCitation,
  buildReferenceBlocks, blocksToPlain, blocksToHtml, buildDocHtml,
} from "../src/common/reference-list.js";

const rec = (p) => toRecord(p);

// A journal article, a conference paper and a preprint, with the data a saved paper really has.
const journal = rec({ key: "j", title: "Deep learning for citation analysis", authorsVenue: "A Smith, B Jones, CD Lee - Journal of Information Science, 2021", year: 2021, venue: "Journal of Information Science", doi: "10.1000/jis.2021.42" });
const conf = rec({ key: "c", title: "Reading papers faster", authorsVenue: "J Doe, K Roe - Proceedings of the 2020 Conference on Scholarly Tools", year: 2020, venue: "Proceedings of the 2020 Conference on Scholarly Tools", url: "https://example.org/paper" });
const pre = rec({ key: "p", title: "Attention is all you need", authorsVenue: "A Vaswani, N Shazeer, N Parmar", year: 2017, venue: "arXiv preprint arXiv:1706.03762" });

describe("sourceKind", () => {
  it("classifies by venue", () => {
    assert.equal(sourceKind(journal), "article");
    assert.equal(sourceKind(conf), "conference");
    assert.equal(sourceKind(pre), "preprint");
    assert.equal(sourceKind(rec({ title: "x" })), "other");
  });
});

describe("APA 7", () => {
  it("journal article", () => {
    assert.equal(formatReference("apa", journal), "Smith, A., Jones, B., & Lee, C. D. (2021). Deep learning for citation analysis. Journal of Information Science. https://doi.org/10.1000/jis.2021.42");
  });
  it("conference paper", () => {
    assert.equal(formatReference("apa", conf), "Doe, J., & Roe, K. (2020). Reading papers faster. In Proceedings of the 2020 Conference on Scholarly Tools. https://example.org/paper");
  });
  it("preprint with the arXiv link derived from the venue text", () => {
    assert.equal(formatReference("apa", pre), "Vaswani, A., Shazeer, N., & Parmar, N. (2017). Attention is all you need [Preprint]. arXiv. https://arxiv.org/abs/1706.03762");
  });
  it("italicises the container (journal, proceedings) and the preprint title only", () => {
    assert.match(partsToHtml(formatParts("apa", journal)), /<i>Journal of Information Science<\/i>\./);
    assert.match(partsToHtml(formatParts("apa", pre)), /<i>Attention is all you need<\/i> \[Preprint\]\./);
  });
  it("one author, no year, no venue, no link", () => {
    assert.equal(formatReference("apa", rec({ title: "A note", authorsVenue: "Z Zed" })), "Zed, Z. (n.d.). A note.");
  });
  it("no authors puts the title first", () => {
    assert.equal(formatReference("apa", rec({ title: "Anonymous report", year: 2019, venue: "Some Journal" })), "Anonymous report. (2019). Some Journal.");
  });
  it("does not double the period after a question mark", () => {
    assert.equal(formatReference("apa", rec({ title: "Is it ready?", authorsVenue: "Q Quill", year: 2018, venue: "Some Journal" })), "Quill, Q. (2018). Is it ready? Some Journal.");
  });
  it("lists up to 20 authors then an ellipsis", () => {
    const authors = Array.from({ length: 22 }, (_, k) => `A Author${String.fromCharCode(65 + (k % 26))}`);
    const out = formatReference("apa", rec({ title: "Big", authorsVenue: authors.join(", "), year: 2022, venue: "J" }));
    assert.match(out, /, \. \. \. Author[A-Z]\. \(2022\)|, \. \. \. Author[A-Z], A\. \(2022\)/);
    assert.ok(out.includes("AuthorS, A.")); // 19th author is shown
    assert.ok(!out.includes("AuthorT, A.")); // 20th and 21st are not
  });
});

describe("MLA 9", () => {
  it("journal article with three authors uses et al.", () => {
    assert.equal(formatReference("mla", journal), "Smith, A., et al. “Deep learning for citation analysis.” Journal of Information Science, 2021, https://doi.org/10.1000/jis.2021.42.");
  });
  it("two authors", () => {
    assert.equal(formatReference("mla", conf), "Doe, J., and K. Roe. “Reading papers faster.” Proceedings of the 2020 Conference on Scholarly Tools, 2020, https://example.org/paper.");
  });
  it("preprint names the repository as the container", () => {
    assert.equal(formatReference("mla", pre), "Vaswani, A., et al. “Attention is all you need.” arXiv, 2017, https://arxiv.org/abs/1706.03762.");
  });
  it("keeps a full first name when Scholar gave one", () => {
    const r = rec({ title: "Full names", authorsVenue: "Maria Garcia, Pedro Ruiz", year: 2015, venue: "J" });
    assert.equal(formatReference("mla", r), "Garcia, Maria, and Pedro Ruiz. “Full names.” J, 2015.");
  });
  it("copes with missing fields", () => {
    assert.equal(formatReference("mla", rec({ title: "Bare" })), "“Bare.”");
  });
});

describe("Chicago 17 bibliography", () => {
  it("journal article", () => {
    assert.equal(formatReference("chicago", journal), "Smith, A., B. Jones, and C. D. Lee. “Deep learning for citation analysis.” Journal of Information Science (2021). https://doi.org/10.1000/jis.2021.42.");
  });
  it("conference paper", () => {
    assert.equal(formatReference("chicago", conf), "Doe, J., and K. Roe. “Reading papers faster.” In Proceedings of the 2020 Conference on Scholarly Tools, 2020. https://example.org/paper.");
  });
  it("preprint", () => {
    assert.equal(formatReference("chicago", pre), "Vaswani, A., N. Shazeer, and N. Parmar. “Attention is all you need.” Preprint, arXiv, 2017. https://arxiv.org/abs/1706.03762.");
  });
  it("more than ten authors: first seven then et al.", () => {
    const authors = Array.from({ length: 12 }, (_, k) => `A Name${k + 1}`).join(", ");
    const out = formatReference("chicago", rec({ title: "T", authorsVenue: authors, year: 2000, venue: "J" }));
    assert.ok(out.startsWith("Name1, A., A. Name2, A. Name3, A. Name4, A. Name5, A. Name6, A. Name7, et al."), out);
  });
});

describe("IEEE", () => {
  it("journal article", () => {
    assert.equal(formatReference("ieee", journal), "A. Smith, B. Jones, and C. D. Lee, “Deep learning for citation analysis,” Journal of Information Science, 2021, doi: 10.1000/jis.2021.42.");
  });
  it("conference paper with an online link and no DOI", () => {
    assert.equal(formatReference("ieee", conf), "J. Doe and K. Roe, “Reading papers faster,” in Proceedings of the 2020 Conference on Scholarly Tools, 2020. [Online]. Available: https://example.org/paper");
  });
  it("preprint uses the arXiv identifier", () => {
    assert.equal(formatReference("ieee", pre), "A. Vaswani, N. Shazeer, and N. Parmar, “Attention is all you need,” 2017, arXiv:1706.03762.");
  });
  it("more than six authors: first author et al.", () => {
    const authors = Array.from({ length: 7 }, (_, k) => `A Name${k + 1}`).join(", ");
    assert.ok(formatReference("ieee", rec({ title: "T", authorsVenue: authors, year: 2000, venue: "J" })).startsWith("A. Name1 et al., “T,”"));
  });
  it("title only", () => {
    assert.equal(formatReference("ieee", rec({ title: "Only" })), "“Only.”");
  });
});

describe("names", () => {
  it("handles hyphenated given names, particles and no given name", () => {
    assert.equal(formatReference("apa", rec({ title: "T", authorsVenue: "Jean-Paul Sartre", year: 1943, venue: "J" })), "Sartre, J.-P. (1943). T. J.");
    assert.equal(formatReference("ieee", rec({ title: "T", authorsVenue: "J van der Berg", year: 2000, venue: "J" })), "J. van der Berg, \u201cT,\u201d J, 2000.");
    assert.equal(formatReference("apa", rec({ title: "T", authorsVenue: "Plato", year: 2000 })), "Plato. (2000). T.");
  });
});

describe("in-text citations", () => {
  it("APA", () => {
    assert.equal(inTextCitation("apa", journal), "(Smith et al., 2021)");
    assert.equal(inTextCitation("apa", conf, "12"), "(Doe & Roe, 2020, p. 12)");
    assert.equal(inTextCitation("apa", conf, "12-14"), "(Doe & Roe, 2020, pp. 12–14)");
  });
  it("MLA, Chicago, IEEE", () => {
    assert.equal(inTextCitation("mla", conf, "12"), "(Doe and Roe 12)");
    assert.equal(inTextCitation("chicago", conf, "12"), "(Doe and Roe 2020, 12)");
    assert.equal(inTextCitation("ieee", conf, "12", 3), "[3, p. 12]");
    assert.equal(inTextCitation("ieee", conf, "", 3), "[3]");
  });
  it("ignores a non-numeric page", () => {
    assert.equal(inTextCitation("apa", conf, "Fig. 2"), "(Doe & Roe, 2020)");
  });
});

describe("document", () => {
  const rich = rec({ key: "r", title: "Notes paper", authorsVenue: "Z Zed", year: 2022, venue: "Journal of Notes", notes: "My <b>note</b>", pdfNotes: { highlights: [{ quote: "A key line", page: "7", note: "why" }] } });
  const recs = sortRecords([journal, rich, pre]);
  it("orders alphabetically with a heading per style", () => {
    const plain = blocksToPlain(buildReferenceBlocks(recs, { style: "mla" }).blocks);
    assert.ok(plain.startsWith("Works Cited\n\nSmith, A., et al."));
    assert.ok(plain.indexOf("Smith") < plain.indexOf("Vaswani") && plain.indexOf("Vaswani") < plain.indexOf("Zed"));
    assert.deepEqual(Object.keys(STYLES), ["apa", "mla", "chicago", "ieee"]);
  });
  it("numbers IEEE entries", () => {
    const plain = blocksToPlain(buildReferenceBlocks(recs, { style: "ieee" }).blocks);
    assert.match(plain, /\n\n\[1\] A\. Smith/);
    assert.match(plain, /\n\n\[3\] Z\. Zed/);
  });
  it("adds the quote sheet only when asked, with page-aware in-text citations", () => {
    assert.ok(!blocksToPlain(buildReferenceBlocks(recs, { style: "apa" }).blocks).includes("Quotes and notes"));
    const plain = blocksToPlain(buildReferenceBlocks(recs, { style: "apa", includeQuotes: true }).blocks);
    assert.match(plain, /Quotes and notes/);
    assert.match(plain, /Notes: My <b>note<\/b>/);
    assert.match(plain, /“A key line” \(Zed, 2022, p\. 7\) Note: why/);
  });
  it("omits the quote section when nothing has notes", () => {
    assert.ok(!blocksToPlain(buildReferenceBlocks([journal], { style: "apa", includeQuotes: true }).blocks).includes("Quotes and notes"));
  });
  it("escapes HTML and gives references a hanging indent", () => {
    const html = blocksToHtml(buildReferenceBlocks(recs, { style: "apa", includeQuotes: true }).blocks);
    assert.match(html, /My &lt;b&gt;note&lt;\/b&gt;/);
    assert.match(html, /<p style="margin-top:0;margin-bottom:6pt;margin-left:0\.5in;text-indent:-0\.5in;">/);
    const ieeeHtml = blocksToHtml(buildReferenceBlocks(recs, { style: "ieee" }).blocks);
    assert.ok(!ieeeHtml.includes("text-indent"));
  });
  it("wraps a Word-readable document", () => {
    const doc = buildDocHtml(buildReferenceBlocks(recs, { style: "apa" }).blocks, { now: new Date("2026-10-12T00:00:00Z") });
    assert.match(doc, /^<html xmlns:o=/);
    assert.match(doc, /<meta charset="utf-8">/);
    assert.match(doc, /Exported 2026-10-12/);
  });
  it("rejects an unknown style and handles an empty list", () => {
    assert.throws(() => buildReferenceBlocks(recs, { style: "harvard" }), /unknown style/);
    assert.equal(buildReferenceBlocks([], { style: "apa" }).blocks.length, 1);
  });
});
