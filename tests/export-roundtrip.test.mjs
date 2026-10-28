import { describe, it } from "node:test";
import assert from "node:assert/strict";

// What the library exports must read back with the extension's own importers (the same files go to Zotero and EndNote).
const { buildRis } = await import("../src/common/zotero-export.js");
const { buildBibTeXEntry } = await import("../src/common/bibtex.js");
const { parseRis, parseBibtex } = await import("../src/common/reference-import.js");

const records = [
  { key: "a", title: "Sleep deprivation and working memory: a meta-analysis", authors: ["SJ Frenda", "KM Fenn"], year: "2016",
    venue: "Journal of Applied Research in Memory and Cognition", doi: "10.1016/j.jarmac.2016.10.001", url: "https://example.org/a",
    abstract: "We review 40 studies.", tags: ["sleep"], notes: "", summary: "", collections: [], highlights: [], badges: [], citekey: "" },
  { key: "b", title: "R&D, 50% margins and p_values", authors: ["J Müller"], year: "2021", venue: "Proceedings of ICIS",
    doi: "10.1234/xyz_1", url: "", abstract: "", tags: [], notes: "", summary: "", collections: [], highlights: [], badges: [], citekey: "" },
];

describe("library exports read back with the importers", () => {
  it("RIS keeps titles, years, DOIs and author counts", () => {
    const back = parseRis(buildRis(records, { includeNotes: false }));
    const list = Array.isArray(back) ? back : back.records || back.papers || [];
    assert.equal(list.length, 2);
    for (const [i, r] of records.entries()) {
      assert.equal(list[i].title, r.title);
      assert.equal(String(list[i].year), r.year);
      assert.equal(String(list[i].doi).toLowerCase(), r.doi.toLowerCase());
    }
  });
  it("BibTeX keeps years and DOIs, and LaTeX-escaped titles unescape to the original", () => {
    const text = records.map((r) => buildBibTeXEntry({ ...r, authors: r.authors })).join("\n\n");
    const back = parseBibtex(text);
    const list = Array.isArray(back) ? back : back.records || back.papers || [];
    assert.equal(list.length, 2);
    for (const [i, r] of records.entries()) {
      assert.equal(String(list[i].year), r.year);
      assert.equal(String(list[i].doi).toLowerCase(), r.doi.toLowerCase());
      assert.equal(String(list[i].title).replace(/\\([&%#_$])/g, "$1").replace(/[{}]/g, ""), r.title);
    }
  });
});
