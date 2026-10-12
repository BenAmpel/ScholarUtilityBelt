import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { toRecord, parseAuthors, highlightsOf, selectPapers, sortRecords, slugify, cleanDoi } from "../src/common/export-model.js";
import { buildNotesMarkdown, buildNotesHtml, escapeMd } from "../src/common/notes-export.js";

const NOW = new Date("2026-10-12T12:00:00Z");
const colls = new Map([["c1", { id: "c1", name: "Thesis" }], ["c2", { id: "c2", name: "Reading" }]]);
const saved = {
  a: { key: "a", title: "Attention  is\nall you need", authorsVenue: "A Vaswani, N Shazeer, … - Advances in neural information processing systems, 2017 - papers.nips.cc", year: 2017, doi: "https://doi.org/10.5555/3295222.3295349", tags: ["nlp"], notes: "Core paper.\n\nRe-read section 3.", collections: ["c1", "gone"], pdfNotes: { summary: "Transformers.", highlights: [{ quote: "Attention is *all* you need", page: "3", note: "key claim" }, { quote: "", page: "9" }, { quote: "multi\nline", page: "" }] } },
  b: { key: "b", title: "Plain paper", authorsVenue: "B Jones", year: "n.d.", collections: ["c2"] },
  c: null,
};

describe("export model", () => {
  it("parses authors and drops the truncation marker", () => {
    assert.deepEqual(parseAuthors(saved.a.authorsVenue), ["A Vaswani", "N Shazeer"]);
    assert.deepEqual(parseAuthors(""), []);
    assert.deepEqual(parseAuthors(undefined), []);
    assert.deepEqual(parseAuthors("Jean-Paul Sartre, A Camus - Gallimard, 1947"), ["Jean-Paul Sartre", "A Camus"]);
  });
  it("builds a record with collection names, venue fallback and cleaned DOI", () => {
    const r = toRecord(saved.a, { collections: colls, venueOf: () => "NeurIPS", badgesOf: (v) => [`B:${v}`] });
    assert.equal(r.title, "Attention is all you need");
    assert.deepEqual(r.collections, ["Thesis"]);
    assert.equal(r.doi, "10.5555/3295222.3295349");
    assert.equal(r.venue, "NeurIPS");
    assert.deepEqual(r.badges, ["B:NeurIPS"]);
    assert.equal(r.year, "2017");
    assert.equal(r.highlights.length, 2);
  });
  it("survives a throwing badge lookup and a missing paper", () => {
    assert.deepEqual(toRecord(saved.b, { badgesOf: () => { throw new Error("x"); }, venueOf: () => "V" }).badges, []);
    const r = toRecord(null);
    assert.equal(r.title, "");
    assert.deepEqual(r.authors, []);
  });
  it("selects by collection and sorts deterministically", () => {
    assert.equal(selectPapers(saved, "c1").length, 1);
    assert.equal(selectPapers(saved, "").length, 2);
    const recs = sortRecords([toRecord(saved.b), toRecord(saved.a)]);
    assert.deepEqual(recs.map((r) => r.key), ["b", "a"]); // Jones before Vaswani
  });
  it("cleanDoi rejects non-DOIs", () => {
    assert.equal(cleanDoi("doi: 10.1000/xyz"), "10.1000/xyz");
    assert.equal(cleanDoi("not a doi"), "");
    assert.equal(cleanDoi(null), "");
  });
  it("highlightsOf handles non-arrays", () => {
    assert.deepEqual(highlightsOf({ pdfNotes: { highlights: "x" } }), []);
    assert.deepEqual(highlightsOf(null), []);
  });
  it("slugify is ascii, bounded and never empty", () => {
    assert.equal(slugify("Café: Déjà vu?"), "cafe-deja-vu");
    assert.equal(slugify("???"), "untitled");
    assert.ok(slugify("word ".repeat(40), 30).length <= 30);
    assert.ok(!slugify("word ".repeat(40), 30).endsWith("-"));
  });
});

describe("notes markdown", () => {
  const recs = sortRecords([toRecord(saved.a, { collections: colls }), toRecord(saved.b, { collections: colls })]);
  const md = buildNotesMarkdown(recs, { scope: "Thesis [draft]", now: NOW });
  it("has a header with scope and date", () => {
    assert.match(md, /^# Highlights and notes: Thesis \\\[draft\\\]/);
    assert.match(md, /2 papers\. Exported 2026-10-12/);
  });
  it("groups by paper with citation, notes and quoted highlights", () => {
    assert.match(md, /## 2\. Attention is all you need/);
    assert.match(md, /Core paper\.\n\nRe-read section 3\./);
    assert.match(md, /> Attention is \*all\* you need\n>\n> p\. 3\. Note: key claim/);
    assert.match(md, /> multi\n> line/);
    assert.match(md, /DOI: https:\/\/doi\.org\/10\.5555/);
  });
  it("says so when a paper has nothing saved", () => {
    assert.match(md, /## 1\. Plain paper[\s\S]*No notes or highlights saved\./);
  });
  it("escapes markdown in titles", () => {
    assert.equal(escapeMd("a_b *c* [d]"), "a\\_b \\*c\\* \\[d\\]");
  });
  it("handles an empty selection", () => {
    assert.match(buildNotesMarkdown([], { now: NOW }), /0 papers/);
  });
  it("uses a custom citation formatter", () => {
    assert.match(buildNotesMarkdown(recs, { cite: () => "CITED", now: NOW }), /\nCITED\n/);
  });
});

describe("notes html", () => {
  it("escapes user text and keeps quotes in blockquotes", () => {
    const r = toRecord({ key: "x", title: "A <b>bold</b> & title", authorsVenue: "Z Zed", year: 2020, notes: "<script>alert(1)</script>", pdfNotes: { highlights: [{ quote: "q \"1\"", page: "2", note: "n" }] } });
    const html = buildNotesHtml([r], { now: NOW });
    assert.ok(!html.includes("<script>"));
    assert.match(html, /A &lt;b&gt;bold&lt;\/b&gt; &amp; title/);
    assert.match(html, /<blockquote><p>q &quot;1&quot;<\/p><footer>p\. 2\. Note: n<\/footer><\/blockquote>/);
    assert.match(html, /<meta charset="utf-8">/);
  });
});
