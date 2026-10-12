import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { splitName, toRecord } from "../src/common/export-model.js";
import { buildRis, buildRisEntry, buildCslJson, noteHtml } from "../src/common/zotero-export.js";

const colls = new Map([["c1", { name: "Thesis; ch. 2" }]]);
const full = toRecord({
  key: "k1", title: "Attention is all you need", authorsVenue: "A Vaswani, N Shazeer, J van der Berg - Proceedings of the 31st Conference on Neural Information Processing Systems, 2017",
  year: 2017, doi: "10.5555/3295222.3295349", url: "https://example.org/p?a=1&b=2", abstract: "The dominant models\nuse recurrence.",
  tags: ["nlp", "deep learning"], notes: "Core <paper> & more.\n\nSecond para\nwith break.", collections: ["c1"],
  pdfNotes: { summary: "Short.", highlights: [{ quote: "Attention is all you need", page: "3", note: "key" }] },
}, { collections: colls, venueOf: () => "Proceedings of the 31st Conference on Neural Information Processing Systems" });
const bare = toRecord({ key: "k2", title: "No metadata" });

describe("splitName", () => {
  it("handles initials, particles, single words and commas", () => {
    assert.deepEqual(splitName("A Vaswani"), { family: "Vaswani", given: "A" });
    assert.deepEqual(splitName("J van der Berg"), { family: "van der Berg", given: "J" });
    assert.deepEqual(splitName("Plato"), { family: "Plato", given: "" });
    assert.deepEqual(splitName("Smith, John A."), { family: "Smith", given: "John A." });
    assert.deepEqual(splitName("  "), { family: "", given: "" });
  });
});

describe("RIS", () => {
  const ris = buildRisEntry(full);
  const lines = ris.split("\r\n");
  it("starts with TY and ends with ER", () => {
    assert.equal(lines[0], "TY  - CPAPER");
    assert.equal(lines[lines.length - 1], "ER  - ");
  });
  it("writes one AU per author as Family, Given", () => {
    assert.deepEqual(lines.filter((l) => l.startsWith("AU")), ["AU  - Vaswani, A", "AU  - Shazeer, N", "AU  - van der Berg, J"]);
  });
  it("writes venue, doi, year, url, abstract on one line each", () => {
    assert.ok(lines.includes("T2  - Proceedings of the 31st Conference on Neural Information Processing Systems"));
    assert.ok(lines.includes("DO  - 10.5555/3295222.3295349"));
    assert.ok(lines.includes("PY  - 2017"));
    assert.ok(lines.includes("AB  - The dominant models use recurrence."));
    assert.ok(lines.some((l) => l.startsWith("UR  - https://example.org/p?a=1&b=2")));
  });
  it("writes tags as KW and notes as one-line HTML N1", () => {
    assert.deepEqual(lines.filter((l) => l.startsWith("KW")), ["KW  - nlp", "KW  - deep learning"]);
    const n1 = lines.filter((l) => l.startsWith("N1"));
    assert.equal(n1.length, 4);
    assert.equal(n1[0], "N1  - <p>Core &lt;paper&gt; &amp; more.</p><p>Second para<br/>with break.</p>");
    assert.match(n1[2], /<blockquote><p>Attention is all you need<\/p><p>p\. 3\. Note: key<\/p><\/blockquote>/);
    assert.match(n1[3], /collections: Thesis; ch\. 2/);
    for (const l of lines) assert.ok(!/[\n]/.test(l));
  });
  it("can leave notes out", () => {
    const l = buildRisEntry(full, { includeNotes: false }).split("\r\n").filter((x) => x.startsWith("N1"));
    assert.equal(l.length, 1);
    assert.match(l[0], /collections/);
  });
  it("copes with a record that has nothing but a title", () => {
    assert.equal(buildRisEntry(bare), "TY  - GEN\r\nTI  - No metadata\r\nER  - ");
  });
  it("uses JOUR for journals and GEN for preprints", () => {
    assert.match(buildRisEntry({ ...bare, venue: "Journal of Things" }), /^TY  - JOUR/);
    assert.match(buildRisEntry({ ...bare, venue: "arXiv preprint arXiv:1706.03762" }), /^TY  - GEN/);
  });
  it("separates records with a blank line", () => {
    const out = buildRis([full, bare]);
    assert.ok(out.includes("ER  - \r\n\r\nTY  - GEN"));
    assert.equal(buildRis([]), "");
  });
});

describe("CSL-JSON", () => {
  const data = JSON.parse(buildCslJson([full, bare]));
  it("is valid JSON with CSL fields", () => {
    assert.equal(data[0].type, "paper-conference");
    assert.deepEqual(data[0].author[2], { family: "van der Berg", given: "J" });
    assert.deepEqual(data[0].issued, { "date-parts": [[2017]] });
    assert.equal(data[0]["container-title"], "Proceedings of the 31st Conference on Neural Information Processing Systems");
    assert.equal(data[0].DOI, "10.5555/3295222.3295349");
    assert.equal(data[0].keyword, "nlp, deep learning");
  });
  it("puts user notes and collections in note", () => {
    assert.match(data[0].note, /^Core <paper> & more\./);
    assert.match(data[0].note, /Scholar Utility Belt collections: Thesis; ch\. 2$/);
  });
  it("omits empty fields and gives every item an id", () => {
    assert.deepEqual(Object.keys(data[1]).sort(), ["id", "title", "type"]);
    assert.ok(data[0].id && data[1].id);
  });
});

describe("noteHtml", () => {
  it("returns empty for blank input", () => assert.equal(noteHtml("  \n\n "), ""));
});
