import { describe, it } from "node:test";
import assert from "node:assert/strict";
import zlib from "node:zlib";

const T = await import("../src/common/tenure-packet.js");
const { buildDocx } = await import("../src/common/docx-writer.js");

const NOW = new Date(2026, 9, 12, 12, 0, 0);

const snapshot = () => ({
  authorName: "Ana <Q> O'Neil & Co",
  profileUrl: "https://scholar.google.com/citations?user=abc123XYZ&hl=en",
  sidebar: {
    headers: ["", "All", "Since 2021"],
    rows: [["Citations", "5,432", "3,100"], ["h-index", "31", "25"], ["i10-index", "70", "55"]]
  },
  histogram: { years: ["2022", "2023", "2024"], counts: ["400", "900", "1,100"] },
  papers: [
    { title: "Trust in online markets", year: 2020, citations: 1200, authorsText: "A O'Neil, B Lee" },
    { title: "A <script>alert(1)</script> title", year: 2022, citations: 40, authorsText: "A O'Neil" },
    { title: "Old paper", year: 2008, citations: 9, authorsText: "A O'Neil, C Wu" },
    { title: "Undated item", year: null, citations: 3, authorsText: "" },
    { title: "Another", year: 2022, citations: 15, authorsText: "" }
  ],
  partial: false,
  qualityBadgesEnabled: true,
  stats: {
    hIndex: 4, gIndex: 7, mIndex: 0.25, h5Index: 2, firstYear: 2008, totalCitations: 1267,
    soloAuthored: 1, firstAuthor: 3, lastAuthor: 0, middleAuthor: 1,
    qualityCounts: { q1: 3, q2: 0, q3: 0, q4: 0, a: 2, vhb: 0, utd24: 1, ft50: 1, abs4star: 0, core: { "A*": 1, A: 0, B: 0, C: 0 }, era: 4 }
  }
});

const textOf = (packet) => packet.blocks.map((b) => [b.text, ...(b.header || []), ...((b.rows || []).flat())].filter((x) => x != null).join(" ")).join("\n");

describe("parseSidebarTable", () => {
  it("reads Scholar's Cited by box", () => {
    const r = T.parseSidebarTable({ headers: ["", "All", "Since 2021"], rows: [["Citations", "5,432", "3,100"], ["h-index", "31", "25"], ["i10-index", "70", "55"]] });
    assert.deepEqual(r.citations, { all: 5432, recent: 3100 });
    assert.deepEqual(r.hIndex, { all: 31, recent: 25 });
    assert.deepEqual(r.i10Index, { all: 70, recent: 55 });
    assert.equal(r.sinceLabel, "Since 2021");
  });
  it("reads the box in another interface language", () => {
    // German Scholar: translated labels and "." as the thousands separator.
    const r = T.parseSidebarTable({ headers: ["", "Alle", "Seit 2021"], rows: [["Zitiert von", "5.432", "3.100"], ["h-Index", "31", "25"], ["i10-Index", "70", "55"]] });
    assert.deepEqual(r.citations, { all: 5432, recent: 3100 });
    assert.deepEqual(r.hIndex, { all: 31, recent: 25 });
    assert.deepEqual(r.i10Index, { all: 70, recent: 55 });
    assert.equal(r.sinceLabel, "Seit 2021");
  });
  it("tolerates missing, reordered or malformed input", () => {
    assert.deepEqual(T.parseSidebarTable(null).citations, null);
    assert.deepEqual(T.parseSidebarTable({ rows: [["h-index", "x", "y"], ["junk"]] }).hIndex, { all: null, recent: null });
  });
});

describe("pairCitationHistogram", () => {
  it("pairs equal-length lists", () => {
    assert.deepEqual(T.pairCitationHistogram({ years: ["2023", "2024"], counts: ["5", "1,000"] }), [{ year: 2023, citations: 5 }, { year: 2024, citations: 1000 }]);
  });
  it("returns null rather than guessing when lengths differ or values are bad", () => {
    assert.equal(T.pairCitationHistogram({ years: ["2023", "2024"], counts: ["5"] }), null);
    assert.equal(T.pairCitationHistogram({ years: ["abc"], counts: ["5"] }), null);
    assert.equal(T.pairCitationHistogram({ years: [], counts: [] }), null);
    assert.equal(T.pairCitationHistogram(null), null);
  });
});

describe("buildTenurePacket", () => {
  const packet = T.buildTenurePacket(snapshot(), NOW);
  const text = textOf(packet);

  it("includes the author, the generation date and the disclaimers", () => {
    assert.ok(text.includes("Ana <Q> O'Neil & Co"));
    assert.ok(text.includes("Generated 12 October 2026"));
    assert.ok(text.includes("reflects the profile as of that date") || text.includes("reflect the profile as of"));
    assert.ok(text.includes("may require its own format"));
    assert.ok(text.includes("does not claim to meet any institution's requirements"));
  });
  it("labels each index with where it came from", () => {
    const idx = packet.blocks.find((b) => b.type === "table" && b.header[0] === "Measure");
    const get = (name) => idx.rows.find((r) => r[0] === name);
    assert.deepEqual(get("Citations, all time").slice(1, 2), ["5,432"]);
    assert.match(get("Citations, all time")[2], /Reported by Google Scholar/);
    assert.match(get("h-index (computed)")[2], /Computed by the extension from the 5 publications loaded/);
    assert.equal(get("h-index, since 2021")[1], "25");
    assert.equal(get("i10-index (computed)")[1], "3");
    assert.equal(get("m-index (computed)")[1], "0.25");
    assert.match(get("m-index (computed)")[2], /2008/);
    for (const r of idx.rows) assert.ok(r[2], `source for ${r[0]}`);
  });
  it("lists citations per year and publications by year", () => {
    const t = packet.blocks.filter((b) => b.type === "table");
    const perYear = t.find((b) => b.header[0] === "Year" && b.header[1] === "Citations");
    assert.deepEqual(perYear.rows.map((r) => r[0]), ["2022", "2023", "2024"]);
    assert.equal(perYear.rows[2][1], "1,100");
    const pubs = t.find((b) => b.header[1] === "Publications" && b.header[0] === "Year");
    assert.deepEqual(pubs.rows, [["2008", "1"], ["2020", "1"], ["2022", "2"]]);
    assert.match(text, /1 had no year and are not counted/);
  });
  it("gives badge counts, hides zero rows and describes the limits", () => {
    const q = packet.blocks.find((b) => b.type === "table" && b.header[0] === "List or rank");
    const labels = q.rows.map((r) => r[0]);
    assert.ok(labels.includes("FT50") && labels.includes("UTD24") && labels.includes("SJR quartile Q1") && labels.includes("ABDC A* or A") && labels.includes("CORE A*"));
    assert.equal(labels.includes("SJR quartile Q2"), false);
    assert.equal(q.rows.find((r) => r[0] === "SJR quartile Q1")[1], "3");
    assert.match(text, /not a judgement of quality/);
  });
  it("reports authorship counts with a caveat", () => {
    const a = packet.blocks.find((b) => b.type === "table" && b.header[0] === "Position");
    assert.deepEqual(a.rows[0], ["First author", "3"]);
    assert.match(text, /abbreviated or cut short/);
  });
  it("sorts top papers by citations", () => {
    const top = packet.blocks.find((b) => b.type === "table" && b.header[0] === "#");
    assert.equal(top.rows[0][1].split("\n")[0], "Trust in online markets");
    assert.equal(top.rows[0][3], "1,200");
    assert.equal(top.rows.length, 5);
  });
  it("says so when the chart cannot be read", () => {
    const s = snapshot();
    s.histogram = { years: ["2023"], counts: [] };
    assert.match(textOf(T.buildTenurePacket(s, NOW)), /Not available. The citations-by-year chart could not be read/);
  });
  it("warns when only part of the profile was loaded", () => {
    const s = snapshot();
    s.partial = true;
    const t = textOf(T.buildTenurePacket(s, NOW));
    assert.match(t, /Only 5 publications had been loaded/);
  });
  it("says so when venue badges were off", () => {
    const s = snapshot();
    s.qualityBadgesEnabled = false;
    assert.match(textOf(T.buildTenurePacket(s, NOW)), /Venue badges were turned off/);
  });
  it("copes with an empty or garbage snapshot", () => {
    for (const bad of [null, undefined, {}, "x", { papers: "no", stats: 5, sidebar: 7 }]) {
      const p = T.buildTenurePacket(bad, NOW);
      assert.ok(p.blocks.length > 5);
      assert.match(textOf(p), /Author name not found/);
      assert.match(textOf(p), /Not available/);
    }
  });
  it("drops a profile URL that is not a Scholar profile link", () => {
    const s = snapshot();
    s.profileUrl = "https://evil.example/citations?user=x";
    assert.equal(textOf(T.buildTenurePacket(s, NOW)).includes("evil.example"), false);
  });
  it("never claims to satisfy requirements", () => {
    assert.equal(/satisf(y|ies)|meets? (the )?requirements/i.test(text.replace("does not claim to meet any institution's requirements", "")), false);
  });
});

describe("packetToHtml", () => {
  const packet = T.buildTenurePacket(snapshot(), NOW);
  const html = T.packetToHtml(packet);
  it("escapes titles, names and attribute characters", () => {
    assert.equal(html.includes("<script>"), false);
    assert.ok(html.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert.ok(html.includes("Ana &lt;Q&gt; O&#39;Neil &amp; Co"));
  });
  it("renders headings, tables and notes", () => {
    assert.ok(html.startsWith('<article class="doc">'));
    assert.ok(html.includes("<h1>Scholar profile summary</h1>"));
    assert.ok(html.includes("<h2>Citation indices</h2>"));
    assert.ok(html.includes('<td class="num">'));
    assert.ok(html.includes('<p class="src">'));
    assert.ok(html.includes('<div class="notice">'));
  });
  it("has no inline styles or event handlers (extension CSP)", () => {
    assert.equal(/\sstyle=|\son\w+=/i.test(html), false);
  });
});

describe("packet to docx", () => {
  it("produces a docx whose document.xml carries the escaped author and tables", () => {
    const packet = T.buildTenurePacket(snapshot(), NOW);
    const bytes = buildDocx(packet.blocks, { title: packet.title, author: "Scholar Utility Belt", created: NOW });
    // read document.xml from the stored zip
    const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let pos = 0, doc = null, ct = false;
    while (dv.getUint32(pos, true) === 0x04034b50) {
      const csize = dv.getUint32(pos + 18, true);
      const nlen = dv.getUint16(pos + 26, true);
      const elen = dv.getUint16(pos + 28, true);
      const name = new TextDecoder().decode(bytes.subarray(pos + 30, pos + 30 + nlen));
      const data = bytes.subarray(pos + 30 + nlen + elen, pos + 30 + nlen + elen + csize);
      assert.equal(zlib.crc32(data) >>> 0, dv.getUint32(pos + 14, true));
      if (name === "word/document.xml") doc = new TextDecoder().decode(data);
      if (name === "[Content_Types].xml") ct = true;
      pos += 30 + nlen + elen + csize;
    }
    assert.ok(ct && doc);
    assert.ok(doc.includes("Ana &lt;Q&gt; O&apos;Neil &amp; Co"));
    assert.ok(doc.includes("&lt;script&gt;alert(1)&lt;/script&gt;"));
    assert.equal(doc.includes("<script>"), false);
    assert.ok((doc.match(/<w:tbl>/g) || []).length >= 6);
  });
});

describe("packetFileName", () => {
  it("makes a safe file name", () => {
    assert.equal(T.packetFileName({ authorName: "Ana O'Neil-Zoë" }, NOW), "scholar-profile-summary-ana-o-neil-zoe-2026-10-12");
    assert.equal(T.packetFileName(null, NOW), "scholar-profile-summary-author-2026-10-12");
  });
});
