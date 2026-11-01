import { describe, it } from "node:test";
import assert from "node:assert/strict";

const A = await import("../src/common/article-page.js");
const { parseRis, parseBibtex } = await import("../src/common/reference-import.js");

// The rows content.js reads from tests/e2e/fixtures/scholar-article.html (a real page, Oct 2026).
const RAW = {
  title: "Behavioral biometrics for continuous authentication in the internet-of-things era: An artificial intelligence perspective",
  url: "https://ieeexplore.ieee.org/abstract/document/9121981/",
  pdfUrl: "https://par.nsf.gov/servlets/purl/10258088",
  ownerName: "Sagar Samtani",
  rows: [
    { label: "Authors", value: "Yunji Liang, Sagar Samtani, Bin Guo, Zhiwen Yu" },
    { label: "Publication date", value: "2020/6/22" },
    { label: "Source", value: "IEEE Internet of Things Journal" },
    { label: "Volume", value: "7" },
    { label: "Issue", value: "9" },
    { label: "Pages", value: "9128-9143" },
    { label: "Publisher", value: "IEEE" },
    { label: "Description", value: "In the Internet-of-Things (IoT) era, user authentication is essential.", isDescription: true },
    { label: "Total citations", value: "Cited by 313", isCitations: true, citedByText: "Cited by 313", citedByHref: "https://scholar.google.com/scholar?oi=bibs&hl=en&cites=2392274550987972984&as_sdt=5" },
  ],
  bars: [[2020, 3], [2021, 24], [2022, 37], [2023, 55], [2024, 72], [2025, 73], [2026, 47]].map(([year, count]) => ({ year, count })),
};

describe("articleFromPage", () => {
  const a = A.articleFromPage(RAW);
  it("reads every field of the page", () => {
    assert.deepEqual(a.authors, ["Yunji Liang", "Sagar Samtani", "Bin Guo", "Zhiwen Yu"]);
    assert.equal(a.year, 2020);
    assert.equal(a.venue, "IEEE Internet of Things Journal");
    assert.equal(a.volume, "7");
    assert.equal(a.issue, "9");
    assert.equal(a.pages, "9128-9143");
    assert.equal(a.publisher, "IEEE");
    assert.equal(a.citedBy, 313);
    assert.equal(a.clusterId, "2392274550987972984");
    assert.equal(a.history.length, 7);
  });
  it("works with labels in another interface language", () => {
    const de = { ...RAW, rows: [
      { label: "Autoren", value: "Yunji Liang, Sagar Samtani" },
      { label: "Veröffentlichungsdatum", value: "2020/6/22" },
      { label: "Quelle", value: "IEEE Internet of Things Journal" },
      { label: "Band", value: "7" },
      { label: "Seiten", value: "9128-9143" },
      { label: "Beschreibung", value: "Text", isDescription: true },
      { label: "Zitate insgesamt", value: "", isCitations: true, citedByText: "Zitiert von: 1.313", citedByHref: "/scholar?cites=1" },
    ] };
    const g = A.articleFromPage(de);
    assert.deepEqual(g.authors, ["Yunji Liang", "Sagar Samtani"]);
    assert.equal(g.year, 2020);
    assert.equal(g.venue, "IEEE Internet of Things Journal");
    assert.equal(g.pages, "9128-9143");
    assert.equal(g.citedBy, 1313);
  });
  it("fills years without a bar with zero and returns null without a title", () => {
    const gap = A.articleFromPage({ ...RAW, bars: [{ year: 2020, count: 2 }, { year: 2023, count: 5 }] });
    assert.deepEqual(gap.history.map((h) => h.count), [2, 0, 0, 5]);
    assert.equal(A.articleFromPage({ rows: [] }), null);
  });
});

describe("citationStats", () => {
  const s = A.citationStats(A.articleFromPage(RAW), 2026);
  it("uses the chart: average, peak among full years, last full year against the three before, this year so far", () => {
    assert.equal(s.perYear, 52.2); // 313 over 6 years
    assert.deepEqual(s.peak, { year: 2025, count: 73 });
    assert.equal(s.trend.pct, 34); // 73 against the 2022-2024 mean of 54.7
    assert.deepEqual(s.soFar, { year: 2026, count: 47 });
    assert.deepEqual(A.citationStatsLines(s), [
      "52 citations a year on average", "peak 73 in 2025", "2025 up 34% on the 2022–2024 average", "47 so far in 2026",
    ]);
  });
  it("says nothing it cannot back", () => {
    const young = A.citationStats({ year: 2025, citedBy: 4, history: [{ year: 2025, count: 1 }, { year: 2026, count: 3 }] }, 2026);
    assert.equal(young.trend, null);
    assert.equal(A.citationStats({ history: [] }, 2026).perYear, null);
  });
});

describe("authorPosition", () => {
  const authors = ["Yunji Liang", "Sagar Samtani", "Bin Guo", "Zhiwen Yu"];
  it("finds the profile's owner by surname and first initial", () => {
    assert.deepEqual(A.authorPosition(authors, "Sagar Samtani"), { index: 2, count: 4, role: "middle" });
    assert.equal(A.authorPosition(authors, "Zhiwen Yu").role, "last");
    assert.equal(A.authorPosition(["S Samtani", "H Chen"], "Sagar Samtani").role, "first");
    assert.equal(A.authorPosition(["José Gómez"], "Jose Gomez").role, "sole");
    assert.equal(A.authorPosition(authors, "Hsinchun Chen"), null);
  });
  it("words it plainly, and never claims last author on a cut-off list", () => {
    assert.equal(A.authorPositionText({ index: 2, count: 4, role: "middle" }, "Sagar Samtani"), "Sagar Samtani is author 2 of 4.");
    assert.equal(A.authorPositionText({ index: 4, count: 4, role: "last" }, "Zhiwen Yu", true), "Zhiwen Yu is author 4 of 4+.");
  });
});

describe("exports with the page's full metadata", () => {
  const a = A.articleFromPage(RAW);
  it("BibTeX has every author, volume, number, pages and publisher, and reads back", () => {
    const bib = A.articleBibTeX(a, "10.1109/JIOT.2020.3004077");
    assert.match(bib, /^@article\{liang2020behavioral,/);
    assert.match(bib, /author=\{Yunji Liang and Sagar Samtani and Bin Guo and Zhiwen Yu\}/);
    assert.match(bib, /volume=\{7\},\n  number=\{9\},\n  pages=\{9128--9143\},\n  publisher=\{IEEE\}/);
    const back = parseBibtex(bib).records[0];
    assert.equal(back.title, a.title);
    assert.equal(back.doi, "10.1109/jiot.2020.3004077");
  });
  it("RIS has VL, IS, SP, EP and PB, and reads back", () => {
    const ris = A.articleRis(a);
    for (const line of ["VL  - 7", "IS  - 9", "SP  - 9128", "EP  - 9143", "PB  - IEEE", "AU  - Samtani, Sagar"]) assert.ok(ris.includes(line), line);
    const back = parseRis(ris).records[0];
    assert.equal(back.title, a.title);
    assert.equal(String(back.year), "2020");
  });
  it("writes the authors-and-venue line the library reads", () => {
    assert.equal(A.authorsVenueLine(a), "Yunji Liang, Sagar Samtani, Bin Guo, Zhiwen Yu - IEEE Internet of Things Journal, 2020");
  });
});
