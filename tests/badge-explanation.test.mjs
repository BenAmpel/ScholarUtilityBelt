import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const {
  compileQualityIndex,
  qualityBadgesForVenue,
  customBadgeForVenue,
  explainBadge,
  renderBadgeExplanation,
  BADGE_SOURCES,
} = await import("../src/common/quality.js");
const { compileCustomVenueIndex } = await import("../src/common/custom-venues.js");

// Minimal DOM: enough for renderBadgeExplanation (createElement, textContent, setAttribute, appendChild).
class El {
  constructor(tag) { this.tag = tag; this.children = []; this.className = ""; this.textContent = ""; this.attrs = {}; }
  setAttribute(k, v) { this.attrs[k] = v; }
  appendChild(c) { this.children.push(c); return c; }
}
const doc = { createElement: (t) => new El(t) };

/** The rendered <dl> as { label: value } pairs, plus the value per data-field. */
function read(dl) {
  const out = {};
  const byField = {};
  for (let i = 0; i < dl.children.length; i += 2) {
    const dt = dl.children[i];
    const dd = dl.children[i + 1];
    assert.equal(dt.tag, "dt");
    assert.equal(dd.tag, "dd");
    out[dt.textContent] = dd.textContent;
    byField[dd.attrs["data-field"]] = dd.textContent;
  }
  return { out, byField };
}

// Fixture: one Scholar result whose venue line reads "MIS Quarterly: Management Information Systems", matched
// against a small SJR index, an ABDC list and FT50.
const qIndex = compileQualityIndex(
  { qualityAbdcRanks: "MIS Quarterly: Management Information Systems,A*\n", qualityFt50List: "MIS Quarterly\n" },
  { quartilesIndex: { "MIS Quarterly: Management Information Systems": "Q1" } }
);
const SCHOLAR_VENUE = "MIS Quarterly: Management Information Systems";
const badges = qualityBadgesForVenue(SCHOLAR_VENUE, qIndex);
const byKind = (k) => badges.find((b) => b.kind === k);

describe("badge popover: list, edition, subject category, venue shown vs matched", () => {
  it("a quartile badge renders all four fields", () => {
    const q = byKind("quartile");
    assert.ok(q, "fixture yields an SJR quartile badge");
    const dl = renderBadgeExplanation(doc, q, { quartilesMeta: { year: 2024, source: "SCImago Journal Rank (CSV export)" } });
    assert.equal(dl.tag, "dl");
    assert.equal(dl.className, "su-badge-source");
    const { out } = read(dl);
    assert.deepEqual(Object.keys(out), ["List", "Edition", "Subject category", "Scholar shows", "Matched in the list"]);
    assert.equal(out.List, "SCImago Journal Rank (SJR)");
    assert.equal(out.Edition, "2024");
    assert.match(out["Subject category"], /best of those quartiles/);
    assert.equal(out["Scholar shows"], SCHOLAR_VENUE);
    assert.match(out["Matched in the list"], /^mis quarterly management information systems \(exact match\)$/);
  });

  it("the quartile edition follows the stored file's year (an imported newer SJR file)", () => {
    assert.equal(explainBadge(byKind("quartile"), { quartilesMeta: { year: 2025 } }).edition, "2025");
  });

  it("a non-quartile list says the subject category is not applicable", () => {
    const { byField } = read(renderBadgeExplanation(doc, byKind("abdc")));
    assert.equal(byField.list, "ABDC Journal Quality List");
    assert.equal(byField.edition, "2025");
    assert.match(byField.category, /^Not applicable\./);
    assert.equal(byField.venueShown, SCHOLAR_VENUE);
  });

  it("a prefix match shows the different list entry it landed on", () => {
    const ft = qualityBadgesForVenue("MIS Quarterly", compileQualityIndex({ qualityFt50List: "MIS Quarterly Executive\n" }))
      .find((b) => b.kind === "ft50");
    assert.ok(ft);
    const info = explainBadge(ft);
    assert.equal(info.venueShown, "MIS Quarterly");
    assert.equal(info.venueMatched, "mis quarterly executive");
    assert.equal(info.category.startsWith("Not applicable"), true);
  });

  it("a CORE alias match names the entry it used, not the venue line", () => {
    const idx = compileQualityIndex({}, {});
    idx.core.set("international conference on information systems", "A*");
    const core = qualityBadgesForVenue("ICIS 2024 Proceedings", idx).find((b) => b.kind === "core");
    assert.ok(core);
    assert.equal(explainBadge(core).venueMatched, "international conference on information systems");
  });

  it("a JCR quartile reports the category from the imported file, or says it was not recorded", () => {
    const withCat = { kind: "jcr", text: "JIF Q1", metadata: { quartile: "Q1", jcrData: { category: "Information Science & Library Science" }, venueShown: "X", venueMatched: "x" } };
    assert.equal(explainBadge(withCat, { jcrMeta: { year: 2023 } }).category, "Information Science & Library Science");
    assert.equal(explainBadge(withCat, { jcrMeta: { year: 2023 } }).edition, "2023");
    const noCat = { kind: "jcr", text: "JIF Q2", metadata: { quartile: "Q2", jcrData: {} } };
    assert.equal(explainBadge(noCat).category, "Not recorded in your imported file");
    assert.equal(explainBadge(noCat).venueShown, "Not available");
  });

  it("a custom-list badge names the user's list", () => {
    const custom = { name: "Dean's list", map: compileCustomVenueIndex([["Decision Support Systems", "B"]]) };
    const b = customBadgeForVenue("Decision Support Systems", custom);
    assert.ok(b);
    const info = explainBadge(b);
    assert.equal(info.list, "Your list: Dean's list");
    assert.match(info.category, /^Not applicable/);
    assert.equal(info.venueShown, "Decision Support Systems");
    assert.match(info.venueMatched, /\(exact match\)$/);
  });

  it("badges that are not from a ranking list get no source block", () => {
    const pre = qualityBadgesForVenue("arXiv preprint arXiv:2401.00001", qIndex).find((b) => b.kind === "preprint");
    assert.ok(pre);
    assert.equal(explainBadge(pre), null);
    assert.equal(renderBadgeExplanation(doc, pre), null);
  });

  it("venue text is set with textContent, never parsed as HTML", () => {
    const b = { kind: "abdc", text: "ABDC A", metadata: { venueShown: "<img src=x onerror=alert(1)>", venueMatched: "y" } };
    const { byField } = read(renderBadgeExplanation(doc, b));
    assert.equal(byField.venueShown, "<img src=x onerror=alert(1)>");
  });

  it("every bundled list in the Options table has a list name and an edition", () => {
    for (const s of BADGE_SOURCES.filter((x) => x.file)) {
      assert.ok(s.list && s.edition && s.name && s.badge, s.kind);
    }
    // Options reads the same rows, so its table and the popover cannot disagree.
    const options = readFileSync(new URL("../src/options/options.js", import.meta.url), "utf8");
    assert.match(options, /BADGE_SOURCES as BUNDLED_BADGE_SOURCES/);
  });
});
