import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const {
  buildWrongMatchIssueUrl,
  renderWrongMatchLink,
  compileQualityIndex,
  qualityBadgesForVenue,
  customBadgeForVenue,
} = await import("../src/common/quality.js");
const { compileCustomVenueIndex } = await import("../src/common/custom-venues.js");

class El {
  constructor(tag) { this.tag = tag; this.children = []; this.className = ""; this.textContent = ""; this.attrs = {}; }
  setAttribute(k, v) { this.attrs[k] = v; }
  appendChild(c) { this.children.push(c); return c; }
}
const doc = { createElement: (t) => new El(t) };

const TRICKY = `Journal of "R&D" #1 – Économie & Société 'Ω'`;

function parse(href) {
  const u = new URL(href);
  return { u, title: u.searchParams.get("title"), body: u.searchParams.get("body") };
}

describe("Wrong match? issue URL builder", () => {
  it("encodes special characters and round-trips through URL/URLSearchParams", () => {
    const href = buildWrongMatchIssueUrl({
      scholarVenue: TRICKY, matchedVenue: "journal of r d", listName: "ABDC Journal Quality List",
      edition: "2025", rank: "ABDC A*", extensionVersion: "1.1.4",
    });
    // Raw specials never leak into the query string unencoded.
    const query = href.split("?")[1];
    assert.doesNotMatch(query.replace(/^title=/, "").replace("&body=", ""), /[&#"' ]/);
    const { u, title, body } = parse(href);
    assert.equal(u.origin + u.pathname, "https://github.com/BenAmpel/ScholarUtilityBelt/issues/new");
    assert.equal(u.hash, "");
    assert.deepEqual([...u.searchParams.keys()], ["title", "body"]);
    assert.ok(title.includes(TRICKY));
    assert.ok(body.includes(`Scholar shows: ${TRICKY}`));
    assert.ok(body.includes("Matched in the list: journal of r d"));
    assert.ok(body.includes("List: ABDC Journal Quality List"));
    assert.ok(body.includes("Edition: 2025"));
    assert.ok(body.includes("Rank shown: ABDC A*"));
    assert.ok(body.includes("Extension version: 1.1.4"));
  });

  it("carries only the allowed fields, even when extra personal fields are passed", () => {
    const href = buildWrongMatchIssueUrl({
      scholarVenue: "MIS Quarterly", matchedVenue: "mis quarterly", listName: "FT50", edition: "x", rank: "FT50",
      extensionVersion: "1.0.0",
      pageUrl: "https://scholar.google.com/scholar?q=secret+query", query: "secret query",
      title: "My Private Paper Title", authors: "Jane Q. Researcher", userId: "user-123",
    });
    for (const leak of ["secret", "scholar.google", "Private Paper", "Jane", "user-123"]) {
      assert.ok(!decodeURIComponent(href).includes(leak), leak);
    }
    const labels = parse(href).body.split("\n").filter((l) => /^[A-Z][^:]*: /.test(l)).map((l) => l.split(":")[0]);
    assert.deepEqual(labels, ["Scholar shows", "Matched in the list", "List", "Edition", "Rank shown", "Extension version"]);
  });

  it("caps long strings so the URL stays well under 2000 characters", () => {
    const huge = "Ü&#\"".repeat(2000);
    const href = buildWrongMatchIssueUrl({
      scholarVenue: huge, matchedVenue: huge, listName: huge, edition: huge, rank: huge, extensionVersion: huge,
    });
    assert.ok(href.length < 2000, `length ${href.length}`);
    const { title, body } = parse(href);
    assert.match(title, /…/);
    assert.match(body, /Scholar shows: (Ü&#"){5,}[^\n]*…/);
    // Astral characters (two UTF-16 units) are never cut in half.
    const emoji = buildWrongMatchIssueUrl({ scholarVenue: "\u{1D4D0}".repeat(500) });
    assert.ok(emoji.length < 2000);
    assert.doesNotThrow(() => decodeURIComponent(emoji));
  });

  it("missing values read 'Not available'", () => {
    const { body } = parse(buildWrongMatchIssueUrl({}));
    assert.match(body, /Scholar shows: Not available/);
    assert.match(body, /Extension version: Not available/);
  });
});

describe("Wrong match? link in the badge popover", () => {
  const qIndex = compileQualityIndex({ qualityAbdcRanks: "MIS Quarterly: Management Information Systems,A*\n" }, {});
  const abdc = qualityBadgesForVenue("MIS Quarterly: Management Information Systems", qIndex).find((b) => b.kind === "abdc");

  it("renders a plain link to the GitHub new-issue page, built with DOM APIs", () => {
    assert.ok(abdc, "fixture yields an ABDC badge");
    const wrap = renderWrongMatchLink(doc, abdc, { extensionVersion: "9.9.9" });
    assert.equal(wrap.className, "su-badge-report");
    const a = wrap.children[0];
    assert.equal(a.tag, "a");
    assert.equal(a.textContent, "Wrong match? Report it on GitHub");
    assert.equal(a.attrs.target, "_blank");
    assert.equal(a.attrs.rel, "noopener noreferrer");
    const { u, body } = parse(a.attrs.href);
    assert.equal(u.host, "github.com");
    assert.equal(u.pathname, "/BenAmpel/ScholarUtilityBelt/issues/new");
    assert.match(body, /List: ABDC Journal Quality List/);
    assert.match(body, /Edition: 2025/);
    assert.match(body, /Rank shown: ABDC/);
    assert.match(body, /Extension version: 9\.9\.9/);
  });

  it("a custom list's own name is not sent", () => {
    const b = customBadgeForVenue("Decision Support Systems", { name: "Dean's secret list", map: compileCustomVenueIndex([["Decision Support Systems", "B"]]) });
    const href = renderWrongMatchLink(doc, b).children[0].attrs.href;
    assert.ok(!decodeURIComponent(href).includes("Dean"));
  });

  it("no link for badges that are not from a ranking list", () => {
    const pre = qualityBadgesForVenue("arXiv preprint arXiv:2401.00001", qIndex).find((b) => b.kind === "preprint");
    assert.ok(pre);
    assert.equal(renderWrongMatchLink(doc, pre), null);
  });

  it("the content script appends the link to the badge popover", () => {
    const src = readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
    assert.match(src, /renderWrongMatchLink\?\.\(document, badge, sourceMeta\)/);
    assert.doesNotMatch(src, /innerHTML[^;\n]*renderWrongMatchLink/);
  });
});
