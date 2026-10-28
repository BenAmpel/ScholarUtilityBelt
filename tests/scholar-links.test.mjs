import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { isQuickReadLink, firstFileColumnLink } = await import("../src/common/scholar-links.js");

// Minimal anchor stand-in: the helpers only read classList.contains and textContent.
const anchor = (text, classes = []) => ({
  textContent: text,
  classList: { contains: (c) => classes.includes(c) },
});

describe("isQuickReadLink", () => {
  it("matches Google's gs_or_aia class", () => {
    assert.equal(isQuickReadLink(anchor("Quick readarxiv.org", ["gs_or_aia"])), true);
  });

  it("falls back to the label when the class changes", () => {
    assert.equal(isQuickReadLink(anchor("Quick read arxiv.org")), true);
    assert.equal(isQuickReadLink(anchor("  quick   read")), true);
  });

  it("does not match file links", () => {
    assert.equal(isQuickReadLink(anchor("[PDF] arxiv.org")), false);
    assert.equal(isQuickReadLink(anchor("[HTML] nature.com")), false);
    assert.equal(isQuickReadLink(anchor("Full-text @ My Library")), false);
  });

  it("is safe on missing input", () => {
    assert.equal(isQuickReadLink(null), false);
    assert.equal(isQuickReadLink({}), false);
  });
});

describe("firstFileColumnLink", () => {
  const pdf = anchor("[PDF] arxiv.org");
  const quick = anchor("Quick readarxiv.org", ["gs_or_aia"]);
  const html = anchor("[HTML] nature.com");

  it("returns the PDF link when it comes first", () => {
    assert.equal(firstFileColumnLink([pdf, quick]), pdf);
  });

  it("skips Quick read when it comes first", () => {
    assert.equal(firstFileColumnLink([quick, pdf]), pdf);
  });

  it("returns null when Quick read is the only link, never the Quick read link", () => {
    assert.equal(firstFileColumnLink([quick]), null);
  });

  it("returns an HTML link as the file link when there is no PDF", () => {
    assert.equal(firstFileColumnLink([html, quick]), html);
  });

  it("handles empty and missing lists", () => {
    assert.equal(firstFileColumnLink([]), null);
    assert.equal(firstFileColumnLink(undefined), null);
  });
});

describe("classifyScholarResponse", async () => {
  const { classifyScholarResponse } = await import("../src/common/scholar-links.js");
  it("passes an ordinary results page", () => {
    assert.equal(classifyScholarResponse({ status: 200, url: "https://scholar.google.com/scholar?q=x", html: '<div id="gs_res_ccl_mid"><div class="gs_r"></div></div>' }), null);
  });
  it("spots a rate limit by status", () => {
    assert.equal(classifyScholarResponse({ status: 429, url: "https://scholar.google.com/scholar?q=x", html: "" }), "rate_limit");
  });
  it("spots the CAPTCHA page by redirect, form or wording, even with a 200", () => {
    assert.equal(classifyScholarResponse({ status: 200, url: "https://www.google.com/sorry/index?continue=x", html: "" }), "captcha");
    assert.equal(classifyScholarResponse({ status: 200, url: "https://scholar.google.com/scholar", html: '<form id="gs_captcha_f">' }), "captcha");
    assert.equal(classifyScholarResponse({ status: 429, url: "https://scholar.google.com/scholar", html: "Our systems have detected unusual traffic from your computer network." }), "captcha");
  });
  it("tolerates missing input", () => {
    assert.equal(classifyScholarResponse(), null);
    assert.equal(classifyScholarResponse({}), null);
  });
});

import { cleanDoi, findResultDoi } from "../src/common/scholar-links.js";
describe("DOIs shown on a result", () => {
  it("cleans publisher URL tails, queries, encoding and punctuation", () => {
    assert.equal(cleanDoi("https://onlinelibrary.wiley.com/doi/full/10.1002/jsr.12345"), "10.1002/jsr.12345");
    assert.equal(cleanDoi("https://onlinelibrary.wiley.com/doi/10.1002/jsr.12345/full"), "10.1002/jsr.12345");
    assert.equal(cleanDoi("https://dl.acm.org/doi/pdf/10.1145/3290605.3300233?casa_token=x"), "10.1145/3290605.3300233");
    assert.equal(cleanDoi("https://doi.org/10.1016%2Fj.neuron.2005.01.001"), "10.1016/j.neuron.2005.01.001");
    assert.equal(cleanDoi("see doi:10.1037/a0012345)."), "10.1037/a0012345");
    assert.equal(cleanDoi("https://example.org/paper.pdf"), null);
  });
  it("prefers the result's links over its snippet", () => {
    assert.equal(findResultDoi(["https://example.org/x", "https://doi.org/10.5555/abc"], "text 10.9999/zzz"), "10.5555/abc");
    assert.equal(findResultDoi(["https://example.org/x"], "Published as 10.9999/zzz."), "10.9999/zzz");
    assert.equal(findResultDoi([], ""), null);
  });
});

describe("safeHref", async () => {
  const { safeHref } = await import("../src/common/scholar-links.js");
  it("keeps absolute http and https URLs", () => {
    assert.equal(safeHref("https://doi.org/10.1000/x"), "https://doi.org/10.1000/x");
    assert.equal(safeHref("  http://example.org/a?b=1 "), "http://example.org/a?b=1");
  });
  it("drops script, data and relative URLs", () => {
    assert.equal(safeHref("javascript:alert(1)"), "");
    assert.equal(safeHref(" JavaScript:alert(1)"), "");
    assert.equal(safeHref("data:text/html,<b>x</b>"), "");
    assert.equal(safeHref("/scholar?q=x"), "");
    assert.equal(safeHref("https://"), "");
    assert.equal(safeHref(null), "");
  });
});

describe("scholarRowLinkKind and citedByCount", async () => {
  const { scholarRowLinkKind, citedByCount } = await import("../src/common/scholar-links.js");
  it("classifies result links by address, whatever the interface language", () => {
    assert.equal(scholarRowLinkKind("/scholar?cites=123&as_sdt=2005&hl=de"), "cited");
    assert.equal(scholarRowLinkKind("https://scholar.google.fr/scholar?cluster=99&hl=fr"), "versions");
    assert.equal(scholarRowLinkKind("/scholar?q=related:abc:scholar.google.com/&hl=ja"), "related");
    assert.equal(scholarRowLinkKind("/scholar?q=sleep"), null);
    assert.equal(scholarRowLinkKind("https://example.org/scholar?cites=1"), null);
    assert.equal(scholarRowLinkKind("/citations?user=x"), null);
  });
  it("reads the count in any language", () => {
    assert.equal(citedByCount("Cited by 1,234"), 1234);
    assert.equal(citedByCount("Zitiert von: 1.234"), 1234);
    assert.equal(citedByCount("Cité 1 234 fois"), 1234);
    assert.equal(citedByCount("被引用次数：87"), 87);
    assert.equal(citedByCount("Citado por 5"), 5);
    assert.equal(citedByCount("Cited by"), null);
  });
});

describe("scholarCooldownLeft", async () => {
  const { scholarCooldownLeft } = await import("../src/common/scholar-links.js");
  const MIN = 60000, COOL = 15 * MIN, blockedAt = 1_000_000, until = blockedAt + COOL;
  it("is zero with no cooldown or after it ends", () => {
    assert.equal(scholarCooldownLeft({ until: 0, now: blockedAt, cooldownMs: COOL }), 0);
    assert.equal(scholarCooldownLeft({ until, now: until + 1, cooldownMs: COOL, pageLoadedAt: 0, pageIsNormal: true }), 0);
  });
  it("keeps the wait on a page loaded before the block", () => {
    assert.equal(scholarCooldownLeft({ until, now: blockedAt + MIN, cooldownMs: COOL, pageLoadedAt: blockedAt - MIN, pageIsNormal: true }), 14 * MIN);
  });
  it("keeps the wait when the newer page is itself a check", () => {
    assert.equal(scholarCooldownLeft({ until, now: blockedAt + 2 * MIN, cooldownMs: COOL, pageLoadedAt: blockedAt + MIN, pageIsNormal: false }), 13 * MIN);
  });
  it("ends the wait on a normal page loaded after the block (the user did the check)", () => {
    assert.equal(scholarCooldownLeft({ until, now: blockedAt + 2 * MIN, cooldownMs: COOL, pageLoadedAt: blockedAt + MIN, pageIsNormal: true }), 0);
  });
});
