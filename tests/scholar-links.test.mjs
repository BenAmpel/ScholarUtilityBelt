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
