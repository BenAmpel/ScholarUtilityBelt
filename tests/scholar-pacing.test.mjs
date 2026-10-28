import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { classifyScholarResponse } from "../src/common/scholar-links.js";

const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");

describe("Scholar request pacing", () => {
  it("Compare authors asks for at most 5 pages of 100 rows, spaced out", () => {
    assert.match(src, /const PAGE_SIZE = 100;\s*\n\s*const MAX_PAGES = 5;\s*\n\s*const PAGE_GAP_MS = 1200;/);
    const loop = src.slice(src.indexOf("for (let page = 1; page < MAX_PAGES; page++)"));
    assert.ok(loop.indexOf("setTimeout(resolve, PAGE_GAP_MS)") < loop.indexOf("await fetchPage(cstart)"), "the pause comes before each extra page");
  });
  it("Compare authors stops on a CAPTCHA or rate limit and starts the cooldown", () => {
    const fetchPage = src.slice(src.indexOf("const fetchPage = async (cstart)"), src.indexOf("const html0 = await fetchPage(0)"));
    assert.match(fetchPage, /classifyScholarResponse\(/);
    assert.match(fetchPage, /await noteScholarBlock\(\)/);
  });
  it("Load more pages and Compare authors both wait out the shared cooldown", () => {
    assert.equal((src.match(/await scholarCooldownLeftMs\(\)/g) || []).length, 3);
    assert.match(src, /if \(out\.blocked\) \{ blockedBy = out\.blocked; await noteScholarBlock\(\); break; \}/);
    assert.match(src, /const SCHOLAR_COOLDOWN_MS = 15 \* 60 \* 1000;/);
  });
  it("the classifier recognises the answers that should stop a loop", () => {
    assert.equal(classifyScholarResponse({ url: "https://www.google.com/sorry/index?continue=x" }), "captcha");
    assert.equal(classifyScholarResponse({ html: "<form id=\"gs_captcha_f\">" }), "captcha");
    assert.equal(classifyScholarResponse({ status: 429, html: "" }), "rate_limit");
    assert.equal(classifyScholarResponse({ status: 200, html: "<div class=gsc_a_tr></div>" }), null);
  });
});

describe("Load all for full stats", () => {
  it("checks the shared Scholar cooldown before clicking Show more", () => {
    const h = src.slice(src.indexOf('const loadAllBtn = e.target.closest("[data-load-all-publications]")'));
    assert.ok(h.indexOf("await scholarCooldownLeftMs()") > 0 && h.indexOf("await scholarCooldownLeftMs()") < h.indexOf("loadAllPublicationsRecursively("));
  });
});
