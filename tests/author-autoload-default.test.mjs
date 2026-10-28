import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { DEFAULT_SETTINGS } from "../src/common/storage.js";

describe("author profile auto-load", () => {
  it("is off by default, so a profile visit sends Scholar no extra requests", () => {
    assert.equal(DEFAULT_SETTINGS.autoLoadAuthorPapers, false);
  });
  it("only runs the automatic loader when the setting is on", () => {
    const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
    const optOut = src.indexOf("!state.authorAutoLoadDisabled && !state.settings.autoLoadAuthorPapers");
    const auto = src.indexOf("loadAllPublicationsRecursively({ maxPubs: AUTHOR_AUTOLOAD_MAX })");
    assert.ok(optOut > 0 && auto > optOut, "the opt-out branch must come before the automatic loader");
  });
  it("has an Options switch that is read and saved", () => {
    const html = fs.readFileSync(new URL("../src/options/options.html", import.meta.url), "utf8");
    const js = fs.readFileSync(new URL("../src/options/options.js", import.meta.url), "utf8");
    assert.match(html, /id="autoLoadAuthorPapers"/);
    assert.match(js, /el\("autoLoadAuthorPapers"\)\.checked = s\.autoLoadAuthorPapers === true/);
    assert.match(js, /autoLoadAuthorPapers: el\("autoLoadAuthorPapers"\)\.checked/);
  });
});

describe("Emerging score Scholar sampling", () => {
  it("is off by default and checked before any neighbouring page is fetched", () => {
    assert.equal(DEFAULT_SETTINGS.emergingSampleScholarPages, false);
    const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
    const fn = src.slice(src.indexOf("async function maybeExpandLocalCohortForPaper"));
    assert.ok(fn.indexOf("if (!state.settings?.emergingSampleScholarPages) return current;") < fn.indexOf("getSparseLocalCohortSampleUrl("));
  });
});
