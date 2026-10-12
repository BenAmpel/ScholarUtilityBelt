import { describe, it } from "node:test";
import assert from "node:assert/strict";

const {
  NOT_STATED, normalizeForMatch, verifyField, verifyTriage, parseTriageResponse, buildTriagePrompt,
  snippetHash, triageCacheKey, readTriageCache, makeTriageCacheEntry,
} = await import("../src/common/triage-verify.js");

const SNIP =
  "We conducted a randomized controlled trial with n = 1,200 adults at three primary care clinics in Zürich. " +
  "Mean age was 54.3 years (SD 9.1). Outcomes improved by 12.5% (p<0.05).";

describe("normalizeForMatch", () => {
  it("lowercases, collapses whitespace and tightens operators", () => {
    assert.equal(normalizeForMatch("  N  =  120 \n adults "), "n=120 adults");
  });
  it("removes thousands separators between digits only", () => {
    assert.equal(normalizeForMatch("1,200 and 3,4 and 12,34"), "1200 and 3,4 and 12,34");
  });
  it("unifies dashes, quotes and strips zero-width characters", () => {
    assert.equal(normalizeForMatch("cross–sectional “survey”"), 'cross-sectional "survey"');
    assert.equal(normalizeForMatch("ran​dom­ized"), "randomized");
  });
  it("folds full-width digits through NFKC", () => {
    assert.equal(normalizeForMatch("n=１２０"), "n=120");
  });
  it("keeps diacritics", () => {
    assert.notEqual(normalizeForMatch("Zürich"), normalizeForMatch("Zurich"));
  });
  it("handles null and undefined", () => {
    assert.equal(normalizeForMatch(null), "");
    assert.equal(normalizeForMatch(undefined), "");
  });
});

describe("verifyField: accepts literal text", () => {
  it("accepts a phrase that is in the snippet", () => {
    const r = verifyField("randomized controlled trial", SNIP);
    assert.equal(r.ok, true);
    assert.equal(r.value, "randomized controlled trial");
  });
  it("is case and whitespace insensitive", () => {
    assert.equal(verifyField("Randomized   Controlled\nTrial", SNIP).ok, true);
  });
  it("accepts a normalized number form (n=1200 for n = 1,200)", () => {
    assert.equal(verifyField("n=1200 adults", SNIP).ok, true);
    assert.equal(verifyField("n = 1,200 adults", SNIP).ok, true);
  });
  it("accepts a value with trailing punctuation", () => {
    assert.equal(verifyField("three primary care clinics in Zürich.", SNIP).ok, true);
  });
  it("accepts dash variants", () => {
    assert.equal(verifyField("cross‐sectional survey", "A cross-sectional survey of nurses.").ok, true);
  });
  it("accepts unicode text that matches exactly", () => {
    assert.equal(verifyField("Zürich", SNIP).ok, true);
    assert.equal(verifyField("東京の病院", "調査は東京の病院で行われた。").ok, true);
  });
  it("accepts a percent value", () => {
    assert.equal(verifyField("12.5%", SNIP).ok, true);
  });
});

describe("verifyField: rejects fabrication", () => {
  it("rejects a fabricated sample size", () => {
    const r = verifyField("n = 1,500 adults", SNIP);
    assert.equal(r.ok, false);
    assert.equal(r.reason, "not_in_snippet");
  });
  it("rejects a number that is only a substring of a larger number", () => {
    assert.equal(verifyField("n=120", SNIP).ok, false);
    assert.equal(verifyField("200", SNIP).ok, false);
    assert.equal(verifyField("2", "n=12 participants").ok, false);
  });
  it("rejects a fraction of a decimal", () => {
    assert.equal(verifyField("5", "improved by 12.5 points").ok, false);
    assert.equal(verifyField("12", "improved by 12.5 points").ok, false);
    assert.equal(verifyField("12.5", "improved by 12.5 points").ok, true);
  });
  it("rejects partial matches that cross a word boundary", () => {
    assert.equal(verifyField("rct", "Arctic sampling in 2019").ok, false);
    assert.equal(verifyField("trial", "a pre-trialled instrument").ok, false);
  });
  it("rejects a paraphrase", () => {
    assert.equal(verifyField("RCT with 1200 participants", SNIP).ok, false);
  });
  it("rejects words reordered or extended", () => {
    assert.equal(verifyField("controlled randomized trial", SNIP).ok, false);
    assert.equal(verifyField("randomized controlled trial in Basel", SNIP).ok, false);
  });
  it("rejects a diacritic change", () => {
    assert.equal(verifyField("Zurich", SNIP).ok, false);
  });
  it("rejects a mixed value where one number is invented", () => {
    assert.equal(verifyField("54.3 years (SD 9.9)", SNIP).ok, false);
  });
  it("rejects values from a different snippet", () => {
    assert.equal(verifyField("randomized controlled trial", "A qualitative interview study.").ok, false);
  });
  it("rejects lookalike characters", () => {
    // Cyrillic small a (U+0430) in place of Latin a.
    assert.equal(verifyField("rаndomized controlled trial", SNIP).ok, false);
  });
});

describe("verifyField: empties and limits", () => {
  it("treats model placeholders as not stated", () => {
    for (const v of ["", "  ", "N/A", "none", "Unknown", "not specified", "Not stated in snippet", "-", "null"]) {
      const r = verifyField(v, SNIP);
      assert.equal(r.ok, false, v);
      assert.equal(r.reason, "empty", v);
    }
  });
  it("rejects non-strings", () => {
    for (const v of [null, undefined, 42, {}, ["randomized"], true]) {
      assert.equal(verifyField(v, SNIP).ok, false);
    }
  });
  it("rejects a value that copies a very long span", () => {
    const long = "word ".repeat(60);
    const r = verifyField(long.trim(), long);
    assert.equal(r.ok, false);
    assert.equal(r.reason, "too_long");
  });
  it("rejects when the snippet is empty or missing", () => {
    assert.equal(verifyField("randomized", "").ok, false);
    assert.equal(verifyField("randomized", null).ok, false);
  });
  it("strips control characters from the displayed value", () => {
    const r = verifyField("randomized\u0007 controlled trial", SNIP);
    assert.equal(r.ok, true);
    assert.equal(r.value, "randomized controlled trial");
  });
});

describe("verifyTriage", () => {
  it("shows verified fields and not-stated for the rest", () => {
    const out = verifyTriage(
      { design: "randomized controlled trial", sample: "n = 1,500 adults", setting: "primary care clinics" },
      SNIP
    );
    assert.deepEqual(out.design, { text: "randomized controlled trial", verified: true });
    assert.deepEqual(out.sample, { text: NOT_STATED, verified: false });
    assert.deepEqual(out.setting, { text: "primary care clinics", verified: true });
  });
  it("returns not-stated for everything when parsing failed", () => {
    const out = verifyTriage(null, SNIP);
    for (const f of ["design", "sample", "setting"]) assert.equal(out[f].text, NOT_STATED);
  });
  it("ignores extra keys", () => {
    const out = verifyTriage({ design: "randomized controlled trial", funding: "NIH" }, SNIP);
    assert.equal(Object.keys(out).sort().join(","), "design,sample,setting");
  });
});

describe("parseTriageResponse", () => {
  it("parses a plain object", () => {
    const r = parseTriageResponse('{"design":"survey","sample":null,"setting":"schools"}');
    assert.deepEqual(r, { design: "survey", sample: null, setting: "schools" });
  });
  it("parses a fenced block with prose around it", () => {
    const r = parseTriageResponse('Sure:\n```json\n{"design":"survey","sample":"n=5","setting":null}\n```\nDone.');
    assert.equal(r.sample, "n=5");
  });
  it("parses an object embedded in prose without fences", () => {
    assert.equal(parseTriageResponse('Here you go {"design":"cohort study"} thanks').design, "cohort study");
  });
  it("drops non-string values", () => {
    const r = parseTriageResponse('{"design":7,"sample":["a"],"setting":"lab"}');
    assert.deepEqual(r, { design: null, sample: null, setting: "lab" });
  });
  it("returns null for malformed or empty output", () => {
    for (const t of ["", "no json here", '{"design": "unterminated', "[1,2]", "{}", '{"other":"x"}', null, undefined, 5]) {
      assert.equal(parseTriageResponse(t), null, String(t));
    }
  });
});

describe("buildTriagePrompt", () => {
  it("includes the snippet and truncates very long input", () => {
    assert.match(buildTriagePrompt("short snippet"), /short snippet/);
    const p = buildTriagePrompt("x".repeat(5000));
    assert.ok(p.length < 2600);
  });
});

describe("triage cache helpers", () => {
  it("keys by paper key", () => {
    assert.equal(triageCacheKey("abc"), "suTriage:abc");
  });
  it("hash differs for different text and is stable", () => {
    assert.equal(snippetHash("a"), snippetHash("a"));
    assert.notEqual(snippetHash("a"), snippetHash("b"));
  });
  it("returns a cached result only for the same snippet", () => {
    const result = verifyTriage({ design: "randomized controlled trial" }, SNIP);
    const entry = makeTriageCacheEntry(SNIP, result);
    assert.deepEqual(readTriageCache(entry, SNIP), result);
    assert.equal(readTriageCache(entry, SNIP + " changed"), null);
  });
  it("rejects malformed cache entries", () => {
    for (const e of [null, undefined, {}, { h: snippetHash(SNIP) }, { h: snippetHash(SNIP), result: { design: {} } }, "x"]) {
      assert.equal(readTriageCache(e, SNIP), null);
    }
  });
});
