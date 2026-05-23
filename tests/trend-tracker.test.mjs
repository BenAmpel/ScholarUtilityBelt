import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { detectMethods, METHOD_KEYWORDS } = await import("../src/content/trend-methods.js");

describe("METHOD_KEYWORDS", () => {
  it("contains at least 10 keywords", () => {
    assert.ok(METHOD_KEYWORDS.length >= 10);
  });

  it("includes known methods", () => {
    const set = new Set(METHOD_KEYWORDS.map(k => k.toLowerCase()));
    assert.ok(set.has("meta-analysis"));
    assert.ok(set.has("deep learning"));
    assert.ok(set.has("llm"));
  });
});

describe("detectMethods", () => {
  function mockElements(texts) {
    return texts.map(t => ({ textContent: t }));
  }

  it("returns empty map for no matches", () => {
    const result = detectMethods(mockElements(["Some random text"]));
    assert.equal(result.size, 0);
  });

  it("detects a single method keyword", () => {
    const result = detectMethods(mockElements(["A systematic review of recent advances"]));
    assert.equal(result.get("systematic review"), 1);
  });

  it("counts multiple occurrences across elements", () => {
    const result = detectMethods(mockElements([
      "Deep learning for image classification",
      "Using deep learning with transformers"
    ]));
    assert.equal(result.get("deep learning"), 2);
  });

  it("is case-insensitive", () => {
    const result = detectMethods(mockElements(["A META-ANALYSIS of trials"]));
    assert.equal(result.get("meta-analysis"), 1);
  });

  it("detects multiple different methods", () => {
    const result = detectMethods(mockElements([
      "A survey using machine learning and NLP techniques"
    ]));
    assert.ok(result.has("survey"));
    assert.ok(result.has("machine learning"));
    assert.ok(result.has("nlp"));
  });

  it("handles empty input", () => {
    const result = detectMethods([]);
    assert.equal(result.size, 0);
  });

  it("handles elements with no textContent", () => {
    const result = detectMethods([{ textContent: null }, { textContent: undefined }]);
    assert.equal(result.size, 0);
  });
});
