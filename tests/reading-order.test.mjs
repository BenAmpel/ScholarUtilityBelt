import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { parseTerms, termRegex, scoreRow, rankRows, hasTerms } = await import("../src/common/reading-order.js");

describe("parseTerms", () => {
  it("splits on commas, semicolons and new lines", () => {
    assert.deepEqual(parseTerms("sleep, memory; attention\nlearning"), ["sleep", "memory", "attention", "learning"]);
  });
  it("keeps quoted phrases together", () => {
    assert.deepEqual(parseTerms('"working memory", sleep'), ["working memory", "sleep"]);
  });
  it("drops empty, one-letter and duplicate terms", () => {
    assert.deepEqual(parseTerms(" , a, Sleep, sleep ,, "), ["Sleep"]);
  });
  it("handles null, numbers and a runaway list", () => {
    assert.deepEqual(parseTerms(null), []);
    assert.deepEqual(parseTerms(undefined), []);
    assert.equal(parseTerms(Array.from({ length: 100 }, (_, i) => `term${i}`).join(",")).length, 12);
  });
});

describe("termRegex", () => {
  it("matches whole words, case-insensitively", () => {
    const re = termRegex("sleep");
    assert.ok(re.test("Sleep helps"));
    re.lastIndex = 0;
    assert.ok(!re.test("sleeping"));
  });
  it("supports a trailing * prefix match", () => {
    assert.ok(termRegex("random*").test("randomized trial"));
  });
  it("escapes regex characters and tolerates symbols", () => {
    assert.ok(termRegex("C++").test("written in C++ here"));
    assert.ok(termRegex("(x)").test("the (x) term"));
    assert.equal(termRegex(""), null);
  });
  it("treats a phrase with any whitespace as one phrase", () => {
    assert.ok(termRegex("working memory").test("working\n  memory"));
  });
});

describe("scoreRow", () => {
  it("scores a title hit above a single snippet hit", () => {
    const a = scoreRow({ title: "Sleep and memory", snippet: "" }, { include: ["sleep"] });
    const b = scoreRow({ title: "Other", snippet: "sleep" }, { include: ["sleep"] });
    assert.ok(a.score > b.score);
  });
  it("caps snippet repeats", () => {
    const r = scoreRow({ title: "", snippet: "sleep sleep sleep sleep sleep sleep" }, { include: ["sleep"] });
    assert.equal(r.score, 3);
  });
  it("penalises exclude terms and explains why", () => {
    const r = scoreRow({ title: "Rats and sleep", snippet: "in mice" }, { include: ["sleep"], exclude: ["rats", "mice"] });
    assert.equal(r.score, 3 - 4 - 2);
    assert.match(r.why, /excluded term "rats" in title/);
    assert.match(r.why, /Score -3/);
  });
  it("counts method and population terms like include terms", () => {
    const r = scoreRow({ title: "", snippet: "adolescents randomized" }, { method: ["randomized"], population: ["adolescents"] });
    assert.equal(r.score, 2);
  });
  it("explains a row with no matches", () => {
    assert.match(scoreRow({ title: "x", snippet: "y" }, { include: ["zzz"] }).why, /No terms matched/);
  });
  it("tolerates missing fields", () => {
    assert.equal(scoreRow({}, { include: ["a1"] }).score, 0);
    assert.equal(scoreRow(null, {}).score, 0);
  });
});

describe("rankRows", () => {
  const rows = [
    { title: "A study of rats", snippet: "rats sleep" },
    { title: "Sleep in adolescents", snippet: "survey of sleep" },
    { title: "Unrelated", snippet: "nothing" },
    { title: "Also unrelated", snippet: "nothing either" },
    { title: "Sleep and memory", snippet: "sleep helps memory" },
  ];
  it("orders by score and keeps original order for ties", () => {
    const { order } = rankRows(rows, { include: ["sleep"], exclude: ["rats"] });
    assert.deepEqual(order, [1, 4, 2, 3, 0]);
  });
  it("returns a permutation: nothing is dropped", () => {
    const { order } = rankRows(rows, { include: ["sleep"], exclude: ["nothing"] });
    assert.deepEqual([...order].sort(), [0, 1, 2, 3, 4]);
  });
  it("with no terms, keeps Scholar's order exactly", () => {
    assert.deepEqual(rankRows(rows, {}).order, [0, 1, 2, 3, 4]);
  });
  it("handles an empty or invalid list", () => {
    assert.deepEqual(rankRows([], { include: ["a1"] }).order, []);
    assert.deepEqual(rankRows(null, {}).order, []);
  });
  it("hasTerms is true only when a group has a term", () => {
    assert.equal(hasTerms({}), false);
    assert.equal(hasTerms({ include: [] }), false);
    assert.equal(hasTerms({ exclude: ["x1"] }), true);
    assert.equal(hasTerms(null), false);
  });
});
