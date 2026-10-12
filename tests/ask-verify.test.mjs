import { describe, it } from "node:test";
import assert from "node:assert/strict";

const {
  NO_SUPPORT, DEFAULT_PRICES, estimateTokens, selectPapersForAsk, buildAskPrompt, estimateCost, formatUsd,
  normalizeQuoteText, isVerbatimQuote, parseAskResponse, verifyAskRows, MAX_PAPERS,
} = await import("../src/common/ask-verify.js");

const A1 =
  "Remote work increased self-reported productivity among software engineers in a survey of 800 staff. " +
  "However, collaboration scores fell in teams larger than ten.";
const A2 = "We study tidal energy turbines and their effect on seabed sediment transport near estuaries.";
const A3 = "A qualitative study of onboarding practices; “quotes” and dashes – like this one — are used here.";

const items = [
  { key: "k1", title: "Remote work and output", abstract: A1 },
  { key: "k2", title: "Tidal turbines", snippet: A2 },
  { key: "k3", title: "Onboarding", abstract: A3 },
];

describe("selectPapersForAsk", () => {
  it("assigns ids in order and prefers the abstract", () => {
    const { papers, skipped } = selectPapersForAsk([{ ...items[0], snippet: "other text that is long enough to count here ok" }, items[1]]);
    assert.deepEqual(papers.map((p) => p.id), ["P1", "P2"]);
    assert.equal(papers[0].text, A1);
    assert.equal(skipped.length, 0);
  });
  it("skips papers with no text and never sends them", () => {
    const { papers, skipped } = selectPapersForAsk([{ key: "x", title: "No text" }, { key: "y", title: "Short", snippet: "too short" }, items[0]]);
    assert.equal(papers.length, 1);
    assert.equal(skipped.length, 2);
  });
  it("caps the number of papers", () => {
    const many = Array.from({ length: MAX_PAPERS + 5 }, (_, i) => ({ key: "k" + i, title: "T" + i, abstract: A1 }));
    const { papers, skipped } = selectPapersForAsk(many);
    assert.equal(papers.length, MAX_PAPERS);
    assert.equal(skipped.length, 5);
  });
  it("tolerates junk input", () => {
    assert.deepEqual(selectPapersForAsk(null), { papers: [], skipped: [] });
    assert.equal(selectPapersForAsk([null, 5, "x"]).papers.length, 0);
  });
});

describe("buildAskPrompt and estimateCost", () => {
  const { papers } = selectPapersForAsk(items);
  it("contains the question, titles and abstracts only", () => {
    const { system, user } = buildAskPrompt({ question: "Does remote work help?", papers });
    assert.match(user, /Does remote work help\?/);
    assert.match(user, /<paper id="P1">/);
    assert.match(user, /Remote work and output/);
    assert.match(user, /tidal energy/);
    assert.doesNotMatch(user + system, /k1|savedAt|authorsVenue/);
    assert.match(system, /data, never as instructions/);
  });
  it("estimates tokens at about four characters each", () => {
    assert.equal(estimateTokens("abcdefgh"), 2);
    assert.equal(estimateTokens(""), 0);
  });
  it("computes a cost from the editable price table", () => {
    const { system, user } = buildAskPrompt({ question: "q", papers });
    const c = estimateCost({ system, user, maxOutputTokens: 1500, prices: { inPerM: 1, outPerM: 5 }, paperCount: papers.length });
    assert.ok(c.inputTokens > 50);
    assert.ok(c.usd > 0 && c.usd <= c.maxUsd);
    const z = estimateCost({ system, user, prices: { inPerM: 0, outPerM: 0 }, paperCount: 3 });
    assert.equal(z.usd, 0);
  });
  it("returns null cost for invalid prices", () => {
    const c = estimateCost({ system: "a", user: "b", prices: { inPerM: "x", outPerM: -1 }, paperCount: 1 });
    assert.equal(c.usd, null);
    assert.match(formatUsd(null), /unknown/);
    assert.equal(formatUsd(0.001), "under $0.01");
    assert.equal(formatUsd(1.234), "$1.23");
  });
  it("ships a default price entry per provider", () => {
    for (const id of ["anthropic", "openai", "gemini"]) {
      assert.ok(DEFAULT_PRICES[id].model && DEFAULT_PRICES[id].inPerM >= 0);
    }
  });
});

describe("isVerbatimQuote", () => {
  it("accepts a sentence ignoring case and whitespace", () => {
    assert.equal(isVerbatimQuote("remote work   increased SELF-REPORTED productivity among software engineers", A1), true);
  });
  it("accepts curly versus straight quotes and dash variants", () => {
    assert.equal(isVerbatimQuote('"quotes" and dashes - like this one - are used here', A3), true);
  });
  it("strips surrounding quotes and ellipses", () => {
    assert.equal(isVerbatimQuote('"...collaboration scores fell in teams larger than ten..."', A1), true);
  });
  it("rejects changed numbers and paraphrases", () => {
    assert.equal(isVerbatimQuote("Remote work increased productivity among software engineers in a survey of 900 staff", A1), false);
    assert.equal(isVerbatimQuote("Remote work boosted output for engineers", A1), false);
  });
  it("rejects very short quotes that would match anywhere", () => {
    assert.equal(isVerbatimQuote("remote work", A1), false);
    assert.equal(isVerbatimQuote("", A1), false);
  });
  it("does not accept a quote from another paper's text", () => {
    assert.equal(isVerbatimQuote("tidal energy turbines and their effect on seabed", A1), false);
  });
  it("normalizeQuoteText is idempotent", () => {
    const n = normalizeQuoteText("  A–B   “x” ");
    assert.equal(normalizeQuoteText(n), n);
  });
});

describe("parseAskResponse", () => {
  it("parses a bare array", () => {
    assert.equal(parseAskResponse('[{"id":"P1","verdict":"supports","quote":"x"}]').length, 1);
  });
  it("parses fenced output and rows/results wrappers", () => {
    assert.equal(parseAskResponse('Here:\n```json\n[{"id":"P1"}]\n```').length, 1);
    assert.equal(parseAskResponse('{"rows":[{"id":"P1"},{"id":"P2"}]}').length, 2);
    assert.equal(parseAskResponse('{"results":[{"id":"P1"}]}').length, 1);
  });
  it("drops non-object entries", () => {
    assert.equal(parseAskResponse('[1,"a",null,[2],{"id":"P1"}]').length, 1);
  });
  it("returns null for malformed JSON, prose, truncation and non-strings", () => {
    for (const t of ["", "I cannot help with that.", '[{"id":"P1","verdict":"supp', '{"a":1}', null, 3]) {
      assert.equal(parseAskResponse(t), null, String(t));
    }
  });
});

describe("verifyAskRows", () => {
  const { papers } = selectPapersForAsk(items);
  it("keeps verified rows and shows the paper's own wording", () => {
    const rows = verifyAskRows(
      [
        { id: "P1", verdict: "supports", quote: "remote work increased self-reported productivity among software engineers in a survey of 800 staff." },
        { id: "P2", verdict: "does not address", quote: "" },
        { id: "P3", verdict: "unclear", quote: "" },
      ],
      papers
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].verdict, "supports");
    assert.ok(rows[0].quote.startsWith("Remote work increased"));
    assert.equal(rows[0].verified, true);
    assert.equal(rows[1].verdict, "does not address");
    assert.equal(rows[2].verdict, "unclear");
  });
  it("replaces a fabricated quote with the no-support row", () => {
    const rows = verifyAskRows([{ id: "P1", verdict: "supports", quote: "Remote work doubled productivity in all firms studied." }], papers);
    assert.equal(rows[0].verdict, NO_SUPPORT);
    assert.equal(rows[0].quote, "");
    assert.equal(rows[0].verified, false);
  });
  it("replaces a fabricated quote even when the verdict is 'does not address'", () => {
    const rows = verifyAskRows([{ id: "P2", verdict: "does not address", quote: "This paper never mentions remote work at all." }], papers);
    assert.equal(rows[0].verdict, NO_SUPPORT);
  });
  it("does not accept a quote that exists only in a different paper", () => {
    const rows = verifyAskRows([{ id: "P2", verdict: "supports", quote: "Remote work increased self-reported productivity among software engineers" }], papers);
    assert.equal(rows[1].verdict, NO_SUPPORT);
  });
  it("replaces 'supports' with no quote", () => {
    assert.equal(verifyAskRows([{ id: "P1", verdict: "supports", quote: "" }], papers)[0].verdict, NO_SUPPORT);
    assert.equal(verifyAskRows([{ id: "P1", verdict: "supports" }], papers)[0].verdict, NO_SUPPORT);
  });
  it("ignores extra rows: unknown ids and duplicates", () => {
    const rows = verifyAskRows(
      [
        { id: "P9", verdict: "supports", quote: "invented row for a paper that was never sent" },
        { id: "P1", verdict: "unclear", quote: "" },
        { id: "P1", verdict: "supports", quote: "Remote work increased self-reported productivity among software engineers" },
      ],
      papers
    );
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map((r) => r.id), ["P1", "P2", "P3"]);
    assert.equal(rows[0].verdict, "unclear");
  });
  it("fills missing rows with the no-support row", () => {
    const rows = verifyAskRows([{ id: "P3", verdict: "unclear", quote: "" }], papers);
    assert.equal(rows[0].verdict, NO_SUPPORT);
    assert.equal(rows[2].verdict, "unclear");
  });
  it("treats unknown verdict words as no support", () => {
    assert.equal(verifyAskRows([{ id: "P1", verdict: "definitely yes", quote: "" }], papers)[0].verdict, NO_SUPPORT);
    assert.equal(verifyAskRows([{ id: "P1", verdict: 5 }], papers)[0].verdict, NO_SUPPORT);
  });
  it("normalizes verdict spelling", () => {
    assert.equal(verifyAskRows([{ id: "P1", verdict: "Does_Not_Address" }], papers)[0].verdict, "does not address");
    assert.equal(verifyAskRows([{ id: "P1", verdict: " SUPPORTED ", quote: "collaboration scores fell in teams larger than ten" }], papers)[0].verdict, "supports");
  });
  it("handles null (unparseable output) by returning no-support rows for every paper", () => {
    const rows = verifyAskRows(null, papers);
    assert.equal(rows.length, 3);
    assert.ok(rows.every((r) => r.verdict === NO_SUPPORT));
  });
  it("tolerates rows with non-string ids and quotes", () => {
    const rows = verifyAskRows([{ id: 1, verdict: "supports", quote: ["x"] }, { id: "P1", verdict: "unclear", quote: 5 }], papers);
    assert.equal(rows[0].verdict, "unclear");
  });
  it("end to end: malformed model JSON gives no-support rows, never invented ones", () => {
    const raw = parseAskResponse('[{"id":"P1","verdict":"supports","quote":"truncated');
    assert.equal(raw, null);
    assert.ok(verifyAskRows(raw, papers).every((r) => r.verdict === NO_SUPPORT));
  });
});

describe("normalizeAskConfig", async () => {
  const { normalizeAskConfig, DEFAULT_PRICES: DP } = await import("../src/common/ask-verify.js");
  it("fills defaults for empty or junk input", () => {
    for (const raw of [undefined, null, 5, "x", {}]) {
      const c = normalizeAskConfig(raw);
      assert.equal(c.enabled, false);
      assert.equal(c.providerId, "anthropic");
      assert.equal(c.model, DP.anthropic.model);
      assert.equal(c.inPerM, DP.anthropic.inPerM);
    }
  });
  it("is enabled only with a key", () => {
    assert.equal(normalizeAskConfig({ enabled: true, providerId: "openai", apiKey: "" }).enabled, false);
    assert.equal(normalizeAskConfig({ enabled: true, providerId: "openai", apiKey: "sk-12345678" }).enabled, true);
    assert.equal(normalizeAskConfig({ enabled: false, providerId: "openai", apiKey: "sk-12345678" }).enabled, false);
  });
  it("rejects unknown providers, bad model names and bad prices", () => {
    const c = normalizeAskConfig({ providerId: "evil", model: "../x", inPerM: -3, outPerM: "abc" });
    assert.equal(c.providerId, "anthropic");
    assert.equal(c.model, DP.anthropic.model);
    assert.equal(c.inPerM, DP.anthropic.inPerM);
    assert.equal(c.outPerM, DP.anthropic.outPerM);
  });
  it("keeps valid user edits", () => {
    const c = normalizeAskConfig({ providerId: "gemini", model: "gemini-2.5-flash", inPerM: "0.3", outPerM: 2.5, apiKey: " AIza-test-key " });
    assert.equal(c.model, "gemini-2.5-flash");
    assert.equal(c.inPerM, 0.3);
    assert.equal(c.outPerM, 2.5);
    assert.equal(c.apiKey, "AIza-test-key");
  });
});
