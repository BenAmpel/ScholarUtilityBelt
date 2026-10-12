import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { PROVIDERS, getProvider, runAsk, redactKey, isValidModelName } = await import("../src/common/ask-provider.js");
const { selectPapersForAsk, NO_SUPPORT } = await import("../src/common/ask-verify.js");

const KEY = "sk-test-SECRET-1234567890";
const A1 = "Remote work increased self-reported productivity among software engineers in a survey of 800 staff.";
const { papers } = selectPapersForAsk([{ key: "k1", title: "Remote work", abstract: A1 }]);
const goodRows = JSON.stringify([{ id: "P1", verdict: "supports", quote: "Remote work increased self-reported productivity among software engineers" }]);

function mockFetch(handler) {
  const calls = [];
  const fn = async (url, init) => {
    calls.push({ url, init });
    return handler(url, init);
  };
  fn.calls = calls;
  return fn;
}
const okRes = (body) => ({ ok: true, status: 200, text: async () => JSON.stringify(body) });

const base = { apiKey: KEY, question: "Does remote work help?", papers, confirmed: true };

describe("adapters build the right request", () => {
  it("anthropic", async () => {
    const f = mockFetch(() => okRes({ content: [{ type: "text", text: goodRows }], usage: { input_tokens: 120, output_tokens: 30 } }));
    const r = await runAsk({ ...base, providerId: "anthropic", model: "claude-haiku-4-5", fetchImpl: f });
    assert.equal(r.ok, true);
    assert.equal(f.calls[0].url, "https://api.anthropic.com/v1/messages");
    assert.equal(f.calls[0].init.headers["x-api-key"], KEY);
    const body = JSON.parse(f.calls[0].init.body);
    assert.equal(body.model, "claude-haiku-4-5");
    assert.ok(body.system && body.messages[0].role === "user");
    assert.deepEqual(r.usage, { inputTokens: 120, outputTokens: 30 });
    assert.equal(r.rows[0].verdict, "supports");
  });
  it("openai", async () => {
    const f = mockFetch(() => okRes({ choices: [{ message: { content: goodRows } }], usage: { prompt_tokens: 100, completion_tokens: 20 } }));
    const r = await runAsk({ ...base, providerId: "openai", model: "gpt-4o-mini", fetchImpl: f });
    assert.equal(f.calls[0].url, "https://api.openai.com/v1/chat/completions");
    assert.equal(f.calls[0].init.headers.authorization, "Bearer " + KEY);
    assert.equal(r.rows[0].verdict, "supports");
    assert.equal(r.usage.outputTokens, 20);
  });
  it("gemini keeps the key out of the URL", async () => {
    const f = mockFetch(() => okRes({ candidates: [{ content: { parts: [{ text: goodRows }] } }], usageMetadata: { promptTokenCount: 90, candidatesTokenCount: 15 } }));
    const r = await runAsk({ ...base, providerId: "gemini", model: "gemini-2.0-flash", fetchImpl: f });
    assert.match(f.calls[0].url, /^https:\/\/generativelanguage\.googleapis\.com\/v1beta\/models\/gemini-2\.0-flash:generateContent$/);
    assert.ok(!f.calls[0].url.includes(KEY));
    assert.equal(f.calls[0].init.headers["x-goog-api-key"], KEY);
    assert.equal(r.rows[0].verdict, "supports");
  });
  it("only ever calls the chosen provider's host, without cookies or redirects", async () => {
    for (const id of Object.keys(PROVIDERS)) {
      const f = mockFetch(() => okRes({}));
      await runAsk({ ...base, providerId: id, model: "some-model", fetchImpl: f });
      assert.equal(new URL(f.calls[0].url).hostname, PROVIDERS[id].host);
      assert.equal(f.calls[0].init.credentials, "omit");
      assert.equal(f.calls[0].init.redirect, "error");
      assert.equal(f.calls[0].init.referrerPolicy, "no-referrer");
    }
  });
  it("sends only the title and abstract text, not other paper fields", async () => {
    const f = mockFetch(() => okRes({}));
    const p = selectPapersForAsk([{ key: "secretkey", title: "Remote work", abstract: A1, notes: "my private note", tags: ["private"], authorsVenue: "A. Person" }]).papers;
    await runAsk({ ...base, papers: p, providerId: "openai", model: "gpt-4o-mini", fetchImpl: f });
    const sent = f.calls[0].init.body;
    assert.ok(sent.includes("Remote work") && sent.includes("productivity"));
    assert.ok(!sent.includes("my private note") && !sent.includes("secretkey") && !sent.includes("A. Person"));
  });
});

describe("runAsk guards", () => {
  it("sends nothing unless confirmed", async () => {
    const f = mockFetch(() => okRes({}));
    for (const c of [undefined, false, "true", 1]) {
      const r = await runAsk({ ...base, confirmed: c, providerId: "openai", model: "gpt-4o-mini", fetchImpl: f });
      assert.equal(r.ok, false);
    }
    assert.equal(f.calls.length, 0);
  });
  it("rejects unknown provider, missing key, bad model, empty selection, empty question", async () => {
    const f = mockFetch(() => okRes({}));
    const cases = [
      { providerId: "evil", model: "m1" },
      { providerId: "openai", model: "m1", apiKey: "" },
      { providerId: "openai", model: "../../etc" },
      { providerId: "gemini", model: "x?key=1" },
      { providerId: "openai", model: "m1", papers: [] },
      { providerId: "openai", model: "m1", question: "   " },
    ];
    for (const c of cases) assert.equal((await runAsk({ ...base, ...c, fetchImpl: f })).ok, false, JSON.stringify(c));
    assert.equal(f.calls.length, 0);
  });
  it("does not treat prototype names as providers", () => {
    assert.equal(getProvider("constructor"), null);
    assert.equal(getProvider("__proto__"), null);
  });
  it("validates model names", () => {
    assert.equal(isValidModelName("claude-haiku-4-5"), true);
    assert.equal(isValidModelName("gpt-4o-mini"), true);
    assert.equal(isValidModelName("models/x"), false);
    assert.equal(isValidModelName(""), false);
    assert.equal(isValidModelName(null), false);
  });
});

describe("runAsk errors never leak the key", () => {
  it("maps HTTP errors and redacts a key echoed by the provider", async () => {
    const f = mockFetch(() => ({ ok: false, status: 401, text: async () => JSON.stringify({ error: { message: `Incorrect API key provided: ${KEY}.` } }) }));
    const r = await runAsk({ ...base, providerId: "openai", model: "gpt-4o-mini", fetchImpl: f });
    assert.equal(r.ok, false);
    assert.match(r.error, /HTTP 401/);
    assert.ok(!r.error.includes(KEY));
    assert.match(r.error, /\[key removed\]/);
  });
  it("handles 429, non-JSON error bodies and unreadable bodies", async () => {
    let r = await runAsk({ ...base, providerId: "anthropic", model: "m1", fetchImpl: mockFetch(() => ({ ok: false, status: 429, text: async () => "slow down" })) });
    assert.match(r.error, /429/);
    r = await runAsk({ ...base, providerId: "anthropic", model: "m1", fetchImpl: mockFetch(() => ({ ok: true, status: 200, text: async () => { throw new Error("boom " + KEY); } })) });
    assert.equal(r.ok, false);
    assert.ok(!r.error.includes(KEY));
  });
  it("network failures do not expose the thrown message", async () => {
    const f = mockFetch(() => { throw new TypeError("Failed to fetch https://x?key=" + KEY); });
    const r = await runAsk({ ...base, providerId: "gemini", model: "gemini-2.0-flash", fetchImpl: f });
    assert.equal(r.ok, false);
    assert.ok(!r.error.includes(KEY));
  });
  it("a timeout becomes a plain message", async () => {
    const f = mockFetch(() => { const e = new Error("aborted"); e.name = "AbortError"; throw e; });
    assert.match((await runAsk({ ...base, providerId: "openai", model: "m1", fetchImpl: f })).error, /timed out/);
  });
  it("invalid JSON from the provider is an error", async () => {
    const r = await runAsk({ ...base, providerId: "openai", model: "m1", fetchImpl: mockFetch(() => ({ ok: true, status: 200, text: async () => "<html>" })) });
    assert.match(r.error, /not valid JSON/);
  });
  it("redactKey leaves short or missing keys alone", () => {
    assert.equal(redactKey("abc", ""), "abc");
    assert.equal(redactKey(`x ${KEY} y`, KEY), "x [key removed] y");
  });
});

describe("model output problems are handled by the verifier", () => {
  it("fabricated quote becomes no-support", async () => {
    const bad = JSON.stringify([{ id: "P1", verdict: "supports", quote: "Remote work tripled output in every firm." }]);
    const r = await runAsk({ ...base, providerId: "openai", model: "m1", fetchImpl: mockFetch(() => okRes({ choices: [{ message: { content: bad } }] })) });
    assert.equal(r.ok, true);
    assert.equal(r.rows[0].verdict, NO_SUPPORT);
  });
  it("malformed JSON and empty content give no-support rows and flag outputParsed false", async () => {
    for (const content of ['[{"id":"P1","verdict":"sup', "", "Sorry, I can't."]) {
      const r = await runAsk({ ...base, providerId: "openai", model: "m1", fetchImpl: mockFetch(() => okRes({ choices: [{ message: { content } }] })) });
      assert.equal(r.ok, true);
      assert.equal(r.outputParsed, false);
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].verdict, NO_SUPPORT);
    }
  });
  it("extra rows from the model are dropped", async () => {
    const extra = JSON.stringify([
      { id: "P1", verdict: "unclear", quote: "" },
      { id: "P2", verdict: "supports", quote: "Some invented second paper that was never sent." },
    ]);
    const r = await runAsk({ ...base, providerId: "anthropic", model: "m1", fetchImpl: mockFetch(() => okRes({ content: [{ type: "text", text: extra }] })) });
    assert.equal(r.rows.length, 1);
  });
  it("a reply with an unexpected shape does not throw", async () => {
    for (const body of [{}, { content: null }, { choices: [] }, { candidates: [{}] }]) {
      for (const id of ["anthropic", "openai", "gemini"]) {
        const r = await runAsk({ ...base, providerId: id, model: "m1", fetchImpl: mockFetch(() => okRes(body)) });
        assert.equal(r.ok, true);
        assert.equal(r.rows[0].verdict, NO_SUPPORT);
      }
    }
  });
});
