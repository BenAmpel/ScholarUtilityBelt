import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

const store = {};
let messages = [];
globalThis.chrome = {
  runtime: {
    sendMessage(msg, cb) {
      messages.push(msg);
      cb({ paid: true, tier: "trial", purchased: true });
    }
  },
  storage: {
    local: {
      get(defaults) {
        const out = {};
        for (const [k, d] of Object.entries(defaults || {})) out[k] = k in store ? JSON.parse(JSON.stringify(store[k])) : d;
        return Promise.resolve(out);
      },
      set(items) {
        for (const [k, v] of Object.entries(items)) store[k] = JSON.parse(JSON.stringify(v));
        return Promise.resolve();
      }
    }
  }
};

const W = await import("../src/common/watchlist.js");
const { maybeRunWatchCheck } = await import("../src/common/watchlist-runner.js");

const NOW = Date.parse("2026-10-12T12:00:00Z");
const noSleep = async () => {};

const work = (n, extra = {}) => ({
  id: `https://openalex.org/W${n}`,
  display_name: `Work number ${n}`,
  publication_date: "2026-10-01",
  publication_year: 2026,
  doi: `https://doi.org/10.1000/w${n}`,
  type: "article",
  primary_location: { source: { display_name: "Journal of Tests" } },
  authorships: [{ author: { display_name: "Ann Lee" } }, { author: { display_name: "Bo Chan" } }],
  ...extra
});

function stateWith(...specs) {
  let st = W.emptyWatchState();
  for (const [type, value] of specs) st = W.addWatchItem(st, { type, value, now: NOW - 5 * W.DAY_MS }).state;
  return st;
}

describe("addWatchItem / removeWatchItem", () => {
  it("adds, trims and rejects duplicates, empties and short values", () => {
    let r = W.addWatchItem(W.emptyWatchState(), { type: "query", value: "  citation   analysis ", now: NOW });
    assert.equal(r.ok, true);
    assert.equal(r.state.items[0].value, "citation analysis");
    assert.equal(W.addWatchItem(r.state, { type: "query", value: "Citation Analysis" }).ok, false);
    assert.equal(W.addWatchItem(r.state, { type: "author", value: "citation analysis" }).ok, true);
    assert.equal(W.addWatchItem(r.state, { type: "query", value: "  " }).ok, false);
    assert.equal(W.addWatchItem(r.state, { type: "query", value: "ab" }).ok, false);
    assert.equal(W.addWatchItem(r.state, { type: "other", value: "abcdef" }).ok, false);
  });

  it("caps the list at 20 items", () => {
    let st = W.emptyWatchState();
    for (let i = 0; i < 20; i++) {
      const r = W.addWatchItem(st, { type: "query", value: `topic number ${i}`, now: NOW });
      assert.equal(r.ok, true);
      st = r.state;
    }
    const over = W.addWatchItem(st, { type: "query", value: "one too many" });
    assert.equal(over.ok, false);
    assert.match(over.error, /at most 20/);
  });

  it("removes by id", () => {
    const st = stateWith(["query", "alpha beta"], ["author", "Ann Lee"]);
    assert.equal(W.removeWatchItem(st, st.items[0].id).items.length, 1);
  });
});

describe("normalizeWatchState", () => {
  it("repairs garbage from storage", () => {
    assert.deepEqual(W.normalizeWatchState(null), W.emptyWatchState());
    assert.deepEqual(W.normalizeWatchState("x"), W.emptyWatchState());
    const st = W.normalizeWatchState({
      errorCount: "bad", backoffUntil: NaN,
      items: [null, { id: "a", type: "weird", value: "x" }, { id: "b", type: "query", value: "ok value", seenIds: [1, "w"], unseen: ["no", { id: "u" }] }, { id: "b", type: "query", value: "dup" }]
    });
    assert.equal(st.items.length, 1);
    assert.deepEqual(st.items[0].seenIds, ["w"]);
    assert.equal(st.items[0].unseen.length, 1);
    assert.equal(st.errorCount, 0);
  });
});

describe("URLs", () => {
  it("builds a works URL for a query with the shared mailto and the optional key", () => {
    const item = { type: "query", value: "retraction detection", lastCheckedAt: 0 };
    const u = new URL(W.buildWorksUrl(item, { now: NOW, apiKey: "oa_key 1" }));
    assert.equal(u.origin, "https://api.openalex.org");
    assert.equal(u.pathname, "/works");
    assert.equal(u.searchParams.get("search"), "retraction detection");
    assert.equal(u.searchParams.get("per_page"), "50");
    assert.equal(u.searchParams.get("sort"), "publication_date:desc");
    assert.equal(u.searchParams.get("api_key"), "oa_key 1");
    assert.equal(u.searchParams.get("mailto"), "scholar-extension@local");
    assert.equal(u.searchParams.get("filter"), "from_publication_date:2026-08-13");
  });
  it("omits api_key when unset, uses author.id for authors, and shortens the window after the first check", () => {
    const item = { type: "author", value: "Ann Lee", authorId: "A123", lastCheckedAt: NOW - W.DAY_MS };
    const u = new URL(W.buildWorksUrl(item, { now: NOW }));
    assert.equal(u.searchParams.has("api_key"), false);
    assert.equal(u.searchParams.has("search"), false);
    assert.equal(u.searchParams.get("filter"), "author.id:A123,from_publication_date:2026-10-04");
  });
  it("encodes hostile query text", () => {
    const u = new URL(W.buildWorksUrl({ type: "query", value: "a&filter=evil:1#x", lastCheckedAt: 0 }, { now: NOW }));
    assert.equal(u.searchParams.get("search"), "a&filter=evil:1#x");
    assert.equal(u.searchParams.getAll("filter").length, 1);
  });
});

describe("parsing", () => {
  it("compacts works and caps results at 50", () => {
    const body = { results: Array.from({ length: 80 }, (_, i) => work(i)) };
    const out = W.parseWorksResponse(body);
    assert.equal(out.length, 50);
    assert.equal(out[0].doi, "10.1000/w0");
    assert.equal(out[0].url, "https://doi.org/10.1000/w0");
    assert.equal(out[0].venue, "Journal of Tests");
    assert.deepEqual(out[0].authors, ["Ann Lee", "Bo Chan"]);
  });
  it("skips malformed records and tolerates odd bodies", () => {
    assert.deepEqual(W.parseWorksResponse(null), []);
    assert.deepEqual(W.parseWorksResponse({ results: "no" }), []);
    const out = W.parseWorksResponse({ results: [null, {}, { id: "x" }, { id: "https://openalex.org/W9", display_name: "T", doi: null, authorships: null }] });
    assert.equal(out.length, 1);
    assert.equal(out[0].url, "https://openalex.org/W9");
  });
  it("picks the exact-name author with the most works, else the first result", () => {
    const body = { results: [
      { id: "https://openalex.org/A1", display_name: "Ann Lee", works_count: 3 },
      { id: "https://openalex.org/A2", display_name: "Ann Lee", works_count: 90 },
      { id: "https://openalex.org/A3", display_name: "Anna Leeds", works_count: 500 }
    ] };
    assert.equal(W.pickAuthor(body, "ann lee").id, "A2");
    assert.equal(W.pickAuthor({ results: [{ id: "A9", display_name: "Someone", works_count: 1 }] }, "Other").id, "A9");
    assert.equal(W.pickAuthor({ results: [] }, "x"), null);
  });
});

describe("shouldRunCheck", () => {
  it("runs only when an item is due, not in backoff, not claimed", () => {
    const st = stateWith(["query", "alpha beta"]);
    assert.equal(W.shouldRunCheck(st, NOW).run, true);
    assert.equal(W.shouldRunCheck(W.emptyWatchState(), NOW).reason, "empty");
    assert.equal(W.shouldRunCheck({ ...st, backoffUntil: NOW + 1000 }, NOW).reason, "backoff");
    assert.equal(W.shouldRunCheck({ ...st, claimUntil: NOW + 1000 }, NOW).reason, "busy");
    const fresh = { ...st, items: st.items.map((i) => ({ ...i, lastCheckedAt: NOW - 3600 * 1000 })) };
    assert.equal(W.shouldRunCheck(fresh, NOW).reason, "not due");
    const old = { ...st, items: st.items.map((i) => ({ ...i, lastCheckedAt: NOW - 25 * 3600 * 1000 })) };
    assert.equal(W.shouldRunCheck(old, NOW).run, true);
  });
});

describe("runWatchCheck", () => {
  it("fetches a due query, stores new works as unseen, and does not re-run within 24 hours", async () => {
    const urls = [];
    const fetchJson = async (url) => { urls.push(url); return { ok: true, status: 200, body: { results: [work(1), work(2)] } }; };
    const st = stateWith(["query", "alpha beta"]);
    const r = await W.runWatchCheck(st, { fetchJson, now: NOW, sleep: noSleep });
    assert.equal(r.checked, 1);
    assert.equal(r.newWorks, 2);
    assert.equal(r.state.items[0].unseen.length, 2);
    assert.equal(r.state.items[0].lastCheckedAt, NOW);
    assert.equal(urls.length, 1);
    const again = await W.runWatchCheck(r.state, { fetchJson, now: NOW + 3600 * 1000, sleep: noSleep });
    assert.equal(again.stopped, "not due");
    assert.equal(urls.length, 1);
  });

  it("only reports works not seen before", async () => {
    let call = 0;
    const fetchJson = async () => ({ ok: true, status: 200, body: { results: call++ === 0 ? [work(1), work(2)] : [work(2), work(3)] } });
    const st = stateWith(["query", "alpha beta"]);
    const r1 = await W.runWatchCheck(st, { fetchJson, now: NOW, sleep: noSleep });
    const r2 = await W.runWatchCheck(r1.state, { fetchJson, now: NOW + 25 * 3600 * 1000, sleep: noSleep });
    assert.equal(r2.newWorks, 1);
    assert.deepEqual(r2.state.items[0].unseen.map((w) => w.id), ["W3", "W1", "W2"].map((n) => `https://openalex.org/${n}`));
    const seen = W.markItemSeen(r2.state, r2.state.items[0].id, NOW);
    assert.equal(W.totalUnseen(seen), 0);
    const r3 = await W.runWatchCheck(seen, { fetchJson: async () => ({ ok: true, body: { results: [work(1), work(2), work(3)] } }), now: NOW + 50 * 3600 * 1000, sleep: noSleep });
    assert.equal(r3.newWorks, 0);
  });

  it("caps unseen results at 50 per item", async () => {
    const fetchJson = async () => ({ ok: true, body: { results: Array.from({ length: 70 }, (_, i) => work(i)) } });
    const r = await W.runWatchCheck(stateWith(["query", "big topic"]), { fetchJson, now: NOW, sleep: noSleep });
    assert.equal(r.state.items[0].unseen.length, 50);
  });

  it("resolves an author once, shows who matched, then fetches works by author id", async () => {
    const urls = [];
    const fetchJson = async (url) => {
      urls.push(url);
      if (url.includes("/authors")) return { ok: true, body: { results: [{ id: "https://openalex.org/A55", display_name: "Ann Lee", works_count: 12 }] } };
      return { ok: true, body: { results: [work(7)] } };
    };
    const r = await W.runWatchCheck(stateWith(["author", "Ann Lee"]), { fetchJson, now: NOW, sleep: noSleep });
    assert.equal(urls.length, 2);
    assert.equal(r.state.items[0].authorId, "A55");
    assert.match(r.state.items[0].matchedName, /Ann Lee \(12 works\)/);
    assert.match(urls[1], /author\.id%3AA55|author\.id:A55/);
    const r2 = await W.runWatchCheck(r.state, { fetchJson, now: NOW + 25 * 3600 * 1000, sleep: noSleep });
    assert.equal(urls.filter((u) => u.includes("/authors")).length, 1);
    assert.equal(r2.checked, 1);
  });

  it("reports an unmatched author without backing off", async () => {
    const fetchJson = async () => ({ ok: true, body: { results: [] } });
    const r = await W.runWatchCheck(stateWith(["author", "Nobody Atall"]), { fetchJson, now: NOW, sleep: noSleep });
    assert.match(r.state.items[0].lastError, /No author/);
    assert.equal(r.state.backoffUntil, 0);
    assert.equal(r.state.items[0].lastCheckedAt, NOW);
  });

  it("pauses between requests", async () => {
    const sleeps = [];
    const fetchJson = async () => ({ ok: true, body: { results: [] } });
    await W.runWatchCheck(stateWith(["query", "one topic"], ["query", "two topic"], ["query", "three topic"]), {
      fetchJson, now: NOW, sleep: async (ms) => sleeps.push(ms)
    });
    assert.deepEqual(sleeps, [1000, 1000]);
  });

  it("stops after the first 429, sets backoff, and keeps working items untouched", async () => {
    let n = 0;
    const fetchJson = async () => { n++; return { ok: false, status: 429 }; };
    const st = stateWith(["query", "one topic"], ["query", "two topic"], ["query", "three topic"]);
    const r = await W.runWatchCheck(st, { fetchJson, now: NOW, sleep: noSleep });
    assert.equal(n, 1);
    assert.equal(r.stopped, "error");
    assert.match(r.error, /429/);
    assert.equal(r.state.errorCount, 1);
    assert.equal(r.state.backoffUntil, NOW + 30 * 60 * 1000);
    assert.equal(r.state.items.every((i) => i.lastCheckedAt === 0), true);
    const blocked = await W.runWatchCheck(r.state, { fetchJson, now: NOW + 60 * 1000, sleep: noSleep });
    assert.equal(blocked.stopped, "backoff");
    assert.equal(n, 1);
  });

  it("doubles the backoff on repeated failures up to 24 hours, and resets after success", async () => {
    assert.equal(W.backoffDelay(1), 30 * 60 * 1000);
    assert.equal(W.backoffDelay(2), 60 * 60 * 1000);
    assert.equal(W.backoffDelay(3), 120 * 60 * 1000);
    assert.equal(W.backoffDelay(20), W.DAY_MS);
    let st = stateWith(["query", "one topic"]);
    const fail = async () => ({ ok: false });
    let t = NOW;
    for (let i = 0; i < 3; i++) {
      st = (await W.runWatchCheck(st, { fetchJson: fail, now: t, sleep: noSleep })).state;
      t = st.backoffUntil + 1;
    }
    assert.equal(st.errorCount, 3);
    const ok = await W.runWatchCheck(st, { fetchJson: async () => ({ ok: true, body: { results: [] } }), now: t, sleep: noSleep });
    assert.equal(ok.state.errorCount, 0);
  });

  it("treats a 400 on one item as that item's problem and continues", async () => {
    const fetchJson = async (url) => (url.includes("bad") ? { ok: false, status: 400 } : { ok: true, body: { results: [work(1)] } });
    const r = await W.runWatchCheck(stateWith(["query", "bad query"], ["query", "good query"]), { fetchJson, now: NOW, sleep: noSleep });
    assert.match(r.state.items[0].lastError, /400/);
    assert.equal(r.state.items[1].unseen.length, 1);
    assert.equal(r.state.backoffUntil, 0);
  });

  it("survives a fetch that throws or returns nothing", async () => {
    const r = await W.runWatchCheck(stateWith(["query", "one topic"]), { fetchJson: async () => { throw new Error("boom"); }, now: NOW, sleep: noSleep });
    assert.equal(r.stopped, "error");
    const r2 = await W.runWatchCheck(stateWith(["query", "one topic"]), { fetchJson: async () => null, now: NOW, sleep: noSleep });
    assert.equal(r2.stopped, "error");
  });

  it("never fetches Scholar or any host but api.openalex.org", async () => {
    const urls = [];
    const fetchJson = async (url) => { urls.push(url); return { ok: true, body: { results: [] } }; };
    await W.runWatchCheck(stateWith(["query", "scholar.google.com topic"], ["author", "Ann Lee"]), { fetchJson, now: NOW, sleep: noSleep });
    assert.ok(urls.length >= 2);
    for (const u of urls) assert.equal(new URL(u).host, "api.openalex.org");
  });
});

describe("mergeCheckResult", () => {
  it("keeps items added during the check and drops results for removed ones", async () => {
    const st = stateWith(["query", "one topic"], ["query", "two topic"]);
    const fetchJson = async () => ({ ok: true, body: { results: [work(1)] } });
    const out = await W.runWatchCheck(st, { fetchJson, now: NOW, sleep: noSleep });
    let latest = W.removeWatchItem(st, st.items[1].id);
    latest = W.addWatchItem(latest, { type: "query", value: "added later", now: NOW }).state;
    const merged = W.mergeCheckResult(latest, out.state);
    assert.deepEqual(merged.items.map((i) => i.value), ["one topic", "added later"]);
    assert.equal(merged.items[0].unseen.length, 1);
    assert.equal(merged.items[1].unseen.length, 0);
  });
});

describe("maybeRunWatchCheck (storage + gate)", () => {
  beforeEach(() => {
    for (const k of Object.keys(store)) delete store[k];
    messages = [];
  });

  it("does nothing, and sends nothing, when there are no watch items", async () => {
    let called = 0;
    const r = await maybeRunWatchCheck({ fetchJson: async () => { called++; return { ok: true, body: {} }; }, now: NOW });
    assert.equal(r.ran, false);
    assert.equal(called, 0);
    assert.equal(messages.length, 0);
  });

  it("runs once for a due item, saves results, and stores the optional API key in the URL", async () => {
    store.watchLists = stateWith(["query", "alpha beta"]);
    store.settings = { openalexApiKey: "oa_abc" };
    const urls = [];
    const fetchJson = async (u) => { urls.push(u); return { ok: true, body: { results: [work(1)] } }; };
    const r1 = await maybeRunWatchCheck({ fetchJson, now: NOW });
    assert.equal(r1.ran, true);
    assert.equal(r1.newWorks, 1);
    assert.ok(urls[0].includes("api_key=oa_abc"));
    assert.equal(store.watchLists.items[0].unseen.length, 1);
    assert.equal(store.watchLists.claimUntil, 0);
    const r2 = await maybeRunWatchCheck({ fetchJson, now: NOW + 1000 });
    assert.equal(r2.ran, false);
    assert.equal(r2.reason, "not due");
    assert.equal(urls.length, 1);
  });

  it("does not run for a user without the Pro feature", async () => {
    store.watchLists = stateWith(["query", "alpha beta"]);
    chrome.runtime.sendMessage = (msg, cb) => cb({ paid: false, tier: "free", purchased: false });
    let called = 0;
    const r = await maybeRunWatchCheck({ fetchJson: async () => { called++; return { ok: true, body: {} }; }, now: NOW });
    assert.equal(r.reason, "locked");
    assert.equal(called, 0);
  });
});
