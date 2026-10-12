import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  parseNameList, orcidChecksumOk, findOrcid, authorSearchUrl, authorVenuesUrl, getJson, normalizeAuthor, rankCandidates,
  searchAuthors, buildReport, worksInLastFiveYears, topVenues, reportToCsv, MAX_NAMES,
} from "../src/common/openalex-authors.js";

const okRes = (data) => ({ ok: true, status: 200, json: async () => data, headers: { get: () => null } });
const failRes = (status, retryAfter) => ({ ok: false, status, json: async () => ({}), headers: { get: (h) => (h === "retry-after" ? retryAfter : null) } });
const author = (id, name, extra = {}) => ({
  id: `https://openalex.org/${id}`, display_name: name, works_count: 10, cited_by_count: 100,
  summary_stats: { h_index: 5, i10_index: 2, "2yr_mean_citedness": 1.234 },
  last_known_institutions: [{ display_name: "State University" }], topics: [{ display_name: "Ecology" }], ...extra,
});
const noSleep = async () => {};

describe("ORCID", () => {
  it("validates the check digit", () => {
    assert.equal(orcidChecksumOk("0000-0002-1825-0097"), true);
    assert.equal(orcidChecksumOk("0000-0001-5109-3700"), true);
    assert.equal(orcidChecksumOk("0000-0002-1694-233X"), true);
    assert.equal(orcidChecksumOk("0000-0002-1825-0098"), false);
    assert.equal(orcidChecksumOk("nonsense"), false);
  });
  it("finds an ORCID in a URL and rejects a bad one", () => {
    assert.equal(findOrcid("https://orcid.org/0000-0002-1825-0097"), "0000-0002-1825-0097");
    assert.equal(findOrcid("0000-0002-1694-233x"), "0000-0002-1694-233X");
    assert.equal(findOrcid("0000-0002-1825-0098"), "");
  });
});

describe("parseNameList", () => {
  it("reads names, institutions and ORCIDs and skips blanks", () => {
    const { entries } = parseNameList("Ada Lovelace\n\n  Grace Hopper | Yale University \n0000-0002-1825-0097\nJosiah Carberry | 0000-0002-1825-0097\n");
    assert.equal(entries.length, 4);
    assert.deepEqual([entries[0].name, entries[0].institution, entries[0].orcid], ["Ada Lovelace", "", ""]);
    assert.deepEqual([entries[1].name, entries[1].institution], ["Grace Hopper", "Yale University"]);
    assert.deepEqual([entries[2].name, entries[2].orcid], ["", "0000-0002-1825-0097"]);
    assert.deepEqual([entries[3].name, entries[3].orcid, entries[3].institution], ["Josiah Carberry", "0000-0002-1825-0097", ""]);
  });
  it("flags an ORCID with a wrong check digit instead of using it", () => {
    const { entries } = parseNameList("A Person | 0000-0002-1825-0098");
    assert.equal(entries[0].orcid, "");
    assert.equal(entries[0].badOrcid, true);
  });
  it("caps at 50 names and reports the rest", () => {
    const text = Array.from({ length: 53 }, (_, i) => `Person ${i}`).join("\n");
    const { entries, truncated } = parseNameList(text);
    assert.equal(entries.length, MAX_NAMES);
    assert.equal(truncated, 3);
  });
  it("handles empty and non-string input", () => {
    assert.equal(parseNameList("").entries.length, 0);
    assert.equal(parseNameList(null).entries.length, 0);
  });
});

describe("urls", () => {
  it("adds mailto and the API key, and encodes the name", () => {
    const u = new URL(authorSearchUrl({ name: "Zoë O'Brien & Sons", orcid: "" }, "KEY"));
    assert.equal(u.host, "api.openalex.org");
    assert.equal(u.pathname, "/authors");
    assert.equal(u.searchParams.get("search"), "Zoë O'Brien & Sons");
    assert.equal(u.searchParams.get("api_key"), "KEY");
    assert.ok(u.searchParams.get("mailto"));
  });
  it("omits the API key when none is set and filters by ORCID when given", () => {
    const u = new URL(authorSearchUrl({ name: "X", orcid: "0000-0002-1825-0097" }));
    assert.equal(u.searchParams.has("api_key"), false);
    assert.equal(u.searchParams.get("filter"), "orcid:https://orcid.org/0000-0002-1825-0097");
    assert.equal(u.searchParams.has("search"), false);
  });
  it("builds the venue url from a long or short id", () => {
    assert.equal(new URL(authorVenuesUrl("https://openalex.org/A123")).searchParams.get("filter"), "author.id:A123");
  });
});

describe("getJson", () => {
  it("retries a 429 then succeeds, waiting with backoff", async () => {
    const waits = [];
    const seq = [failRes(429), failRes(503), okRes({ a: 1 })];
    const r = await getJson("https://api.openalex.org/x", { fetchFn: async () => seq.shift(), sleep: async (ms) => waits.push(ms) });
    assert.deepEqual(r, { ok: true, data: { a: 1 } });
    assert.deepEqual(waits, [2000, 4000]);
  });
  it("honours Retry-After", async () => {
    const waits = [];
    const seq = [failRes(429, "7"), okRes({})];
    await getJson("u", { fetchFn: async () => seq.shift(), sleep: async (ms) => waits.push(ms) });
    assert.deepEqual(waits, [7000]);
  });
  it("gives up after the retries and does not retry a 404", async () => {
    let calls = 0;
    const r = await getJson("u", { fetchFn: async () => { calls++; return failRes(500); }, sleep: noSleep });
    assert.equal(r.ok, false);
    assert.equal(calls, 4);
    calls = 0;
    const r2 = await getJson("u", { fetchFn: async () => { calls++; return failRes(404); }, sleep: noSleep });
    assert.equal(calls, 1);
    assert.equal(r2.status, 404);
  });
  it("times out a request that never answers", async () => {
    const fetchFn = (u, init) => new Promise((_, reject) => init.signal.addEventListener("abort", () => reject(Object.assign(new Error("x"), { name: "AbortError" }))));
    const r = await getJson("u", { fetchFn, timeoutMs: 20, maxRetries: 1, sleep: noSleep });
    assert.equal(r.ok, false);
    assert.match(r.error, /No answer/);
  });
  it("reports a network error and bad JSON", async () => {
    const r = await getJson("u", { fetchFn: async () => { throw new TypeError("net"); }, maxRetries: 0, sleep: noSleep });
    assert.match(r.error, /network/);
    const bad = await getJson("u", { fetchFn: async () => ({ ok: true, status: 200, json: async () => { throw new Error("bad"); } }), sleep: noSleep });
    assert.equal(bad.ok, false);
  });
});

describe("candidates", () => {
  it("normalises new and old institution/topic shapes and tolerates junk", () => {
    const old = normalizeAuthor({ id: "https://openalex.org/A9", display_name: "Old", last_known_institution: { display_name: "Old U" }, x_concepts: [{ display_name: "Physics" }] });
    assert.equal(old.institution, "Old U");
    assert.deepEqual(old.topics, ["Physics"]);
    assert.equal(old.hIndex, null);
    assert.equal(normalizeAuthor(null), null);
    assert.equal(normalizeAuthor({ display_name: "No id" }), null);
  });
  it("shows at most 3, suggests only the top one and pre-confirms nothing without an ORCID match", () => {
    const res = [1, 2, 3, 4, 5].map((n) => author(`A${n}`, `Jane Smith ${n}`));
    const c = rankCandidates({ name: "Jane Smith", institution: "", orcid: "" }, res);
    assert.equal(c.length, 3);
    assert.deepEqual(c.map((x) => x.match), ["suggested", "", ""]);
  });
  it("ranks an institution match first", () => {
    const res = [author("A1", "Jane Smith"), author("A2", "Jane Smith", { last_known_institutions: [{ display_name: "Yale University" }] })];
    const c = rankCandidates({ name: "Jane Smith", institution: "yale", orcid: "" }, res);
    assert.equal(c[0].shortId, "A2");
    assert.equal(c[0].match, "suggested");
  });
  it("marks an exact ORCID match", () => {
    const res = [author("A1", "Someone Else"), author("A2", "Josiah Carberry", { orcid: "https://orcid.org/0000-0002-1825-0097" })];
    const c = rankCandidates({ name: "", institution: "", orcid: "0000-0002-1825-0097" }, res);
    assert.equal(c[0].shortId, "A2");
    assert.equal(c[0].match, "orcid");
    assert.equal(c[1].match, "suggested");
  });
});

describe("searchAuthors", () => {
  it("makes one request per name with a gap, confirms only exact ORCID matches, and reports errors per name", async () => {
    const urls = [];
    const gaps = [];
    const fetchFn = async (url) => {
      urls.push(url);
      const u = new URL(url);
      if (u.searchParams.get("search") === "Bad Name") return failRes(400);
      if (u.searchParams.get("search") === "Nobody") return okRes({ results: [] });
      if (u.searchParams.get("filter")) return okRes({ results: [author("A7", "Josiah Carberry", { orcid: "https://orcid.org/0000-0002-1825-0097" })] });
      return okRes({ results: [author("A1", "Jane Smith")] });
    };
    const { entries } = parseNameList("Jane Smith\n0000-0002-1825-0097\nNobody\nBad Name\n0000-0002-1825-0098");
    const rows = await searchAuthors(entries, { fetchFn, apiKey: "K", sleep: async (ms) => gaps.push(ms) });
    assert.equal(urls.length, 4);
    assert.ok(urls.every((u) => u.includes("api_key=K")));
    assert.deepEqual(gaps, [1000, 1000, 1000]);
    assert.equal(rows[0].status, "ok");
    assert.equal(rows[0].confirmedId, "");
    assert.equal(rows[1].confirmedId, "https://openalex.org/A7");
    assert.equal(rows[2].status, "none");
    assert.equal(rows[3].status, "error");
    assert.equal(rows[4].status, "error");
    assert.match(rows[4].error, /check digit|name/i);
  });
  it("makes no request once cancelled", async () => {
    let calls = 0;
    const { entries } = parseNameList("A B\nC D");
    const rows = await searchAuthors(entries, { fetchFn: async () => { calls++; return okRes({ results: [] }); }, sleep: noSleep, shouldCancel: () => true });
    assert.equal(calls, 0);
    assert.ok(rows.every((r) => r.status === "skipped"));
  });
});

describe("report", () => {
  it("counts the last five calendar years and lists up to five venues", () => {
    const counts = [2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026].map((y) => ({ year: y, works: 1 }));
    assert.equal(worksInLastFiveYears(counts, new Date("2026-10-12")), 5);
    assert.equal(worksInLastFiveYears([], new Date()), null);
    const v = topVenues([{ key: "unknown", count: 99 }, ...Array.from({ length: 7 }, (_, i) => ({ key: `S${i}`, key_display_name: `Journal ${i}`, count: i + 1 }))]);
    assert.equal(v.length, 5);
    assert.equal(v[0].name, "Journal 6");
  });
  it("reports confirmed authors only and survives a venue failure", async () => {
    const a1 = normalizeAuthor(author("A1", "Jane Smith", { counts_by_year: [{ year: 2026, works_count: 3 }, { year: 2010, works_count: 9 }] }));
    const a2 = normalizeAuthor(author("A2", "Joe Roe"));
    let n = 0;
    const fetchFn = async () => (++n === 1
      ? okRes({ group_by: [{ key: "S1", key_display_name: "Nature, Science", count: 4 }] })
      : failRes(404));
    const rows = await buildReport([{ entry: { raw: "Jane Smith" }, candidate: a1 }, { entry: { raw: "Joe Roe" }, candidate: a2 }], { fetchFn, sleep: noSleep, now: new Date("2026-10-12") });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].worksLastFiveYears, 3);
    assert.equal(rows[0].hIndex, 5);
    assert.equal(rows[0].meanCitedness2y, 1.234);
    assert.equal(rows[0].topVenues[0].name, "Nature, Science");
    assert.equal(rows[1].topVenues.length, 0);
    assert.match(rows[1].venueError, /404/);
    assert.equal(rows[1].worksLastFiveYears, null);
  });
  it("writes CSV with quoting and a guard against formulas", () => {
    const csv = reportToCsv([{ typed: "=cmd", name: 'Smith, "J"', openalexId: "https://openalex.org/A1", orcid: "", institution: "U", worksCount: 1, citedBy: 2, hIndex: 3, meanCitedness2y: 1.5, worksLastFiveYears: null, topVenues: [{ name: "J1", count: 2 }, { name: "J2", count: 1 }], venueError: "" }]);
    const lines = csv.trim().split("\n");
    assert.equal(lines.length, 2);
    assert.ok(lines[1].startsWith("'=cmd,"));
    assert.ok(lines[1].includes('"Smith, ""J"""'));
    assert.ok(lines[1].includes("J1 (2); J2 (1)"));
    assert.ok(lines[1].includes(",1.50,"));
  });
});
