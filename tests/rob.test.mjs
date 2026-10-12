import { describe, it } from "node:test";
import assert from "node:assert/strict";

const R = await import("../src/common/rob.js");
const {
  ROB_TEMPLATES, ROB_NOTICE, normalizeRob, templateFor, setJudgement, setOverall, setPaperTemplate, setDefaultTemplate,
  addCustomDomain, renameCustomDomain, removeCustomDomain, summarizePaper, robToCsv, robToMarkdown, MAX_CUSTOM_DOMAINS,
} = R;

const reviewers = [{ id: "a", name: "Ana" }, { id: "b", name: "Ben" }];

describe("templates", () => {
  it("have the expected domains and unique keys", () => {
    assert.equal(ROB_TEMPLATES.rob2.domains.length, 5);
    assert.equal(ROB_TEMPLATES.robinsi.domains.length, 7);
    assert.deepEqual(ROB_TEMPLATES.nos.domains.map((d) => d.maxStars), [4, 2, 3]);
    const keys = Object.values(ROB_TEMPLATES).flatMap((t) => t.domains.map((d) => d.key));
    assert.equal(new Set(keys).size, keys.length);
  });
  it("normalizes missing and malformed data", () => {
    assert.equal(normalizeRob(undefined).defaultTemplate, "rob2");
    assert.equal(normalizeRob({ defaultTemplate: "zzz", papers: [] }).defaultTemplate, "rob2");
    assert.ok(normalizeRob({ customDomains: "x" }).customDomains.length > 0);
    assert.deepEqual(normalizeRob({ customDomains: [null, { key: "k" }, { label: "no key" }] }).customDomains, [{ key: "k", label: "k" }]);
  });
  it("uses a per-paper template over the default", () => {
    const rob = normalizeRob({});
    assert.equal(templateFor(rob, "p1").id, "rob2");
    setPaperTemplate(rob, "p1", "nos");
    assert.equal(templateFor(rob, "p1").id, "nos");
    assert.equal(templateFor(rob, "p2").id, "rob2");
    setPaperTemplate(rob, "p1", "");
    assert.equal(templateFor(rob, "p1").id, "rob2");
    assert.equal(setPaperTemplate(rob, "p1", "nope"), false);
    assert.equal(setDefaultTemplate(rob, "robinsi"), true);
    assert.equal(templateFor(rob, "p2").id, "robinsi");
  });
});

describe("setJudgement", () => {
  it("stores per reviewer and per domain, validates, and clears", () => {
    const rob = normalizeRob({});
    assert.equal(setJudgement(rob, "p1", "a", "rob2_randomization", "low"), true);
    assert.equal(setJudgement(rob, "p1", "b", "rob2_randomization", "high"), true);
    assert.equal(setJudgement(rob, "p1", "a", "rob2_randomization", "medium"), false);
    assert.equal(setJudgement(rob, "p1", "a", "robinsi_confounding", "low"), false, "domain not in this paper's template");
    assert.equal(rob.papers.p1.reviewers.a.rob2_randomization, "low");
    assert.equal(rob.papers.p1.reviewers.b.rob2_randomization, "high");
    assert.equal(setJudgement(rob, "p1", "a", "rob2_randomization", ""), true);
    assert.equal(rob.papers.p1.reviewers.a.rob2_randomization, undefined);
    assert.equal(setJudgement(rob, "", "a", "rob2_randomization", "low"), false);
  });
  it("accepts stars within range for the star template, including 0 and numeric strings", () => {
    const rob = normalizeRob({ defaultTemplate: "nos" });
    assert.equal(setJudgement(rob, "p1", "a", "nos_selection", 4), true);
    assert.equal(setJudgement(rob, "p1", "a", "nos_selection", 5), false);
    assert.equal(setJudgement(rob, "p1", "a", "nos_comparability", "2"), true);
    assert.equal(setJudgement(rob, "p1", "a", "nos_outcome", 0), true);
    assert.equal(setJudgement(rob, "p1", "a", "nos_outcome", 1.5), false);
    assert.equal(setJudgement(rob, "p1", "a", "nos_outcome", "low"), false);
    assert.equal(rob.papers.p1.reviewers.a.nos_comparability, 2);
    assert.equal(rob.papers.p1.reviewers.a.nos_outcome, 0);
  });
});

describe("overall judgement", () => {
  it("is only what the user sets, never derived", () => {
    const rob = normalizeRob({});
    setJudgement(rob, "p1", "a", "rob2_randomization", "high");
    assert.equal(summarizePaper(rob, "p1", reviewers).overall.judgement, "");
    assert.equal(setOverall(rob, "p1", "some", "see note"), true);
    assert.equal(summarizePaper(rob, "p1", reviewers).overall.judgement, "some");
    assert.equal(setOverall(rob, "p1", "terrible"), false);
    assert.equal(setOverall(rob, "p1", null, "just a note"), true);
    assert.equal(rob.papers.p1.overall.judgement, "some");
    assert.equal(rob.papers.p1.overall.note, "just a note");
  });
});

describe("custom domains", () => {
  it("add, rename, remove, reject duplicates and cap", () => {
    const rob = normalizeRob({ customDomains: [] });
    const d = addCustomDomain(rob, " Blinding ");
    assert.equal(d.label, "Blinding");
    assert.equal(addCustomDomain(rob, "blinding"), null);
    assert.equal(addCustomDomain(rob, "  "), null);
    assert.equal(renameCustomDomain(rob, d.key, "Blinding of assessors"), true);
    setPaperTemplate(rob, "p1", "custom");
    assert.equal(setJudgement(rob, "p1", "a", d.key, "some"), true);
    assert.equal(removeCustomDomain(rob, d.key), true);
    assert.equal(setJudgement(rob, "p1", "a", d.key, "some"), false);
    assert.equal(removeCustomDomain(rob, "nope"), false);
    for (let i = 0; i < MAX_CUSTOM_DOMAINS + 5; i += 1) addCustomDomain(rob, `Item ${i}`);
    assert.equal(rob.customDomains.length, MAX_CUSTOM_DOMAINS);
    assert.equal(new Set(rob.customDomains.map((x) => x.key)).size, MAX_CUSTOM_DOMAINS);
  });
});

describe("summarizePaper", () => {
  it("counts per reviewer and finds disagreements without judging", () => {
    const rob = normalizeRob({});
    setJudgement(rob, "p1", "a", "rob2_randomization", "low");
    setJudgement(rob, "p1", "a", "rob2_missing", "high");
    setJudgement(rob, "p1", "b", "rob2_randomization", "some");
    setJudgement(rob, "p1", "b", "rob2_missing", "high");
    const s = summarizePaper(rob, "p1", reviewers);
    assert.equal(s.byReviewer[0].low, 1);
    assert.equal(s.byReviewer[0].high, 1);
    assert.equal(s.byReviewer[0].notAssessed, 3);
    assert.equal(s.differing, 1);
  });
  it("sums stars and reports the maximum", () => {
    const rob = normalizeRob({ defaultTemplate: "nos" });
    setJudgement(rob, "p1", "a", "nos_selection", 3);
    setJudgement(rob, "p1", "a", "nos_outcome", 2);
    const s = summarizePaper(rob, "p1", reviewers);
    assert.equal(s.byReviewer[0].stars, 5);
    assert.equal(s.byReviewer[0].maxStars, 9);
    assert.equal(s.byReviewer[0].assessed, 2);
  });
  it("tolerates unknown papers and no reviewers", () => {
    assert.equal(summarizePaper(undefined, "x", undefined).byReviewer.length, 0);
  });
});

describe("exports", () => {
  const papers = { p1: { title: 'Trial, "A" | b', year: "2020" }, p2: { title: "Cohort", year: "2019" } };
  const rob = normalizeRob({});
  setJudgement(rob, "p1", "a", "rob2_randomization", "low");
  setOverall(rob, "p1", "some", "note, with comma");
  setPaperTemplate(rob, "p2", "nos");
  setJudgement(rob, "p2", "b", "nos_selection", 3);
  const input = { rob, paperIds: ["p1", "p2"], papers, reviewers };
  it("CSV has one row per paper and reviewer and quotes fields", () => {
    const csv = robToCsv(input);
    const lines = csv.split("\n");
    assert.equal(lines.length, 1 + 4);
    assert.match(lines[0], /Randomization process/);
    assert.match(lines[0], /Selection,/);
    assert.match(csv, /"Trial, ""A"" \| b"/);
    assert.match(csv, /"note, with comma"/);
    assert.match(csv, /Some concerns/);
  });
  it("Markdown carries the notice, escapes pipes and groups by template", () => {
    const md = robToMarkdown({ ...input, title: "Quality" });
    assert.ok(md.includes(ROB_NOTICE));
    assert.match(md, /## RoB 2/);
    assert.match(md, /## Newcastle-Ottawa/);
    assert.match(md, /Trial, "A" \\\| b/);
    assert.match(md, /Selection \(max 4\)/);
  });
  it("handles empty input", () => {
    assert.match(robToMarkdown({ rob: undefined, paperIds: [], papers: {}, reviewers: [] }), /No papers/);
    assert.equal(robToCsv({ rob: undefined }).split("\n").length, 1);
  });
});
