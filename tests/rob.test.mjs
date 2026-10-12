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

describe("JBI-style checklist templates", () => {
  const ids = { jbi_xs: 8, jbi_cohort: 11, jbi_rct: 13, jbi_cc: 10 };
  it("have the expected item counts and unique keys and labels", () => {
    for (const [id, n] of Object.entries(ids)) {
      assert.equal(ROB_TEMPLATES[id].scale, "checklist");
      assert.equal(ROB_TEMPLATES[id].domains.length, n, id);
    }
    const all = Object.values(ROB_TEMPLATES).flatMap((t) => t.domains);
    assert.equal(new Set(all.map((d) => d.key)).size, all.length);
    for (const id of Object.keys(ids)) {
      const labels = ROB_TEMPLATES[id].domains.map((d) => d.label);
      assert.equal(new Set(labels).size, labels.length);
    }
  });
  it("accept yes, no, unclear and na per reviewer and reject anything else", () => {
    const rob = normalizeRob({ defaultTemplate: "jbi_cohort" });
    assert.equal(setJudgement(rob, "p1", "a", "jbico_1", "yes"), true);
    assert.equal(setJudgement(rob, "p1", "b", "jbico_1", "no"), true);
    assert.equal(setJudgement(rob, "p1", "a", "jbico_2", "na"), true);
    assert.equal(setJudgement(rob, "p1", "a", "jbico_3", "low"), false);
    assert.equal(setJudgement(rob, "p1", "a", "jbico_12", "yes"), false, "cohort has 11 items");
    assert.equal(setJudgement(rob, "p1", "a", "jbixs_1", "yes"), false, "item of another template");
    assert.equal(rob.papers.p1.reviewers.b.jbico_1, "no");
    assert.equal(setJudgement(rob, "p1", "a", "jbico_1", ""), true);
    assert.equal(rob.papers.p1.reviewers.a.jbico_1, undefined);
  });
  it("can be set per paper while other papers keep the default", () => {
    const rob = normalizeRob({});
    assert.equal(setPaperTemplate(rob, "p1", "jbi_rct"), true);
    assert.equal(templateFor(rob, "p1").domains.length, 13);
    assert.equal(templateFor(rob, "p2").id, "rob2");
  });
  it("summarize as counts and leave the overall judgement to the user", () => {
    const rob = normalizeRob({ defaultTemplate: "jbi_cc" });
    setJudgement(rob, "p1", "a", "jbicc_1", "yes");
    setJudgement(rob, "p1", "a", "jbicc_2", "yes");
    setJudgement(rob, "p1", "a", "jbicc_3", "unclear");
    setJudgement(rob, "p1", "a", "jbicc_4", "no");
    setJudgement(rob, "p1", "a", "jbicc_5", "na");
    setJudgement(rob, "p1", "b", "jbicc_1", "no");
    const s = summarizePaper(rob, "p1", reviewers);
    const a = s.byReviewer[0];
    assert.deepEqual([a.yes, a.no, a.unclear, a.na, a.assessed, a.notAssessed], [2, 1, 1, 1, 5, 5]);
    assert.equal(s.differing, 1);
    assert.equal(s.overall.judgement, "", "never auto-computed");
  });
  it("export to CSV with a labelled yes count, and to Markdown with a legend", () => {
    const rob = normalizeRob({});
    setPaperTemplate(rob, "p1", "jbi_xs");
    setJudgement(rob, "p1", "a", "jbixs_1", "yes");
    setJudgement(rob, "p1", "a", "jbixs_2", "unclear");
    setJudgement(rob, "p1", "a", "jbixs_3", "na");
    const input = { rob, paperIds: ["p1"], papers: { p1: { title: "Survey", year: "2022" } }, reviewers };
    const csv = robToCsv(input);
    const lines = csv.split("\n");
    assert.equal(lines.length, 3);
    assert.match(lines[0], /Yes count \(a count only\)/);
    const header = lines[0].split(",");
    const row = lines[1].split(",");
    assert.equal(row[header.indexOf("Yes count (a count only)")], "1");
    assert.match(lines[1], /Not applicable/);
    const md = robToMarkdown({ ...input, title: "Quality" });
    assert.match(md, /## JBI-style checklist: analytical cross-sectional/);
    assert.match(md, /- Q1: Inclusion criteria/);
    assert.match(md, /\| Q1 \| Q2 /);
    assert.ok(md.includes(ROB_NOTICE));
  });
  it("keep CSV columns apart when two designs share item wording", () => {
    const rob = normalizeRob({});
    setPaperTemplate(rob, "p1", "jbi_xs");
    setPaperTemplate(rob, "p2", "jbi_cohort");
    const header = robToCsv({ rob, paperIds: ["p1", "p2"], papers: {}, reviewers: [reviewers[0]] }).split("\n")[0];
    assert.match(header, /Cross-sectional: 3\. Exposure/);
    assert.match(header, /Cohort: 3\. Exposure/);
  });
  it("leave the CSV of a project with no checklist papers unchanged", () => {
    const rob = normalizeRob({});
    setJudgement(rob, "p1", "a", "rob2_randomization", "low");
    const csv = robToCsv({ rob, paperIds: ["p1"], papers: { p1: { title: "T" } }, reviewers });
    assert.doesNotMatch(csv, /Yes count/);
  });
});

describe("stored data from the previous version", () => {
  // A project saved before checklist templates existed.
  const old = JSON.parse(JSON.stringify({
    defaultTemplate: "robinsi",
    customDomains: [{ key: "c_1", label: "Blinding" }],
    papers: {
      p1: { template: "nos", reviewers: { a: { nos_selection: 3, nos_outcome: 2 } }, overall: { judgement: "some", note: "n" } },
      p2: { reviewers: { a: { robinsi_confounding: "high" } }, overall: { judgement: "", note: "" } },
      p3: { template: "custom", reviewers: { a: { c_1: "low" } }, overall: { judgement: "low", note: "" } },
    },
  }));
  it("keeps every stored answer and template and summarizes as before", () => {
    const rob = normalizeRob(JSON.parse(JSON.stringify(old)));
    assert.equal(templateFor(rob, "p1").id, "nos");
    assert.equal(templateFor(rob, "p2").id, "robinsi");
    const s1 = summarizePaper(rob, "p1", reviewers);
    assert.equal(s1.byReviewer[0].stars, 5);
    assert.equal(summarizePaper(rob, "p2", reviewers).byReviewer[0].high, 1);
    assert.equal(summarizePaper(rob, "p3", reviewers).byReviewer[0].low, 1);
    assert.equal(s1.overall.judgement, "some");
  });
  it("exports without a yes-count column and accepts a checklist added afterwards", () => {
    const rob = normalizeRob(JSON.parse(JSON.stringify(old)));
    const input = { rob, paperIds: ["p1", "p2", "p3"], papers: {}, reviewers: [reviewers[0]] };
    assert.doesNotMatch(robToCsv(input), /Yes count/);
    setPaperTemplate(rob, "p2", "jbi_rct");
    assert.equal(setJudgement(rob, "p2", "a", "jbirc_1", "yes"), true);
    assert.equal(rob.papers.p2.reviewers.a.robinsi_confounding, "high", "old answers are retained, just not shown for the new template");
    assert.equal(summarizePaper(rob, "p2", reviewers).byReviewer[0].yes, 1);
  });
});
