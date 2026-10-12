import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { analyzeClaims, SNIPPET_NOTE, MAX_CHIPS, DEFAULT_MIN_N } = await import("../src/common/claim-signals.js");

const ids = (text, opts) => analyzeClaims(text, opts).map((c) => c.id);

describe("causal wording", () => {
  it("flags a causal verb next to a cross-sectional design", () => {
    const t = "In this cross-sectional study of 412 adults, screen time causes poorer sleep quality and reduces daytime alertness across all age groups.";
    assert.ok(ids(t).includes("causal-observational"));
  });
  it("flags a causal verb next to survey data", () => {
    const t = "Using survey data from 5,000 employees, we show that remote work improves job satisfaction and leads to lower turnover intentions in firms.";
    assert.ok(ids(t).includes("causal-observational"));
  });
  it("flags a causal verb when no design is named", () => {
    const t = "Social media use drives anxiety among teenagers, and heavy use prevents face-to-face friendships from forming in the school years that follow.";
    assert.ok(ids(t).includes("causal-no-design"));
  });
  it("stays quiet in a randomized trial", () => {
    const t = "In a randomized controlled trial of 240 patients, the drug reduces systolic blood pressure and improves sleep compared with placebo over twelve weeks.";
    assert.ok(!ids(t).some((i) => i.startsWith("causal")));
  });
  it("stays quiet in experiments and quasi-experiments", () => {
    for (const t of [
      "Across three online experiments, framing the message as a loss leads to greater donations than framing it as a gain among adult participants.",
      "Using a difference-in-differences design on state policy changes, the minimum wage increase reduces teen employment in affected counties modestly.",
      "A Mendelian randomization analysis suggests that higher BMI causes increased risk of type 2 diabetes in this large European population sample.",
    ]) assert.ok(!ids(t).some((i) => i.startsWith("causal")), t);
  });
  it("stays quiet when a randomized design appears alongside a cohort", () => {
    const t = "We combine a prospective cohort with a randomized sub-study; vitamin D supplementation reduces fracture risk in the older adults we followed.";
    assert.ok(!ids(t).some((i) => i.startsWith("causal")));
  });
  it("ignores noun uses and non-finite forms", () => {
    for (const t of [
      "We review the main causes of hospital readmission and describe how discharge planning has changed over the past twenty years in several countries.",
      "Hard drives and flash drives differ in failure modes, which the authors measure over a three-year period in a large data centre fleet.",
      "Exercise can reduce depressive symptoms, and some analyses suggest it may improve outcomes; whether this holds in the long run is unclear to date.",
    ]) assert.ok(!ids(t).some((i) => i.startsWith("causal")), t);
  });
  it("ignores negated and conditional use", () => {
    assert.ok(!ids("Our correlational data show that income does not cause happiness, and the survey cannot tell whether wealth improves wellbeing at all.").some((i) => i === "causal-observational"));
  });
  it("does not treat a machine learning benchmark claim as causal", () => {
    const t = "We propose a new architecture whose attention mechanism improves accuracy and reduces training time on three standard benchmarks.";
    assert.ok(!ids(t).some((i) => i.startsWith("causal")));
  });
  it("names the verb and the design in the tooltip", () => {
    const [c] = analyzeClaims("A cross-sectional survey of 800 nurses shows that long overtime causes burnout and reduces patient safety across several hospital wards.");
    assert.match(c.tip, /causes/);
    assert.match(c.tip, /cross-sectional/);
  });
});

describe("significant without a statistic", () => {
  it("flags a statistical use with no number", () => {
    const t = "Participants in the treatment group reported significantly higher well-being than controls, and the difference was statistically significant after the intervention period.";
    assert.ok(ids(t).includes("significant-no-stat"));
  });
  it("stays quiet with a p-value", () => {
    assert.ok(!ids("Treated participants scored significantly higher than controls on the wellbeing scale (p < .05) across the full eight-week study period.").includes("significant-no-stat"));
    assert.ok(!ids("The association was statistically significant (P = 0.003) in the adjusted model, and held across all three age strata we examined here.").includes("significant-no-stat"));
  });
  it("stays quiet with an interval, effect size or percentage", () => {
    for (const t of [
      "Exercise led to significantly lower depression scores (mean difference -3.2, 95% CI -4.1 to -2.3) in the pooled sample of included studies.",
      "The intervention produced a significant improvement with a medium effect size across all measures in the sample of schools we visited here.",
      "The program significantly reduced hospital readmissions by 18% over twelve months in the participating clinics, compared with matched controls.",
      "Women scored significantly higher than men (d = 0.42) on the verbal measure, and the gap persisted across all the age bands we tested.",
    ]) assert.ok(!ids(t).includes("significant-no-stat"), t);
  });
  it("ignores 'significant' meaning important", () => {
    for (const t of [
      "This work makes a significant contribution to the literature on organisational learning and opens significant challenges for future research in the field.",
      "Significant progress has been made in battery chemistry over the past decade, and several significant barriers to deployment remain open for the community.",
    ]) assert.ok(!ids(t).includes("significant-no-stat"), t);
  });
  it("ignores negated results", () => {
    assert.ok(!ids("We found no significant differences between the two groups on any outcome measure in the pilot comparison of the two teaching formats used here.").includes("significant-no-stat"));
  });
});

describe("small stated N", () => {
  it("flags n = 12", () => {
    const c = analyzeClaims("A within-subjects comparison with n = 12 showed shorter completion times with the revised interface compared with the original layout.");
    assert.ok(c.some((x) => x.id === "small-n" && /12/.test(x.label)));
  });
  it("flags 18 participants", () => {
    assert.ok(ids("Eighteen is small but we recruited 18 participants who each completed the task twice under both lighting conditions in the laboratory.").includes("small-n"));
  });
  it("respects the configurable threshold", () => {
    const t = "A survey of 45 respondents examined attitudes towards remote supervision in small firms, with responses collected over a two-week period in spring.";
    assert.ok(!ids(t).includes("small-n"));
    assert.ok(ids(t, { minN: 100 }).includes("small-n"));
    assert.equal(DEFAULT_MIN_N, 30);
  });
  it("uses the largest stated N so a subgroup never triggers it", () => {
    assert.ok(!ids("Of 1,204 participants, 15 participants withdrew early and the remaining sample completed all three waves of the longitudinal assessment.").includes("small-n"));
  });
  it("stays quiet for qualitative, pilot and case-study work", () => {
    for (const t of [
      "We conducted semi-structured interviews with 12 participants about their experience of hybrid work and analysed the transcripts thematically in detail.",
      "This pilot study enrolled 20 patients to test feasibility of the protocol before a larger trial is planned at three additional hospital sites.",
      "A single case study of one firm, following 8 employees over a year, describes how the new reporting system was adopted and adapted over time.",
    ]) assert.ok(!ids(t).includes("small-n"), t);
  });
  it("ignores N of 1 and non-numeric text", () => {
    assert.ok(!ids("A sample of 1 participant is not a sample; the report describes one patient presenting with an unusual combination of symptoms at admission.").includes("small-n"));
  });
  it("handles a malformed threshold", () => {
    assert.ok(Array.isArray(analyzeClaims("n = 5 participants completed a long task in the lab, and their completion times were recorded for later analysis.", { minN: "abc" })));
  });
});

describe("novelty wording", () => {
  it("flags first-of-its-kind and novel method phrasing", () => {
    assert.ok(ids("To our knowledge, this is the first study to examine sleep and screen time together, using a novel approach to measurement across two cities.").includes("novelty-wording"));
    assert.ok(ids("The model achieves state-of-the-art results on two benchmarks, and for the first time we show stable training at this scale in practice.").includes("novelty-wording"));
  });
  it("ignores 'novel' in biology and 'first' in ordinary use", () => {
    for (const t of [
      "A novel coronavirus was isolated from patients with pneumonia in the city, and the genome was sequenced and compared with known bat viruses in the region.",
      "The first author collected the samples in the first year of the project and the second year was spent on analysis and replication of the main findings.",
      "First-order logic is used to encode the rules, and a review of state-of-the-art tools is given in the second half of the chapter on verification.",
    ]) assert.ok(!ids(t).includes("novelty-wording"), t);
  });
});

describe("no method words", () => {
  it("flags a long snippet with no method vocabulary", () => {
    const t = "Cities are growing faster than anyone planned for. Housing costs keep climbing while wages lag behind, and the gap between neighbourhoods is widening every single year as families move out of the centre and into the outer suburbs.";
    assert.ok(ids(t).includes("no-method-words"));
  });
  it("stays quiet on short snippets", () => {
    assert.ok(!ids("Cities are growing faster than anyone planned for and housing costs keep climbing for every family in them.").includes("no-method-words"));
  });
  it("stays quiet when a method word is present", () => {
    assert.ok(!ids("Cities are growing faster than anyone planned for. Using census records we trace how housing costs keep climbing while wages lag behind in every neighbourhood and region studied.").includes("no-method-words"));
  });
  it("stays quiet for essays and reviews", () => {
    assert.ok(!ids("In this essay we reflect on how cities are growing faster than anyone planned for, and why housing costs keep climbing while wages lag behind in nearly every region.").includes("no-method-words"));
  });
});

describe("output rules", () => {
  it("never returns more than 3 chips, even if more rules fire", () => {
    const t = "This cross-sectional survey of n = 10 students found that social media causes stress and was significantly worse for heavy users; it is the first study to do so, using a novel approach.";
    const chips = analyzeClaims(t);
    assert.ok(chips.length <= MAX_CHIPS);
    assert.equal(chips.length, 3);
  });
  it("honours a smaller maxChips", () => {
    const t = "This cross-sectional survey of n = 10 students found that social media causes stress and was significantly worse for heavy users; it is the first study to do so, using a novel approach.";
    assert.equal(analyzeClaims(t, { maxChips: 1 }).length, 1);
    assert.equal(analyzeClaims(t, { maxChips: 0 }).length, 0);
  });
  it("every tooltip says it is judged from the snippet only", () => {
    const t = "This cross-sectional survey of n = 10 students found that social media causes stress and was significantly worse for heavy users; it is the first study to do so, using a novel approach.";
    for (const c of analyzeClaims(t, { maxChips: 3 })) assert.ok(c.tip.includes(SNIPPET_NOTE), c.id);
    for (const c of [...analyzeClaims("Cities are growing faster than anyone planned for. Housing costs keep climbing while wages lag behind, and the gap between neighbourhoods is widening every single year as families move out of the centre and into the outer suburbs.")]) {
      assert.ok(c.tip.includes(SNIPPET_NOTE));
    }
    assert.match(SNIPPET_NOTE, /snippet only/);
  });
  it("never claims a paper lacks something", () => {
    const t = "This cross-sectional survey of n = 10 students found that social media causes stress and was significantly worse for heavy users; it is the first study to do so, using a novel approach.";
    for (const c of analyzeClaims(t)) {
      assert.doesNotMatch(`${c.label} ${c.tip}`, /\b(lacks?|lacking|missing|fails? to|does not (?:have|report|include))\b/i);
    }
  });
  it("handles empty, null, short and malformed input", () => {
    for (const v of [undefined, null, "", "   ", 42, {}, "short"]) assert.deepEqual(analyzeClaims(v), []);
    assert.doesNotThrow(() => analyzeClaims("\u0000\uD800 weird \n\n\t text ".repeat(50)));
  });
  it("is stable across repeated calls (global regex state)", () => {
    const t = "In this cross-sectional study of 412 adults, screen time causes poorer sleep quality and reduces daytime alertness across all age groups.";
    assert.deepEqual(analyzeClaims(t), analyzeClaims(t));
  });
});

describe("realistic snippets that should stay unflagged", () => {
  const clean = [
    "We conducted a randomized controlled trial with 1,200 participants. Mindfulness training reduced perceived stress (95% CI -3.1 to -1.2, p < 0.001) relative to a waitlist control group.",
    "Using a transformer encoder and a contrastive loss, we train on 4M image-text pairs and evaluate on ImageNet, reaching 81.2% top-1 accuracy with fewer parameters than prior work.",
    "This systematic review of 54 studies examines the association between sleep duration and cardiovascular outcomes; pooled risk ratios are reported with random-effects models.",
    "Drawing on 32 interviews with school leaders, we show how accountability reforms are interpreted locally and how leaders negotiate competing demands from staff and parents.",
    "Dark matter halos are modelled with N-body simulations of 1024^3 particles. We measure the concentration-mass relation across redshifts and compare with analytic predictions.",
    "A panel of 14,000 firms over 20 years is used to estimate the effect of R&D tax credits on patenting. Instrumental variable estimates show an elasticity of 0.4.",
  ];
  for (const t of clean) {
    it(`no chips: ${t.slice(0, 50)}...`, () => assert.deepEqual(analyzeClaims(t), []));
  }
});
