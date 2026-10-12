import { describe, it } from "node:test";
import assert from "node:assert/strict";

const {
  reviewerStatus, computeAgreement, kappaLabel, appendResolution, latestResolution,
  methodsParagraph, resolutionsToCsv, MAX_RESOLUTIONS,
} = await import("../src/common/screening-agreement.js");

// Builds a project from a list of [statusA, statusB] pairs; null means no vote.
function project(pairs) {
  const papers = {};
  const decisions = {};
  pairs.forEach(([a, b], i) => {
    const id = `p${i}`;
    papers[id] = { id, title: `Paper ${i}` };
    const votes = [];
    if (a) votes.push({ reviewerId: "A", status: a });
    if (b) votes.push({ reviewerId: "B", status: b });
    decisions[id] = { votes };
  });
  return { papers, decisions };
}
function repeat(a, b, n) { return Array.from({ length: n }, () => [a, b]); }

describe("reviewerStatus", () => {
  it("returns unscreened for missing, unscreened or unknown votes", () => {
    assert.equal(reviewerStatus(undefined, "A"), "unscreened");
    assert.equal(reviewerStatus({ votes: [{ reviewerId: "A", status: "unscreened" }] }, "A"), "unscreened");
    assert.equal(reviewerStatus({ votes: [{ reviewerId: "A", status: "banana" }] }, "A"), "unscreened");
    assert.equal(reviewerStatus({ votes: "nope" }, "A"), "unscreened");
    assert.equal(reviewerStatus({ votes: [{ reviewerId: "A", status: "maybe" }] }, "A"), "maybe");
  });
});

describe("computeAgreement", () => {
  it("matches the hand-computed 2x2 example (po 0.7, pe 0.5, kappa 0.4)", () => {
    const p = project([...repeat("include", "include", 20), ...repeat("include", "exclude", 5), ...repeat("exclude", "include", 10), ...repeat("exclude", "exclude", 15)]);
    const r = computeAgreement(p, "A", "B");
    assert.equal(r.compared, 50);
    assert.equal(r.agreed, 35);
    assert.equal(r.percent, 70);
    assert.ok(Math.abs(r.po - 0.7) < 1e-12);
    assert.ok(Math.abs(r.pe - 0.5) < 1e-12);
    assert.ok(Math.abs(r.kappa - 0.4) < 1e-12);
    assert.equal(r.kappaLabel, "fair");
    assert.equal(r.conflicts.length, 15);
  });

  const threeByThree = [
    ...repeat("include", "include", 10), ...repeat("include", "exclude", 2), ...repeat("include", "maybe", 3),
    ...repeat("exclude", "include", 1), ...repeat("exclude", "exclude", 20), ...repeat("exclude", "maybe", 4),
    ...repeat("maybe", "include", 2), ...repeat("maybe", "exclude", 3), ...repeat("maybe", "maybe", 5),
  ];
  it("matches the hand-computed 3x3 example with maybe separate (po 0.7, pe 0.376, kappa 0.5192)", () => {
    const r = computeAgreement(project(threeByThree), "A", "B");
    assert.equal(r.compared, 50);
    assert.ok(Math.abs(r.pe - 0.376) < 1e-12);
    assert.ok(Math.abs(r.kappa - 0.324 / 0.624) < 1e-12);
    assert.equal(r.kappaLabel, "moderate");
    assert.equal(r.matrix.include.maybe, 3);
  });

  it("collapses maybe into include: same data gives po 0.8, kappa 0.6", () => {
    const r = computeAgreement(project(threeByThree), "A", "B", { maybeMode: "include" });
    assert.equal(r.agreed, 40);
    assert.ok(Math.abs(r.po - 0.8) < 1e-12);
    assert.ok(Math.abs(r.kappa - 0.6) < 1e-12);
    assert.deepEqual(r.categories, ["include", "exclude"]);
  });

  it("collapses maybe into exclude", () => {
    // include/maybe agrees only if maybe counts as include; under exclude it is a conflict
    const p = project([["include", "maybe"], ["exclude", "maybe"]]);
    assert.equal(computeAgreement(p, "A", "B", { maybeMode: "exclude" }).agreed, 1);
    assert.equal(computeAgreement(p, "A", "B", { maybeMode: "include" }).agreed, 1);
    assert.equal(computeAgreement(p, "A", "B", { maybeMode: "separate" }).agreed, 0);
  });

  it("degenerate: both reviewers pick one category for everything -> 100% agreement, kappa undefined", () => {
    const r = computeAgreement(project(repeat("include", "include", 12)), "A", "B");
    assert.equal(r.percent, 100);
    assert.equal(r.kappa, null);
    assert.match(r.kappaNote, /undefined/);
  });

  it("degenerate: opposite single categories -> 0% agreement, kappa 0", () => {
    const r = computeAgreement(project(repeat("include", "exclude", 10)), "A", "B");
    assert.equal(r.percent, 0);
    assert.equal(r.kappa, 0);
  });

  it("degenerate: no overlap -> nothing computed, unscreened papers counted separately", () => {
    const p = project([["include", null], [null, "exclude"], [null, null], ["include", "unscreened"]]);
    const r = computeAgreement(p, "A", "B");
    assert.equal(r.compared, 0);
    assert.equal(r.percent, null);
    assert.equal(r.kappa, null);
    assert.equal(r.onlyA, 2);
    assert.equal(r.onlyB, 1);
    assert.equal(r.neither, 1);
  });

  it("leaves papers screened by only one reviewer out of the statistics", () => {
    const p = project([...repeat("include", "include", 3), ["exclude", null], [null, "include"]]);
    const r = computeAgreement(p, "A", "B");
    assert.equal(r.compared, 3);
    assert.equal(r.percent, 100);
    assert.equal(r.onlyA, 1);
    assert.equal(r.onlyB, 1);
  });

  it("handles same reviewer twice, empty and malformed projects", () => {
    assert.equal(computeAgreement(project([["include", "include"]]), "A", "A").kappa, null);
    assert.equal(computeAgreement({}, "A", "B").compared, 0);
    assert.equal(computeAgreement(null, "A", "B").compared, 0);
    assert.equal(computeAgreement({ papers: { x: {} }, decisions: { x: null } }, "A", "B").neither, 1);
  });
});

describe("kappaLabel", () => {
  it("uses the Landis and Koch bands", () => {
    assert.equal(kappaLabel(-0.1), "poor");
    assert.equal(kappaLabel(0.1), "slight");
    assert.equal(kappaLabel(0.5), "moderate");
    assert.equal(kappaLabel(0.7), "substantial");
    assert.equal(kappaLabel(0.9), "almost perfect");
    assert.equal(kappaLabel(null), "");
  });
});

describe("resolution log", () => {
  const t = new Date("2026-10-12T10:00:00Z");
  it("appends without editing earlier entries and finds the latest", () => {
    const a = appendResolution([], { paperId: "p1", resolvedBy: "Dana", decision: "include", note: " ok " }, t);
    const b = appendResolution(a, { paperId: "p1", resolvedBy: "Dana", decision: "exclude" }, new Date("2026-10-13T10:00:00Z"));
    assert.equal(a.length, 1);
    assert.equal(b.length, 2);
    assert.equal(b[0].note, "ok");
    assert.equal(latestResolution(b, "p1").decision, "exclude");
    assert.equal(latestResolution(b, "zz"), null);
  });
  it("rejects invalid entries", () => {
    assert.equal(appendResolution([], { paperId: "p1", resolvedBy: "", decision: "include" }), null);
    assert.equal(appendResolution([], { paperId: "p1", resolvedBy: "x", decision: "unscreened" }), null);
    assert.equal(appendResolution([], { paperId: "", resolvedBy: "x", decision: "include" }), null);
  });
  it("drops the oldest beyond the cap", () => {
    let log = [];
    for (let i = 0; i < MAX_RESOLUTIONS + 3; i += 1) log = appendResolution(log, { paperId: `p${i}`, resolvedBy: "x", decision: "include" }, t);
    assert.equal(log.length, MAX_RESOLUTIONS);
    assert.equal(log[0].paperId, "p3");
  });
  it("exports CSV with quoting", () => {
    const log = appendResolution([], { paperId: "p1", resolvedBy: "Dana", decision: "include", note: 'said "yes", twice' }, t);
    const csv = resolutionsToCsv(log, () => "A, B");
    assert.match(csv, /"A, B"/);
    assert.match(csv, /"said ""yes"", twice"/);
  });
});

describe("methodsParagraph", () => {
  it("states the numbers and how maybe was treated", () => {
    const p = project([...repeat("include", "include", 20), ...repeat("include", "exclude", 5), ...repeat("exclude", "include", 10), ...repeat("exclude", "exclude", 15), ["include", null]]);
    const r = computeAgreement(p, "A", "B");
    const text = methodsParagraph(r, { a: "Dana", b: "Lee" }, { resolvedCount: 0, blind: true });
    assert.match(text, /Dana and Lee/);
    assert.match(text, /50 of 51/);
    assert.match(text, /35 of 50 records \(70\.0%\)/);
    assert.match(text, /kappa was 0\.40/);
    assert.match(text, /fair/);
    assert.match(text, /own category/);
    assert.match(text, /blind screening/);
    assert.match(text, /none has a recorded resolution/);
  });
  it("says so when kappa is undefined or nothing overlaps", () => {
    const r = computeAgreement(project(repeat("include", "include", 4)), "A", "B");
    assert.match(methodsParagraph(r), /could not be calculated/);
    assert.match(methodsParagraph(computeAgreement(project([["include", null]]), "A", "B")), /cannot be reported/);
  });
});
