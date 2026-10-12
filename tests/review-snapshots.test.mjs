import { describe, it } from "node:test";
import assert from "node:assert/strict";

const S = await import("../src/common/review-snapshots.js");
const {
  takeSnapshot, addSnapshot, deleteSnapshot, restoreSnapshot, diffDecisions, appendLog, logToCsv,
  summarizeCounts, approxBytes, MAX_SNAPSHOTS, MAX_LOG_ENTRIES, OVERRIDE_REVIEWER,
} = S;

function project() {
  return {
    id: "r1", name: "Review",
    papers: { p1: { title: "One" }, p2: { title: "Two" } },
    decisions: { p1: { status: "include", votes: [{ reviewerId: "a", status: "include" }, { reviewerId: "b", status: "exclude" }], overrideStatus: "include" } },
    extraction: { p1: { sample: "10" } },
    quality: { p1: { risk: "low" } },
    tags: { p1: ["x"] },
    notes: { p1: "hello" },
    searchLog: [{ id: "keep" }],
    snapshots: [{ id: "old" }],
  };
}
const t0 = new Date("2026-10-12T09:00:00Z");

describe("takeSnapshot", () => {
  it("deep-copies the review work, names it, and leaves out papers, search log and other snapshots", () => {
    const p = project();
    const s = takeSnapshot(p, "  Before pilot  ", { now: t0 });
    assert.equal(s.name, "Before pilot");
    assert.equal(s.createdAt, "2026-10-12T09:00:00.000Z");
    assert.equal(s.kind, "manual");
    assert.deepEqual(s.counts, { papers: 2, decisions: 1, votes: 2, extracted: 1 });
    assert.equal(s.data.papers, undefined);
    assert.equal(s.data.searchLog, undefined);
    assert.equal(s.data.snapshots, undefined);
    p.decisions.p1.votes[0].status = "exclude";
    assert.equal(s.data.decisions.p1.votes[0].status, "include", "later edits do not leak into the snapshot");
  });
  it("gives a default name and handles bad input", () => {
    assert.match(takeSnapshot(project(), "", { now: t0 }).name, /^Snapshot 2026-10-12 09:00$/);
    assert.match(takeSnapshot(project(), "", { now: t0, kind: "auto" }).name, /^Automatic/);
    assert.equal(takeSnapshot(null, "x"), null);
    assert.equal(takeSnapshot(project(), "x".repeat(200)).name.length, 80);
    assert.deepEqual(summarizeCounts({}), { papers: 0, decisions: 0, votes: 0, extracted: 0 });
  });
});

describe("addSnapshot cap", () => {
  it("keeps the newest MAX_SNAPSHOTS and drops the oldest", () => {
    let list = [];
    for (let i = 0; i < MAX_SNAPSHOTS + 4; i += 1) list = addSnapshot(list, { id: `s${i}` });
    assert.equal(list.length, MAX_SNAPSHOTS);
    assert.equal(list[0].id, "s4");
    assert.equal(list.at(-1).id, `s${MAX_SNAPSHOTS + 3}`);
  });
  it("ignores invalid snapshots and deletes by id", () => {
    assert.deepEqual(addSnapshot([{ id: "a" }], null), [{ id: "a" }]);
    assert.deepEqual(deleteSnapshot([{ id: "a" }, { id: "b" }], "a"), [{ id: "b" }]);
    assert.deepEqual(deleteSnapshot(undefined, "a"), []);
  });
  it("estimates size", () => { assert.ok(approxBytes({ a: "x" }) > 0); });
});

describe("restoreSnapshot", () => {
  it("brings back snapshot content, keeps papers and search log, and reports decision changes", () => {
    const p = project();
    const snap = takeSnapshot(p, "base", { now: t0 });
    p.decisions.p1.votes[1].status = "include";
    p.decisions.p1.overrideStatus = "";
    p.decisions.p2 = { votes: [{ reviewerId: "a", status: "maybe" }] };
    p.extraction.p1.sample = "99";
    p.notes.p2 = "later";
    const changes = restoreSnapshot(p, snap);
    assert.equal(p.decisions.p1.votes[1].status, "exclude");
    assert.equal(p.decisions.p2, undefined);
    assert.equal(p.extraction.p1.sample, "10");
    assert.equal(p.notes.p2, undefined);
    assert.equal(p.searchLog[0].id, "keep");
    assert.equal(Object.keys(p.papers).length, 2);
    const key = (c) => `${c.paperId}/${c.reviewerId}:${c.old}>${c.new}`;
    assert.deepEqual(changes.map(key).sort(), [
      `p1/${OVERRIDE_REVIEWER}:none>include`, "p1/b:include>exclude", "p2/a:maybe>unscreened"
    ].sort());
  });
  it("restores a snapshot that lacks keys by emptying them, and is a deep copy", () => {
    const p = project();
    const snap = { id: "s", data: { notes: { p1: "n" } } };
    restoreSnapshot(p, snap);
    assert.deepEqual(p.decisions, {});
    assert.deepEqual(p.notes, { p1: "n" });
    snap.data.notes.p1 = "changed";
    assert.equal(p.notes.p1, "n");
  });
  it("refuses an unusable snapshot without touching the project", () => {
    const p = project();
    assert.equal(restoreSnapshot(p, null), null);
    assert.equal(restoreSnapshot(p, { id: "x" }), null);
    assert.equal(p.decisions.p1.status, "include");
  });
});

describe("diffDecisions", () => {
  it("is empty for identical or empty input and tolerates junk", () => {
    assert.deepEqual(diffDecisions({}, {}), []);
    assert.deepEqual(diffDecisions(null, undefined), []);
    const d = project().decisions;
    assert.deepEqual(diffDecisions(d, JSON.parse(JSON.stringify(d))), []);
  });
});

describe("change log", () => {
  it("appends, skips no-ops, is append-only and drops the oldest at the cap", () => {
    const a = appendLog([], [{ paperId: "p1", reviewerId: "a", old: "unscreened", new: "include" }, { paperId: "p1", reviewerId: "a", old: "x", new: "x" }, null, { reviewerId: "a", old: "1", new: "2" }], t0);
    assert.equal(a.length, 1);
    assert.equal(a[0].at, "2026-10-12T09:00:00.000Z");
    const b = appendLog(a, [{ paperId: "p2", reviewerId: "b", old: "include", new: "exclude", source: "restore" }], t0);
    assert.equal(b.length, 2);
    assert.equal(b[0], a[0]);
    assert.equal(a.length, 1, "input array not mutated");
    assert.equal(appendLog(b, [], t0), b);
    let log = [];
    const batch = Array.from({ length: 1000 }, (_, i) => ({ paperId: `p${i}`, reviewerId: "a", old: "unscreened", new: "include" }));
    for (let i = 0; i < 6; i += 1) log = appendLog(log, batch, t0);
    assert.equal(log.length, MAX_LOG_ENTRIES);
    assert.equal(new Set(log.map((e) => e.id)).size, MAX_LOG_ENTRIES, "ids stay unique");
  });
  it("exports CSV with lookups and quoting", () => {
    const log = appendLog([], [{ paperId: "p1", reviewerId: "a", old: "unscreened", new: "include" }], t0);
    const csv = logToCsv(log, () => 'T, "x"', () => "Ana");
    assert.match(csv.split("\n")[0], /^When,Paper id,Title,Reviewer,Old,New,Source$/);
    assert.match(csv, /"T, ""x"""/);
    assert.match(csv, /,Ana,unscreened,include,screening$/);
    assert.equal(logToCsv(undefined).split("\n").length, 1);
  });
});
