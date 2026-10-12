import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { createReviewWorkspace } = await import("../src/content/review-workspace.js");
const store = {};
const listeners = {};
const content = { innerHTML: "", scrollTop: 0, querySelector: () => null };
const nodes = { "#su-review-content": content };
const overlay = {
  innerHTML: "", classList: { add() {}, remove() {}, toggle() {} },
  addEventListener(t, f) { listeners[t] = f; },
  querySelector(sel) { return nodes[sel] || { value: "", checked: false, innerHTML: "", files: [] }; },
  querySelectorAll() { return []; },
};
globalThis.document = { getElementById: () => null, createElement: () => overlay, body: { appendChild() {} }, execCommand() {} };
globalThis.window = globalThis; globalThis.chrome = { storage: { local: { get() {}, set() {} } }, runtime: { sendMessage() {} } };
window.confirm = () => true;
document.execCommand = () => true;
const downloads = [];
let pro = true;
const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
const ws = createReviewWorkspace({
  escapeHtml: esc, csvEscape: (v) => `"${String(v).replace(/"/g, '""')}"`, extractDOIFromResult() {}, extractPaperFromResult() {},
  fetchOpenAlexWorkById() {}, fetchOpenAlexWorkForPaper() {}, getBestPdfUrl() {}, getCachedAuthorCitationCount() {}, getCachedAuthorPaper() {},
  getCitationCountFromResult() {}, getScholarSearchQuery: () => "q", getSnippetText() {}, hashString: String, normalizeDoi: (x) => x, normalizeTitleForMatch: (x) => x,
  scanResults() {}, downloadBlob: (n, m, t) => downloads.push([n, t]), getStorageMap: async () => ({
    r1: { id: "r1", name: "Rev", papers: { p1: { id: "p1", title: "One <b>" , year: "2020" }, p2: { id: "p2", title: "Two", year: "2021" } },
      decisions: { p1: { votes: [{ reviewerId: "you", status: "include" }, { reviewerId: "r2", status: "exclude" }] }, p2: { votes: [{ reviewerId: "you", status: "include" }, { reviewerId: "r2", status: "include" }] } },
      reviewers: [{ id: "you", name: "You" }, { id: "r2", name: "Rae" }], tags: { p1: ["t1"] }, extraction: { p1: { design: "RCT" } } } }),
  setStorageMap: async (k, m) => { store[k] = JSON.parse(JSON.stringify(m)); },
  setStorageMapEntry: async (k, e, v) => { store[k] = { ...(store[k] || {}), [e]: JSON.parse(JSON.stringify(v)) }; }, CLS_BTN_SEC: "btn",
  getEntitlementStatus: async () => ({ purchased: pro }), canUseFeature: () => pro,
});
await ws.open();
const click = async (attrs, extra = {}) => {
  const el = { tagName: "BUTTON", dataset: attrs, value: extra.value, closest(sel) { if (sel === "[data-rv]") return this; if (sel === "[data-review-id]") return extra.row || null; return null; } };
  await listeners.click({ target: el });
};
const change = async (attrs, extra = {}) => {
  const el = { tagName: "SELECT", dataset: attrs, value: extra.value, closest(sel) { if (sel === "[data-rv]") return this; if (sel === "[data-review-id]") return extra.row || null; return null; } };
  await listeners.change({ target: el });
};
const tabClick = async (tab) => listeners.click({ target: { closest: (s) => (s === "[data-review-tab]" ? { dataset: { reviewTab: tab } } : null) } });
const row = { dataset: { reviewId: "p1" }, querySelector: (s) => ({ value: s.includes("by") ? "you" : s.includes("decision") ? "include" : "n" }) };

// A fake DOM drives the real event handlers. It checks wiring and rendering, not browser layout.
describe("review tools wiring (fake DOM)", () => {
  it("renders the four tabs unlocked, escaping paper titles", async () => {
    for (const t of ["rob", "agreement", "matrix", "snapshots"]) {
      await tabClick(t);
      assert.ok(content.innerHTML.length > 500, t);
      assert.ok(!/Unlock Pro/.test(content.innerHTML), t);
    }
    await tabClick("agreement");
    assert.ok(content.innerHTML.includes("One &lt;b>"));
    assert.ok(!content.innerHTML.includes("One <b>"));
  });
  it("saves judgements, exports, resolves a conflict, snapshots and restores", async () => {
    await tabClick("rob");
    await change({ rv: "rob-judge", robDomain: "rob2_randomization" }, { value: "low", row });
    await change({ rv: "rob-overall" }, { value: "some", row });
    await click({ rv: "rob-export-csv" });
    await click({ rv: "rob-export-md" });
    assert.equal(store.reviewProjects.r1.rob.papers.p1.reviewers.you.rob2_randomization, "low");
    assert.equal(store.reviewProjects.r1.rob.papers.p1.overall.judgement, "some");
    assert.deepEqual(downloads.map((d) => d[0]), ["rev-risk-of-bias.csv", "rev-risk-of-bias.md"]);
    await tabClick("agreement");
    assert.match(content.innerHTML, /<strong>50\.0%<\/strong> agreement/);
    await click({ rv: "agr-resolve" }, { row });
    let p = store.reviewProjects.r1;
    assert.equal(p.agreementResolutions.length, 1);
    assert.equal(p.decisions.p1.overrideStatus, "include");
    assert.equal(p.changeLog.length, 1);
    await tabClick("snapshots");
    await click({ rv: "snap-save" });
    p = store.reviewProjects.r1;
    assert.equal(p.snapshots.length, 1);
    await change({ rv: "rob-judge", robDomain: "rob2_randomization" }, { value: "high", row });
    await click({ rv: "snap-restore", snapId: p.snapshots[0].id });
    p = store.reviewProjects.r1;
    assert.equal(p.snapshots.length, 2, "an automatic snapshot is taken before restoring");
    assert.equal(p.snapshots[1].kind, "auto");
    assert.equal(p.rob.papers.p1.reviewers.you.rob2_randomization, "low");
  });
  it("shows the locked card and refuses writes once the entitlement is gone", async () => {
    pro = false;
    await tabClick("matrix");
    assert.match(content.innerHTML, /Unlock Pro/);
    await change({ rv: "rob-judge", robDomain: "rob2_randomization" }, { value: "high", row });
    assert.equal(store.reviewProjects.r1.rob.papers.p1.reviewers.you.rob2_randomization, "low");
    pro = true;
  });
});

describe("saving from this tab", () => {
  it("does not erase a project another tab saved after this one opened", async () => {
    store.reviewProjects.r9 = { id: "r9", name: "Other tab", papers: {} };
    await tabClick("rob");
    await change({ rv: "rob-judge", robDomain: "rob2_randomization" }, { value: "high", row });
    assert.ok(store.reviewProjects.r9, "the other tab's project is still there");
    assert.equal(store.reviewProjects.r1.rob.papers.p1.reviewers.you.rob2_randomization, "high");
  });
});
