// The top-2% panel's display paths: confident match, pick list, "None of these", stored picks, load failure, and copy.
// A small fake DOM stands in for the page (no DOM library in this repo).
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { build } from "../scripts/build_top2pct.mjs";
import { runTop2pct, COPY, PICKS_MAX, validPick } from "../src/content/top2pct.js";

class El {
  constructor(tag, doc) {
    this.tagName = tag.toUpperCase(); this.doc = doc; this.children = []; this.parent = null; this.attrs = {};
    this._text = ""; this.hidden = false; this.listeners = {}; this.id = ""; this.className = "";
  }
  get parentNode() { return this.parent; }
  get classList() { return { add: (c) => { this.className = (this.className + " " + c).trim(); } }; }
  get isConnected() { let n = this; while (n.parent) n = n.parent; return n === this.doc.body; }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text + this.children.map((c) => c.textContent).join(""); }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  remove() { if (this.parent) { this.parent.children = this.parent.children.filter((x) => x !== this); this.parent = null; } }
  replaceWith(n) { const p = this.parent; const i = p.children.indexOf(this); p.children[i] = n; n.parent = p; this.parent = null; }
  insertAdjacentElement(where, n) {
    assert.equal(where, "afterend");
    const p = this.parent; p.children.splice(p.children.indexOf(this) + 1, 0, n); n.parent = p; return n;
  }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  dispatch(type) { const e = { type, preventDefault() {} }; for (const fn of this.listeners[type] || []) fn(e); }
  click() { this.dispatch("click"); }
  focus() { this.doc.activeElement = this; }
  *walk() { for (const c of this.children) { yield c; yield* c.walk(); } }
  all(pred) { return [...this.walk()].filter(pred); }
  querySelector(sel) {
    if (sel.startsWith(".")) return this.all((n) => n.className.split(" ").includes(sel.slice(1)))[0] || null;
    const m = sel.match(/^input\[name='([^']+)'\]:checked$/);
    if (m) return this.all((n) => n.tagName === "INPUT" && n.name === m[1] && n.checked)[0] || null;
    throw new Error(`fake DOM: unsupported selector ${sel}`);
  }
}
function makeDoc() {
  const doc = { activeElement: null };
  doc.createElement = (t) => new El(t, doc);
  doc.body = new El("body", doc);
  doc.getElementById = (id) => doc.body.all((n) => n.id === id)[0] || null;
  const row = doc.body.appendChild(doc.createElement("div"));
  const nameEl = row.appendChild(doc.createElement("div"));
  nameEl.id = "gsc_prf_in";
  const anchor = doc.body.appendChild(doc.createElement("div"));
  return { doc, anchor, nameEl, row };
}
const memoryStorage = (init = {}) => {
  const s = { map: { ...init }, writes: 0 };
  s.get = async () => s.map;
  s.set = async (m) => { s.writes++; s.map = JSON.parse(JSON.stringify(m)); };
  return s;
};

const HEADER = ["authfull", "inst_name", "cntry", "firstyr", "lastyr", "self%", "cprat", "np6025_rw", "sm-subfield-1", "rank sm-subfield-1 (ns)", "sm-subfield-1 count"];
const ROWS = [
  ["Kresse, Georg", "Universität Wien", "aut", 1990, 2025, 0.0349, 1.234, 0, "Chemical Physics", 3, 50000],
  ["Smith, John", "University of Oxford", "gbr", 1980, 2025, 0.145, 1.1, 2, "Oncology", 10, 90000],
  ["Smith, John", "Harvard University", "usa", 1985, 2025, 0.2, 1.05, 0, "Oncology", 20, 90000],
];
const csv = (rows) => [HEADER, ...rows].map((r) => r.map((x) => (/[",\n]/.test(String(x)) ? `"${String(x).replace(/"/g, '""')}"` : x)).join(",")).join("\n") + "\n";
let OBJ;
{
  const dir = mkdtempSync(path.join(tmpdir(), "top2pct-panel-"));
  try {
    writeFileSync(path.join(dir, "career.csv"), csv(ROWS));
    writeFileSync(path.join(dir, "singleyr.csv"), csv(ROWS.slice(0, 1)));
    OBJ = build({ career: path.join(dir, "career.csv"), singleyr: path.join(dir, "singleyr.csv") }).obj;
  } finally { rmSync(dir, { recursive: true, force: true }); }
}
const load = async () => JSON.parse(JSON.stringify(OBJ));
const KRESSE = { userId: "K1", name: "Georg Kresse", affiliation: "University of Vienna, Universität Wien" };
const SMITH = { userId: "S1", name: "John Smith", affiliation: "University of Oxford; Harvard University" };

async function run(profile, opts = {}) {
  const page = opts.page || makeDoc();
  const { doc, anchor } = page;
  const storage = opts.storage || memoryStorage();
  const nameEl = opts.noBadge ? null : page.nameEl;
  const state = await runTop2pct({ doc, anchor, nameEl, load: opts.load || load, storage, now: () => 1000, ...profile });
  return { state, doc, anchor, storage, page, panel: doc.getElementById("su-top2pct"), badge: doc.getElementById("su-top2pct-badge") };
}
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("top2pct panel", () => {
  it("shows a foil badge after the name; the badge opens the collapsed panel with ranks and detail", async () => {
    const { state, panel, anchor, badge, page } = await run(KRESSE);
    assert.equal(state, "confident");
    assert.equal(anchor.parent.children[anchor.parent.children.indexOf(anchor) + 1], panel);
    // The badge sits right after the name element, never inside it, so the name's text stays clean.
    assert.equal(page.row.children[1], badge);
    assert.equal(page.nameEl.textContent, "");
    assert.match(page.row.className, /su-top2pct-namerow/);
    assert.equal(badge.textContent, COPY.badge);
    assert.equal(badge.getAttribute("aria-controls"), "su-top2pct");
    assert.equal(badge.getAttribute("aria-expanded"), "false");
    assert.equal(panel.hidden, true);
    badge.click();
    assert.equal(badge.getAttribute("aria-expanded"), "true");
    assert.equal(panel.hidden, false);
    const text = panel.textContent;
    assert.match(text, /Matched: Kresse, Georg · Universität Wien · AUT/);
    assert.match(text, /Career: rank 3 of 50,000 researchers in Chemical Physics \(self-citations excluded\)/);
    assert.match(text, /2025 alone: rank 3 of 50,000/);
    assert.doesNotMatch(text, /Retraction Watch/); // a zero count is never shown
    const detail = panel.querySelector(".su-top2pct-detail");
    assert.match(detail.textContent, /Career: 3% of citations to this person's papers are self-citations\./);
    assert.match(detail.textContent, /each citing paper cites this person 1\.2 times on average/);
    badge.click();
    assert.equal(panel.hidden, true);
    // A second run on the same page replaces the badge instead of adding another.
    await runTop2pct({ doc: page.doc, anchor, nameEl: page.nameEl, load, storage: memoryStorage(), now: () => 1, ...KRESSE });
    assert.equal(page.row.all((n) => n.id === "su-top2pct-badge").length, 1);
  });

  it("shows the panel open, with no badge, when the name element is missing", async () => {
    const r = await run(KRESSE, { noBadge: true });
    assert.equal(r.state, "confident");
    assert.equal(r.badge, null);
    assert.equal(r.panel.hidden, false);
  });

  it("offers a pick list with None of these first, and a choice is stored and shown with its retraction line", async () => {
    const r = await run(SMITH);
    assert.equal(r.state, "pick");
    assert.equal(r.badge.textContent, COPY.badgePick);
    assert.equal(r.panel.hidden, true);
    await tick();
    assert.equal(r.doc.activeElement, null); // page load never moves focus
    r.badge.click();
    assert.equal(r.panel.hidden, false);
    const radios = r.panel.all((n) => n.tagName === "INPUT");
    assert.equal(radios[0].value, "none");
    assert.equal(radios[0].checked, true);
    const oxford = radios.find((n) => n.parent.textContent.includes("Oxford"));
    radios[0].checked = false; oxford.checked = true;
    r.panel.querySelector(".su-top2pct-pick").dispatch("submit");
    await tick();
    const shown = r.doc.getElementById("su-top2pct");
    assert.match(shown.textContent, /You chose this person/);
    assert.equal(shown.hidden, false); // stays open after the choice
    assert.equal(r.doc.getElementById("su-top2pct-badge").textContent, COPY.badge);
    assert.match(shown.textContent, /2 papers by Smith, John \(University of Oxford\) are listed in Retraction Watch/);
    assert.equal(r.doc.activeElement.className, "su-top2pct-heading");
    const saved = r.storage.map.S1;
    assert.ok(validPick(saved));
    assert.equal(saved.inst, "University of Oxford");
    // Next visit: the stored pick wins.
    const again = await run(SMITH, { storage: r.storage });
    assert.equal(again.state, "picked");
    // A changed affiliation drops the pick and matches again.
    const moved = await run({ ...SMITH, affiliation: "University of Oxford; Harvard University; Imperial College" }, { storage: r.storage });
    assert.equal(moved.state, "pick");
    assert.equal(r.storage.map.S1, undefined);
  });

  it("None of these hides the panel and is remembered until the dataset version changes", async () => {
    const r = await run(SMITH);
    r.panel.querySelector(".su-top2pct-pick").dispatch("submit"); // None of these is checked by default
    await tick();
    assert.equal(r.doc.getElementById("su-top2pct"), null);
    assert.equal(r.doc.getElementById("su-top2pct-badge"), null);
    assert.deepEqual(r.storage.map.S1, { v: "9", none: true, t: 1000 });
    assert.equal((await run(SMITH, { storage: r.storage })).state, "none");
    r.storage.map.S1.v = "8";
    assert.equal((await run(SMITH, { storage: r.storage })).state, "pick");
  });

  it("Not this person on a single candidate stores none and hides the panel", async () => {
    const r = await run(KRESSE);
    r.panel.all((n) => n.textContent === COPY.notThisPerson)[0].click();
    await tick();
    assert.equal(r.doc.getElementById("su-top2pct"), null);
    assert.equal(r.storage.map.K1.none, true);
  });

  it("shows nothing for a non-Latin name, and does not load the data", async () => {
    let loaded = 0;
    const r = await run({ userId: "W1", name: "王伟", affiliation: "Peking University" }, { load: async () => { loaded++; return OBJ; } });
    assert.equal(r.state, "none");
    assert.equal(r.panel, null);
    assert.equal(loaded, 0);
  });

  it("retries a failed load once, then fails closed", async () => {
    let calls = 0;
    const flaky = async () => { calls++; if (calls === 1) throw new Error("net"); return load(); };
    assert.equal((await run(KRESSE, { load: flaky })).state, "confident");
    calls = 0;
    const r = await run(KRESSE, { load: async () => { calls++; throw new Error("net"); } });
    assert.equal(r.state, "none");
    assert.equal(r.panel, null);
    assert.equal(calls, 2);
    assert.equal((await run(KRESSE, { load: async () => ({ format: 99 }) })).state, "none");
  });

  it("ignores a malformed stored entry and survives a failing storage", async () => {
    const storage = memoryStorage({ K1: { v: 9, name: 5 } });
    assert.equal((await run(KRESSE, { storage })).state, "confident");
    const broken = { get: async () => { throw new Error("quota"); }, set: async () => { throw new Error("quota"); } };
    const r = await run(KRESSE, { storage: broken });
    assert.equal(r.state, "confident");
    r.panel.all((n) => n.textContent === COPY.notThisPerson)[0].click();
    await tick();
    assert.equal(r.doc.getElementById("su-top2pct"), null);
  });

  it("keeps at most PICKS_MAX stored entries, dropping the oldest", async () => {
    const init = {};
    for (let i = 0; i < PICKS_MAX; i++) init[`u${i}`] = { v: "9", none: true, t: i };
    const storage = memoryStorage(init);
    const r = await run(KRESSE, { storage });
    r.panel.all((n) => n.textContent === COPY.notThisPerson)[0].click();
    await tick();
    assert.equal(Object.keys(storage.map).length, PICKS_MAX);
    assert.ok(storage.map.K1);
    assert.equal(storage.map.u0, undefined);
  });
});

describe("top2pct panel copy", () => {
  const strings = [];
  const collect = (v) => {
    if (typeof v === "string") strings.push(v);
    else if (typeof v === "function") strings.push(String(v(1, 2, "Oncology")));
    else if (v && typeof v === "object") Object.values(v).forEach(collect);
  };
  collect(COPY);
  it("has no em dashes, emoji or exclamation marks", () => {
    for (const s of strings) {
      assert.doesNotMatch(s, /—/, s);
      assert.doesNotMatch(s, /\p{Extended_Pictographic}/u, s);
      assert.doesNotMatch(s, /!/, s);
    }
  });
  it("never claims a person is absent from the list or that a retraction is theirs", () => {
    for (const s of strings) assert.doesNotMatch(s, /not (on|in) the (top|list)|not listed\b/i, s);
    assert.match(COPY.retracted(2, "X"), /does not show who was responsible/);
    assert.match(COPY.credit("9"), /CC BY-NC 3\.0/);
    assert.match(COPY.credit("9"), /Rounded and reduced/);
  });
});
