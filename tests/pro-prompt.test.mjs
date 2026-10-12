import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { PRO_FEATURES } = await import("../src/common/entitlement.js");
const { PRO_FEATURE_LABELS, proPromptCopy, showProPrompt, TRIAL_LABEL } = await import("../src/common/pro-prompt.js");

// A small stand-in for the DOM: just what showProPrompt touches.
class FakeEl {
  constructor(tag) {
    this.tagName = tag; this.children = []; this.parent = null; this.attrs = {}; this.listeners = {};
    this.className = ""; this.id = ""; this.hidden = false; this.disabled = false; this.textContent = ""; this.type = "";
  }
  setAttribute(k, v) { this.attrs[k] = v; }
  append(...kids) { kids.forEach((k) => this.appendChild(k)); }
  appendChild(k) { k.parent = this; this.children.push(k); return k; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  removeEventListener(type, fn) { this.listeners[type] = (this.listeners[type] || []).filter((f) => f !== fn); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); this.parent = null; }
  focus() { this.focused = true; }
  async click() { for (const fn of this.listeners.click || []) await fn({ target: this }); }
  find(cls) {
    if ((this.className || "").split(" ").includes(cls)) return this;
    for (const c of this.children) { const f = c.find(cls); if (f) return f; }
    return null;
  }
}
function makeDoc() {
  const body = new FakeEl("body");
  const doc = new FakeEl("doc");
  doc.body = body; doc.activeElement = null; doc.location = { protocol: "https:" };
  doc.createElement = (t) => new FakeEl(t);
  doc.querySelectorAll = (sel) => body.children.filter((c) => sel === ".su-pro-prompt-backdrop" && c.className === "su-pro-prompt-backdrop");
  doc.getElementById = () => null;
  return doc;
}

let sent;
let nextResponse;
beforeEach(() => {
  sent = [];
  nextResponse = {};
  globalThis.chrome = { runtime: { lastError: undefined, sendMessage: (msg, cb) => { sent.push(msg); cb && cb(nextResponse); } } };
});

describe("proPromptCopy", () => {
  it("names the feature and says how it is unlocked", () => {
    const c = proPromptCopy({ featureId: "referenceImport" });
    assert.equal(c.title, "This is a Pro feature");
    assert.match(c.body, /^Reference import is part of Scholar Utility Belt Pro\./);
    assert.match(c.body, /free trial, a purchase or an App Pass/);
    assert.match(c.note, /payment page/);
    assert.equal(c.showTrial, true);
  });
  it("prefers an explicit feature name, falls back for unknown ids, and handles a used trial", () => {
    assert.match(proPromptCopy({ feature: "Compare authors" }).body, /^Compare authors is part of/);
    assert.match(proPromptCopy({ featureId: "nope" }).body, /^This feature is part of/);
    const used = proPromptCopy({ trialUsed: true });
    assert.equal(used.showTrial, false);
    assert.match(used.note, /already used your free trial/);
  });
  it("has a label for every Pro feature id", () => {
    for (const id of Object.keys(PRO_FEATURES)) assert.ok(PRO_FEATURE_LABELS[id], `no label for ${id}`);
  });
});

describe("showProPrompt", () => {
  it("returns null with no document", () => {
    assert.equal(showProPrompt({}, null), null);
  });
  it("draws an accessible dialog with three buttons and sends nothing until a click", () => {
    const doc = makeDoc();
    showProPrompt({ featureId: "latexPack" }, doc);
    const dialog = doc.body.find("su-pro-prompt");
    assert.equal(dialog.attrs.role, "dialog");
    assert.equal(dialog.attrs["aria-modal"], "true");
    assert.equal(dialog.find("su-pro-prompt-title").textContent, "This is a Pro feature");
    assert.match(dialog.find("su-pro-prompt-body").textContent, /LaTeX citation pack/);
    const labels = dialog.find("su-pro-prompt-actions").children.map((b) => b.textContent);
    assert.deepEqual(labels, ["Unlock Pro", TRIAL_LABEL, "Not now"]);
    assert.equal(sent.length, 0);
  });
  it("Unlock Pro opens the payment page and closes", async () => {
    const doc = makeDoc();
    showProPrompt({}, doc);
    await doc.body.find("su-pro-prompt-primary").click();
    assert.deepEqual(sent, [{ action: "openUpsellModal" }]);
    assert.equal(doc.body.children.length, 0);
  });
  it("Not now, Escape and a backdrop click close it without sending anything", async () => {
    for (const how of ["later", "escape", "backdrop"]) {
      const doc = makeDoc();
      showProPrompt({}, doc);
      const backdrop = doc.body.children[0];
      if (how === "later") await backdrop.find("su-pro-prompt-actions").children[2].click();
      if (how === "escape") for (const fn of doc.listeners.keydown) fn({ key: "Escape", stopPropagation() {} });
      if (how === "backdrop") for (const fn of backdrop.listeners.click) fn({ target: backdrop });
      assert.equal(doc.body.children.length, 0, how);
      assert.equal(sent.length, 0, how);
    }
  });
  it("a click inside the dialog does not dismiss it", () => {
    const doc = makeDoc();
    showProPrompt({}, doc);
    const backdrop = doc.body.children[0];
    for (const fn of backdrop.listeners.click) fn({ target: backdrop.find("su-pro-prompt") });
    assert.equal(doc.body.children.length, 1);
  });
  it("only one prompt exists at a time", () => {
    const doc = makeDoc();
    showProPrompt({}, doc);
    showProPrompt({}, doc);
    assert.equal(doc.body.children.length, 1);
  });
  it("the trial button starts the trial; an already-used trial hides it and keeps the dialog open", async () => {
    const doc = makeDoc();
    showProPrompt({}, doc);
    const trial = doc.body.find("su-pro-prompt-actions").children[1];
    nextResponse = { ok: false, reason: "already_used" };
    await trial.click();
    assert.deepEqual(sent, [{ action: "startTrial" }]);
    assert.equal(trial.hidden, true);
    assert.match(doc.body.find("su-pro-prompt-note").textContent, /already used your free trial/);
    assert.equal(doc.body.children.length, 1);
  });
  it("a started trial closes the dialog, and a failed one says so", async () => {
    let doc = makeDoc();
    showProPrompt({}, doc);
    nextResponse = { ok: true };
    await doc.body.find("su-pro-prompt-actions").children[1].click();
    assert.equal(doc.body.children.length, 0);
    doc = makeDoc();
    showProPrompt({}, doc);
    nextResponse = { ok: false, reason: "error" };
    await doc.body.find("su-pro-prompt-actions").children[1].click();
    assert.match(doc.body.find("su-pro-prompt-note").textContent, /Couldn't open the trial page/);
  });
});

describe("stylesheets", () => {
  it("keeps the Scholar-page copy of the dialog rules in content.css in step with pro-prompt.css", () => {
    const standalone = readFileSync(new URL("../src/common/pro-prompt.css", import.meta.url), "utf8");
    const content = readFileSync(new URL("../src/content/content.css", import.meta.url), "utf8");
    const rules = standalone.split("@media (prefers-color-scheme: dark)")[0].split("\n").map((l) => l.trim()).filter((l) => l && !l.startsWith("/*"));
    for (const line of rules) assert.ok(content.includes(line), `content.css is missing: ${line}`);
  });
});
