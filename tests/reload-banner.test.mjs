import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { showReloadBanner, RELOAD_MESSAGE } = await import("../src/common/reload-banner.js");

class El {
  constructor(tag) { this.tag = tag; this.children = []; this.parent = null; this.listeners = {}; this.className = ""; this.textContent = ""; this.attrs = {}; }
  setAttribute(k, v) { this.attrs[k] = v; }
  append(...k) { k.forEach((c) => this.appendChild(c)); }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  addEventListener(t, f) { (this.listeners[t] ||= []).push(f); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); }
  click() { (this.listeners.click || []).forEach((f) => f()); }
}
const makeDoc = () => {
  const body = new El("body");
  return { body, createElement: (t) => new El(t), querySelector: (sel) => (sel === ".su-reload-banner" ? body.children.find((c) => c.className === "su-reload-banner") || null : null) };
};

describe("showReloadBanner", () => {
  it("shows one alert with the message, a reload button and a dismiss button", () => {
    const doc = makeDoc();
    const bar = showReloadBanner(doc, () => {});
    assert.equal(bar.attrs.role, "alert");
    assert.equal(bar.children[0].textContent, RELOAD_MESSAGE);
    assert.deepEqual(bar.children.slice(1).map((b) => b.textContent), ["Reload page", "Dismiss"]);
    assert.equal(showReloadBanner(doc, () => {}), null, "never stacks a second banner");
    assert.equal(doc.body.children.length, 1);
  });
  it("Reload page reloads and Dismiss removes the banner", () => {
    let reloads = 0;
    const doc = makeDoc();
    const bar = showReloadBanner(doc, () => { reloads++; });
    bar.children[1].click();
    assert.equal(reloads, 1);
    bar.children[2].click();
    assert.equal(doc.body.children.length, 0);
  });
  it("does nothing without a document", () => {
    assert.equal(showReloadBanner(null), null);
  });
  it("has styles in content.css", () => {
    const css = readFileSync(new URL("../src/content/content.css", import.meta.url), "utf8");
    for (const c of [".su-reload-banner", ".su-reload-banner-btn", ".su-reload-banner-text"]) assert.ok(css.includes(c), c);
  });
});
