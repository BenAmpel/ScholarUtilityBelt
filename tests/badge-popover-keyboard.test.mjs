import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { makeBadgePopoverAccessible } = await import("../src/common/quality.js");

// Minimal DOM: attributes, listeners, a focus tracker and contains().
class El {
  constructor(doc) { this.doc = doc; this.attrs = {}; this.listeners = {}; this.children = []; this.id = ""; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  addEventListener(t, f) { (this.listeners[t] ||= []).push(f); }
  contains(n) { return n === this || this.children.some((c) => c.contains(n)); }
  focus() { this.doc.active = this; }
  fire(type, init = {}) {
    const e = { type, target: this, defaultPrevented: false, stopped: false, preventDefault() { this.defaultPrevented = true; }, stopPropagation() { this.stopped = true; }, ...init };
    (this.listeners[type] || []).forEach((f) => f(e));
    return e;
  }
}

function setup() {
  const doc = { active: null };
  const badge = new El(doc);
  const popover = new El(doc);
  const link = new El(doc);
  popover.children.push(link);
  badge.children.push(popover);
  const state = { open: false, opens: 0 };
  let setOpen;
  const open = () => { state.open = true; state.opens++; setOpen(true); };
  const close = () => { state.open = false; setOpen(false); };
  setOpen = makeBadgePopoverAccessible(badge, popover, { open, close });
  return { doc, badge, popover, link, state, open, close };
}

describe("badge popover keyboard access", () => {
  it("makes the badge focusable and links it to the popover", () => {
    const { badge, popover } = setup();
    assert.equal(badge.attrs.tabindex, "0");
    assert.equal(badge.attrs.role, "button");
    assert.equal(badge.attrs["aria-expanded"], "false");
    assert.ok(popover.id);
    assert.equal(badge.attrs["aria-controls"], popover.id);
  });

  it("Enter opens the popover", () => {
    const { badge, state } = setup();
    const e = badge.fire("keydown", { key: "Enter" });
    assert.equal(state.open, true);
    assert.equal(e.defaultPrevented, true);
    assert.equal(badge.attrs["aria-expanded"], "true");
  });

  it("Space opens the popover without scrolling the page", () => {
    const { badge, state } = setup();
    const e = badge.fire("keydown", { key: " " });
    assert.equal(state.open, true);
    assert.equal(e.defaultPrevented, true);
    assert.equal(badge.attrs["aria-expanded"], "true");
  });

  it("Escape closes the popover and returns focus to the badge", () => {
    const { doc, badge, link, state } = setup();
    badge.fire("keydown", { key: "Enter" });
    link.focus();
    const e = badge.fire("keydown", { key: "Escape", target: link });
    assert.equal(state.open, false);
    assert.equal(doc.active, badge);
    assert.equal(e.stopped, true);
    assert.equal(badge.attrs["aria-expanded"], "false");
  });

  it("Escape does nothing while the popover is closed", () => {
    const { badge, state } = setup();
    const e = badge.fire("keydown", { key: "Escape" });
    assert.equal(e.stopped, false);
    assert.equal(state.open, false);
  });

  it("focusing out closes it, but moving into the popover link does not", () => {
    const { badge, link, state } = setup();
    badge.fire("keydown", { key: "Enter" });
    badge.fire("focusout", { relatedTarget: link });
    assert.equal(state.open, true);
    badge.fire("focusout", { relatedTarget: null });
    assert.equal(state.open, false);
  });

  it("keys pressed on the popover link are left alone", () => {
    const { badge, link, state } = setup();
    const e = badge.fire("keydown", { key: "Enter", target: link });
    assert.equal(state.opens, 0);
    assert.equal(e.defaultPrevented, false);
  });

  it("is wired into content.js next to the hover handlers, with a focus-visible style", () => {
    const js = readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
    assert.match(js, /makeBadgePopoverAccessible\(span, tooltip,/);
    assert.match(js, /addEventListener\("mouseenter", openPopover\)/);
    const css = readFileSync(new URL("../src/content/content.css", import.meta.url), "utf8");
    assert.match(css, /\.su-badge\[aria-controls\]:focus-visible/);
  });
});
