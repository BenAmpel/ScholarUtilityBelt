import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { clearCache, getCachedElement, getCachedElements } from "../src/content/dom-cache.js";

// A container that counts its queries and serves elements from a selector map; isConnected is what the cache checks.
function fakeContainer(bySelector) {
  const c = {
    queries: 0,
    querySelector(sel) { c.queries++; return (bySelector[sel] || [])[0] || null; },
    querySelectorAll(sel) { c.queries++; return bySelector[sel] || []; },
  };
  return c;
}
const elem = () => ({ isConnected: true });

describe("dom-cache", () => {
  it("queries once and serves the cached element after that", () => {
    const a = elem();
    const c = fakeContainer({ ".gs_a": [a] });
    assert.equal(getCachedElement(c, ".gs_a"), a);
    assert.equal(getCachedElement(c, ".gs_a"), a);
    assert.equal(c.queries, 1);
  });

  it("queries again once the cached element leaves the page", () => {
    const a = elem(), b = elem();
    const map = { ".gs_a": [a] };
    const c = fakeContainer(map);
    getCachedElement(c, ".gs_a");
    a.isConnected = false;
    map[".gs_a"] = [b];
    assert.equal(getCachedElement(c, ".gs_a"), b);
    assert.equal(c.queries, 2);
  });

  it("does not cache a miss, so an element that appears later is found", () => {
    const map = {};
    const c = fakeContainer(map);
    assert.equal(getCachedElement(c, ".gs_a"), null);
    const a = elem();
    map[".gs_a"] = [a];
    assert.equal(getCachedElement(c, ".gs_a"), a);
  });

  it("caches lists separately from single elements and drops a list when any member leaves", () => {
    const x = elem(), y = elem();
    const map = { "div.gs_gray": [x, y] };
    const c = fakeContainer(map);
    assert.deepEqual(getCachedElements(c, "div.gs_gray"), [x, y]);
    assert.equal(getCachedElement(c, "div.gs_gray"), x);
    assert.equal(c.queries, 2);
    getCachedElements(c, "div.gs_gray");
    assert.equal(c.queries, 2);
    y.isConnected = false;
    map["div.gs_gray"] = [x];
    assert.deepEqual(getCachedElements(c, "div.gs_gray"), [x]);
    assert.equal(c.queries, 3);
  });

  it("returns an array, never a NodeList, so callers can map over it", () => {
    const c = fakeContainer({ p: { 0: elem(), length: 1, [Symbol.iterator]: function* () { yield this[0]; } } });
    assert.ok(Array.isArray(getCachedElements(c, "p")));
  });

  it("clearCache forces a fresh query", () => {
    const c = fakeContainer({ ".gs_a": [elem()] });
    getCachedElement(c, ".gs_a");
    clearCache(c);
    getCachedElement(c, ".gs_a");
    assert.equal(c.queries, 2);
  });

  it("handles a missing container or selector", () => {
    assert.equal(getCachedElement(null, ".gs_a"), null);
    assert.equal(getCachedElement(fakeContainer({}), ""), null);
    assert.deepEqual(getCachedElements(undefined, "p"), []);
    assert.doesNotThrow(() => clearCache(null));
  });
});

import { createDeferredSweep, isOnlyOwnTooltipMutation } from "../src/content/dom-cache.js";

const el = (cls) => ({ nodeType: 1, classList: { contains: (c) => cls.split(" ").includes(c) } });

describe("isOnlyOwnTooltipMutation", () => {
  it("ignores records that only add or remove the extension's tooltips", () => {
    assert.equal(isOnlyOwnTooltipMutation([{ addedNodes: [el("su-floating-tooltip")], removedNodes: [] }]), true);
  });
  it("does not ignore a row render or an empty batch", () => {
    assert.equal(isOnlyOwnTooltipMutation([{ addedNodes: [el("gs_r")], removedNodes: [] }]), false);
    assert.equal(isOnlyOwnTooltipMutation([{ addedNodes: [el("su-floating-tooltip"), el("gs_r")], removedNodes: [] }]), false);
    assert.equal(isOnlyOwnTooltipMutation([]), false);
  });
});

describe("createDeferredSweep", () => {
  function harness() {
    const h = { hidden: false, runs: 0, timers: [] };
    h.sweep = createDeferredSweep(() => { h.runs++; }, 1000, { isHidden: () => h.hidden, setTimer: (fn) => { h.timers.push(fn); return h.timers.length; } });
    return h;
  }
  it("coalesces a burst of requests into one timer and one sweep", () => {
    const h = harness();
    h.sweep.request(); h.sweep.request(); h.sweep.request();
    assert.equal(h.timers.length, 1);
    h.timers[0]();
    assert.equal(h.runs, 1);
  });
  it("starts no timer while the tab is hidden, and sweeps once when it is shown again", () => {
    const h = harness();
    h.hidden = true;
    h.sweep.request(); h.sweep.request();
    assert.equal(h.timers.length, 0);
    h.sweep.resume();            // still hidden: nothing
    assert.equal(h.timers.length, 0);
    h.hidden = false;
    h.sweep.resume();
    assert.equal(h.timers.length, 1);
    h.sweep.resume();            // nothing held any more
    assert.equal(h.timers.length, 1);
  });
});
