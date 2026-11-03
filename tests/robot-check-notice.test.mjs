import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const {
  SCHOLAR_COOLDOWN_KEY, SCHOLAR_COOLDOWN_KIND_KEY, SCHOLAR_PAUSED_FEATURES,
  formatCooldownLeft, scholarCooldownNotice, clearScholarCooldown, scholarCooldownLeft,
} = await import("../src/common/scholar-links.js");
const { showScholarCooldownNotice, hideScholarCooldownNotice } = await import("../src/common/reload-banner.js");

// Just enough DOM for the notice: elements with classes, children, attributes and click listeners.
class El {
  constructor(tag) { this.tag = tag; this.children = []; this.parent = null; this.listeners = {}; this.className = ""; this.textContent = ""; this.attrs = {}; this.disabled = false; }
  setAttribute(k, v) { this.attrs[k] = v; }
  append(...k) { k.forEach((c) => this.appendChild(c)); }
  appendChild(c) { c.parent = this; this.children.push(c); return c; }
  addEventListener(t, f) { (this.listeners[t] ||= []).push(f); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter((c) => c !== this); this.parent = null; }
  click() { return Promise.all((this.listeners.click || []).map((f) => f())); }
  all() { return this.children.flatMap((c) => [c, ...c.all()]); }
  querySelector(sel) { const cls = sel.replace(/^\./, ""); return this.all().find((c) => String(c.className).split(" ").includes(cls)) || null; }
}
const makeDoc = () => {
  const body = new El("body");
  return { body, createElement: (t) => new El(t), querySelector: (sel) => body.querySelector(sel) };
};
// A chrome.storage.local stand-in holding scratch data only.
const makeArea = (data) => ({
  data,
  async get(k) { return { [k]: this.data[k] }; },
  async remove(keys) { for (const k of [].concat(keys)) delete this.data[k]; },
});

const MIN = 60000;
const PAGE = "https://scholar.google.com/scholar?q=phishing&start=10";

describe("formatCooldownLeft", () => {
  it("rounds minutes up and says when under a minute is left", () => {
    assert.equal(formatCooldownLeft(14 * MIN + 1), "15 min left");
    assert.equal(formatCooldownLeft(15 * MIN), "15 min left");
    assert.equal(formatCooldownLeft(MIN), "1 min left");
    assert.equal(formatCooldownLeft(59000), "Less than a minute left");
    assert.equal(formatCooldownLeft(0), "");
    assert.equal(formatCooldownLeft(-5), "");
    assert.equal(formatCooldownLeft(NaN), "");
  });
});

describe("scholarCooldownNotice", () => {
  it("is null when no cooldown is running", () => {
    assert.equal(scholarCooldownNotice({ leftMs: 0, kind: "captcha", pageUrl: PAGE }), null);
    assert.equal(scholarCooldownNotice(), null);
  });
  it("names the robot check, the time left, the paused features and links to this Scholar page", () => {
    const n = scholarCooldownNotice({ leftMs: 10 * MIN, kind: "captcha", pageUrl: PAGE });
    assert.equal(n.title, "Scholar asked for a robot check");
    assert.equal(n.timeLeft, "10 min left");
    assert.equal(n.solveUrl, PAGE);
    assert.deepEqual(n.features, ["Load more pages (search results)", "Load all for full stats (author profiles)", "Compare authors (author profiles)"]);
  });
  it("falls back to Scholar's home page for a non-Scholar or non-https URL", () => {
    for (const pageUrl of ["https://evil.example/scholar", "http://scholar.google.com/", "javascript:alert(1)", "", undefined]) {
      assert.equal(scholarCooldownNotice({ leftMs: MIN, kind: "captcha", pageUrl }).solveUrl, "https://scholar.google.com/", String(pageUrl));
    }
    assert.equal(scholarCooldownNotice({ leftMs: MIN, pageUrl: "https://scholar.google.de/citations?user=x" }).solveUrl, "https://scholar.google.de/citations?user=x");
  });
  it("uses rate-limit wording after a 429, and robot-check wording when the kind is unknown", () => {
    assert.equal(scholarCooldownNotice({ leftMs: MIN, kind: "rate_limit" }).title, "Scholar is limiting requests");
    assert.equal(scholarCooldownNotice({ leftMs: MIN, kind: undefined }).title, "Scholar asked for a robot check");
  });
  it("copy has no emojis", () => {
    const n = scholarCooldownNotice({ leftMs: MIN, kind: "captcha" });
    const text = [n.title, n.hint, n.linkText, n.timeLeft, ...n.features].join(" ");
    assert.ok(!/\p{Extended_Pictographic}/u.test(text));
  });
});

describe("cooldown notice on the page", () => {
  it("appears when the cooldown flag is set and clears on Retry, removing the flag from storage", async () => {
    const now = Date.UTC(2026, 10, 3, 12, 0, 0);
    const area = makeArea({ [SCHOLAR_COOLDOWN_KEY]: now + 12 * MIN, [SCHOLAR_COOLDOWN_KIND_KEY]: "captcha", unrelated: 1 });
    const until = (await area.get(SCHOLAR_COOLDOWN_KEY))[SCHOLAR_COOLDOWN_KEY];
    const kind = (await area.get(SCHOLAR_COOLDOWN_KIND_KEY))[SCHOLAR_COOLDOWN_KIND_KEY];
    const leftMs = scholarCooldownLeft({ until, now, cooldownMs: 15 * MIN, pageLoadedAt: now - 5 * MIN, pageIsNormal: true });
    const notice = scholarCooldownNotice({ leftMs, kind, pageUrl: PAGE });

    const doc = makeDoc();
    let reran = 0;
    const bar = showScholarCooldownNotice(doc, notice, {
      onRetry: async () => { await clearScholarCooldown(area); reran++; },
    });
    assert.ok(bar, "notice shows");
    assert.equal(doc.body.children.length, 1);
    const title = bar.querySelector(".su-cooldown-notice-title");
    assert.equal(title.attrs.role, "status");
    assert.match(title.textContent, /^Scholar asked for a robot check\./);
    assert.equal(bar.querySelector(".su-cooldown-notice-time").textContent, "12 min left");
    assert.deepEqual(bar.querySelector(".su-cooldown-notice-list").children.map((li) => li.textContent), [...SCHOLAR_PAUSED_FEATURES]);
    const link = bar.querySelector(".su-cooldown-notice-link");
    assert.equal(link.tag, "a");
    assert.equal(link.href, PAGE);
    assert.equal(link.target, "_blank");
    assert.equal(link.rel, "noopener noreferrer");
    const retry = bar.querySelector(".su-cooldown-notice-retry");
    assert.equal(retry.tag, "button", "Retry is a real button, so it is keyboard reachable");
    assert.equal(retry.type, "button");
    assert.equal(retry.textContent, "Retry");

    // A second show only updates the time left; it never stacks a second notice.
    const again = showScholarCooldownNotice(doc, { ...notice, timeLeft: "11 min left" }, {});
    assert.equal(again, bar);
    assert.equal(doc.body.children.length, 1);
    assert.equal(bar.querySelector(".su-cooldown-notice-time").textContent, "11 min left");

    await retry.click();
    assert.equal(reran, 1, "Retry runs the paused work");
    assert.ok(!(SCHOLAR_COOLDOWN_KEY in area.data), "cooldown flag removed");
    assert.ok(!(SCHOLAR_COOLDOWN_KIND_KEY in area.data), "cooldown kind removed");
    assert.equal(area.data.unrelated, 1, "other keys untouched");
    assert.equal(doc.body.children.length, 0, "notice gone");
    assert.equal(scholarCooldownNotice({ leftMs: 0, kind: null, pageUrl: PAGE }), null, "nothing to show afterwards");
  });
  it("Hide removes it and tells the page; hideScholarCooldownNotice removes it too", async () => {
    const doc = makeDoc();
    let hidden = 0;
    const notice = scholarCooldownNotice({ leftMs: 3 * MIN, kind: "captcha", pageUrl: PAGE });
    const bar = showScholarCooldownNotice(doc, notice, { onHide: () => { hidden++; } });
    const hide = bar.querySelector(".su-cooldown-notice-actions").children.find((b) => b.textContent === "Hide");
    await hide.click();
    assert.equal(hidden, 1);
    assert.equal(doc.body.children.length, 0);
    showScholarCooldownNotice(doc, notice, {});
    hideScholarCooldownNotice(doc);
    assert.equal(doc.body.children.length, 0);
  });
  it("does nothing without a document or a notice", () => {
    assert.equal(showScholarCooldownNotice(null, {}), null);
    assert.equal(showScholarCooldownNotice(makeDoc(), null), null);
    assert.doesNotThrow(() => hideScholarCooldownNotice(null));
  });
  it("clearScholarCooldown tolerates a missing storage area", async () => {
    await clearScholarCooldown(null);
  });
});

describe("cooldown notice wiring", () => {
  const css = readFileSync(new URL("../src/content/content.css", import.meta.url), "utf8");
  const content = readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");
  it("has styles built on the theme tokens, with a visible focus ring", () => {
    for (const c of [".su-cooldown-notice {", ".su-cooldown-notice-btn", ".su-cooldown-notice-link", ".su-cooldown-notice-list"]) assert.ok(css.includes(c), c);
    const block = css.slice(css.indexOf(".su-cooldown-notice {"), css.indexOf(".su-cooldown-notice-btn:focus-visible"));
    for (const token of ["var(--su-surface)", "var(--su-gray-900)", "var(--su-accent)", "var(--su-chip-bg)"]) assert.ok(block.includes(token), token);
    assert.ok(!/animation|transition/.test(block), "no motion");
    assert.ok(css.includes(".su-cooldown-notice-btn:focus-visible { outline: var(--su-focus-ring)"));
  });
  it("content.js uses the same storage keys and the paused features are the ones that check the cooldown", () => {
    assert.ok(content.includes(`"${SCHOLAR_COOLDOWN_KEY}"`));
    assert.ok(content.includes(`"${SCHOLAR_COOLDOWN_KIND_KEY}"`));
    assert.equal((content.match(/await scholarCooldownLeftMs\(\)/g) || []).length, SCHOLAR_PAUSED_FEATURES.length + 1,
      "one check per paused feature, plus the notice's own refresh");
    for (const label of ["Load more pages", "Load all for full stats"]) assert.ok(content.includes(`"${label}"`), label);
    assert.ok(content.includes("su-compare-do"));
  });
});
