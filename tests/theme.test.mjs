import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const { THEME_CHOICES, THEME_LABELS, normalizeThemeChoice, resolveTheme, applyPageTheme, watchPageTheme } =
  await import("../src/common/theme.js");
const { DEFAULT_SETTINGS } = await import("../src/common/storage.js");

const read = (p) => readFileSync(new URL(p, import.meta.url), "utf8");
const EARLY_SRC = read("../src/content/content-early.js");

// A system color scheme that can be flipped while a page is open.
function makeSystem(dark) {
  const listeners = [];
  return {
    get dark() { return dark; },
    set(next) { dark = next; listeners.forEach((f) => f({ matches: dark })); },
    matchMedia: (q) => ({
      get matches() { return q === "(prefers-color-scheme: dark)" ? dark : false; },
      addEventListener: (t, f) => { if (t === "change") listeners.push(f); },
      addListener: (f) => listeners.push(f),
    }),
  };
}

function makeLocalStorage(init = {}) {
  const m = new Map(Object.entries(init));
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), _m: m };
}

// One Scholar page load running the real content-early.js. chrome.storage answers only when answer() is called,
// so a test can check the first paint and then a slow (or very late) storage answer.
function loadScholarPage({ saved, system, localStorage }) {
  const attrs = {};
  const inserted = [];
  const documentElement = { setAttribute: (k, v) => { attrs[k] = v; }, insertBefore: (n) => inserted.push(n) };
  const document = {
    head: null,
    documentElement,
    createElement: () => ({ id: "", textContent: "" }),
    getElementById: (id) => inserted.find((n) => n.id === id) || null,
  };
  let pending = null;
  const chrome = { storage: { local: { get: (defaults, cb) => { pending = cb; } } } };
  const window = { matchMedia: system.matchMedia, localStorage, addEventListener: () => {} };
  const timers = [];
  vm.runInNewContext(EARLY_SRC, { window, document, chrome, setTimeout: (f) => timers.push(f) });
  const style = inserted[0];
  const shown = () => ({ attr: attrs["data-su-theme"], bg: /background: (#[0-9a-f]+)/.exec(style.textContent)[1] });
  return {
    shown,
    elapse() { timers.splice(0).forEach((f) => f()); },
    answer() { pending({ settings: saved === undefined ? {} : { theme: saved } }); return shown(); },
  };
}

const LIGHT = { attr: "light", bg: "#fff" };
const DARK = { attr: "dark", bg: "#202124" };

describe("theme choices", () => {
  it("offers Follow system, Light and Dark, defaulting to Follow system", () => {
    assert.deepEqual(THEME_CHOICES, ["auto", "light", "dark"]);
    assert.deepEqual(THEME_LABELS, { auto: "Follow system", light: "Light", dark: "Dark" });
    assert.equal(DEFAULT_SETTINGS.theme, "auto");
    const html = read("../src/options/options.html");
    const select = html.slice(html.indexOf('<select id="theme"'), html.indexOf("</select>", html.indexOf('<select id="theme"')));
    assert.deepEqual([...select.matchAll(/<option value="(\w+)">([^<]+)</g)].map((m) => [m[1], m[2]]),
      [["auto", "Follow system"], ["light", "Light"], ["dark", "Dark"]]);
  });
  it("an explicit choice wins over the system; anything else follows the system", () => {
    assert.equal(resolveTheme("light", true), "light");
    assert.equal(resolveTheme("dark", false), "dark");
    assert.equal(resolveTheme("auto", true), "dark");
    assert.equal(resolveTheme("auto", false), "light");
    for (const odd of [undefined, null, "", "system", "Light"]) assert.equal(normalizeThemeChoice(odd), "auto");
  });
});

describe("Scholar page first paint (content-early.js)", () => {
  it("(a) a saved Light choice survives a reload and wins while the system prefers dark", () => {
    const system = makeSystem(true);
    const ls = makeLocalStorage();
    const first = loadScholarPage({ saved: "light", system, localStorage: ls });
    assert.deepEqual(first.shown(), DARK, "first visit, nothing cached yet: system guess");
    assert.deepEqual(first.answer(), LIGHT, "the saved choice replaces the guess");
    const reload = loadScholarPage({ saved: "light", system, localStorage: ls });
    assert.deepEqual(reload.shown(), LIGHT, "after a reload the very first paint is already Light");
    assert.deepEqual(reload.answer(), LIGHT);
  });
  it("(a) a storage answer that arrives late still wins (no cut-off back to the system theme)", () => {
    const page = loadScholarPage({ saved: "light", system: makeSystem(true), localStorage: makeLocalStorage() });
    // The old script gave up after 2 s and ignored a later answer, so a slow profile kept the system theme.
    page.elapse();
    assert.deepEqual(page.answer(), LIGHT);
  });
  it("(b) a saved Dark choice wins under a light system", () => {
    const system = makeSystem(false);
    const ls = makeLocalStorage();
    const first = loadScholarPage({ saved: "dark", system, localStorage: ls });
    assert.deepEqual(first.answer(), DARK);
    const reload = loadScholarPage({ saved: "dark", system, localStorage: ls });
    assert.deepEqual(reload.shown(), DARK);
    assert.deepEqual(reload.answer(), DARK);
  });
  it("(c) Follow system tracks the system setting", () => {
    const ls = makeLocalStorage();
    assert.deepEqual(loadScholarPage({ saved: "auto", system: makeSystem(true), localStorage: ls }).answer(), DARK);
    assert.deepEqual(loadScholarPage({ saved: "auto", system: makeSystem(false), localStorage: ls }).answer(), LIGHT);
    assert.deepEqual(loadScholarPage({ saved: undefined, system: makeSystem(true), localStorage: ls }).answer(), DARK, "no saved theme = Follow system");
  });
  it("a stale cached value never overrides the saved setting", () => {
    const ls = makeLocalStorage({ "su-theme-choice": "light" });
    const page = loadScholarPage({ saved: "auto", system: makeSystem(true), localStorage: ls });
    assert.deepEqual(page.shown(), LIGHT, "the cache only guides the first paint");
    assert.deepEqual(page.answer(), DARK);
    assert.equal(ls.getItem("su-theme-choice"), "auto", "the cache is corrected");
  });
  it("still paints when page localStorage is blocked", () => {
    const blocked = { getItem: () => { throw new Error("denied"); }, setItem: () => { throw new Error("denied"); } };
    const page = loadScholarPage({ saved: "light", system: makeSystem(true), localStorage: blocked });
    assert.deepEqual(page.shown(), DARK);
    assert.deepEqual(page.answer(), LIGHT);
  });
});

describe("Scholar page after load (content.js)", () => {
  it("applyTheme resolves the same way and refreshes the first-paint cache", () => {
    const src = read("../src/content/content.js");
    const fn = src.slice(src.indexOf("function applyTheme(theme)"), src.indexOf("function applyBadgePalette"));
    assert.match(fn, /theme === "dark" \? "dark" : theme === "light" \? "light" : \(window\.matchMedia/);
    assert.match(fn, /localStorage\.setItem\("su-theme-choice"/);
    assert.match(src, /if \(window\.suState\?\.settings\?\.theme === "auto"\) applyTheme\("auto"\)/, "only Follow system reacts to a system change");
  });
  it("the toolbar button records the new choice before saving, so a quick second click is not lost", () => {
    const src = read("../src/content/content.js");
    const handler = src.slice(src.indexOf('const next = { auto: "light", light: "dark", dark: "auto" }'));
    const set = handler.indexOf("state.settings.theme = next;");
    const save = handler.indexOf("await setSettings({ theme: next });");
    assert.ok(set > 0 && save > set);
  });
});

describe("Options, popup and library pages", () => {
  function makeDoc() {
    const attrs = {};
    return { attrs, documentElement: { setAttribute: (k, v) => { attrs[k] = v; } } };
  }
  it("(a) Light stays Light while the system is or turns dark", () => {
    const system = makeSystem(true);
    const doc = makeDoc();
    watchPageTheme(doc, () => "light", system);
    assert.equal(doc.attrs["data-su-theme"], "light");
    system.set(false); system.set(true);
    assert.equal(doc.attrs["data-su-theme"], "light");
  });
  it("(b) Dark stays Dark under a light system", () => {
    const system = makeSystem(false);
    const doc = makeDoc();
    assert.equal(applyPageTheme(doc, "dark", system), "dark");
    watchPageTheme(doc, () => "dark", system);
    system.set(false);
    assert.equal(doc.attrs["data-su-theme"], "dark");
  });
  it("(c) Follow system tracks a system change while the page is open", () => {
    const system = makeSystem(false);
    const doc = makeDoc();
    watchPageTheme(doc, () => "auto", system);
    assert.equal(doc.attrs["data-su-theme"], "light");
    system.set(true);
    assert.equal(doc.attrs["data-su-theme"], "dark");
    system.set(false);
    assert.equal(doc.attrs["data-su-theme"], "light");
  });
  it("the stylesheets let an explicit choice override prefers-color-scheme", () => {
    for (const [file, sys, forced] of [
      ["../src/options/options.css", "dark", "light"],
      ["../src/popup/popup.css", "dark", "light"],
      ["../src/library/library.css", "light", "dark"],
    ]) {
      const css = read(file);
      const blocks = [...css.matchAll(new RegExp(`@media \\(prefers-color-scheme: ${sys}\\) \\{([\\s\\S]*?)\\n\\}`, "g"))].map((m) => m[1]);
      assert.ok(blocks.length > 0, file);
      for (const b of blocks) {
        for (const rule of b.split("\n").filter((l) => /^\s{2}\S.*\{/.test(l))) {
          assert.ok(rule.includes(`:root:not([data-su-theme="${forced}"])`), `${file}: system rule not gated: ${rule.trim()}`);
        }
      }
      assert.ok(css.includes(`:root[data-su-theme="${sys}"]`), `${file}: no rule for an explicit ${sys} choice`);
    }
  });
  it("Options saves the theme the moment it is picked, and each page applies it", () => {
    const js = read("../src/options/options.js");
    const onChange = js.slice(js.indexOf('el("theme")?.addEventListener("change"'));
    assert.ok(onChange.length > 0 && /await setSettings\(\{ theme: themeChoice \}\)/.test(onChange.slice(0, 400)));
    assert.match(js, /if \(el\.id === "theme"\) continue;/, "a theme pick is never left as an unsaved change");
    assert.match(read("../src/popup/popup.js"), /watchPageTheme\(document/);
    assert.match(read("../src/library/library.js"), /watchPageTheme\(document/);
  });
});
