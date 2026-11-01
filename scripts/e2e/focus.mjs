#!/usr/bin/env node
// Keyboard focus check: tabs through the extension's controls on the offline results and profile fixtures and
// fails for any extension control that takes focus without a visible change (outline, box-shadow, border or
// background). Same offline routing as run.mjs: nothing reaches Google. Run: node scripts/e2e/focus.mjs
import { chromium } from "playwright";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const EXT = path.resolve(ROOT, process.env.EXT_DIR || ".");
const EDGE = process.env.EDGE_PATH || "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const FX = (n) => fs.readFileSync(path.join(ROOT, "tests/e2e/fixtures", n), "utf8");

const ctx = await chromium.launchPersistentContext(fs.mkdtempSync(path.join(os.tmpdir(), "sub-focus-")), {
  executablePath: EDGE, headless: false,
  args: [`--disable-extensions-except=${EXT}`, `--load-extension=${EXT}`, "--headless=new", "--no-first-run"],
  viewport: { width: 1400, height: 900 },
});
await ctx.route("**/*", (route) => {
  const u = new URL(route.request().url());
  if (/^scholar\.google\./.test(u.hostname)) {
    if (u.pathname === "/citations" && u.searchParams.get("view_op") === "view_citation") return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-article.html") });
    if (u.pathname === "/scholar" && !u.searchParams.has("start")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-results.html") });
    if (u.pathname === "/citations" && u.searchParams.get("user") === "FIXTURE" && !u.searchParams.has("cstart")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-profile.html") });
    return route.fulfill({ status: 204, body: "" });
  }
  if (u.protocol === "chrome-extension:" || u.protocol === "data:") return route.continue();
  return route.abort();
});
await (ctx.serviceWorkers()[0] || ctx.waitForEvent("serviceworker", { timeout: 20000 }));

const STYLE_KEYS = ["outlineStyle", "outlineWidth", "outlineColor", "boxShadow", "borderTopColor", "borderBottomColor", "backgroundColor", "textDecorationLine"];
const failures = [];
let seen = 0;
for (const url of ["https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory", "https://scholar.google.com/citations?user=FIXTURE&hl=en", "https://scholar.google.com/citations?view_op=view_citation&hl=en&user=eEcALuEAAAAJ&citation_for_view=eEcALuEAAAAJ:0EnyYjriUFMC"]) {
  const p = await ctx.newPage();
  await p.goto(url);
  await p.waitForSelector(".su-badge", { timeout: 20000 }).catch(() => {});
  await p.waitForTimeout(2500);
  // SELFTEST=1 strips focus styles from extension buttons, to prove the check can fail.
  if (process.env.SELFTEST) await p.addStyleTag({ content: "button[class*=su-]:focus, button[class*=su-]:focus-visible { outline: none !important; box-shadow: none !important; }" });
  // Unfocused styles of every focusable extension element, taken before any Tab press.
  await p.evaluate((keys) => {
    const sel = "a[href], button, input, select, textarea, [tabindex]:not([tabindex='-1']), summary";
    window.__suBase = new Map();
    for (const el of document.querySelectorAll(sel)) {
      if (!el.closest("[class*='su-'], [id^='su-']")) continue;
      const cs = getComputedStyle(el);
      window.__suBase.set(el, Object.fromEntries(keys.map((k) => [k, cs[k]])));
    }
  }, STYLE_KEYS);
  for (let i = 0; i < 400; i++) {
    await p.keyboard.press("Tab");
    const r = await p.evaluate((keys) => {
      const el = document.activeElement;
      if (!el || el === document.body) return { done: true };
      const base = window.__suBase.get(el);
      if (!base) return { skip: true };
      const cs = getComputedStyle(el);
      const changed = keys.some((k) => cs[k] !== base[k]) && !(cs.outlineStyle === "none" && keys.filter((k) => cs[k] !== base[k]).every((k) => k.startsWith("outline")));
      const id = `${el.tagName.toLowerCase()}.${[...el.classList].join(".")} "${(el.getAttribute("aria-label") || el.textContent || "").trim().slice(0, 40)}"`;
      return { changed, id, visible: el.getClientRects().length > 0 };
    }, STYLE_KEYS);
    if (r.done) break;
    if (r.skip || !r.visible) continue;
    seen++;
    if (!r.changed && !failures.includes(r.id)) failures.push(r.id);
  }
  await p.close();
}
await ctx.close();
console.log(`${seen} focus stops on extension controls; ${failures.length} without a visible focus change`);
for (const f of failures) console.log("  NO FOCUS STYLE  " + f);
process.exit(failures.length ? 1 : 0);
