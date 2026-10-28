// Theme switch check: picking a theme must change the open Scholar page at once and stick after a reload.
// (A 2026-06 store review: "light theme won't save".) content-early.js paints the page background at document_start,
// before settings load; this checks that a later switch (toolbar button or Options) repaints it too. Offline fixtures only.
//   node build.js && node scripts/e2e/theme.mjs          EDGE_PATH=/path/to/Microsoft\ Edge to override the browser
import { chromium } from "playwright";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const EXT = path.resolve(ROOT, process.env.EXT_DIR || ".");
const EDGE = process.env.EDGE_PATH || "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const FX = (n) => fs.readFileSync(path.join(ROOT, "tests/e2e/fixtures", n), "utf8");
const URL_RESULTS = "https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory";

const results = [];
const check = (name, ok, detail = "") => { results.push(ok); console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? "  (" + detail + ")" : ""}`); };
// Brightness of the page background as Scholar's own header and body show it: 0 black .. 255 white.
const shade = (p) => p.evaluate(() => {
  const lum = (el) => { const m = getComputedStyle(el).backgroundColor.match(/\d+/g).map(Number); return Math.round((m[0] + m[1] + m[2]) / 3); };
  return { body: lum(document.body), header: lum(document.getElementById("gs_hdr") || document.body), attr: document.documentElement.getAttribute("data-su-theme") };
});
const isLight = (s) => s.body > 200 && s.header > 200 && s.attr === "light";
const isDark = (s) => s.body < 80 && s.header < 80 && s.attr === "dark";

async function run(colorScheme) {
  const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), "sub-theme-"));
  const ctx = await chromium.launchPersistentContext(profileDir, {
    executablePath: EDGE, headless: false, colorScheme,
    args: [`--disable-extensions-except=${EXT}`, `--load-extension=${EXT}`, "--headless=new", "--no-first-run"],
    viewport: { width: 1400, height: 900 },
  });
  await ctx.route("**/*", (route) => {
    const u = new URL(route.request().url());
    if (/^scholar\.google\./.test(u.hostname)) {
      if (u.pathname === "/scholar" && !u.searchParams.has("start")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-results.html") });
      return route.fulfill({ status: 204, body: "" });
    }
    if (u.protocol === "chrome-extension:" || u.protocol === "data:") return route.continue();
    return route.abort();
  });
  try {
    const sw = ctx.serviceWorkers()[0] || await ctx.waitForEvent("serviceworker", { timeout: 20000 });
    const setTheme = (theme) => sw.evaluate(async (t) => {
      const { settings = {} } = await chrome.storage.local.get({ settings: {} });
      await chrome.storage.local.set({ settings: { ...settings, theme: t } });
    }, theme);
    const p = await ctx.newPage();
    await p.goto(URL_RESULTS);
    await p.waitForSelector("#su-theme-toggle", { timeout: 20000 });
    await p.waitForTimeout(500);
    const want = colorScheme === "dark" ? isDark : isLight;
    const other = colorScheme === "dark" ? "light" : "dark";
    const wantOther = colorScheme === "dark" ? isLight : isDark;
    let s = await shade(p);
    check(`system ${colorScheme}, Auto: page follows the system`, want(s), JSON.stringify(s));

    // Toolbar button cycles Auto -> Light -> Dark.
    await p.click("#su-theme-toggle");
    if (other === "dark") await p.click("#su-theme-toggle");
    await p.waitForTimeout(300);
    s = await shade(p);
    check(`system ${colorScheme}: toolbar switch to ${other} repaints the open page`, wantOther(s), JSON.stringify(s));
    await p.reload();
    await p.waitForSelector("#su-theme-toggle", { timeout: 20000 });
    await p.waitForTimeout(500);
    s = await shade(p);
    check(`system ${colorScheme}: ${other} sticks after a reload`, wantOther(s), JSON.stringify(s));

    // A change saved on the Options page reaches the open tab through storage.onChanged.
    await setTheme(colorScheme);
    await p.waitForTimeout(800);
    s = await shade(p);
    check(`system ${colorScheme}: Options change back to ${colorScheme} repaints the open page`, want(s), JSON.stringify(s));
  } catch (e) {
    check(`system ${colorScheme}: run`, false, String(e.message || e).split("\n")[0]);
  } finally {
    await ctx.close();
    fs.rmSync(profileDir, { recursive: true, force: true });
  }
}

await run("dark");
await run("light");
const failed = results.filter((ok) => !ok).length;
console.log(failed ? `${failed} theme check(s) failed` : "Theme checks passed");
process.exit(failed ? 1 : 0);
