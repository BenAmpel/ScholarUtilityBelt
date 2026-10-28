#!/usr/bin/env node
// Offline end-to-end check of the built extension in Microsoft Edge (branded Chrome no longer loads unpacked
// extensions from the command line). Every Google Scholar URL is answered from tests/e2e/fixtures and counted, and
// every other page request is blocked, so nothing reaches Google. Run: node scripts/e2e/run.mjs
//   EDGE_PATH=/path/to/Microsoft\ Edge   (default: the copy on the Extreme SSD)
import { chromium } from "playwright";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const EDGE = process.env.EDGE_PATH || "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const FX = (n) => fs.readFileSync(path.join(ROOT, "tests/e2e/fixtures", n), "utf8");
const results = [];
const check = (name, ok, detail = "") => { results.push({ name, ok, detail }); console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? "  (" + detail + ")" : ""}`); };

const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), "sub-e2e-"));
const ctx = await chromium.launchPersistentContext(profileDir, {
  executablePath: EDGE, headless: false,
  args: [`--disable-extensions-except=${ROOT}`, `--load-extension=${ROOT}`, "--headless=new", "--no-first-run"],
  viewport: { width: 1400, height: 900 },
});
const scholarHits = [];
await ctx.route("**/*", (route) => {
  const u = new URL(route.request().url());
  if (/^scholar\.google\./.test(u.hostname)) {
    scholarHits.push(u.pathname + u.search);
    if (u.pathname === "/scholar" && !u.searchParams.has("start")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-results.html") });
    if (u.pathname === "/citations" && u.searchParams.get("user") === "FIXTURE" && !u.searchParams.has("cstart")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-profile.html") });
    return route.fulfill({ status: 204, body: "" });
  }
  if (u.protocol === "chrome-extension:" || u.protocol === "data:") return route.continue();
  return route.abort();
});
const sw = ctx.serviceWorkers()[0] || await ctx.waitForEvent("serviceworker", { timeout: 20000 });
const extId = new URL(sw.url()).host;
// Errors from the extension: uncaught exceptions on any page, and console errors that mention the extension or its
// [SU] log prefix (Scholar's own fixture pages log nothing). Blocked third-party requests are expected and ignored.
const errors = [];
const watch = (page, label) => {
  page.on("pageerror", (e) => errors.push(`${label}: uncaught ${e.message}`));
  page.on("console", (m) => {
    if (m.type() !== "error") return;
    const t = m.text();
    if (/net::ERR_FAILED|ERR_BLOCKED|Failed to load resource/i.test(t)) return;
    errors.push(`${label}: console ${t.slice(0, 200)}`);
  });
};
ctx.on("page", (pg) => watch(pg, "page"));

try {
  // 1. Results page: badges render.
  const p = await ctx.newPage();
  const t0 = Date.now();
  await p.goto("https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory");
  await p.waitForSelector(".su-badge", { timeout: 20000 }).catch(() => {});
  const firstBadgeMs = Date.now() - t0;
  await p.waitForTimeout(2500);
  const badges = await p.locator(".su-badge").count();
  const rowsWithQuality = await p.evaluate(() => [...document.querySelectorAll(".gs_r.gs_or")].filter((r) => r.querySelector(".su-quartile, .su-ft50, .su-utd24, .su-abdc, .su-h5")).length);
  check("results page shows badges", badges > 0, `${badges} badges, ${rowsWithQuality}/10 rows with a ranking badge, first after ${firstBadgeMs} ms`);
  check("first badge within 5 s", firstBadgeMs < 5000, `${firstBadgeMs} ms`);
  const extraOnResults = scholarHits.filter((h) => !h.startsWith("/scholar?hl=en&q=")).length;
  check("results page sends no extra Scholar requests", extraOnResults === 0, `${extraOnResults} extra: ${scholarHits.slice(1).join(", ")}`);
  await p.close();

  // 1b. Your own journal list and the Quick read filter.
  await sw.evaluate(() => chrome.storage.local.set({ customVenueList: { name: "Fixture list", uploadedAt: new Date().toISOString(), entries: [["Sleep", "A"], ["Nature", "B"]] } }));
  const q = await ctx.newPage();
  await q.goto("https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory");
  await q.waitForSelector(".su-badge", { timeout: 20000 }).catch(() => {});
  await q.waitForTimeout(2000);
  const custom = await q.locator(".su-custom").count();
  check("custom journal list adds its badges", custom >= 2, `${custom} custom badges`);
  const filterBtn = q.locator("button", { hasText: /^Filter/ }).first();
  await filterBtn.click().catch(() => {});
  await q.waitForTimeout(500);
  const qr = q.locator('input[title^="Only results that have Scholar\'s Quick read"]').first();
  await qr.check().catch(() => {});
  await q.waitForTimeout(1200);
  const visibleRows = await q.evaluate(() => [...document.querySelectorAll(".gs_r.gs_or")].filter((r) => r.offsetParent !== null && !r.classList.contains("su-filtered-out")).length);
  check("Quick read filter keeps only the 3 rows with Quick read", visibleRows === 3, `${visibleRows} visible`);
  await sw.evaluate(() => chrome.storage.local.remove("customVenueList"));
  await q.close();

  // 2. Author profile with default settings: no automatic "Show more".
  scholarHits.length = 0;
  const a = await ctx.newPage();
  await a.goto("https://scholar.google.com/citations?user=FIXTURE&hl=en");
  await a.waitForTimeout(7000);
  const extraOnProfile = scholarHits.filter((h) => h.includes("cstart=")).length;
  check("profile visit (default) sends no extra Scholar requests", extraOnProfile === 0, `${extraOnProfile} extra: ${scholarHits.filter((h) => h.includes("cstart=")).join(", ")}`);
  const partial = await a.evaluate(() => !!document.querySelector(".su-author-partial-note") && !!document.querySelector("[data-load-all-publications]"));
  check("profile shows partial-stats note and Load all", partial);
  await a.close();

  // 3. Opt-in switch brings the automatic loading back.
  await sw.evaluate(async () => { const { settings } = await chrome.storage.local.get("settings"); await chrome.storage.local.set({ settings: { ...(settings || {}), autoLoadAuthorPapers: true } }); });
  scholarHits.length = 0;
  const b = await ctx.newPage();
  await b.goto("https://scholar.google.com/citations?user=FIXTURE&hl=en");
  await b.waitForTimeout(7000);
  const extraOptIn = scholarHits.filter((h) => h.includes("cstart=")).length;
  check("with the switch on, the profile loads more automatically", extraOptIn > 0, `${extraOptIn} extra`);
  await b.close();
  // 4. The extension's own pages load without errors.
  for (const pg of ["src/options/options.html", "src/library/library.html", "src/popup/popup.html"]) {
    const x = await ctx.newPage();
    await x.goto(`chrome-extension://${extId}/${pg}`);
    await x.waitForTimeout(1500);
    await x.close();
  }
  check("no uncaught exceptions or console errors from the extension", errors.length === 0, errors.slice(0, 6).join(" | "));
} finally {
  await ctx.close();
  fs.rmSync(profileDir, { recursive: true, force: true });
}
const failed = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - failed}/${results.length} passed`);
process.exit(failed ? 1 : 0);
