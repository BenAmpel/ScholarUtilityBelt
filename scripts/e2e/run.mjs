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
  // The first page also pays for the extension's cold start (worker boot, data files), which on a busy CI runner can
  // pass 5 s; the budget applies to a second page load, as users see on every page after the first.
  console.log(`info  cold first badge after ${firstBadgeMs} ms`);
  const p2 = await ctx.newPage();
  const t1 = Date.now();
  await p2.goto("https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory&warm=1");
  await p2.waitForSelector(".su-badge", { timeout: 20000 }).catch(() => {});
  const warmBadgeMs = Date.now() - t1;
  await p2.close();
  check("first badge within 5 s on a warm page load", warmBadgeMs < 5000, `${warmBadgeMs} ms`);
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

  // 1c. Pro review workspace: import a PubMed and a Scopus file and check the search log and duplicate matching.
  // Pro comes from ExtensionPay's own offline cache (an active trial), which the worker trusts when it cannot reach
  // ExtensionPay; every non-Scholar request is blocked here, so it always takes that path.
  await sw.evaluate(async () => {
    const user = { paid: false, paidAt: null, installedAt: new Date(Date.now() - 864e5).toISOString(), trialStartedAt: new Date().toISOString(), subscriptionStatus: null };
    // A key makes ExtPay try the network (blocked here, so it fails over to the cache); without one it reports a free user.
    await chrome.storage.sync.set({ extensionpay_user: user, extensionpay_api_key: "e2e-offline-key" });
    await chrome.storage.local.set({ extensionpay_user: user, suExtpayOkAt: Date.now() });
  });
  const w = await ctx.newPage();
  w.on("dialog", (d) => d.accept(d.type() === "prompt" ? "E2E review" : undefined));
  await w.goto("https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory");
  await w.waitForSelector(".su-badge", { timeout: 20000 }).catch(() => {});
  await w.locator("button", { hasText: /^Review workspace$/ }).first().click();
  await w.waitForSelector('[data-review-new="1"]', { timeout: 10000 }).catch(async () => {
    const seen = await w.evaluate(() => [...document.querySelectorAll('[class*="su-pro-prompt"], [class*="su-review"], [role="dialog"]')].filter((e) => e.offsetParent !== null).map((e) => (e.className + ": " + e.textContent.trim().slice(0, 120))).slice(0, 4));
    const ent = await sw.evaluate(async () => { try { const r = await new Promise((res) => chrome.runtime.sendMessage({ action: "getEntitlementStatus" }, res)); return r; } catch (e) { return String(e); } });
    console.log("DEBUG visible:", JSON.stringify(seen), "| entitlement:", JSON.stringify(ent));
    throw new Error("workspace did not open");
  });
  const projectsBefore = await w.locator("#su-review-project-select option").count();
  await w.click('[data-review-new="1"]');
  // wait until the new project exists before importing into it
  await w.waitForFunction((n) => document.querySelectorAll("#su-review-project-select option").length > n, projectsBefore, { timeout: 10000 }).catch(() => {});
  await w.click('[data-review-tab="import"]');
  const logRows = () => w.evaluate(() => document.querySelectorAll(".su-review-import tbody tr").length);
  const doImport = async (db, query, file) => {
    await w.waitForSelector("#su-review-ref-file", { timeout: 10000 });
    const before = await logRows();
    await w.fill("#su-review-imp-db", db);
    await w.fill("#su-review-imp-query", query);
    await w.setInputFiles("#su-review-ref-file", path.join(ROOT, "tests/e2e/fixtures", file));
    await w.click('[data-review-run-import="1"]');
    // wait for this import's search-log row instead of a fixed pause (a slow run used to overwrite the first import)
    await w.waitForFunction((n) => document.querySelectorAll(".su-review-import tbody tr").length > n, before, { timeout: 15000 }).catch(() => {});
  };
  await doImport("PubMed", "test AND synthetic", "pubmed-sample.nbib");
  await doImport("Scopus", "TITLE-ABS(test)", "scopus-sample.ris");
  const log = await w.evaluate(() => [...document.querySelectorAll(".su-review-import tbody tr")].map((tr) => [...tr.children].map((td) => td.textContent.trim())));
  const row = (db) => log.find((r) => r[0] === db) || [];
  check("review workspace logs both imports", log.length === 2, JSON.stringify(log.map((r) => [r[0], r[4], r[5], r[6]])));
  check("PubMed import adds 3 new records", row("PubMed")[4] === "3" && row("PubMed")[5] === "3" && row("PubMed")[6] === "0");
  check("Scopus import matches 1 duplicate by DOI and adds 2", row("Scopus")[4] === "3" && row("Scopus")[5] === "2" && row("Scopus")[6] === "1");
  await w.close();

  // 2. Author profile with default settings: no automatic "Show more".
  scholarHits.length = 0;
  const a = await ctx.newPage();
  await a.goto("https://scholar.google.com/citations?user=FIXTURE&hl=en");
  await a.waitForTimeout(7000);
  const extraOnProfile = scholarHits.filter((h) => h.includes("cstart=")).length;
  check("profile visit (default) sends no extra Scholar requests", extraOnProfile === 0, `${extraOnProfile} extra: ${scholarHits.filter((h) => h.includes("cstart=")).join(", ")}`);
  const partial = await a.evaluate(() => !!document.querySelector(".su-author-partial-note") && !!document.querySelector("[data-load-all-publications]"));
  check("profile shows partial-stats note and Load all", partial);
  // During a Scholar cooldown, Load all asks Scholar for nothing.
  await sw.evaluate(() => chrome.storage.local.set({ scholarCooldownUntil: Date.now() + 10 * 60 * 1000 }));
  scholarHits.length = 0;
  await a.click("[data-load-all-publications]").catch(() => {});
  await a.waitForTimeout(2500);
  const duringCooldown = scholarHits.filter((h) => h.includes("cstart=")).length;
  const msg = await a.locator("[data-load-all-publications]").first().textContent().catch(() => "");
  check("Load all waits out a Scholar cooldown", duringCooldown === 0 && /Try again in/.test(msg || ""), `${duringCooldown} requests, "${(msg || "").trim()}"`);
  await sw.evaluate(() => chrome.storage.local.remove("scholarCooldownUntil"));
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
