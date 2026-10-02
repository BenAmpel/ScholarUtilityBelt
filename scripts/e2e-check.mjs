#!/usr/bin/env node
/**
 * E2E battery: loads the real unpacked extension into a real Chromium via
 * Playwright and drives it against local HTML fixtures — not live
 * scholar.google.com. Complements tests/*.test.mjs (pure-logic unit tests)
 * by covering what only shows up with the actual extension running in an
 * actual browser: content-script injection, the background service
 * worker, and the freemium paid/free render paths side by side.
 *
 * Why fixtures instead of the real site: an earlier version of this script
 * navigated live Scholar pages and got blocked by Google's bot detection
 * mid-run ("Our systems have detected unusual traffic..."), which not only
 * skipped real checks but also polluted page console output with the
 * captcha page's own broken inline script (a stray
 * "solveSimpleChallenge is not defined" error that looked like our bug and
 * wasn't). Fixtures make every run deterministic, fast, and offline-safe.
 *
 * The fixtures are real captured/reconstructed Scholar markup (see
 * scripts/fixtures/*.html), served via Playwright's request interception:
 * the browser still navigates to https://scholar.google.com/... (so the
 * extension's manifest content_scripts.matches rules apply exactly as in
 * production), but the response body comes from disk, never the network.
 * Non-Google external APIs (OpenAlex, Semantic Scholar, ExtensionPay,
 * etc.) are NOT mocked — those hit the real network for genuine
 * integration coverage; only scholar.google.com is intercepted, since
 * that's the one origin with bot-detection risk.
 *
 * Usage:
 *   npm run build && node scripts/e2e-check.mjs
 *   (On a heavily loaded machine: E2E_TIMEOUT_MS=900000 E2E_SLOW_FACTOR=3 node scripts/e2e-check.mjs)
 *
 * Exits non-zero if any check fails.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURES_DIR = path.join(__dirname, "fixtures");
// A fresh temp profile per run, not a reused fixed directory: a persistent
// context's profile can be left with a stale Chrome SingletonLock (or a
// crash-recovery prompt that blocks JS eval) if a previous run was
// interrupted mid-flight — this bit us in practice, turning "run it twice
// to check determinism" into an indefinite hang on the second run. A
// disposable directory makes every run hermetic instead.
const USER_DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), "su-e2e-profile-"));

const SEARCH_FIXTURE = fs.readFileSync(path.join(FIXTURES_DIR, "scholar-search.html"), "utf8");
const AUTHOR_FIXTURE = fs.readFileSync(path.join(FIXTURES_DIR, "scholar-author.html"), "utf8");

const SEARCH_URL = "https://scholar.google.com/scholar?q=convolutional+neural+network+image+classification";
const AUTHOR_URL = "https://scholar.google.com/citations?user=JicYPdAAAAAJ";

// Fixed settle times and selector timeouts were tuned on an idle machine; multiply them on a loaded one.
const SLOW = Number(process.env.E2E_SLOW_FACTOR) || 1;

const results = [];
let consoleErrors = [];

// OpenAlex rate-limits unauthenticated callers hard (documented, expected
// since Feb 2026 — see the API-key onboarding banner). This test profile
// has no key configured, so author-page bibliometrics (self-citation,
// p-index venue-year distributions, related-works) legitimately hit 429s
// here. The extension already degrades gracefully on this (try/catch,
// null results) — it's not a bug, so don't fail the suite on it. Anything
// else stays a hard failure.
const EXPECTED_NOISE_DOMAINS = [/api\.openalex\.org/];

function isFromExpectedDomain(url) {
  return EXPECTED_NOISE_DOMAINS.some((re) => re.test(url));
}

// Chrome's generic "Failed to load resource: the server responded with a
// status of NNN ()" console message never includes the URL (only
// msg.location() does, which Playwright doesn't surface via msg.text()) —
// so it can't be domain-filtered here. It's always a pure duplicate of a
// `response` event with a non-2xx status, which we DO filter by URL below,
// so it's safe to drop this one unconditionally rather than mis-attribute it.
const GENERIC_FAILED_RESOURCE = /^Failed to load resource: the server responded with a status of \d+/;

function recordConsole(page) {
  page.on("console", (msg) => {
    if (msg.type() !== "error") return;
    if (GENERIC_FAILED_RESOURCE.test(msg.text())) return;
    consoleErrors.push(`[console] ${msg.text()}`);
  });
  page.on("pageerror", (err) => {
    consoleErrors.push(`[pageerror] ${err.message}`);
  });
  page.on("requestfailed", (req) => {
    if (isFromExpectedDomain(req.url())) return;
    consoleErrors.push(`[requestfailed] ${req.url()} — ${req.failure()?.errorText}`);
  });
  page.on("response", (res) => {
    if (res.status() < 400) return;
    if (isFromExpectedDomain(res.url())) return;
    consoleErrors.push(`[response ${res.status()}] ${res.url()}`);
  });
}

async function check(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`  ✓ ${name}`);
  } catch (err) {
    results.push({ name, ok: false, error: err.message });
    console.log(`  ✗ ${name}\n      ${err.message}`);
  }
}

// App Pass (joinapppass.com) is mocked, not hit for real: this battery asserts how
// often and when the extension contacts it (never, until the user opts in), which
// needs a log of every request, plus a reply we can flip between "no pass" and "ok".
const appPassRequests = [];
let appPassReply = { status: "no_apppass" };

async function routeAppPass(context) {
  await context.route("https://joinapppass.com/**", (route) => {
    const req = route.request();
    appPassRequests.push({ url: req.url(), type: req.resourceType(), extensionid: req.headers()["extensionid"] });
    if (req.resourceType() === "document") {
      return route.fulfill({ status: 200, contentType: "text/html; charset=utf-8", body: "<title>App Pass (mock)</title>" });
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      headers: { "access-control-allow-origin": req.headers()["origin"] || "*", "access-control-allow-credentials": "true" },
      body: JSON.stringify(appPassReply),
    });
  });
}

async function routeScholarToFixtures(context) {
  await context.route("https://scholar.google.com/**", (route) => {
    const url = route.request().url();
    if (url.includes("/citations")) {
      return route.fulfill({ status: 200, contentType: "text/html; charset=utf-8", body: AUTHOR_FIXTURE });
    }
    if (url.includes("/scholar")) {
      return route.fulfill({ status: 200, contentType: "text/html; charset=utf-8", body: SEARCH_FIXTURE });
    }
    // Any other scholar.google.com request (assets, XHR follow-ups) — no
    // real network call, no artificial content either; just no-op it.
    return route.fulfill({ status: 204, body: "" });
  });
}

async function main() {
  console.log("\nE2E battery — Scholar Utility Belt (fixture-driven, no live Scholar traffic)\n");

  const context = await chromium.launchPersistentContext(USER_DATA_DIR, {
    headless: false,
    args: [`--disable-extensions-except=${ROOT}`, `--load-extension=${ROOT}`],
  });

  try {
    await routeScholarToFixtures(context);
    await routeAppPass(context);

    // ── Extension identity + service worker ──────────────────────────────
    let sw = context.serviceWorkers()[0];
    if (!sw) sw = await context.waitForEvent("serviceworker", { timeout: 15000 });
    const extensionId = new URL(sw.url()).host;
    console.log(`Extension ID: ${extensionId}\n`);

    await check("service worker registers without throwing", async () => {
      const alive = await sw.evaluate(() => typeof chrome !== "undefined" && typeof chrome.runtime !== "undefined");
      assert.equal(alive, true);
    });

    await check("ExtPay library loaded into the service worker global scope", async () => {
      // `const extpay = ExtPay(...)` in sw.js is a top-level const, which does
      // NOT attach to globalThis even in a classic script — only `var`
      // declarations do. Check the imported library itself instead of the
      // local binding.
      const type = await sw.evaluate(() => typeof globalThis.ExtPay);
      assert.equal(type, "function");
    });

    // Note: we don't test getEntitlementStatus by sending it a message from
    // the service worker's own context — Chrome deliberately excludes a
    // sender from its own runtime.onMessage listeners, so that message
    // would never be delivered regardless of whether the handler works.
    // The paid/free author-page checks below exercise the real handler from
    // a genuine content-script sender instead, which is both realistic and
    // the only context that can actually reach it.

    // ── Search-results page (fixture) ────────────────────────────────────
    console.log("\nSearch-results page (fixture)");
    const searchPage = await context.newPage();
    recordConsole(searchPage);
    await searchPage.goto(SEARCH_URL, { waitUntil: "domcontentloaded" });
    await searchPage.waitForTimeout(3000 * SLOW);

    await check("badges/action-grid rendered on at least one result", async () => {
      const count = await searchPage.evaluate(() => document.querySelectorAll(".su-quality, .su-related-works-btn, [data-act]").length);
      assert.ok(count > 0, `expected extension-rendered elements, found ${count}`);
    });

    await check("filter row shows Filter / Review workspace / Related buttons", async () => {
      const found = await searchPage.evaluate(() => ({
        filter: !!document.querySelector(".su-filter-toggle"),
        review: !!document.querySelector(".su-review-workspace"),
        related: !!document.querySelector(".su-related-works-btn"),
      }));
      assert.equal(found.filter, true, "Filter toggle missing");
      assert.equal(found.review, true, "Review workspace button missing");
      assert.equal(found.related, true, "Related (OpenAlex) button missing");
    });

    await check("Related (OpenAlex) panel opens and shows content", async () => {
      await searchPage.click(".su-related-works-btn");
      await searchPage.waitForSelector("#su-related-works", { timeout: 20000 * SLOW });
      const hasContent = await searchPage.evaluate(() => (document.getElementById("su-related-works")?.textContent || "").trim().length > 0);
      assert.equal(hasContent, true);
    });

    await check("Related (OpenAlex) panel re-opens instantly from cache", async () => {
      await searchPage.click(".su-related-works-btn"); // close
      await searchPage.waitForTimeout(150);
      const start = Date.now();
      await searchPage.click(".su-related-works-btn"); // re-open
      await searchPage.waitForSelector("#su-related-works", { timeout: 5000 });
      const elapsed = Date.now() - start;
      assert.ok(elapsed < 2000, `cached re-open took ${elapsed}ms — cache may not be hitting`);
    });

    await check("Review workspace lazy module loads and opens", async () => {
      await sw.evaluate(() => chrome.storage.local.set({ grandfathered: true }));
      await searchPage.click(".su-review-workspace");
      await searchPage.waitForSelector("#su-review-overlay.su-visible", { timeout: 20000 * SLOW });
    });

    await check("no console errors on the search-results fixture", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // ── Author-profile page (fixture): paid/grandfathered path ───────────
    console.log("\nAuthor-profile page, fixture (grandfathered = true)");
    consoleErrors = [];
    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: true }));
    const authorPage = await context.newPage();
    recordConsole(authorPage);
    await authorPage.goto(AUTHOR_URL, { waitUntil: "domcontentloaded" });
    // Wait for the paid-only control rather than a fixed delay (checks below report what is missing).
    await authorPage.waitForSelector("[data-narrative-toggle]", { timeout: 30000 * SLOW }).catch(() => {});

    await check("author stats render", async () => {
      const present = await authorPage.evaluate(() => !!document.getElementById("su-author-stats"));
      assert.equal(present, true);
    });

    await check("paid user sees Compare authors / Narrative / PoP report, no lock", async () => {
      const state = await authorPage.evaluate(() => ({
        compare: !!document.getElementById("su-compare-authors-btn"),
        narrative: !!document.querySelector("[data-narrative-toggle]"),
        pop: !!document.querySelector("[data-pop-report]"),
        locked: !!document.querySelector(".su-locked-feature"),
      }));
      assert.equal(state.compare, true, "Compare authors button missing");
      assert.equal(state.narrative, true, "Narrative button missing for paid user");
      assert.equal(state.pop, true, "PoP report button missing");
      assert.equal(state.locked, false, "locked-feature block should not show for a paid/grandfathered user");
    });

    const paidMetricsCount = await authorPage.evaluate(() => document.querySelectorAll(".su-stat-item").length);

    await check("no console errors on the paid author-page fixture render", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // ── Author-profile page (fixture): free path ─────────────────────────
    console.log("\nAuthor-profile page, fixture (grandfathered = false)");
    consoleErrors = [];
    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: false }));
    await authorPage.reload({ waitUntil: "domcontentloaded" });
    await authorPage.waitForSelector(".su-locked-feature", { timeout: 30000 * SLOW }).catch(() => {});

    await check("free user sees the Pro locked-feature block", async () => {
      const state = await authorPage.evaluate(() => ({
        locked: !!document.querySelector(".su-locked-feature"),
        unlockBtn: !!document.querySelector("[data-unlock-feature]"),
      }));
      assert.equal(state.locked, true, "locked-feature block should render for a free user");
      assert.equal(state.unlockBtn, true, "Unlock button missing");
    });

    await check("free user's stat row has fewer metrics than the paid render (p-index/FWCI hidden)", async () => {
      const freeMetricsCount = await authorPage.evaluate(() => document.querySelectorAll(".su-stat-item").length);
      assert.ok(freeMetricsCount < paidMetricsCount, `expected fewer metrics for free user: paid=${paidMetricsCount} free=${freeMetricsCount}`);
    });

    await check("clicking Compare authors as a free user does not open the real overlay", async () => {
      const before = await authorPage.evaluate(() => !!document.getElementById("su-author-compare-overlay")?.classList.contains("su-visible"));
      assert.equal(before, false);
      await authorPage.click("#su-compare-authors-btn");
      await authorPage.waitForTimeout(1500);
      const after = await authorPage.evaluate(() => !!document.getElementById("su-author-compare-overlay")?.classList.contains("su-visible"));
      assert.equal(after, false, "compare overlay should stay closed for a free user — upsell should fire instead");
    });

    await check("no console errors on the free author-page fixture render", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // Restore the real profile's normal state for any manual testing after this run.
    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: true }));

    // ── Options page ──────────────────────────────────────────────────────
    console.log("\nOptions page");
    consoleErrors = [];
    const optionsPage = await context.newPage();
    recordConsole(optionsPage);
    await optionsPage.goto(`chrome-extension://${extensionId}/src/options/options.html`, { waitUntil: "domcontentloaded" });
    await optionsPage.waitForTimeout(1500);

    await check("options page loads and shows a Pro status", async () => {
      const statusText = await optionsPage.evaluate(() => document.getElementById("su-pro-status")?.textContent?.trim() || "");
      assert.ok(statusText.length > 0, "su-pro-status is empty");
    });

    await check("no console errors on the options page", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // ── App Pass (opt-in) ─────────────────────────────────────────────────
    console.log("\nApp Pass (opt-in)");

    await check("a default install never contacts joinapppass.com (incl. the free-user pages above)", async () => {
      assert.deepEqual(appPassRequests, []);
    });

    await check("grandfathered users don't see the App Pass block", async () => {
      const hidden = await optionsPage.evaluate(() => document.getElementById("su-apppass").hidden);
      assert.equal(hidden, true);
    });

    consoleErrors = [];
    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: false }));
    await optionsPage.reload({ waitUntil: "domcontentloaded" });
    await optionsPage.waitForTimeout(1500);

    const appPassText = () => optionsPage.evaluate(() => document.getElementById("su-apppass-status").textContent);
    const proText = () => optionsPage.evaluate(() => document.getElementById("su-pro-status").textContent);
    const clickButton = (label) =>
      optionsPage.evaluate((l) => {
        const b = [...document.querySelectorAll("#su-apppass-actions button")].find((x) => x.textContent.trim() === l);
        if (!b) throw new Error(`no "${l}" button`);
        b.click();
      }, label);

    await check("a free user sees App Pass offered but off, and nothing was sent", async () => {
      const hidden = await optionsPage.evaluate(() => document.getElementById("su-apppass").hidden);
      assert.equal(hidden, false);
      assert.match(await appPassText(), /App Pass is off/);
      assert.equal(appPassRequests.length, 0);
    });

    await check("Use App Pass opts in and asks joinapppass.com with this extension's id", async () => {
      await clickButton("Use App Pass");
      await optionsPage.waitForTimeout(2000);
      assert.equal(appPassRequests.length, 1, JSON.stringify(appPassRequests));
      assert.equal(appPassRequests[0].url, "https://joinapppass.com/api/check-app-pass");
      assert.equal(appPassRequests[0].extensionid, extensionId);
      assert.match(await appPassText(), /No active App Pass/);
      assert.match(await proText(), /^Free plan/);
    });

    await check("Activate App Pass opens joinapppass.com/add/<extension id>", async () => {
      const [tab] = await Promise.all([
        context.waitForEvent("page", { timeout: 10000 }),
        clickButton("Activate App Pass"),
      ]);
      await tab.waitForLoadState("domcontentloaded");
      assert.equal(tab.url(), `https://joinapppass.com/add/${extensionId}`);
      await tab.close();
      await optionsPage.bringToFront();
      await optionsPage.waitForTimeout(1500);
    });

    await check("an active App Pass unlocks Pro and is cached rather than re-checked", async () => {
      appPassReply = { status: "ok", email: "test@example.com", appPassToken: "tok" };
      await clickButton("Check again");
      await optionsPage.waitForTimeout(1500);
      assert.match(await appPassText(), /App Pass is active/);
      assert.match(await proText(), /unlocked through your App Pass/);
      const before = appPassRequests.length;
      await optionsPage.reload({ waitUntil: "domcontentloaded" });
      await optionsPage.waitForTimeout(1500);
      assert.match(await proText(), /unlocked through your App Pass/);
      assert.equal(appPassRequests.length, before, "a valid pass should be cached, not re-checked on every load");
    });

    await check("an active App Pass unlocks Pro features on an author page", async () => {
      await authorPage.reload({ waitUntil: "domcontentloaded" });
      // Wait for the paid-only control rather than a fixed delay: free users also have a
      // Compare button, and entitlement resolution includes a real ExtensionPay round trip.
      await authorPage.waitForSelector("[data-narrative-toggle]", { timeout: 25000 });
      const locked = await authorPage.evaluate(() => !!document.querySelector(".su-locked-feature"));
      assert.equal(locked, false, "Pro should be unlocked for an App Pass subscriber");
    });

    await check("Turn off App Pass stops all contact and returns to the free plan", async () => {
      await clickButton("Turn off App Pass");
      await optionsPage.waitForTimeout(1500);
      assert.match(await appPassText(), /App Pass is off/);
      assert.match(await proText(), /^Free plan/);
      const stored = await sw.evaluate(() => chrome.storage.local.get(["appPassOptIn", "appPassCache"]));
      assert.equal(stored.appPassOptIn, false);
      assert.equal(stored.appPassCache, undefined);
      const before = appPassRequests.length;
      await authorPage.reload({ waitUntil: "domcontentloaded" });
      await authorPage.waitForSelector(".su-locked-feature", { timeout: 25000 });
      assert.equal(appPassRequests.length, before, "no joinapppass.com traffic once turned off");
    });

    await check("no console errors across the App Pass flow", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // Restore the normal profile state for manual testing after this run.
    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: true, appPassOptIn: false }));

    // ── Free trial + grandfathering scope ─────────────────────────────────
    // ExtensionPay is replaced inside the service worker with a stub for this
    // section: a real trial can't be started or aged on demand, and the point
    // here is how our code reads the user ExtPay reports, not ExtPay itself.
    console.log("\nFree trial and grandfathering scope (ExtensionPay stubbed in the service worker)");
    consoleErrors = [];
    const DAY = 24 * 60 * 60 * 1000;
    const stubExtPay = (user) =>
      sw.evaluate((u) => {
        globalThis.__realExtPay ||= globalThis.ExtPay;
        globalThis.__extpayCalls = 0;
        globalThis.__trialPeriod = null;
        globalThis.ExtPay = () => ({
          getUser: async () => {
            globalThis.__extpayCalls++;
            return { paid: false, ...u, trialStartedAt: u.trialStartedAt ? new Date(u.trialStartedAt) : null };
          },
          openTrialPage: async (period) => { globalThis.__trialPeriod = period; },
          openPaymentPage: async () => {},
          openLoginPage: async () => {},
        });
      }, user);
    const swState = () => sw.evaluate(() => ({ calls: globalThis.__extpayCalls, period: globalThis.__trialPeriod }));
    const askEntitlement = (needPurchased) =>
      optionsPage.evaluate(
        (n) => new Promise((resolve) => chrome.runtime.sendMessage({ action: "getEntitlementStatus", needPurchased: n }, resolve)),
        needPurchased
      );
    const reloadAuthor = async () => {
      await authorPage.reload({ waitUntil: "domcontentloaded" });
    };

    await sw.evaluate(() => chrome.storage.local.set({ grandfathered: false }));

    await check("a free user's locked feature offers the 14-day trial", async () => {
      await stubExtPay({ paid: false });
      await reloadAuthor();
      await authorPage.waitForSelector(".su-locked-feature [data-start-trial]", { timeout: 25000 });
      const text = await authorPage.evaluate(() => document.querySelector("[data-start-trial]").textContent.trim());
      assert.equal(text, "Try free for 14 days");
    });

    await check("Options' trial button opens ExtensionPay's trial page with the 14-day period", async () => {
      await optionsPage.reload({ waitUntil: "domcontentloaded" });
      await optionsPage.waitForSelector("#su-pro-actions button", { timeout: 15000 });
      await optionsPage.evaluate(() => {
        const b = [...document.querySelectorAll("#su-pro-actions button")].find((x) => /free trial/.test(x.textContent));
        if (!b) throw new Error("no trial button");
        b.click();
      });
      await optionsPage.waitForTimeout(1000);
      assert.equal((await swState()).period, "14-day");
    });

    await check("a trial already used is reported, not reopened", async () => {
      await stubExtPay({ paid: false, trialStartedAt: Date.now() - 30 * DAY });
      await optionsPage.reload({ waitUntil: "domcontentloaded" });
      await optionsPage.waitForSelector("#su-pro-actions button", { timeout: 15000 });
      await optionsPage.evaluate(() => {
        [...document.querySelectorAll("#su-pro-actions button")].find((x) => /free trial/.test(x.textContent)).click();
      });
      await optionsPage.waitForTimeout(1000);
      assert.match(await optionsPage.evaluate(() => document.getElementById("su-pro-status").textContent), /already used your free trial/);
      assert.equal((await swState()).period, null);
    });

    await check("an active trial unlocks Pro and says when it ends", async () => {
      await stubExtPay({ paid: false, trialStartedAt: Date.now() - DAY });
      await reloadAuthor();
      await authorPage.waitForSelector("[data-narrative-toggle]", { timeout: 25000 });
      assert.equal(await authorPage.evaluate(() => !!document.querySelector(".su-locked-feature")), false);
      await optionsPage.reload({ waitUntil: "domcontentloaded" });
      await optionsPage.waitForTimeout(1500);
      assert.match(await optionsPage.evaluate(() => document.getElementById("su-pro-status").textContent), /free trial, ends/);
    });

    await check("an expired trial locks Pro again", async () => {
      await stubExtPay({ paid: false, trialStartedAt: Date.now() - 20 * DAY });
      await reloadAuthor();
      await authorPage.waitForSelector(".su-locked-feature", { timeout: 25000 });
    });

    await check("grandfathered users are answered offline unless a purchase check is asked for", async () => {
      await sw.evaluate(() => chrome.storage.local.set({ grandfathered: true }));
      await stubExtPay({ paid: false });
      const fast = await askEntitlement(false);
      assert.deepEqual(fast, { paid: true, tier: "grandfathered", purchased: false });
      assert.equal((await swState()).calls, 0, "default path must not call ExtensionPay for a grandfathered user");
    });

    await check("a grandfathered user without a purchase is NOT purchased (new Pro features stay locked)", async () => {
      const res = await askEntitlement(true);
      assert.deepEqual(res, { paid: true, tier: "grandfathered", purchased: false });
      assert.equal((await swState()).calls, 1);
    });

    await check("a grandfathered user who bought Pro or is in a trial counts as purchased", async () => {
      await stubExtPay({ paid: true, plan: { nickname: "lifetime" } });
      assert.deepEqual(await askEntitlement(true), { paid: true, tier: "grandfathered", purchased: true });
      await stubExtPay({ paid: false, trialStartedAt: Date.now() - DAY });
      assert.equal((await askEntitlement(true)).purchased, true);
    });

    await check("no console errors across the trial flow", async () => {
      assert.deepEqual(consoleErrors, []);
    });

    // Put the real ExtensionPay and the normal profile state back.
    await sw.evaluate(() => {
      if (globalThis.__realExtPay) globalThis.ExtPay = globalThis.__realExtPay;
      return chrome.storage.local.set({ grandfathered: true, appPassOptIn: false });
    });
  } finally {
    await context.close().catch(() => {});
    fs.rmSync(USER_DATA_DIR, { recursive: true, force: true });
  }

  console.log("\n" + "─".repeat(50));
  const passed = results.filter((r) => r.ok).length;
  const failed = results.filter((r) => !r.ok);
  console.log(`${passed}/${results.length} checks passed`);
  if (failed.length) {
    console.log("\nFailed:");
    for (const f of failed) console.log(`  ✗ ${f.name}: ${f.error}`);
    process.exitCode = 1;
  }
}

// A hard ceiling so a stuck browser/profile (seen once in practice — a
// corrupted persistent-profile lock hung indefinitely) fails loudly within
// two minutes instead of hanging a CI job or this script forever.
const OVERALL_TIMEOUT_MS = Number(process.env.E2E_TIMEOUT_MS) || 120000;
const timeout = new Promise((_, reject) =>
  setTimeout(() => reject(new Error(`E2E battery exceeded ${OVERALL_TIMEOUT_MS}ms overall timeout`)), OVERALL_TIMEOUT_MS)
);

Promise.race([main(), timeout]).catch((err) => {
  console.error("E2E battery crashed:", err.message || err);
  process.exitCode = 1;
  // Promise.race doesn't cancel the loser — force-exit so a hung browser
  // process doesn't keep the script (and whatever invoked it) alive.
  setTimeout(() => process.exit(process.exitCode || 1), 500);
});
