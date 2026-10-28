// Idle cost check: an open Scholar tab must not keep the CPU busy once the extension has drawn its badges.
// (A 2026-02 store review reported "1 cpu core busy all the time" with a Scholar tab open.) Loads the built extension in
// headless Edge on the offline Scholar fixtures (results page and author profile), waits for the page to settle, then
// measures the page's main-thread task time and the extension's work over an idle window. No network, no Scholar requests.
//   node build.js && node scripts/e2e/idle.mjs          EDGE_PATH=/path/to/Microsoft\ Edge to override the browser
import { chromium } from "playwright";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const EXT = path.resolve(ROOT, process.env.EXT_DIR || ".");
const EDGE = process.env.EDGE_PATH || "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const FX = (n) => fs.readFileSync(path.join(ROOT, "tests/e2e/fixtures", n), "utf8");
const IDLE_MS = Number(process.env.IDLE_MS || 10000);
// A busy loop costs ~1000 ms of task time per idle second; a quiet tab costs a few ms. The limit sits far from both.
const LIMIT_MS_PER_S = 50;

const results = [];
const check = (name, ok, detail = "") => { results.push(ok); console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? "  (" + detail + ")" : ""}`); };
const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), "sub-idle-"));
const ctx = await chromium.launchPersistentContext(profileDir, {
  executablePath: EDGE, headless: false,
  args: [`--disable-extensions-except=${EXT}`, `--load-extension=${EXT}`, "--headless=new", "--no-first-run"],
  viewport: { width: 1400, height: 900 },
});
await ctx.route("**/*", (route) => {
  const u = new URL(route.request().url());
  if (/^scholar\.google\./.test(u.hostname)) {
    if (u.pathname === "/scholar" && !u.searchParams.has("start")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-results.html") });
    if (u.pathname === "/citations" && u.searchParams.get("user") === "FIXTURE" && !u.searchParams.has("cstart")) return route.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-profile.html") });
    return route.fulfill({ status: 204, body: "" });
  }
  if (u.protocol === "chrome-extension:" || u.protocol === "data:") return route.continue();
  return route.abort();
});
try {
  await (ctx.serviceWorkers()[0] || ctx.waitForEvent("serviceworker", { timeout: 20000 }));
  const pages = [
    ["results page", "https://scholar.google.com/scholar?hl=en&q=sleep+deprivation+working+memory", ".su-badge"],
    ["author profile", "https://scholar.google.com/citations?user=FIXTURE&hl=en", "#gsc_rsb_cit"],
  ];
  for (const [label, url, ready] of pages) {
    const p = await ctx.newPage();
    try {
    await p.goto(url);
    await p.waitForSelector(ready, { timeout: 20000 }).catch(() => {});
    await p.waitForTimeout(4000); // let first render, enrichment and the 1 s sweeps finish
    const cdp = await ctx.newCDPSession(p);
    await cdp.send("Performance.enable");
    const metric = async () => Object.fromEntries((await cdp.send("Performance.getMetrics")).metrics.map((m) => [m.name, m.value]));
    // Count DOM changes during the idle window: a self-triggering observer shows up as a steady stream of mutations.
    await p.evaluate(() => {
      window.__idleMutations = 0;
      new MutationObserver((ms) => { window.__idleMutations += ms.length; }).observe(document.documentElement, { childList: true, subtree: true, attributes: true, characterData: true });
    });
    const a = await metric();
    await p.waitForTimeout(IDLE_MS);
    const b = await metric();
    const mutations = await p.evaluate(() => window.__idleMutations);
    const taskMs = (b.TaskDuration - a.TaskDuration) * 1000;
    const scriptMs = (b.ScriptDuration - a.ScriptDuration) * 1000;
    const perS = taskMs / (IDLE_MS / 1000);
    check(`${label}: idle tab stays quiet`, perS < LIMIT_MS_PER_S,
      `${taskMs.toFixed(0)} ms tasks (${scriptMs.toFixed(0)} ms script) in ${IDLE_MS / 1000} s idle = ${perS.toFixed(1)} ms/s, limit ${LIMIT_MS_PER_S}; ${mutations} DOM changes`);
    check(`${label}: no DOM churn while idle`, mutations < 20, `${mutations} mutation records`);
    } catch (e) {
      // A page so busy that it stops answering is the worst case this check exists for.
      check(`${label}: page answers while idle`, false, `the page stopped responding: ${String(e.message || e).split("\n")[0]}`);
    }
    await p.close().catch(() => {});
  }
} finally {
  await ctx.close();
  fs.rmSync(profileDir, { recursive: true, force: true });
}
const failed = results.filter((ok) => !ok).length;
console.log(failed ? `${failed} idle check(s) failed` : "Idle checks passed");
process.exit(failed ? 1 : 0);
