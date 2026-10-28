#!/usr/bin/env node
// WCAG 2.1 AA check (axe-core) of the extension UI in light and dark mode: its elements on the Scholar fixtures, and its
// own Options, Library and popup pages. Fixture pages are local; nothing reaches Google. Run: npm run a11y
import { chromium } from "playwright";
import fs from "node:fs"; import os from "node:os"; import path from "node:path"; import { createRequire } from "node:module"; import { fileURLToPath } from "node:url";
const require = createRequire(import.meta.url);
const AXE = fs.readFileSync(require.resolve("axe-core/axe.min.js"), "utf8");
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../.."); const EDGE = "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const FX = (n) => fs.readFileSync(path.join(ROOT, "tests/e2e/fixtures", n), "utf8");
const dir = fs.mkdtempSync(path.join(os.tmpdir(), "sub-a11y-"));
const ctx = await chromium.launchPersistentContext(dir, { executablePath: EDGE, headless: false, args: [`--disable-extensions-except=${ROOT}`, `--load-extension=${ROOT}`, "--headless=new", "--no-first-run"], viewport: { width: 1400, height: 900 } });
await ctx.route("**/*", (r) => { const u = new URL(r.request().url()); if (/^scholar\.google\./.test(u.hostname)) { if (u.pathname === "/scholar") return r.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-results.html") }); if (u.pathname === "/citations") return r.fulfill({ status: 200, contentType: "text/html", body: FX("scholar-profile.html") }); return r.fulfill({ status: 204, body: "" }); } if (u.protocol === "chrome-extension:") return r.continue(); return r.abort(); });
const sw = ctx.serviceWorkers()[0] || await ctx.waitForEvent("serviceworker"); const id = new URL(sw.url()).host;
const summary = {};
async function audit(url, label, onlyExtension, scheme) {
  const p = await ctx.newPage(); await p.emulateMedia({ colorScheme: scheme });
  await p.goto(url); await p.waitForTimeout(3500);
  await p.evaluate(AXE);
  const res = await p.evaluate(async () => (await window.axe.run(document, { resultTypes: ["violations"], runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21aa"] } })).violations);
  const rows = [];
  for (const v of res) {
    if (v.id === "html-has-lang") continue; // the fixture page itself, not the extension
    const nodes = v.nodes.filter((n) => !onlyExtension || n.target.join(" ").includes("su-") || (n.html || "").includes("su-"));
    if (nodes.length) rows.push({ id: v.id, impact: v.impact, count: nodes.length, sample: nodes[0].target.join(" ").slice(0, 90), msg: (nodes[0].failureSummary || "").split("\n")[1]?.trim().slice(0, 110) });
  }
  summary[`${label} (${scheme})`] = rows;
  await p.close();
}
for (const scheme of ["light", "dark"]) {
  await audit("https://scholar.google.com/scholar?hl=en&q=x", "results", true, scheme);
  await audit("https://scholar.google.com/citations?user=FIXTURE&hl=en", "profile", true, scheme);
  for (const pg of ["options/options.html", "library/library.html", "popup/popup.html"]) await audit(`chrome-extension://${id}/src/${pg}`, pg.split("/")[0], false, scheme);
}
let total = 0;
for (const [k, rows] of Object.entries(summary)) { total += rows.length; console.log(`== ${k}: ${rows.length ? "" : "no violations"}`); for (const r of rows) console.log(`   ${r.impact} ${r.id} x${r.count} | ${r.sample} | ${r.msg}`); }
await ctx.close(); fs.rmSync(dir, { recursive: true, force: true });
console.log(`\n${total} violation group(s)`);
process.exit(total ? 1 : 0);
