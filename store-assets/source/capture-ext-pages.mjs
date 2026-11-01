// usage: node store-assets/source/capture-ext-pages.mjs <out dir>. Captures the extension's own pages (library, popup) with five
// real papers saved (no Google Scholar traffic: every non-extension request is aborted).
import { chromium } from "playwright";
import { fileURLToPath } from "node:url";
import fs from "node:fs"; import os from "node:os"; import path from "node:path";
const EXT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../.."); // run `node build.js` first
const EDGE = process.env.EDGE_PATH || "/Volumes/Extreme SSD/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge";
const OUT = process.argv[2];
const prof = fs.mkdtempSync(path.join(os.tmpdir(), "sub-ext-"));
const ctx = await chromium.launchPersistentContext(prof, { executablePath: EDGE, headless: false,
  args: [`--disable-extensions-except=${EXT}`, `--load-extension=${EXT}`, "--headless=new", "--no-first-run", "--lang=en-US"],
  viewport: { width: 1160, height: 760 }, deviceScaleFactor: 2, locale: "en-US" });
await ctx.route("**/*", (r) => { const u = r.request().url(); return /^(chrome-extension|data|blob):/.test(u) ? r.continue() : r.abort(); });
const sw = ctx.serviceWorkers()[0] || await ctx.waitForEvent("serviceworker", { timeout: 20000 });
const id = new URL(sw.url()).host;
for (const pg of ctx.pages()) if (!pg.url().startsWith("chrome-extension")) await pg.close().catch(() => {});
const P = (key, o) => ({ key, savedAt: o.savedAt || "2026-10-2" + (key.length % 9) + "T10:00:00.000Z", tags: [], notes: "", ...o });
const papers = {
  lim2010: P("lim2010", { title: "A meta-analysis of the impact of short-term sleep deprivation on cognitive variables", authorsVenue: "J Lim, DF Dinges - Psychological Bulletin", venue: "Psychological Bulletin", year: 2010, doi: "10.1037/a0018883", tags: ["meta-analysis"], notes: "Effect sizes by cognitive domain; start here." }),
  walker2004: P("walker2004", { title: "Sleep-dependent learning and memory consolidation", authorsVenue: "MP Walker, R Stickgold - Neuron", venue: "Neuron", year: 2004, doi: "10.1016/j.neuron.2004.08.031", tags: ["review"] }),
  chee2004: P("chee2004", { title: "Functional imaging of working memory after 24 hr of total sleep deprivation", authorsVenue: "MWL Chee, WC Choo - The Journal of Neuroscience", venue: "The Journal of Neuroscience", year: 2004, tags: ["fMRI"] }),
  frenda2016: P("frenda2016", { title: "Sleep less, think worse: The effect of sleep deprivation on working memory", authorsVenue: "SJ Frenda, KM Fenn - Journal of Applied Research in Memory and Cognition", venue: "Journal of Applied Research in Memory and Cognition", year: 2016, tags: ["working memory"] }),
  mu2005: P("mu2005", { title: "Decreased cortical response to verbal working memory following sleep deprivation", authorsVenue: "Q Mu, Z Nahas, KA Johnson, K Yamanaka, A Mishory - Sleep", venue: "Sleep", year: 2005, tags: ["fMRI"] }),
};
const p = await ctx.newPage();
await p.goto(`chrome-extension://${id}/src/library/library.html`);
await p.waitForTimeout(2500);
await p.evaluate(async (papers) => { await chrome.storage.local.set({ savedPapers: papers }); }, papers);
await p.reload(); await p.waitForTimeout(2500);
await p.getByText("Dismiss", { exact: true }).first().click().catch(() => {});
await p.waitForTimeout(800);
await p.screenshot({ path: path.join(OUT, "library.png") });
await p.goto(`chrome-extension://${id}/src/popup/popup.html`); await p.setViewportSize({ width: 380, height: 600 }); await p.waitForTimeout(1500);
await p.screenshot({ path: path.join(OUT, "popup.png"), fullPage: true });
await ctx.close();
console.log("ok", id);
