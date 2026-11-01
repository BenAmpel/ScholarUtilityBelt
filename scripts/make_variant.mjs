#!/usr/bin/env node
// Makes the Microsoft Edge or Firefox package from the Chrome one: same code, with the store links and wording
// swapped, and for Firefox the manifest keys it needs. Run after scripts/package_release.sh.
//   node scripts/make_variant.mjs edge      -> output/ScholarUtilityBelt-<version>-edge.zip
//   node scripts/make_variant.mjs firefox   -> output/ScholarUtilityBelt-<version>-firefox.zip
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { variantManifest } from "./variant-manifest.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const target = process.argv[2];
const CWS_LISTING = "https://chromewebstore.google.com/detail/scholar-utility-belt/omcogfcgldfmihfogbffflbocdbjockn";
const CWS_REVIEWS = `${CWS_LISTING}/reviews`;
const TARGETS = {
  edge: {
    store: "Microsoft Edge Add-ons",
    // Until the listing exists its id is unknown; search finds it. Replace with the listing's reviews URL once published.
    reviews: "https://microsoftedge.microsoft.com/addons/search/Scholar%20Utility%20Belt",
    listing: "https://microsoftedge.microsoft.com/addons/search/Scholar%20Utility%20Belt",
  },
  firefox: {
    store: "Firefox Add-ons",
    reviews: "https://addons.mozilla.org/firefox/addon/google-scholar-utility-belt/reviews/",
    listing: "https://addons.mozilla.org/firefox/addon/google-scholar-utility-belt/",
  },
};
const t = TARGETS[target];
if (!t) { console.error("usage: make_variant.mjs edge|firefox"); process.exit(2); }

const manifest = JSON.parse(fs.readFileSync(path.join(ROOT, "manifest.json"), "utf8"));
const version = manifest.version;
const src = path.join(ROOT, "output", `ScholarUtilityBelt-${version}.zip`);
if (!fs.existsSync(src)) { console.error(`missing ${src}: run scripts/package_release.sh first`); process.exit(1); }
const work = path.join(ROOT, "output", "variants", target);
if (!work.startsWith(path.join(ROOT, "output", "variants") + path.sep)) throw new Error("bad work dir");
fs.rmSync(work, { recursive: true, force: true });
fs.mkdirSync(work, { recursive: true });
execFileSync("unzip", ["-q", src, "-d", work]);

// 1. Store links and wording, in every page and bundle.
const swaps = [
  [CWS_REVIEWS, t.reviews],
  [CWS_LISTING, t.listing],
  ["Chrome Web Store", t.store],
  ["Chrome's built-in Prompt API (Gemini Nano)", "the browser's built-in Prompt API"],
  ["Chrome's built-in", "the browser's built-in"],
];
let changed = 0;
const walk = (d) => {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) { if (!p.includes(`${path.sep}data${path.sep}`) && !p.endsWith(`${path.sep}data`)) walk(p); continue; }
    if (!/\.(html|js|css|json)$/.test(e.name) || e.name === "manifest.json") continue;
    let s = fs.readFileSync(p, "utf8"); const before = s;
    for (const [a, b] of swaps) s = s.split(a).join(b);
    if (s !== before) { fs.writeFileSync(p, s); changed++; }
  }
};
walk(work);

// 2. Manifest.
const m = variantManifest(JSON.parse(fs.readFileSync(path.join(work, "manifest.json"), "utf8")), target);
if (m.background?.scripts) for (const f of m.background.scripts) if (!fs.existsSync(path.join(work, f))) throw new Error(`background file missing: ${f}`);
fs.writeFileSync(path.join(work, "manifest.json"), JSON.stringify(m, null, 2) + "\n");

// 3. Zip.
const out = path.join(ROOT, "output", `ScholarUtilityBelt-${version}-${target}.zip`);
fs.rmSync(out, { force: true });
execFileSync("zip", ["-q", "-r", out, "."], { cwd: work });
let left = "";
try { left = execFileSync("grep", ["-rl", "chromewebstore.google.com", work], { encoding: "utf8" }).trim(); } catch (_) { /* none left */ }
console.log(`${target}: ${changed} files rewritten; wrote ${path.relative(ROOT, out)}`);
if (left) console.log("still mentions chromewebstore.google.com:\n" + left);
