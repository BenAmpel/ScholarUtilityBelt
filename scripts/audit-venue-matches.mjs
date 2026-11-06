// Venue match audit: every journal title in the bundled SCImago index is run through the badge matcher with the
// bundled lists (ABDC, ABS, VHB, FNEGE, FT50, UTD24, CORE, CCF, Scholar h5, ERA, Norwegian, Journal Impact Factor). A close (prefix) match that lands on a *different* SCImago title gives a
// journal another journal's rank, so any such match fails the run. Close matches to non-SCImago entries (bilingual
// titles, "UK"/"ASCE" suffixes) are only counted. About 10 s.
//   node --import ./tests/register-loader.mjs scripts/audit-venue-matches.mjs [--list]
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const q = await import(path.join(ROOT, "src/common/quality.js"));
// The larger lists ship gzipped (src/data/<name>.gz); read whichever form is in the tree.
const D = (f) => {
  const p = path.join(ROOT, "src/data", f);
  return fs.existsSync(p + ".gz") ? zlib.gunzipSync(fs.readFileSync(p + ".gz")).toString("utf8") : fs.readFileSync(p, "utf8");
};
const sjr = JSON.parse(D("scimago_2024_quartiles.json")).index;
const h5Index = JSON.parse(D("venue_h5_index.json"));
const absIndex = new Map(D("abs2024.csv").split(/\r?\n/).map((l) => {
  const i = l.lastIndexOf(",");
  return [l.slice(0, i).replace(/^"|"$/g, ""), l.slice(i + 1).trim()];
}).filter(([n, r]) => n && r));
const eraSet = new Set(D("era2023.txt").split(/\r?\n/).map((l) => l.trim()).filter((l) => l && !l.startsWith("#")).map(q.normalizeVenueName).filter(Boolean));
const norwegianMap = new Map(Object.entries(JSON.parse(D("norwegian_compact.json"))).filter(([, lv]) => lv === "1" || lv === "2"));
// Parsed as data-loader.js loadImpactIndex does: the value is after the last comma, names may be quoted.
const impactIndex = new Map(D("journal_impact_2024.csv").split(/\r?\n/).slice(1).map((l) => {
  const i = l.lastIndexOf(",");
  return [l.slice(0, i).trim().replace(/^"|"$/g, ""), l.slice(i + 1).trim()];
}).filter(([n, v]) => n && v));
const idx = q.compileQualityIndex({
  qualityAbdcRanks: D("abdc2025.csv"), qualityVhbRanks: D("vhb2024.csv"), qualityFnegeRanks: D("fnege2025.csv"),
  qualityFt50List: D("ft50.txt"), qualityUtd24List: D("utd24.txt"), qualityCcfRanks: D("ccf_ranks.csv"),
  // sw.js loads both CORE files into one setting.
  qualityCoreRanks: D("core_portal_ranks.csv") + "\n" + D("core_icore2026.csv"),
}, { h5Index, absIndex, quartilesIndex: sjr, eraSet, norwegianMap, impactIndex });
const titles = new Set(Object.keys(sjr));
const wrong = [];
const close = {};
for (const title of titles) {
  for (const b of q.qualityBadgesForVenue(title, idx)) {
    const m = b.metadata || {};
    if (!m.nearMatch || b.kind === "quartile") continue;
    close[b.kind] = (close[b.kind] || 0) + 1;
    if (titles.has(m.nearMatch)) wrong.push(`${title} -> ${m.nearMatch} (${b.text})`);
    if (process.argv.includes("--list")) console.log(`  ${b.kind}: ${title} -> ${m.nearMatch} (${b.text})`);
  }
}
console.log(`${titles.size} SCImago titles checked; close matches by list: ${JSON.stringify(close)}`);
if (wrong.length) {
  console.log(`FAIL ${wrong.length} titles take a different journal's badge:`);
  for (const w of wrong.slice(0, 30)) console.log("  " + w);
  process.exit(1);
}
console.log("PASS no title takes a different journal's badge");
