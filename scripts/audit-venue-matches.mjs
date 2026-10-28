// Venue match audit: every journal title in the bundled SCImago index is run through the badge matcher with the
// bundled lists (ABDC, ABS, Scholar h5). A close (prefix) match that lands on a *different* SCImago title gives a
// journal another journal's rank, so any such match fails the run. Close matches to non-SCImago entries (bilingual
// titles, "UK"/"ASCE" suffixes) are only counted. About 25 s.
//   node --import ./tests/register-loader.mjs scripts/audit-venue-matches.mjs [--list]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const q = await import(path.join(ROOT, "src/common/quality.js"));
const D = (f) => fs.readFileSync(path.join(ROOT, "src/data", f), "utf8");
const sjr = JSON.parse(D("scimago_2024_quartiles.json")).index;
const h5Index = JSON.parse(D("venue_h5_index.json"));
const absIndex = new Map(D("abs2024.csv").split(/\r?\n/).map((l) => {
  const i = l.lastIndexOf(",");
  return [l.slice(0, i).replace(/^"|"$/g, ""), l.slice(i + 1).trim()];
}).filter(([n, r]) => n && r));
const eraSet = new Set(D("era2023.txt").split(/\r?\n/).map((l) => l.trim()).filter((l) => l && !l.startsWith("#")).map(q.normalizeVenueName).filter(Boolean));
const norwegianMap = new Map(Object.entries(JSON.parse(D("norwegian_compact.json"))).filter(([, lv]) => lv === "1" || lv === "2"));
const idx = q.compileQualityIndex({ qualityAbdcRanks: D("abdc2025.csv") }, { h5Index, absIndex, quartilesIndex: sjr, eraSet, norwegianMap });
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
