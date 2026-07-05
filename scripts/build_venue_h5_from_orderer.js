#!/usr/bin/env node
/**
 * Build src/data/venue_h5_index.json from Google-Scholar-Orderer data/core-rankings.json.
 * Run after extracting the Orderer zip, e.g.:
 *   node scripts/build_venue_h5_from_orderer.js /path/to/Google-Scholar-Orderer-main/data/core-rankings.json
 * Or from repo root with extracted zip in project:
 *   node scripts/build_venue_h5_from_orderer.js "Google-Scholar-Orderer-main/data/core-rankings.json"
 *
 * Uses the same normalization as src/common/quality.js normalizeVenueName() so keys match at runtime.
 */

const fs = require("fs");
const path = require("path");

function normalizeVenueName(s) {
  let t = String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  t = t.replace(/^proceedings of the\s+/, "").replace(/^proceedings of\s+/, "").trim();
  t = t.replace(/^the\s+/, "").trim();
  t = t.replace(/^\d+(st|nd|rd|th)\s+/, "").trim();
  t = t.replace(/^(19|20)\d{2}\s+/, "").trim();
  t = t.replace(/\s+\d+\s*-\s*\d+$/, "").trim();
  while (/^\S.*\s+\d+\s+\d+$/.test(t)) {
    t = t.replace(/\s+\d+\s+\d+$/, "").trim();
  }
  t = t.replace(/\s+\d+$/, "").trim();
  const parentheticalAbbrevs = /\s+(?:tois|isr|isj|jmis|misq|ejis|jais|jsis|dss|kais|tkdd|tocs|tods|tis)$/i;
  t = t.replace(parentheticalAbbrevs, "").trim();
  return t;
}

const inputPath = process.argv[2];
if (!inputPath) {
  console.error("Usage: node build_venue_h5_from_orderer.js <path-to-core-rankings.json>");
  process.exit(1);
}

const absPath = path.isAbsolute(inputPath) ? inputPath : path.join(process.cwd(), inputPath);
if (!fs.existsSync(absPath)) {
  console.error("File not found:", absPath);
  process.exit(1);
}

const data = JSON.parse(fs.readFileSync(absPath, "utf8"));
const out = {};

function add(entry) {
  const fullName = entry && entry.fullName;
  const h5 = entry && typeof entry.h5 === "number" && entry.h5 > 0 ? entry.h5 : 0;
  if (!fullName || h5 === 0) return;
  const n = normalizeVenueName(fullName);
  if (!n) return;
  if (out[n] == null || entry.h5 > out[n]) out[n] = entry.h5;
}

if (data.conferences) {
  for (const entry of Object.values(data.conferences)) add(entry);
}
if (data.journals) {
  for (const entry of Object.values(data.journals)) add(entry);
}

const root = path.resolve(__dirname, "..");
const outPath = path.join(root, "src", "data", "venue_h5_index.json");
fs.writeFileSync(outPath, JSON.stringify(out, null, 0), "utf8");
console.log("Wrote", outPath, "with", Object.keys(out).length, "venue h5 entries.");
process.exit(0);
