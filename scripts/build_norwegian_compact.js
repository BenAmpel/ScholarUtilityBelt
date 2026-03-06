#!/usr/bin/env node
/**
 * Build-time script: pre-process norwegian_register.csv (15 MB) into a compact
 * JSON lookup of { normalizedName: level } containing only Level 1 and 2 entries.
 *
 * Run: node scripts/build_norwegian_compact.js
 *
 * Output: src/data/norwegian_compact.json  (~150-300 KB, loaded instead of the 15 MB CSV)
 */

const fs   = require("fs");
const path = require("path");

const IN_PATH  = path.join(__dirname, "..", "src", "data", "norwegian_register.csv");
const OUT_PATH = path.join(__dirname, "..", "src", "data", "norwegian_compact.json");

// Mirror of normalizeVenueName from quality.js so we produce identical keys.
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

// Minimal semicolon-delimited CSV line parser (handles quoted fields).
function parseCsvLine(line, delim = ";") {
  const s = String(line || "");
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') { cur += '"'; i++; }
        else inQ = false;
      } else cur += ch;
    } else {
      if (ch === '"') inQ = true;
      else if (ch === delim) { out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim()); cur = ""; }
      else cur += ch;
    }
  }
  out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim());
  return out;
}

if (!fs.existsSync(IN_PATH)) {
  console.error("Input file not found:", IN_PATH);
  process.exit(1);
}

const text  = fs.readFileSync(IN_PATH, "utf8");
const lines = text.split(/\r?\n/).filter(l => l.trim());

if (!lines.length) {
  console.error("Empty input file.");
  process.exit(1);
}

const header    = parseCsvLine(lines[0], ";");
const titleIdx  = header.findIndex(h => /International Title|Original Title/i.test(String(h)));
const levelIdx  = header.findIndex(h => /^Level 20\d{2}$/.test(String(h).trim()));
const useTitleIdx = titleIdx >= 0 ? titleIdx : 2;
const useLevelIdx = levelIdx >= 0 ? levelIdx : 9;

console.log(`Using title column ${useTitleIdx} ("${header[useTitleIdx]}") ` +
            `and level column ${useLevelIdx} ("${header[useLevelIdx]}")`);

const result = {};
let total = 0, kept = 0;

for (let i = 1; i < lines.length; i++) {
  const cells = parseCsvLine(lines[i], ";");
  const name  = (cells[useTitleIdx] || "").trim();
  const level = String(cells[useLevelIdx] || "").replace(/\D/g, "").slice(0, 1);
  total++;
  if (!name || (level !== "1" && level !== "2")) continue;
  const n = normalizeVenueName(name);
  if (!n) continue;
  // Keep the highest level when a venue appears multiple times.
  if (!result[n] || (level === "2" && result[n] === "1")) {
    result[n] = level;
  }
  kept++;
}

fs.writeFileSync(OUT_PATH, JSON.stringify(result), "utf8");
const kbIn  = Math.round(fs.statSync(IN_PATH).size  / 1024);
const kbOut = Math.round(fs.statSync(OUT_PATH).size / 1024);
console.log(`Done. Rows: ${total} total, ${kept} kept (Level 1 or 2).`);
console.log(`Input: ${kbIn} KB  →  Output: ${kbOut} KB  (${Math.round(kbIn/kbOut)}× reduction)`);
