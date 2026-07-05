#!/usr/bin/env node
/**
 * Build-time script: fetch Crossref Retraction Watch CSV, extract OriginalPaperDOI,
 * build a Bloom filter, and write src/data/retraction_bloom.json.
 *
 * Run: node scripts/build_retraction_bloom.js
 *
 * Dataset: https://gitlab.com/crossref/retraction-watch-data
 * CSV: retraction_watch.csv, column OriginalPaperDOI
 *
 * Bloom: ~50k items, target 0.1% FPR → m ≈ 720k bits (~90KB), k = 10.
 */

const https = require("https");
const fs = require("fs");
const path = require("path");

const CSV_URL =
  "https://gitlab.com/crossref/retraction-watch-data/-/raw/main/retraction_watch.csv";
const OUT_PATH = path.join(__dirname, "..", "src", "data", "retraction_bloom.json");

const BLOOM_M = 720000; // bits (~90KB)
const BLOOM_K = 10;

function fnv1a32(str) {
  let h = 2166136261;
  const prime = 16777619;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, prime);
  }
  return h >>> 0;
}

function bloomIndices(doi, m, k) {
  const s = String(doi).toLowerCase().trim();
  const h1 = fnv1a32(s);
  const h2 = fnv1a32(s + "\u0001salt") | 1;
  const indices = [];
  for (let i = 0; i < k; i++) {
    indices.push(((h1 + i * h2) >>> 0) % m);
  }
  return indices;
}

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "User-Agent": "ScholarUtilityBelt-Build/1.0" } }, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        const chunks = [];
        res.on("data", (ch) => chunks.push(ch));
        res.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
      })
      .on("error", reject);
  });
}

function parseCSV(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };
  const headerLine = lines[0];
  const headers = [];
  let i = 0;
  let inQuotes = false;
  let cur = "";
  while (i < headerLine.length) {
    const c = headerLine[i];
    if (inQuotes) {
      if (c === '"') {
        if (headerLine[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
      i++;
    } else {
      if (c === '"') {
        inQuotes = true;
        i++;
      } else if (c === ",") {
        headers.push(cur.trim());
        cur = "";
        i++;
      } else {
        cur += c;
        i++;
      }
    }
  }
  headers.push(cur.trim());

  const parseRow = (line) => {
    const row = [];
    let j = 0;
    let inQ = false;
    let cell = "";
    while (j < line.length) {
      const c = line[j];
      if (inQ) {
        if (c === '"') {
          if (line[j + 1] === '"') {
            cell += '"';
            j++;
          } else inQ = false;
        } else cell += c;
        j++;
      } else {
        if (c === '"') {
          inQ = true;
          j++;
        } else if (c === ",") {
          row.push(cell.trim());
          cell = "";
          j++;
        } else {
          cell += c;
          j++;
        }
      }
    }
    row.push(cell.trim());
    return row;
  };

  const rows = lines.slice(1).map(parseRow);
  return { headers, rows };
}

function main() {
  console.log("Fetching Retraction Watch CSV...");
  fetchUrl(CSV_URL)
    .then((csvText) => {
      const { headers, rows } = parseCSV(csvText);
      const colIndex = headers.findIndex(
        (h) => h === "OriginalPaperDOI" || h.toLowerCase() === "originalpaperdoi"
      );
      if (colIndex < 0) {
        throw new Error("OriginalPaperDOI column not found. Headers: " + headers.join(", "));
      }

      const dois = new Set();
      for (const row of rows) {
        if (row.length <= colIndex) continue;
        const raw = (row[colIndex] || "").trim().toLowerCase();
        if (!raw || raw === "unavailable" || raw === "n/a") continue;
        dois.add(raw);
      }
      console.log(`Unique DOIs: ${dois.size}`);

      const numBytes = Math.ceil(BLOOM_M / 8);
      const bits = new Uint8Array(numBytes);
      for (const doi of dois) {
        for (const idx of bloomIndices(doi, BLOOM_M, BLOOM_K)) {
          const byteIndex = idx >>> 3;
          const bitIndex = idx & 7;
          bits[byteIndex] |= 1 << bitIndex;
        }
      }

      const base64 = Buffer.from(bits).toString("base64");
      const out = {
        m: BLOOM_M,
        k: BLOOM_K,
        bits: base64,
        source: "Crossref Retraction Watch",
        built: new Date().toISOString().slice(0, 10),
        count: dois.size
      };
      const outDir = path.dirname(OUT_PATH);
      if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
      fs.writeFileSync(OUT_PATH, JSON.stringify(out), "utf-8");
      console.log(`Wrote ${OUT_PATH} (${(numBytes / 1024).toFixed(1)} KB)`);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}

main();
