// Builds src/data/top2pct.json.gz and src/data/top2pct.meta.json for the free top-2% panel.
//
// Source: Ioannidis JPA, "Updated science-wide author databases of standardized citation indicators", version 9,
// Elsevier Data Repository, DOI 10.17632/btchxktzyw.9, licence CC BY-NC 3.0. The two author tables used:
//   Table_1_Authors_career_...xlsx    sha256 cf5fd523dcb369b066a41b901f861c703d696ca7d3be907a94ff6ec0f59e4ec1
//   Table_1_Authors_singleyr_...xlsx  sha256 8bd46b5cbdc4de05ea09afc662623ca3059b4d705d4582045c5244edf94b02f5
//
// Refresh (a new dataset version):
//   1. Download both author tables from the DOI page into an empty folder outside the repository.
//   2. From the repository root, dump each to CSV (build time only, never in CI):
//        python3 -I scripts/top2pct_xlsx_to_csv.py <career.xlsx> <out>/career.csv
//        python3 -I scripts/top2pct_xlsx_to_csv.py <singleyr.xlsx> <out>/singleyr.csv
//   3. Update DATASET and SOURCE_SHA256 below (the dump prints each sha256), then run:
//        node scripts/build_top2pct.mjs <out>/career.csv <out>/singleyr.csv
//   4. npm test, then record the zip size change in the PR.
// Removal requests: add {"name": "Surname, Given", "inst": "...", "cntry": "..."} to scripts/top2pct-exclude.json and
// rebuild. Changes made to the data (stated in the panel credit and THIRD_PARTY.md): only the columns below are kept,
// shares are rounded, and listed removals are dropped.
//
// The output is deterministic: the same CSVs give the same bytes (gzip mtime 0, sorted rows, no build date).
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { FORMAT_VERSION, splitDatasetName, nameKey, decodeTop2pct, keyAt } from "../src/content/top2pct-core.js";

const DATASET = { version: "9", doi: "10.17632/btchxktzyw.9", year: 2025, license: "CC BY-NC 3.0" };
const SOURCE_SHA256 = {
  career: "cf5fd523dcb369b066a41b901f861c703d696ca7d3be907a94ff6ec0f59e4ec1",
  singleyr: "8bd46b5cbdc4de05ea09afc662623ca3059b4d705d4582045c5244edf94b02f5",
};
const TABLES = ["career", "singleyr"];
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

/** RFC 4180 CSV: quoted fields, doubled quotes, newlines inside quotes. */
export function parseCsv(text) {
  const rows = [];
  let row = [], field = "", q = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (q) {
      if (c === '"') { if (text[i + 1] === '"') { field += '"'; i++; } else q = false; }
      else field += c;
    } else if (c === '"') q = true;
    else if (c === ",") { row.push(field); field = ""; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); rows.push(row); row = []; field = "";
    } else field += c;
  }
  if (field || row.length) { row.push(field); rows.push(row); }
  return rows;
}

// Fixed scaling, rounded half up. toFixed(6) first so 0.145 * 100 = 14.499999... still rounds to 15.
const scaled = (x, f) => Math.floor(Number((Number(x) * f).toFixed(6)) + 0.5);
const intOr = (x, d) => (x === "" || !Number.isFinite(Number(x)) ? d : Math.trunc(Number(x)));

function readTable(file, exclude) {
  const rows = parseCsv(fs.readFileSync(file, "utf8"));
  const header = rows.shift();
  const col = (name) => {
    const i = header.indexOf(name);
    if (i < 0) throw new Error(`${file}: no column ${name}`);
    return i;
  };
  const C = {
    name: col("authfull"), inst: col("inst_name"), cntry: col("cntry"), fy: col("firstyr"), ly: col("lastyr"),
    self: col("self%"), cpr: col("cprat"), rw: col("np6025_rw"), sub: col("sm-subfield-1"),
    rank: col("rank sm-subfield-1 (ns)"), subCount: col("sm-subfield-1 count"),
  };
  const out = [];
  let skipped = 0;
  for (const r of rows) {
    if (r.length < header.length || !r[C.name]) continue;
    const name = r[C.name].trim(), inst = r[C.inst].trim(), cntry = r[C.cntry].trim().toLowerCase();
    if (exclude.has(`${name}\u0000${inst}\u0000${cntry}`)) { skipped++; continue; }
    const key = nameKey(splitDatasetName(name));
    const rank = intOr(r[C.rank], -1), subCount = intOr(r[C.subCount], -1);
    if (!key || !r[C.sub] || rank < 1 || subCount < rank) { skipped++; continue; }
    out.push({
      key, name, inst, cntry, fy: intOr(r[C.fy], 0), ly: intOr(r[C.ly], 0),
      sub: r[C.sub].trim(), rank, subCount,
      self: scaled(r[C.self] || 0, 100), cpr: scaled(r[C.cpr] || 0, 10), rw: intOr(r[C.rw], 0),
    });
  }
  return { rows: out, skipped };
}

function stringTable(values) {
  const list = [...new Set(values)].filter((s) => s !== "").sort();
  const index = new Map(list.map((s, i) => [s, i]));
  return { list, at: (s) => (s === "" ? -1 : index.get(s)) };
}

export function build(csvByTable, exclude = []) {
  const ex = new Set(exclude.map((e) => `${e.name}\u0000${e.inst || ""}\u0000${(e.cntry || "").toLowerCase()}`));
  const read = Object.fromEntries(TABLES.map((t) => [t, readTable(csvByTable[t], ex)]));

  // One person per (name, institution, country). An identity that appears more than once in a table (an exact
  // duplicate) is not joined across tables: each such row is its own person, so no numbers are mixed between two
  // people who share a name and institution.
  const ident = (r) => `${r.name}\u0000${r.inst}\u0000${r.cntry}`;
  const counts = Object.fromEntries(TABLES.map((t) => {
    const m = new Map();
    for (const r of read[t].rows) m.set(ident(r), (m.get(ident(r)) || 0) + 1);
    return [t, m];
  }));
  const joinable = (id) => TABLES.every((t) => (counts[t].get(id) || 0) <= 1);
  const people = [];
  const byIdent = new Map();
  for (const t of TABLES) {
    for (const r of read[t].rows) {
      const id = ident(r);
      let p = joinable(id) ? byIdent.get(id) : undefined;
      if (!p) {
        p = { key: r.key, name: r.name, inst: r.inst, cntry: r.cntry, fy: r.fy, ly: r.ly, rows: {} };
        people.push(p);
        if (joinable(id)) byIdent.set(id, p);
      }
      p.rows[t] = r;
    }
  }
  const sortKey = (p) => [p.key, p.name, p.inst, p.cntry, TABLES.map((t) => (p.rows[t] ? `${p.rows[t].sub}|${p.rows[t].rank}` : "")).join("|")].join("\u0000");
  people.sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0));

  const inst = stringTable(people.map((p) => p.inst));
  const cntry = stringTable(people.map((p) => p.cntry));
  const sub = stringTable(TABLES.flatMap((t) => read[t].rows.map((r) => r.sub)));
  const obj = {
    format: FORMAT_VERSION,
    meta: { ...DATASET, tables: TABLES.length, people: people.length },
    tables: TABLES,
    inst: inst.list, cntry: cntry.list, sub: sub.list,
    people: {
      name: people.map((p) => p.name),
      inst: people.map((p) => inst.at(p.inst)), cntry: people.map((p) => cntry.at(p.cntry)),
      fy: people.map((p) => p.fy), ly: people.map((p) => p.ly),
    },
    rows: {},
  };
  for (const t of TABLES) {
    const T = { p: [], sub: [], rank: [], self: [], cpr: [], rw: [], subCount: new Array(sub.list.length).fill(0) };
    let last = -1;
    people.forEach((p, i) => {
      const r = p.rows[t];
      if (!r) return;
      T.p.push(i - last); last = i;
      const s = sub.at(r.sub);
      T.sub.push(s); T.rank.push(r.rank); T.self.push(r.self); T.cpr.push(r.cpr); T.rw.push(r.rw);
      if (T.subCount[s] && T.subCount[s] !== r.subCount) throw new Error(`${t}: sub-field ${r.sub} has two sizes`);
      T.subCount[s] = r.subCount;
    });
    obj.rows[t] = T;
  }
  return { obj, stats: Object.fromEntries(TABLES.map((t) => [t, { rows: read[t].rows.length, skipped: read[t].skipped }])) };
}

function main() {
  const [career, singleyr] = process.argv.slice(2);
  if (!career || !singleyr) {
    console.error("usage: node scripts/build_top2pct.mjs <career.csv> <singleyr.csv>");
    process.exit(2);
  }
  const exclude = JSON.parse(fs.readFileSync(path.join(ROOT, "scripts", "top2pct-exclude.json"), "utf8"));
  const { obj, stats } = build({ career, singleyr }, exclude);
  const db = decodeTop2pct(obj);
  if (!db) throw new Error("built file does not decode");
  for (let i = 1; i < db.size; i++) if (keyAt(db, i) < keyAt(db, i - 1)) throw new Error(`people not sorted by key at ${i}`);
  const json = Buffer.from(JSON.stringify(obj));
  const gz = zlib.gzipSync(json, { level: 9 });
  const out = path.join(ROOT, "src", "data", "top2pct.json.gz");
  fs.writeFileSync(out, gz);
  const meta = {
    ...DATASET,
    title: "Updated science-wide author databases of standardized citation indicators",
    author: "John P.A. Ioannidis",
    publisher: "Elsevier Data Repository",
    url: `https://doi.org/${DATASET.doi}`,
    changes: "Only name, institution, country, years, sub-field rank (self-citations excluded), sub-field size, self-citation share (whole percent), citations per citing paper (one decimal) and retracted-paper count are kept; removal requests are dropped.",
    source_sha256: SOURCE_SHA256,
    file: "top2pct.json.gz",
    file_sha256: crypto.createHash("sha256").update(gz).digest("hex"),
    format: FORMAT_VERSION,
    people: obj.meta.people,
    rows: stats,
  };
  fs.writeFileSync(path.join(ROOT, "src", "data", "top2pct.meta.json"), JSON.stringify(meta, null, 2) + "\n");
  console.log(`people ${obj.meta.people}; ${JSON.stringify(stats)}; json ${json.length} bytes; gzip ${gz.length} bytes`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
