// The free top-2% panel: name keys, institution evidence, the data file, the match gate and the Pro fence.
// A wrong match puts another person's citation numbers on a named profile, so the gate below runs the matcher over
// every name key the dataset shares between two or more people and fails on any wrong confident match.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import * as core from "../src/content/top2pct-core.js";
import { build } from "../scripts/build_top2pct.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DATA = path.join(ROOT, "src/data/top2pct.json.gz");
const META = JSON.parse(readFileSync(path.join(ROOT, "src/data/top2pct.meta.json"), "utf8"));
const loadShipped = () => core.decodeTop2pct(JSON.parse(gunzipSync(readFileSync(DATA)).toString("utf8")));

describe("top2pct names", () => {
  const split = (s) => core.splitScholarName(s);
  it("drops apostrophes and joins hyphens and en dashes", () => {
    assert.deepEqual(split("Patrick O'Brien"), { surname: "obrien", given: ["patrick"] });
    assert.deepEqual(split("Mary-Jane Smith-Jones"), { surname: "smithjones", given: ["maryjane"] });
    assert.deepEqual(split("Xu Yi–Jun"), { surname: "yijun", given: ["xu"] });
    assert.equal(core.nameKey(core.splitDatasetName("Xu, Yi–Jun")), core.nameKey(core.splitDatasetName("Xu, Yi-Jun")));
  });
  it("strips titles only at the start and suffixes only at the end", () => {
    assert.deepEqual(split("Dr. John Smith Jr."), { surname: "smith", given: ["john"] });
    assert.deepEqual(split("Jane Doe, PhD"), { surname: "doe", given: ["jane"] });
    assert.deepEqual(core.splitDatasetName("Smith, John, Jr."), { surname: "smith", given: ["john"] });
  });
  it("joins a particle to the surname only when two tokens precede the last", () => {
    assert.deepEqual(split("Di Wang"), { surname: "wang", given: ["di"] });
    assert.deepEqual(split("Ludwig van Beethoven"), { surname: "vanbeethoven", given: ["ludwig"] });
    assert.deepEqual(core.splitDatasetName("van Beethoven, Ludwig"), { surname: "vanbeethoven", given: ["ludwig"] });
  });
  it("folds accents and gives nothing for a name in another script", () => {
    assert.equal(core.nameKey(split("José Núñez")), "nunez|j");
    assert.equal(core.nameKey(split("Søren Kierkegaard")), "kierkegaard|s");
    assert.equal(split("王伟"), null);
    assert.equal(split("Madonna"), null);
  });
  it("tries the other order only as an alternate key", () => {
    const k = core.scholarKeys("Lei Cheng");
    assert.equal(k.primary, "cheng|l");
    assert.deepEqual(k.alternates.map((a) => a.key), ["lei|c"]);
  });
  it("needs full, agreeing given names for a confident match", () => {
    assert.equal(core.strongGiven(["john"], ["john"]), true);
    assert.equal(core.strongGiven(["john", "a"], ["john"]), true);
    assert.equal(core.strongGiven(["john", "a"], ["john", "b"]), false);
    assert.equal(core.strongGiven(["j"], ["john"]), false);
    assert.equal(core.strongGiven(["john"], ["jane"]), false);
  });
});

describe("top2pct institution evidence", () => {
  const ev = (inst, aff, email) => core.institutionContainment(inst, core.affiliationTokens(aff, email));
  it("counts the dataset institution's words found in the affiliation", () => {
    assert.equal(ev("University of Oxford", "Professor of Medicine, University of Oxford"), 1);
    assert.equal(ev("Massachusetts Institute of Technology", "MIT"), 1);
    assert.equal(ev("Duke University", "Duke University School of Medicine"), 1);
    assert.equal(ev("Chinese Academy of Sciences", "Chinese Academy of Sciences"), 1);
    assert.equal(ev("University of Toronto", "", "cs.toronto.edu"), 1);
  });
  it("does not let one shared word name another institution", () => {
    assert.equal(ev("University of York", "State University of New York at Buffalo"), 0);
    assert.equal(ev("University of Washington", "Washington University in St. Louis"), 0);
    assert.equal(ev("University of Delhi", "Delhi Pharmaceutical Sciences and Research University"), 0);
    assert.equal(ev("National Research Council", "Stanford University"), 0);
  });
});

// A tiny table in the dataset's column layout, to check the build and the decoder together.
const HEADER = ["authfull", "inst_name", "cntry", "firstyr", "lastyr", "self%", "cprat", "np6025_rw", "sm-subfield-1", "rank sm-subfield-1 (ns)", "sm-subfield-1 count"];
const ROWS = [
  ["Kresse, Georg", "Universität Wien", "aut", 1990, 2025, 0.0349, 1.234, 0, "Chemical Physics", 3, 50000],
  ["Smith, John", "University of Oxford", "gbr", 1980, 2025, 0.145, 1.1, 2, "Oncology", 10, 90000],
  ["Smith, John", "Harvard University", "usa", 1985, 2025, 0.2, 1.05, 0, "Oncology", 20, 90000],
  ["Doe, Jane", "", "usa", 2000, 2025, 0.1, 1.0, 0, "Oncology", 40, 90000],
];
const csv = (rows) => [HEADER, ...rows].map((r) => r.map((x) => (/[",\n]/.test(String(x)) ? `"${String(x).replace(/"/g, '""')}"` : x)).join(",")).join("\n") + "\n";

describe("top2pct data file", () => {
  it("builds, decodes and matches a small table", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "top2pct-"));
    try {
      writeFileSync(path.join(dir, "career.csv"), csv(ROWS));
      writeFileSync(path.join(dir, "singleyr.csv"), csv(ROWS.slice(0, 2)));
      const { obj } = build({ career: path.join(dir, "career.csv"), singleyr: path.join(dir, "singleyr.csv") });
      const db = core.decodeTop2pct(JSON.parse(JSON.stringify(obj)));
      assert.ok(db);
      assert.equal(db.size, 4);
      const m = core.matchProfile(db, { name: "Georg Kresse", affiliation: "Professor, University of Vienna, Universität Wien" });
      assert.equal(m.state, "confident");
      const r = m.candidates[0];
      assert.deepEqual(r.tables.career, { subfield: "Chemical Physics", rank: 3, of: 50000, selfPct: 3, citesPerCitingPaper: 1.2, retracted: 0 });
      assert.ok(r.tables.singleyr);
      // 0.145 is 14.4999... in floating point; the build still rounds it half up.
      const smith = core.matchProfile(db, { name: "John Smith", affiliation: "University of Oxford" });
      assert.equal(smith.state, "confident");
      assert.equal(smith.candidates[0].tables.career.selfPct, 15);
      assert.equal(core.matchProfile(db, { name: "John Smith", affiliation: "" }).state, "none");
      assert.equal(core.matchProfile(db, { name: "Jane Doe", affiliation: "Anywhere" }).state, "none");
      assert.equal(core.resolveIdentity(db, core.identityOf(smith.candidates[0])).index, smith.candidates[0].index);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("returns null for a wrong format or an index past its table", () => {
    const obj = JSON.parse(gunzipSync(readFileSync(DATA)).toString("utf8"));
    assert.ok(core.decodeTop2pct(obj));
    const bad = (mutate) => { const o = structuredClone({ ...obj, people: { ...obj.people, inst: obj.people.inst.slice() }, rows: { career: { ...obj.rows.career, sub: obj.rows.career.sub.slice() }, singleyr: obj.rows.singleyr } }); mutate(o); return core.decodeTop2pct(o); };
    assert.equal(bad((o) => { o.format = 999; }), null);
    assert.equal(bad((o) => { o.people.inst[0] = o.inst.length; }), null);
    assert.equal(bad((o) => { o.rows.career.sub[5] = -1; }), null);
    assert.equal(bad((o) => { o.rows.career.p = o.rows.career.p.concat([1]); }), null);
    assert.equal(core.decodeTop2pct(null), null);
    assert.equal(core.decodeTop2pct("x"), null);
  });

  it("the shipped file matches its meta file and is sorted by name key", () => {
    assert.equal(createHash("sha256").update(readFileSync(DATA)).digest("hex"), META.file_sha256);
    const db = loadShipped();
    assert.ok(db);
    assert.equal(db.size, META.people);
    assert.equal(db.meta.doi, "10.17632/btchxktzyw.9");
    for (let i = 1; i < db.size; i += 97) assert.ok(core.keyAt(db, i - 1) <= core.keyAt(db, i), `unsorted at ${i}`);
  });

  it("the packaged zip carries a file that decodes (when output/ has one)", (t) => {
    const out = path.join(ROOT, "output");
    const zips = existsSync(out) ? readdirSync(out).filter((f) => /^ScholarUtilityBelt-.*\.zip$/.test(f)) : [];
    if (!zips.length) { t.skip("no packaged zip; CI runs this after scripts/package_release.sh"); return; }
    for (const z of zips) {
      const edge = z.endsWith("-edge.zip");
      const entry = edge ? "src/data/top2pct.json" : "src/data/top2pct.json.gz";
      const bytes = execFileSync("unzip", ["-p", path.join(out, z), entry], { maxBuffer: 64 << 20 });
      const db = core.decodeTop2pct(JSON.parse((edge ? bytes : gunzipSync(bytes)).toString("utf8")));
      assert.ok(db, z);
      assert.equal(db.size, META.people, z);
    }
  });
});

describe("top2pct match gate", () => {
  // Synthetic profiles for every person whose name key is shared: the exact institution, its first naming word, no
  // affiliation, another group's institution, and the name in the other order. Groups alternate between a
  // development half and a held-out half; the threshold was tuned on the first and checked on the second.
  it("gives no wrong confident match on either half of the shared-name groups", () => {
    const db = loadShipped();
    const groups = new Map();
    for (let i = 0; i < db.size; i++) {
      const k = core.keyAt(db, i);
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(i);
    }
    const shared = [...groups.values()].filter((g) => g.length >= 2);
    const instOf = (i) => (db.people.inst[i] >= 0 ? db.inst[db.people.inst[i]] : "");
    const scholarName = (i) => { const n = db.people.name[i]; const c = n.indexOf(","); return c < 0 ? n : `${n.slice(c + 1).trim()} ${n.slice(0, c).trim()}`; };
    const fold = (i) => core.nameTokens(db.people.name[i]).join("");
    const dsGiven = (i) => (core.splitDatasetName(db.people.name[i]) || { given: [] }).given;
    const near = (a, b) => a && b && core.institutionContainment(a, core.affiliationTokens(b)) >= core.INST_THRESHOLD;
    // Not wrong: the dataset split one person into two rows (same folded name, or both given names agree and one
    // institution names the other).
    const isWrong = (i, j) => !(fold(i) === fold(j)
      || (core.strongGiven(dsGiven(i), dsGiven(j)) && core.strongGiven(dsGiven(j), dsGiven(i))
        && (instOf(i) === instOf(j) || near(instOf(i), instOf(j)) || near(instOf(j), instOf(i)))));
    const result = [];
    for (const half of [0, 1]) {
      let total = 0, confident = 0, pick = 0;
      const wrong = [];
      shared.forEach((g, gi) => {
        if (gi % 2 !== half) return;
        const groupInst = new Set(g.map(instOf));
        const evidences = (aff) => g.some((i) => instOf(i) && core.institutionContainment(instOf(i), core.affiliationTokens(aff)) >= core.INST_THRESHOLD);
        let other = "", step = 7;
        while (!other || groupInst.has(other) || evidences(other)) { other = instOf(shared[(gi + step) % shared.length][0]); step++; }
        for (const i of g) {
          const name = scholarName(i);
          const variants = [[name, instOf(i)], [name, core.institutionTokens(instOf(i)).slice(0, 1).join(" ")], [name, ""], [name, other], [db.people.name[i].replace(",", ""), instOf(i)]];
          for (const [n, aff] of variants) {
            total++;
            const m = core.matchProfile(db, { name: n, affiliation: aff });
            if (m.state === "pick") pick++;
            if (m.state !== "confident") continue;
            confident++;
            const j = m.candidates[0].index;
            if (j !== i && isWrong(i, j)) wrong.push(`${db.people.name[i]} [${instOf(i)}] as "${n}" @ "${aff}" -> ${db.people.name[j]} [${instOf(j)}]`);
          }
        }
      });
      result.push({ half: half ? "held-out" : "development", total, confident, pick, wrong: wrong.length });
      assert.deepEqual(wrong.slice(0, 20), [], `${half ? "held-out" : "development"} half`);
    }
    // Reported, not gated: how often a profile gets a confident match or a pick list.
    console.log(`top2pct gate: ${JSON.stringify(result)}`);
  });
});

describe("top2pct fence (free only; Pro never reads the data)", () => {
  const walk = (dir) => readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) return ["data", "node_modules", "fixtures"].includes(e.name) ? [] : walk(p);
    return /\.(js|mjs|cjs|html|json)$/.test(e.name) ? [p] : [];
  });
  const files = ["src", "scripts", "tests"].flatMap((d) => walk(path.join(ROOT, d))).concat(["build.js", "manifest.template.json", "manifest.json"].map((f) => path.join(ROOT, f)));
  const rel = (p) => path.relative(ROOT, p).split(path.sep).join("/");
  const allowed = (r) => /^src\/content\/top2pct[\w-]*\.js$/.test(r) || ["scripts/build_top2pct.mjs", "scripts/variant-data.mjs", "tests/top2pct.test.mjs"].includes(r);
  const panelFiles = files.filter((p) => /^src\/content\/top2pct[\w-]*\.js$/.test(rel(p)));

  it("only the panel, its build and its tests name the data file", () => {
    for (const p of files) {
      if (allowed(rel(p))) continue;
      assert.doesNotMatch(readFileSync(p, "utf8"), /top2pct\.(json|bin|meta)/, rel(p));
    }
  });

  it("the panel's imports never reach the entitlement or payment modules", () => {
    assert.ok(panelFiles.length >= 1);
    const seen = new Set();
    const queue = panelFiles.map(rel);
    while (queue.length) {
      const r = queue.pop();
      if (seen.has(r)) continue;
      seen.add(r);
      assert.doesNotMatch(r, /entitlement|extpay|apppass|payment|pro-prompt/, `reached ${r}`);
      const src = readFileSync(path.join(ROOT, r), "utf8");
      for (const m of src.matchAll(/(?:from|import)\s*\(?\s*["'](\.{1,2}\/[^"']+\.js)["']/g)) queue.push(path.posix.normalize(path.posix.join(path.posix.dirname(r), m[1])));
      for (const m of src.matchAll(/dist\/((?:common|content)\/[\w.-]+\.js)/g)) queue.push(`src/${m[1]}`);
    }
  });

  it("the panel never asks about a paid plan", () => {
    for (const p of panelFiles) assert.doesNotMatch(readFileSync(p, "utf8"), /\b(entitlement|isPro|isPaid)\b/, rel(p));
  });
});
