import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execSync } from "node:child_process";
import { mkdtempSync, writeFileSync, readFileSync, rmSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { gunzipSync } from "node:zlib";

const IMPORT_SCRIPT = join(import.meta.dirname, "..", "scripts", "import_genealogy_csv.py");
const MERGE_SCRIPT = join(import.meta.dirname, "..", "scripts", "merge_genealogy_datasets.py");

function run(cmd) {
  return execSync(cmd, { encoding: "utf-8", timeout: 15000 }).trim();
}

function readNames(path) {
  const buf = readFileSync(path);
  const json = gunzipSync(buf).toString("utf-8");
  return JSON.parse(json).n;
}

function readEdges(path) {
  const buf = readFileSync(path);
  const raw = gunzipSync(buf);
  const arr = new Uint32Array(raw.buffer, raw.byteOffset, raw.byteLength / 4);
  const edges = [];
  for (let i = 0; i < arr.length; i += 2) {
    edges.push([arr[i], arr[i + 1]]);
  }
  return edges;
}

// ---------------------------------------------------------------------------
// import_genealogy_csv.py
// ---------------------------------------------------------------------------
describe("import_genealogy_csv.py", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "genealogy-test-"));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("imports a CSV with header", () => {
    const csv = "advisor,student\nAlice,Bob\nBob,Carol\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);

    const namesPath = join(tmpDir, "test.names.json.gz");
    const edgesPath = join(tmpDir, "test.edges.bin.gz");
    assert.ok(existsSync(namesPath));
    assert.ok(existsSync(edgesPath));

    const names = readNames(namesPath);
    assert.equal(names.length, 3);
    assert.ok(names.includes("Alice"));
    assert.ok(names.includes("Bob"));
    assert.ok(names.includes("Carol"));

    const edges = readEdges(edgesPath);
    // Edges stored as (student, advisor) per existing convention
    assert.equal(edges.length, 2);
  });

  it("detects header columns by name", () => {
    const csv = "mentor,mentee\nDr. X,Student Y\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);
    const names = readNames(join(tmpDir, "test.names.json.gz"));
    assert.ok(names.includes("Dr. X"));
    assert.ok(names.includes("Student Y"));
  });

  it("handles CSV without header", () => {
    const csv = "John Smith,Jane Doe\nJane Doe,Kid Three\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);
    const names = readNames(join(tmpDir, "test.names.json.gz"));
    assert.equal(names.length, 3);
  });

  it("deduplicates names", () => {
    const csv = "advisor,student\nAlice,Bob\nAlice,Carol\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);
    const names = readNames(join(tmpDir, "test.names.json.gz"));
    assert.equal(names.length, 3); // Alice, Bob, Carol — Alice not duplicated
  });

  it("skips self-edges", () => {
    const csv = "advisor,student\nAlice,Alice\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);
    const edges = readEdges(join(tmpDir, "test.edges.bin.gz"));
    assert.equal(edges.length, 0);
  });

  it("imports JSON format", () => {
    const data = [
      { advisor: "Prof A", student: "Student B" },
      { advisor: "Prof A", student: "Student C" },
    ];
    const jsonPath = join(tmpDir, "test.json");
    writeFileSync(jsonPath, JSON.stringify(data));

    run(`python3 "${IMPORT_SCRIPT}" "${jsonPath}" --out-dir "${tmpDir}" --label test`);
    const names = readNames(join(tmpDir, "test.names.json.gz"));
    assert.equal(names.length, 3);
    const edges = readEdges(join(tmpDir, "test.edges.bin.gz"));
    assert.equal(edges.length, 2);
  });

  it("stores edges as (student, advisor) pairs", () => {
    const csv = "advisor,student\nAdvisorName,StudentName\n";
    const csvPath = join(tmpDir, "test.csv");
    writeFileSync(csvPath, csv);

    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label test`);
    const names = readNames(join(tmpDir, "test.names.json.gz"));
    const edges = readEdges(join(tmpDir, "test.edges.bin.gz"));

    const advisorIdx = names.indexOf("AdvisorName");
    const studentIdx = names.indexOf("StudentName");
    assert.ok(advisorIdx >= 0 && studentIdx >= 0);

    // Edge should be (student, advisor)
    assert.equal(edges[0][0], studentIdx);
    assert.equal(edges[0][1], advisorIdx);
  });
});

// ---------------------------------------------------------------------------
// merge_genealogy_datasets.py
// ---------------------------------------------------------------------------
describe("merge_genealogy_datasets.py", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "genealogy-merge-test-"));
  });

  afterEach(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it("merges two datasets and deduplicates names", () => {
    // Create dataset A
    const csvA = "advisor,student\nAlice,Bob\n";
    const csvAPath = join(tmpDir, "a.csv");
    writeFileSync(csvAPath, csvA);
    run(`python3 "${IMPORT_SCRIPT}" "${csvAPath}" --out-dir "${tmpDir}" --label a`);

    // Create dataset B with overlapping name
    const csvB = "advisor,student\nBob,Carol\n";
    const csvBPath = join(tmpDir, "b.csv");
    writeFileSync(csvBPath, csvB);
    run(`python3 "${IMPORT_SCRIPT}" "${csvBPath}" --out-dir "${tmpDir}" --label b`);

    // Merge
    const outNames = join(tmpDir, "merged.names.json.gz");
    const outEdges = join(tmpDir, "merged.edges.bin.gz");
    run(
      `python3 "${MERGE_SCRIPT}" ` +
      `--datasets a=${join(tmpDir, "a.names.json.gz")},${join(tmpDir, "a.edges.bin.gz")} ` +
      `b=${join(tmpDir, "b.names.json.gz")},${join(tmpDir, "b.edges.bin.gz")} ` +
      `--out-names "${outNames}" --out-edges "${outEdges}"`
    );

    const names = readNames(outNames);
    const edges = readEdges(outEdges);

    // Bob should be deduplicated: Alice, Bob, Carol = 3 names
    assert.equal(names.length, 3);
    assert.equal(edges.length, 2);
  });

  it("handles empty dataset merge", () => {
    const csv = "advisor,student\nX,Y\n";
    const csvPath = join(tmpDir, "only.csv");
    writeFileSync(csvPath, csv);
    run(`python3 "${IMPORT_SCRIPT}" "${csvPath}" --out-dir "${tmpDir}" --label only`);

    const outNames = join(tmpDir, "merged.names.json.gz");
    const outEdges = join(tmpDir, "merged.edges.bin.gz");
    run(
      `python3 "${MERGE_SCRIPT}" ` +
      `--datasets only=${join(tmpDir, "only.names.json.gz")},${join(tmpDir, "only.edges.bin.gz")} ` +
      `--out-names "${outNames}" --out-edges "${outEdges}"`
    );

    const names = readNames(outNames);
    assert.equal(names.length, 2);
  });
});
