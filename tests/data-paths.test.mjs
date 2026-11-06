// Bundled data paths must be written out in full ("src/data/<name>"), because the Edge package rewrites the literal
// "src/data/<name>.gz" to "src/data/<name>" when it stores the file uncompressed (scripts/variant-data.mjs). A path
// built as base + name would escape that rewrite and 404 on Edge only.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { BADGE_SOURCES } from "../src/common/quality.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const GZIPPED = ["scimago_2024_quartiles.json", "norwegian_compact.json", "era2023.txt", "journal_impact_2024.csv", "venue_h5_index.json"];

function sourceFiles(dir) {
  const out = [];
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) { if (e.name !== "data") out.push(...sourceFiles(p)); } else if (/\.(js|mjs|html)$/.test(e.name)) out.push(p);
  }
  return out;
}
const SOURCES = sourceFiles(path.join(ROOT, "src")).map((p) => [path.relative(ROOT, p), readFileSync(p, "utf8")]);

describe("bundled data paths", () => {
  it("no code joins a path onto a src/data/ base", () => {
    for (const [f, s] of SOURCES) {
      assert.doesNotMatch(s, /["'`]src\/data\/["'`]/, `${f} builds a src/data path from a base; write the full literal path`);
      assert.doesNotMatch(s, /src\/data\/\$\{/, `${f} builds a src/data path from a template; write the full literal path`);
    }
  });

  it("every literal src/data path names a file that exists", () => {
    let seen = 0;
    for (const [f, s] of SOURCES) {
      for (const m of s.matchAll(/["'`](src\/data\/[^"'`*<]+)["'`]/g)) {
        seen++;
        assert.ok(existsSync(path.join(ROOT, m[1])), `${f}: ${m[1]} does not exist`);
      }
    }
    assert.ok(seen >= 10, `expected the data readers to name their files, found ${seen}`);
  });

  it("the five large lists ship gzipped only, and each unzips to non-empty text", () => {
    for (const n of GZIPPED) {
      assert.ok(!existsSync(path.join(ROOT, "src/data", n)), `${n} should ship only as ${n}.gz`);
      const text = gunzipSync(readFileSync(path.join(ROOT, "src/data", `${n}.gz`))).toString("utf8");
      assert.ok(text.length > 100000, n);
      if (n.endsWith(".json")) JSON.parse(text);
    }
  });

  it("every Options badge source is a full path to an existing file", () => {
    for (const s of BADGE_SOURCES.filter((x) => x.file)) {
      assert.match(s.file, /^src\/data\//, s.kind);
      assert.ok(existsSync(path.join(ROOT, s.file)), s.file);
    }
  });
});
