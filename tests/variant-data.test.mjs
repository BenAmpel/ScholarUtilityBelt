// Edge Add-ons rejects a package with a compressed file inside; make_variant.mjs edge stores src/data/*.gz uncompressed
// and points the code at the new names (scripts/variant-data.mjs).
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { gzipSync } from "node:zlib";
import { tmpdir } from "node:os";
import path from "node:path";
import { uncompressDataFiles } from "../scripts/variant-data.mjs";

describe("uncompressDataFiles (Edge package)", () => {
  it("replaces each src/data .gz file with its contents and rewrites the code's references", () => {
    const work = mkdtempSync(path.join(tmpdir(), "sub-variant-"));
    try {
      mkdirSync(path.join(work, "src", "data"), { recursive: true });
      mkdirSync(path.join(work, "dist", "content"), { recursive: true });
      writeFileSync(path.join(work, "src", "data", "g.names.json.gz"), gzipSync('["Ada"]'));
      writeFileSync(path.join(work, "src", "data", "g.edges.bin.gz"), gzipSync(Buffer.from([1, 0, 0, 0])));
      writeFileSync(path.join(work, "src", "data", "list.txt"), "x");
      const code = 'namesUrl: "src/data/g.names.json.gz", edgesUrl: "src/data/g.edges.bin.gz"';
      writeFileSync(path.join(work, "dist", "content", "content.js"), code);

      assert.deepEqual(uncompressDataFiles(work).sort(), ["g.edges.bin.gz", "g.names.json.gz"]);
      assert.deepEqual(readdirSync(path.join(work, "src", "data")).sort(), ["g.edges.bin", "g.names.json", "list.txt"]);
      assert.equal(readFileSync(path.join(work, "src", "data", "g.names.json"), "utf8"), '["Ada"]');
      assert.deepEqual([...readFileSync(path.join(work, "src", "data", "g.edges.bin"))], [1, 0, 0, 0]);
      assert.equal(readFileSync(path.join(work, "dist", "content", "content.js"), "utf8"),
        'namesUrl: "src/data/g.names.json", edgesUrl: "src/data/g.edges.bin"');
    } finally {
      rmSync(work, { recursive: true, force: true });
    }
  });

  it("the genealogy loaders only gunzip URLs that end in .gz", () => {
    for (const f of ["src/content/content.js", "src/content/genealogy-lineage.js"]) {
      const s = readFileSync(new URL(`../${f}`, import.meta.url), "utf8");
      assert.match(s, /if \(!url\.endsWith\("\.gz"\) \|\| !res\.body/, f);
    }
  });
});
