// The ABS Academic Journal Guide stays in this extension (Ben, 2026-10-28; Vitae dropped it). These tests fail if the
// list stops shipping or stops reaching the badges: the file, the package script, the manifest and the loader.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const read = (p) => readFileSync(path.join(ROOT, p), "utf8");

globalThis.window = {};
globalThis.atob = (b64) => Buffer.from(b64, "base64").toString("binary");
globalThis.chrome = { runtime: { getURL: (p) => p } };
// Serve the real data files, as the extension's own URLs would.
globalThis.fetch = async (p) => {
  try {
    const text = read(p);
    return { ok: true, text: async () => text, json: async () => JSON.parse(text) };
  } catch (_) {
    return { ok: false, text: async () => "", json: async () => null };
  }
};

const { loadEraAndNorwegian } = await import("../src/content/data-loader.js");
const { compileQualityIndex, qualityBadgesForVenue } = await import("../src/common/quality.js");

const csv = read("src/data/abs2024.csv");
const rows = csv.trim().split(/\r?\n/).slice(1);

describe("ABS 2024 list", () => {
  it("is the full list with valid ranks", () => {
    assert.equal(csv.split(/\r?\n/)[0], "Venue,Rank");
    assert.ok(rows.length >= 1600, `only ${rows.length} rows`);
    for (const r of rows) assert.match(r.slice(r.lastIndexOf(",") + 1), /^(1|2|3|4|4\*)$/, r);
  });

  it("is packaged: not excluded by package_release.sh and readable by pages", () => {
    assert.ok(!/abs2024/.test(read("scripts/package_release.sh")), "package_release.sh must not exclude abs2024.csv");
    const manifest = JSON.parse(read("manifest.json"));
    const war = manifest.web_accessible_resources.flatMap((w) => w.resources);
    assert.ok(war.includes("src/data/*.csv") || war.includes("src/data/abs2024.csv"), "abs2024.csv is not web-accessible");
  });

  it("loads every row and gives the right badges", async () => {
    const { absIndex } = await loadEraAndNorwegian();
    assert.equal(absIndex.size, rows.length, "the loader dropped rows");
    const idx = compileQualityIndex({}, { absIndex });
    const abs = (venue) => qualityBadgesForVenue(venue, idx).find((b) => b.kind === "abs")?.text;
    assert.equal(abs("MIS Quarterly"), "ABS 4*");
    assert.equal(abs("Information Systems Research"), "ABS 4*");
    assert.equal(abs("Journal of Management Information Systems"), "ABS 4");
    assert.equal(abs("Accounting, Organizations and Society"), "ABS 4*");
    assert.equal(abs("Decision Support Systems"), "ABS 3");
  });

  it("is listed in the sources file the Options page shows", () => {
    const sources = JSON.parse(read("src/data/quality_sources.json")).sources;
    assert.equal(sources.abs2024.rows, rows.length);
  });
});
