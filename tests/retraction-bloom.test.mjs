import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

globalThis.atob ??= (b) => Buffer.from(b, "base64").toString("binary");
const { bloomHasDoi } = await import("../src/content/data-loader.js");
const data = JSON.parse(readFileSync(new URL("../src/data/retraction_bloom.json", import.meta.url), "utf8"));
const bloom = { m: data.m, k: data.k, bits: Uint8Array.from(Buffer.from(data.bits, "base64")) };

describe("bundled retraction list", () => {
  it("matches well-known retracted papers", () => {
    // Wakefield et al. 1998 and the 2020 Surgisphere hydroxychloroquine paper, both retracted by The Lancet.
    for (const doi of ["10.1016/S0140-6736(97)11096-0", "10.1016/S0140-6736(20)31180-6", "10.1016/s0140-6736(97)11096-0"]) {
      assert.equal(bloomHasDoi(doi, bloom), true, doi);
    }
  });
  it("flags under 0.1% of DOIs that are not on the list", () => {
    // Each false positive puts "Potential update/retraction" on a paper that was never retracted.
    const n = 50000;
    let hits = 0;
    for (let i = 0; i < n; i++) if (bloomHasDoi(`10.99999/not-retracted.${i}`, bloom)) hits++;
    assert.ok(hits / n < 0.001, `${hits} of ${n} (${((100 * hits) / n).toFixed(3)}%)`);
  });
  it("is sized for the DOIs it holds", () => {
    assert.ok(data.count > 60000, `count ${data.count}`);
    assert.ok(data.m / data.count >= 14, `bits per DOI ${(data.m / data.count).toFixed(1)}`);
  });
});
