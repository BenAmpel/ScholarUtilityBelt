import { describe, it } from "node:test";
import assert from "node:assert/strict";

// data-loader.js uses chrome.runtime, fetch, atob, window — mock them.
globalThis.atob = (b64) => Buffer.from(b64, "base64").toString("binary");
globalThis.window = {};
globalThis.chrome = { runtime: { getURL: (p) => `chrome-ext://${p}` } };
globalThis.fetch = () => Promise.resolve({ ok: false, text: () => "" });

const { bloomHasDoi } = await import("../src/content/data-loader.js");

// ---------------------------------------------------------------------------
// bloomHasDoi (Bloom filter membership check)
// ---------------------------------------------------------------------------
describe("bloomHasDoi", () => {
  it("returns true when all bits are set (any DOI matches)", () => {
    const bits = new Uint8Array(64).fill(0xFF);
    const bloom = { m: 512, k: 3, bits };
    assert.equal(bloomHasDoi("10.1234/test.doi", bloom), true);
    assert.equal(bloomHasDoi("10.5678/other.doi", bloom), true);
  });

  it("returns false when no bits are set", () => {
    const bits = new Uint8Array(64).fill(0);
    const bloom = { m: 512, k: 3, bits };
    assert.equal(bloomHasDoi("10.1234/test.doi", bloom), false);
  });

  it("returns false for null/empty DOI", () => {
    const bits = new Uint8Array(64).fill(0xFF);
    const bloom = { m: 512, k: 3, bits };
    assert.equal(bloomHasDoi("", bloom), false);
    assert.equal(bloomHasDoi(null, bloom), false);
    assert.equal(bloomHasDoi(undefined, bloom), false);
  });

  it("returns false for null/undefined bloom", () => {
    assert.equal(bloomHasDoi("10.1234/test.doi", null), false);
    assert.equal(bloomHasDoi("10.1234/test.doi", undefined), false);
  });

  it("returns false for bloom missing required fields", () => {
    assert.equal(bloomHasDoi("10.1234/test.doi", {}), false);
    assert.equal(bloomHasDoi("10.1234/test.doi", { m: 64 }), false);
    assert.equal(bloomHasDoi("10.1234/test.doi", { m: 64, k: 3 }), false);
  });

  it("handles sparse bloom filter correctly", () => {
    // Set only bit 0 — most DOIs won't hash to only bit 0
    const bits = new Uint8Array(64);
    bits[0] = 1;
    const bloom = { m: 512, k: 3, bits };
    // With only 1 of 512 bits set and k=3, probability of false positive is ~(1/512)^3 ≈ 0
    assert.equal(bloomHasDoi("10.1234/test.doi", bloom), false);
  });

  it("is case-insensitive (DOI lowercased before hashing)", () => {
    // Both should produce the same result
    const bits = new Uint8Array(64).fill(0xFF);
    const bloom = { m: 512, k: 3, bits };
    const r1 = bloomHasDoi("10.1234/TEST", bloom);
    const r2 = bloomHasDoi("10.1234/test", bloom);
    assert.equal(r1, r2);
  });
});
