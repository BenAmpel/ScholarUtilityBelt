import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildNameSearch, findNameIndices } from "../src/common/genealogy-search.js";

// A stand-in with the same shape as content.js normalizeAuthorName: drops titles and credentials, folds
// diacritics, lower-cases. The search must find exactly what a full index built with it would.
const normalize = (name) => String(name || "")
  .replace(/^(?:dr|prof)\.?\s+/i, "")
  .replace(/,?\s+(?:phd|md|jr)\.?$/i, "")
  .normalize("NFKD").replace(/[̀-ͯ]/g, "")
  .toLowerCase().trim();

const names = ["Wei Zhang", "Dr. Wei Zhang", "Wei Zhang, PhD", "Jürgen Müller", "Jurgen Muller", "Ann Lee", "Weiss Zhangfeld", "Wei\tZhang", "李伟"];
const search = buildNameSearch(names, normalize);
const fullIndex = (key) => names.map((n, i) => [normalize(n), i]).filter(([k]) => k === key).map(([, i]) => i);

describe("genealogy name search", () => {
  it("finds every spelling that normalizes to the key, in order, like a full index", () => {
    assert.deepEqual(findNameIndices(search, "wei zhang", normalize), [0, 1, 2]);
  });
  it("matches non-ASCII names through their folded form", () => {
    assert.deepEqual(findNameIndices(search, "jurgen muller", normalize), [3, 4]);
    assert.deepEqual(findNameIndices(search, "李伟", normalize), [8]);
  });
  it("does not return names that only contain the surname", () => {
    assert.deepEqual(findNameIndices(search, "ann lee", normalize), [5]);
    assert.deepEqual(findNameIndices(search, "zhang", normalize), []);
  });
  it("agrees with a full index for every key", () => {
    for (const n of names) {
      const key = normalize(n);
      assert.deepEqual(findNameIndices(search, key, normalize), fullIndex(key), key);
    }
  });
  it("returns nothing for an empty key or search", () => {
    assert.deepEqual(findNameIndices(search, "", normalize), []);
    assert.deepEqual(findNameIndices(null, "wei zhang", normalize), []);
  });
});
