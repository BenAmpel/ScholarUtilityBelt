import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const src = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");

describe("reading-queue BibTeX fallback", () => {
  it("uses its own builder, which reads the queue item's link, not the result builder", () => {
    const fn = src.slice(src.indexOf("async function getBibTeXForQueueItem(item)"));
    const body = fn.slice(0, fn.indexOf("\n  }\n"));
    assert.match(body, /return buildQueueItemBibTeX\(item\);/);
    assert.doesNotMatch(body, /buildMinimalBibTeX/);
  });
  it("has exactly one definition of each builder, so neither shadows the other", () => {
    assert.equal((src.match(/function buildMinimalBibTeX\(/g) || []).length, 1);
    assert.equal((src.match(/function buildQueueItemBibTeX\(/g) || []).length, 1);
    const q = src.slice(src.indexOf("function buildQueueItemBibTeX(item)"));
    assert.match(q.slice(0, 900), /item\.link/);
  });
});
