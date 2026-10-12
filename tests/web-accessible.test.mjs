import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

// web_accessible_resources lets Scholar's own page load these files, so it must list what the content
// script imports and nothing broader (a wildcard also exposed extpay.js and the service-worker bundles).
// The list is checked against the real import graph, so a new module cannot be forgotten, and a stale entry shows up.
const root = fileURLToPath(new URL("..", import.meta.url)); // not .pathname: a path with spaces comes back percent-encoded
const read = (p) => readFileSync(join(root, p), "utf8");
const manifest = JSON.parse(read("manifest.template.json"));
const listed = manifest.web_accessible_resources[0].resources.filter((r) => r.startsWith("dist/")).sort();

// Everything the content script can load by name, in any of the files it runs.
const CONTENT_SIDE = ["content/content.js", "content/content-author.js", "content/review-workspace.js", "content/signals-ui.js",
  "content/trend-tracker.js", "content/genealogy-lineage.js", "content/data-loader.js"];
const importsOf = (rel) => [...read(`src/${rel}`).matchAll(/(?:from|import)\s*\(?\s*["'](\.{1,2}\/[^"']+\.js)["']/g)]
  .map((m) => normalize(join(dirname(rel), m[1])));

describe("web_accessible_resources", () => {
  it("lists exactly the dist files the content script imports (directly or through other modules)", () => {
    const wanted = new Set();
    const queue = [];
    for (const f of CONTENT_SIDE) for (const m of read(`src/${f}`).matchAll(/dist\/((?:common|content)\/[A-Za-z0-9_.-]+\.js)/g)) queue.push(m[1]);
    while (queue.length) {
      const rel = queue.pop();
      if (wanted.has(rel)) continue;
      wanted.add(rel);
      if (existsSync(join(root, "src", rel))) queue.push(...importsOf(rel));
    }
    // worker.js is started as a Worker by name rather than imported.
    wanted.add("content/worker.js");
    assert.deepEqual(listed, [...wanted].map((r) => `dist/${r}`).sort());
  });
  it("has no wildcard over dist and does not expose the extension payment or service-worker bundles", () => {
    assert.ok(!listed.some((r) => r.includes("*")));
    for (const hidden of ["dist/common/extpay.js", "dist/common/entitlement.sw.js", "dist/common/apppass.sw.js", "dist/common/ask-sw.js", "dist/common/url-policy.sw.js"]) {
      assert.ok(!listed.includes(hidden), hidden);
    }
  });
});
