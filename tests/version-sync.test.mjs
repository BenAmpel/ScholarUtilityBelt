import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

// build.js renders manifest.json from manifest.template.json, so a version bump that skips the template ships the old
// version (1.1.1 was first merged that way and built as 1.1.0). Every place the version lives must agree.
const root = fileURLToPath(new URL("..", import.meta.url)); // not .pathname: a path with spaces comes back percent-encoded
const json = (p) => JSON.parse(readFileSync(join(root, p), "utf8"));

describe("version", () => {
  it("is the same in the manifest template, manifest.json, package.json and package-lock.json", () => {
    const lock = json("package-lock.json");
    const versions = {
      "manifest.template.json": json("manifest.template.json").version,
      "manifest.json": json("manifest.json").version,
      "package.json": json("package.json").version,
      "package-lock.json": lock.version,
      'package-lock.json packages[""]': lock.packages[""].version,
    };
    assert.equal(new Set(Object.values(versions)).size, 1, JSON.stringify(versions, null, 2));
  });
});
