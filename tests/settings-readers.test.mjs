import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const { DEFAULT_SETTINGS } = await import("../src/common/storage.js");

// Every source file except the Options page and storage itself, which only show and save settings.
function readers(dir, out = []) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) {
      if (e.name !== "vendor" && e.name !== "data" && e.name !== "options") readers(p, out);
    } else if (/\.m?js$/.test(e.name) && !p.endsWith(join("common", "storage.js"))) {
      out.push(readFileSync(p, "utf8"));
    }
  }
  return out;
}

describe("settings", () => {
  it("every default setting is read by some feature, so no Options control does nothing", () => {
    const sources = readers(join(root, "src"));
    const unread = Object.keys(DEFAULT_SETTINGS).filter((k) => !sources.some((t) => new RegExp(`\\b${k}\\b`).test(t)));
    assert.deepEqual(unread, []);
  });
});
