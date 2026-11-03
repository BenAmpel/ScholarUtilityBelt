// Firefox can cancel a download whose blob URL is revoked in the same tick as the link click, so every download
// helper revokes later. downloadBlob is driven with fake timers; the scan below covers the helpers that are not
// importable (content.js, the popup and library pages).
import { describe, it, mock } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const events = [];
globalThis.document = {
  body: { appendChild: () => {} },
  createElement: () => ({ click: () => events.push("click"), remove: () => {} }),
};
globalThis.chrome = { runtime: { getURL: (p) => p, sendMessage: async () => ({}) }, storage: { local: { get: async () => ({}), set: async () => {} } } };
URL.createObjectURL = () => "blob:test";
URL.revokeObjectURL = () => events.push("revoke");

const { downloadBlob } = await import("../src/library/export-ui.js");

describe("download blob URLs", () => {
  it("downloadBlob revokes after the click, not in the same tick", () => {
    mock.timers.enable({ apis: ["setTimeout"] });
    try {
      events.length = 0;
      downloadBlob("x.txt", new Blob(["x"]));
      assert.deepEqual(events, ["click"], "revoked in the same tick as the click");
      mock.timers.tick(999);
      assert.deepEqual(events, ["click"]);
      mock.timers.tick(1);
      assert.deepEqual(events, ["click", "revoke"]);
    } finally {
      mock.timers.reset();
    }
  });

  it("no download helper in src revokes its URL right after the click", () => {
    const root = fileURLToPath(new URL("../src", import.meta.url));
    const offenders = [];
    const walk = (d) => {
      for (const e of readdirSync(d)) {
        const p = path.join(d, e);
        if (statSync(p).isDirectory()) { walk(p); continue; }
        if (!p.endsWith(".js")) continue;
        const lines = readFileSync(p, "utf8").split("\n");
        lines.forEach((line, i) => {
          if (!/^\s*URL\.revokeObjectURL\(/.test(line)) return;
          // A bare revoke within three lines of a .click() runs in the click's tick.
          if (lines.slice(Math.max(0, i - 3), i).some((l) => /\.click\(\)/.test(l))) offenders.push(`${path.relative(root, p)}:${i + 1}`);
        });
      }
    };
    walk(root);
    assert.deepEqual(offenders, []);
  });
});
