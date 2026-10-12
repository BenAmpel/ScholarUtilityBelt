import { describe, it } from "node:test";
import assert from "node:assert/strict";

// There is no DOM in the node test environment, so only the parts that do not touch
// the document are covered here. Rendering and re-ordering are untested in a browser.
const ui = await import("../src/content/signals-ui.js");

describe("signals-ui (no DOM)", () => {
  it("loads and exposes the three feature ids", () => {
    assert.deepEqual(ui.SIGNAL_FEATURE_IDS, ["claimEvidenceChips", "reproSignals", "readingOrder"]);
  });
  it("builds no toolbar row when every feature is switched off", () => {
    const row = ui.buildSignalsToolbarRow({
      settings: { showClaimEvidenceChips: false, showReproSignals: false, showReadingOrder: false },
      allowed: {},
      onUnlock() {},
    });
    assert.equal(row, null);
  });
  it("builds no toolbar row when features are unlocked but reading order is off", () => {
    const row = ui.buildSignalsToolbarRow({
      settings: { showReadingOrder: false },
      allowed: { claimEvidenceChips: true, reproSignals: true, readingOrder: true },
      onUnlock() {},
    });
    assert.equal(row, null);
  });
});
