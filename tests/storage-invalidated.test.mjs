import { describe, it } from "node:test";
import assert from "node:assert/strict";

// Runs in its own process (one test file per process), because the signal fires once per load.
globalThis.chrome = {
  storage: { local: { async get() { throw new Error("Extension context invalidated."); }, async set() { throw new Error("Extension context invalidated."); } } },
};
const s = await import("../src/common/storage.js");

describe("context invalidated signal", () => {
  it("tells listeners once, even for listeners added later, and the setters stay quiet", async () => {
    let calls = 0;
    s.onContextInvalidated(() => { calls++; });
    assert.equal(calls, 0, "nothing has failed yet");

    const saved = await s.upsertPaper({ key: "p1", title: "x" });
    assert.equal(saved.key, "p1");
    assert.equal(calls, 1);

    await s.setCitationSnapshot("c1", 5, "2026-10-12");
    await s.addHiddenPaper("p1");
    assert.equal(calls, 1, "fires once, not once per failure");

    let late = 0;
    s.onContextInvalidated(() => { late++; });
    assert.equal(late, 1, "a listener added after the event is told immediately");
  });

  it("ignores a listener that throws and non-function input", () => {
    assert.doesNotThrow(() => s.onContextInvalidated(() => { throw new Error("bad listener"); }));
    assert.doesNotThrow(() => s.onContextInvalidated(null));
  });
});
