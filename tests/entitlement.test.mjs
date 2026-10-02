import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  decideGrandfathered,
  describeEntitlement,
  getEntitlementStatus,
  isAppPassCacheFresh,
  APP_PASS_TTL_MS,
} from "../src/common/entitlement.js";

describe("decideGrandfathered", () => {
  it("marks a fresh install as NOT grandfathered", () => {
    assert.equal(decideGrandfathered("install", undefined), false);
  });

  it("marks an existing user's update as grandfathered", () => {
    assert.equal(decideGrandfathered("update", undefined), true);
  });

  it("treats chrome_update the same as update (never re-classify)", () => {
    assert.equal(decideGrandfathered("chrome_update", undefined), true);
  });

  it("never overwrites an already-decided value", () => {
    assert.equal(decideGrandfathered("update", false), false);
    assert.equal(decideGrandfathered("install", true), true);
  });
});

describe("describeEntitlement", () => {
  it("reports grandfathered users as paid, regardless of ExtPay state", () => {
    const result = describeEntitlement({ grandfathered: true, extpayUser: { paid: false, plan: null } });
    assert.deepEqual(result, { paid: true, tier: "grandfathered" });
  });

  it("reports a real ExtPay lifetime purchase", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "lifetime" } },
    });
    assert.deepEqual(result, { paid: true, tier: "lifetime" });
  });

  it("reports a real ExtPay subscription", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "monthly" } },
    });
    assert.deepEqual(result, { paid: true, tier: "monthly" });
  });

  it("reports an unpaid, non-grandfathered user as free", () => {
    const result = describeEntitlement({ grandfathered: false, extpayUser: { paid: false, plan: null } });
    assert.deepEqual(result, { paid: false, tier: "free" });
  });
});

describe("describeEntitlement with App Pass", () => {
  const free = { paid: false, plan: null };

  it("treats an active App Pass as paid", () => {
    const result = describeEntitlement({ grandfathered: false, extpayUser: free, appPass: { status: "ok" } });
    assert.deepEqual(result, { paid: true, tier: "app-pass" });
  });

  it("keeps a direct ExtPay purchase ahead of App Pass", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "yearly" } },
      appPass: { status: "ok" },
    });
    assert.deepEqual(result, { paid: true, tier: "yearly" });
  });

  it("keeps grandfathered ahead of App Pass", () => {
    const result = describeEntitlement({ grandfathered: true, extpayUser: null, appPass: { status: "ok" } });
    assert.deepEqual(result, { paid: true, tier: "grandfathered" });
  });

  it("stays free for every non-ok App Pass status", () => {
    for (const status of ["no_apppass", "rate_limited", "unknown_error", "", undefined]) {
      const result = describeEntitlement({ grandfathered: false, extpayUser: free, appPass: { status } });
      assert.deepEqual(result, { paid: false, tier: "free" }, `status ${status}`);
    }
  });

  it("stays free when the user never opted in (no appPass at all)", () => {
    assert.deepEqual(describeEntitlement({ grandfathered: false, extpayUser: free, appPass: null }), { paid: false, tier: "free" });
    assert.deepEqual(describeEntitlement({ grandfathered: false, extpayUser: free }), { paid: false, tier: "free" });
  });
});

describe("isAppPassCacheFresh", () => {
  const now = 1_000_000_000;

  it("reuses a valid pass for hours", () => {
    assert.equal(isAppPassCacheFresh({ status: "ok", checkedAt: now - 5 * 60 * 60 * 1000 }, now), true);
    assert.equal(isAppPassCacheFresh({ status: "ok", checkedAt: now - APP_PASS_TTL_MS.ok }, now), false);
  });

  it("rechecks 'no pass' within minutes so a fresh activation shows up", () => {
    assert.equal(isAppPassCacheFresh({ status: "no_apppass", checkedAt: now - 5 * 60 * 1000 }, now), true);
    assert.equal(isAppPassCacheFresh({ status: "no_apppass", checkedAt: now - 11 * 60 * 1000 }, now), false);
  });

  it("backs off briefly after errors and rate limits", () => {
    for (const status of ["rate_limited", "unknown_error"]) {
      assert.equal(isAppPassCacheFresh({ status, checkedAt: now - 30 * 1000 }, now), true);
      assert.equal(isAppPassCacheFresh({ status, checkedAt: now - 61 * 1000 }, now), false);
    }
  });

  it("never trusts missing, malformed, unknown-status, or future-dated entries", () => {
    assert.equal(isAppPassCacheFresh(undefined, now), false);
    assert.equal(isAppPassCacheFresh({}, now), false);
    assert.equal(isAppPassCacheFresh({ status: "ok" }, now), false);
    assert.equal(isAppPassCacheFresh({ status: "something_else", checkedAt: now }, now), false);
    assert.equal(isAppPassCacheFresh({ status: "ok", checkedAt: now + 1000 }, now), false);
  });
});

describe("getEntitlementStatus (client helper)", () => {
  it("resolves with whatever the background sends back", async () => {
    globalThis.chrome = {
      runtime: {
        sendMessage: (msg, cb) => {
          assert.equal(msg.action, "getEntitlementStatus");
          cb({ paid: true, tier: "lifetime" });
        },
      },
    };
    const result = await getEntitlementStatus();
    assert.deepEqual(result, { paid: true, tier: "lifetime" });
  });

  it("falls back to free when the background sends no response", async () => {
    globalThis.chrome = {
      runtime: {
        sendMessage: (_msg, cb) => cb(undefined),
      },
    };
    const result = await getEntitlementStatus();
    assert.deepEqual(result, { paid: false, tier: "free" });
  });
});
