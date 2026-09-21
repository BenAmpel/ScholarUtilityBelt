import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  decideGrandfathered,
  describeEntitlement,
  getEntitlementStatus,
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
