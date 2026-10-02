import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  decideGrandfathered,
  describeEntitlement,
  getEntitlementStatus,
  isAppPassCacheFresh,
  APP_PASS_TTL_MS,
  TRIAL_DAYS,
  trialEndsAt,
  isTrialActive,
  PRO_FEATURES,
  canUseFeature,
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
    assert.deepEqual(result, { paid: true, tier: "grandfathered", purchased: false });
  });

  it("reports a real ExtPay lifetime purchase", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "lifetime" } },
    });
    assert.deepEqual(result, { paid: true, tier: "lifetime", purchased: true });
  });

  it("reports a real ExtPay subscription", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "monthly" } },
    });
    assert.deepEqual(result, { paid: true, tier: "monthly", purchased: true });
  });

  it("reports an unpaid, non-grandfathered user as free", () => {
    const result = describeEntitlement({ grandfathered: false, extpayUser: { paid: false, plan: null } });
    assert.deepEqual(result, { paid: false, tier: "free", purchased: false });
  });
});

describe("describeEntitlement with App Pass", () => {
  const free = { paid: false, plan: null };

  it("treats an active App Pass as paid", () => {
    const result = describeEntitlement({ grandfathered: false, extpayUser: free, appPass: { status: "ok" } });
    assert.deepEqual(result, { paid: true, tier: "app-pass", purchased: true });
  });

  it("keeps a direct ExtPay purchase ahead of App Pass", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "yearly" } },
      appPass: { status: "ok" },
    });
    assert.deepEqual(result, { paid: true, tier: "yearly", purchased: true });
  });

  it("keeps grandfathered ahead of App Pass", () => {
    const result = describeEntitlement({ grandfathered: true, extpayUser: null, appPass: { status: "ok" } });
    // Display tier stays "grandfathered"; the App Pass still counts as a purchase for new features.
    assert.deepEqual(result, { paid: true, tier: "grandfathered", purchased: true });
  });

  it("stays free for every non-ok App Pass status", () => {
    for (const status of ["no_apppass", "rate_limited", "unknown_error", "", undefined]) {
      const result = describeEntitlement({ grandfathered: false, extpayUser: free, appPass: { status } });
      assert.deepEqual(result, { paid: false, tier: "free", purchased: false }, `status ${status}`);
    }
  });

  it("stays free when the user never opted in (no appPass at all)", () => {
    assert.deepEqual(describeEntitlement({ grandfathered: false, extpayUser: free, appPass: null }), { paid: false, tier: "free", purchased: false });
    assert.deepEqual(describeEntitlement({ grandfathered: false, extpayUser: free }), { paid: false, tier: "free", purchased: false });
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
          assert.equal(msg.needPurchased, false);
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
    assert.deepEqual(result, { paid: false, tier: "free", purchased: false });
  });

  it("asks the background for a purchase check when gating a new Pro feature", async () => {
    let sent;
    globalThis.chrome = { runtime: { sendMessage: (msg, cb) => { sent = msg; cb({ paid: true, tier: "grandfathered", purchased: false }); } } };
    await getEntitlementStatus({ needPurchased: true });
    assert.deepEqual(sent, { action: "getEntitlementStatus", needPurchased: true });
  });
});

describe("free trial", () => {
  const DAY = 24 * 60 * 60 * 1000;
  const now = Date.UTC(2026, 9, 10);

  it("is active for TRIAL_DAYS after it starts and not a moment longer", () => {
    const started = new Date(now - 3 * DAY);
    assert.equal(isTrialActive({ trialStartedAt: started }, now), true);
    assert.equal(isTrialActive({ trialStartedAt: new Date(now - TRIAL_DAYS * DAY + 1) }, now), true);
    assert.equal(isTrialActive({ trialStartedAt: new Date(now - TRIAL_DAYS * DAY) }, now), false);
    assert.equal(isTrialActive({ trialStartedAt: new Date(now - 30 * DAY) }, now), false);
  });

  it("accepts the Date objects ExtPay returns and ISO strings alike", () => {
    const iso = new Date(now - DAY).toISOString();
    assert.equal(isTrialActive({ trialStartedAt: iso }, now), true);
    assert.equal(trialEndsAt(iso), new Date(iso).getTime() + TRIAL_DAYS * DAY);
  });

  it("is never active without a usable start date", () => {
    for (const v of [null, undefined, "", "not a date"]) {
      assert.equal(isTrialActive({ trialStartedAt: v }, now), false, String(v));
    }
    assert.equal(isTrialActive(null, now), false);
    assert.equal(trialEndsAt(null), null);
  });

  it("describeEntitlement treats an active trial as purchased, with its end date", () => {
    const started = new Date(now - 2 * DAY);
    const result = describeEntitlement({ grandfathered: false, extpayUser: { paid: false, trialStartedAt: started }, now });
    assert.deepEqual(result, { paid: true, tier: "trial", purchased: true, trialEndsAt: started.getTime() + TRIAL_DAYS * DAY });
  });

  it("falls back to free once the trial has ended", () => {
    const result = describeEntitlement({ grandfathered: false, extpayUser: { paid: false, trialStartedAt: new Date(now - 20 * DAY) }, now });
    assert.deepEqual(result, { paid: false, tier: "free", purchased: false });
  });

  it("a real purchase outranks a trial", () => {
    const result = describeEntitlement({
      grandfathered: false,
      extpayUser: { paid: true, plan: { nickname: "yearly" }, trialStartedAt: new Date(now - DAY) },
      now,
    });
    assert.deepEqual(result, { paid: true, tier: "yearly", purchased: true });
  });
});

describe("grandfathering covers legacy Pro features only", () => {
  const grandfathered = { paid: true, tier: "grandfathered", purchased: false };
  const free = { paid: false, tier: "free", purchased: false };

  it("a grandfathered user's real purchase is still visible as purchased", () => {
    const result = describeEntitlement({ grandfathered: true, extpayUser: { paid: true, plan: { nickname: "lifetime" } } });
    assert.deepEqual(result, { paid: true, tier: "grandfathered", purchased: true });
  });

  it("a grandfathered user in a trial or on App Pass counts as purchased for new features", () => {
    const trial = describeEntitlement({ grandfathered: true, extpayUser: { paid: false, trialStartedAt: new Date() } });
    assert.equal(trial.purchased, true);
    const pass = describeEntitlement({ grandfathered: true, extpayUser: { paid: false }, appPass: { status: "ok" } });
    assert.equal(pass.purchased, true);
  });

  it("unlocks every legacy feature for a grandfathered user", () => {
    for (const [id, kind] of Object.entries(PRO_FEATURES)) {
      if (kind === "legacy") assert.equal(canUseFeature(id, grandfathered), true, id);
    }
  });

  it("keeps a new feature locked for a grandfathered user who has not purchased", () => {
    const saved = { ...PRO_FEATURES };
    PRO_FEATURES.__futureFeature = "new";
    try {
      assert.equal(canUseFeature("__futureFeature", grandfathered), false);
      assert.equal(canUseFeature("__futureFeature", free), false);
      assert.equal(canUseFeature("__futureFeature", { paid: true, tier: "grandfathered", purchased: true }), true);
      assert.equal(canUseFeature("__futureFeature", { paid: true, tier: "trial", purchased: true }), true);
      assert.equal(canUseFeature("__futureFeature", { paid: true, tier: "app-pass", purchased: true }), true);
    } finally {
      for (const k of Object.keys(PRO_FEATURES)) if (!(k in saved)) delete PRO_FEATURES[k];
    }
  });

  it("locks legacy features for free users and fails closed on unknown ids", () => {
    assert.equal(canUseFeature("compareAuthors", free), false);
    assert.equal(canUseFeature("compareAuthors", { paid: true, tier: "monthly", purchased: true }), true);
    assert.equal(canUseFeature("noSuchFeature", { paid: true, tier: "lifetime", purchased: true }), false);
    assert.equal(canUseFeature("compareAuthors", undefined), false);
  });

  it("only knows the two promise tiers", () => {
    for (const kind of Object.values(PRO_FEATURES)) assert.ok(kind === "legacy" || kind === "new");
  });
});
