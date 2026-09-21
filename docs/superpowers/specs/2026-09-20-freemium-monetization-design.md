# Freemium Monetization - Design Spec

**Date**: 2026-09-20
**Status**: Approved
**Scope**: New feature for Scholar Utility Belt v0.6.0

## Overview

Scholar Utility Belt has ~1,500 users and a 4.5-star rating but no monetization beyond a Buy Me a Coffee link. Ads were considered and rejected: extension ad surfaces run $0.50-$2 RPM, which at this scale nets an estimated $6-24/month, while carrying real Chrome Web Store delisting risk (ad/affiliate injection is a known 2024-2025 enforcement target) and contradicting the extension's "local-first, no telemetry" brand positioning.

Instead, this spec adds a freemium paid tier: a defined set of advanced features move behind a one-time or subscription unlock, sold through ExtensionPay. The core design constraint is that **no current user loses anything they already have** — the existing v0.5.0 feature set (already live on the Chrome Web Store as of 2026-07-05) stays free forever for everyone who has already installed the extension. Only fresh installs after this release see the paid tier.

Target outcome: real, sustainable revenue from an engaged niche audience, using a model (freemium/one-time purchase) that the research on comparable indie extensions consistently shows outperforms ads at this scale, without damaging the goodwill behind the 4.5-star rating.

## 1. Grandfathering Existing Users

### Mechanism

Chrome's extension lifecycle API distinguishes new installs from updates natively via `chrome.runtime.onInstalled`, which fires with `details.reason`:

- `"install"` — a genuinely new installation. No prior extension data exists.
- `"update"` — the extension was already installed and is updating to a new version.
- `"chrome_update"` / `"shared_module_update"` — not relevant here, treat as update (never re-classify an existing user as new).

On this event, in `src/sw.js`:

```js
chrome.runtime.onInstalled.addListener(async (details) => {
  const { grandfathered } = await chrome.storage.local.get('grandfathered');
  if (grandfathered !== undefined) return; // already set, never overwrite

  const isNewInstall = details.reason === 'install';
  await chrome.storage.local.set({ grandfathered: !isNewInstall });
});
```

This must ship as the **first** change in the v0.6.0 release, and must be verified (see Section 5) before any gating UI is added on top of it. Every one of the current 1,500 users will receive this update via Chrome's normal auto-update mechanism, hit the `"update"` branch, and be permanently flagged `grandfathered: true` — before they ever see a paywall.

### Why not a timestamp

An install-date timestamp would need to be added retroactively and has no reliable value for existing users (nothing currently records install date). `details.reason` requires no backfill and is exact.

## 2. Feature Gating

### Gated features (paid tier, new installs only)

**Revision note (2026-09-20, post-audit):** the original gated list below drew its line around the four features that happened to ship in the immediately-preceding v0.5.0 release, rather than a deliberate inventory of "advanced/power-user" vs. "basic" functionality. An audit of the full feature set found several comparably (or more) sophisticated features — the systematic-review workspace, the citation-graph overlay, and the Publish-or-Perish peer-comparison report — that were free by omission simply because they predated this spec. The list below has been expanded to close that gap, using this principle: **individual reading/browsing aids stay free forever; workflow tools that save significant time for power users (systematic reviews, citation mapping, cross-cohort benchmarking, cross-database author metrics) are Pro.**

These are gated for anyone whose `grandfathered` flag is `false`:

- Author compare-overlay (side-by-side author profile comparison)
- Citation-lineage / idea-lineage view
- p-index, FWCI (Field-Weighted Citation Impact), Relative Citation Ratio (NIH iCite), and Influential Citations (Semantic Scholar) metrics
- Narrative CV generation
- Systematic-review workspace (PRISMA/PICO screening, multi-reviewer consensus, active-learning-assisted prioritization, export)
- Citation-graph / network overlay (build-from-seeds, expand references/citations, save/load graph collections, export)
- Publish-or-Perish report (peer-cohort benchmarking, CSV/JSON/Markdown export) — previously implicitly free under the single "Enable Publish or Perish metrics" toggle; now explicitly Pro, consistent with author-compare and narrative CV

### Free tier (all users, always)

- Venue-quality badges (FT50, UTD24, ABDC, VHB, ABS/AJG, CORE/ICORE, CCF, SCImago, ERA, Norwegian register)
- Search-result action grid (save/remove, PDF, abstract, citation utilities)
- Citation-velocity and "Emerging" signals
- Local saved-paper library (notes, tags, collections, export/import)
- Author-profile summary metrics (h-index, m-index, L-index, g-index, h5-index, OWPI, top-10% share, open-access share) and basic filters
- Query Trend Tracker panel (existing feature, unrelated to this spec)
- Tortured-phrase screening badge and OpenAlex "Related works" panel (both new, low-effort, integrity/discovery-oriented — kept free to reinforce trust ahead of the paywall rollout)

### Gating check

A single helper, `src/common/entitlement.js`, exposed to both content scripts and the popup/options UI:

```js
export async function isPaidUser() {
  const { grandfathered } = await chrome.storage.local.get('grandfathered');
  if (grandfathered) return true;
  const user = await extpay.getUser();
  return user.paid;
}
```

Every gated feature's render path checks `isPaidUser()` before rendering. If `false`, render a locked-state placeholder (see Section 4) instead of the feature.

## 3. Pricing & Licensing

### Pricing

Configured live in the ExtensionPay dashboard (`scholar-utility-belt` extension, plan nicknames `lifetime` / `monthly` / `yearly` — confirmed 2026-09-20, supersedes this doc's earlier $19/$3/$24 draft figures):

- **Lifetime unlock**: $40 one-time.
- **Subscription**: $3/month or $20/year.

Both unlock the same gated feature set; the choice is presented to the user as "pay once" vs. "pay less up front."

### ExtensionPay integration

ExtensionPay is a Stripe-backed licensing service purpose-built for browser extensions:

- Funds settle directly to Purplelink's own Stripe account; ExtensionPay takes a 5% transaction fee, no monthly cost.
- Handles Stripe Checkout, webhooks, and receipt emails — no new Netlify function or backend endpoint needed.
- Solves cross-device/reinstall license recognition via email-link reactivation, which is the one part of DIY licensing that's genuinely hard to get right.
- Supports one-time and recurring (monthly/yearly) pricing natively, matching the pricing model above.

Integration points:
- Bundle the ExtensionPay client library (`extpay.js`) in `src/common/`.
- Initialize in `src/sw.js` (background service worker) with the extension's ExtensionPay ID.
- `extpay.getUser()` is called from `entitlement.js` (Section 2) to check paid status; ExtensionPay caches this locally so it works offline after the first check.
- `extpay.openPaymentPage()` and `extpay.openLoginPage()` (for existing-license reactivation on a new device) are wired to the upsell UI (Section 4).

### Account setup (not automatable)

Ben creates the ExtensionPay account and connects Purplelink's Stripe account through ExtensionPay's own dashboard. This is a real business account with payout details and cannot be created on his behalf.

**Status: done.** The `scholar-utility-belt` extension is registered on ExtensionPay with a connected Stripe account and three plans (`lifetime`, `monthly`, `yearly`) configured — `EXTPAY_ID = "scholar-utility-belt"`.

## 4. Upsell UI

When a non-paid, non-grandfathered user reaches a gated feature (any of the seven listed in Section 2: author compare, lineage view, p-index/FWCI/RCR/influential-citations display, narrative CV, review workspace, citation graph, or Publish-or-Perish report):

- The feature's normal UI slot renders a locked-state placeholder instead: a short one-line description of what the feature does, a "Pro" badge, and a single "Unlock" button.
- "Unlock" opens a small modal (rendered in the extension's own popup/options context, not injected into the Scholar page) showing both pricing options (lifetime vs. subscription) and calls `extpay.openPaymentPage()` for the selected option.
- A secondary "Already purchased on another device?" link calls `extpay.openLoginPage()`.
- No gated feature is ever partially rendered or shown then hidden — the check happens before any render, so there's no flash-of-paid-content.

The options page gets a new "Scholar Utility Belt Pro" section summarizing the paid features and current entitlement status (Free / Pro - Lifetime / Pro - Subscription), consistent with the existing options page structure.

## 5. Rollout & Verification

1. Ship the `onInstalled` grandfathering logic (Section 1) alone first. Verify manually: load the current published version's data into a test profile, then load the unpacked v0.6.0 build over it (simulating an update) and confirm `grandfathered: true` is set. Separately, install v0.6.0 fresh into a clean profile and confirm `grandfathered: false`.
2. Only after grandfathering is verified, add the gating checks and upsell UI from Sections 2 and 4.
3. Update the Chrome Web Store listing description and the extension's privacy policy to disclose the ExtensionPay network calls (this is a real, if small, exception to the "no backend, no telemetry" positioning and should be stated plainly rather than discovered by a user).
4. Version bump to 0.6.0; changelog entry explicitly states "existing users keep all current features free" to preempt confusion.

## Out of Scope

- No changes to any currently-free feature's behavior.
- No ad network integration (rejected per the economics in the Overview).
- No custom backend/licensing server — ExtensionPay replaces this entirely.
- Team/institutional licensing is not addressed here; single-user licenses only.
