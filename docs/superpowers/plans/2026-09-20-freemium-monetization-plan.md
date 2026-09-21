# Freemium Monetization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a freemium paid tier (author compare, citation lineage, p-index/FWCI/RCR/influential-citations, narrative CV, systematic-review workspace, citation-graph overlay, Publish-or-Perish report) sold via ExtensionPay, while every currently-installed user keeps free access to everything they already have.

**Revision note (2026-09-20, post-audit):** expanded from the original 3-feature gate (compare, lineage, narrative/p-index/FWCI) to 7, per the design spec's revision note — see `docs/superpowers/specs/2026-09-20-freemium-monetization-design.md` Section 2. `EXTPAY_ID` is resolved: `scholar-utility-belt`, with real `lifetime`/`monthly`/`yearly` plans already configured — Task 3 no longer needs a placeholder.

**Architecture:** A pure, unit-tested decision module (`src/common/entitlement.js`) determines grandfathered/paid status; `src/sw.js` wires it to `chrome.runtime.onInstalled` and to ExtensionPay's background SDK, exposing status via `chrome.runtime.onMessage`; `content.js`, `popup.js`, and `options.js` all query that same message handler rather than talking to ExtensionPay directly (ExtensionPay's own docs only document background-context usage, and content scripts can't safely load third-party scripts into the Scholar page's CSP context).

**Tech Stack:** Manifest V3 (classic/non-module service worker + classic content scripts using dynamic `import()`), esbuild (existing `build.js`), Node's built-in test runner (`node --test`, existing `tests/*.test.mjs` convention with a `globalThis.chrome` mock), ExtensionPay (`ExtPay.js`, Stripe-backed).

---

## Before You Start

- Repo root: `/Volumes/Extreme SSD/Chrome Scholar Extension` (separate git repo, `main` branch).
- The working tree currently has unrelated uncommitted changes (`manifest.json`, `manifest.template.json`, several `src/content/*` and `src/options/*` files, plus an untracked `src/data/tortured_phrases.txt`) from other in-progress work. **Do not touch, stage, or commit these files as part of this plan.** Every `git add` step below names exact files — never use `git add -A` or `git add .`.
- Test command: `npm test` (runs `node --import ./tests/register-loader.mjs --test tests/*.test.mjs`). The custom loader (`tests/loader.mjs`) forces `.js` files under `src/` to load as ES modules even though `package.json` says `"type": "commonjs"`.
- Build command: `npm run build` (runs `build.js`, which regenerates `manifest.json` from `manifest.template.json` via `scripts/build_manifest.js`, then runs esbuild over each target in `build.js`'s `TARGETS` array into `dist/`).
- Manual verification throughout: `chrome://extensions` → Developer mode → "Load unpacked" → select the repo root → after any change, click the reload icon on the extension card.
- **Resolved 2026-09-20**: Ben's ExtensionPay account is live at `extensionpay.com/home`, extension `scholar-utility-belt`, with three plans configured: `lifetime` ($40 once), `monthly` ($3/mo), `yearly` ($20/yr). `EXTPAY_ID = "scholar-utility-belt"` — Task 3 uses this constant directly, no placeholder needed.

---

## Implementation Status (2026-09-20)

All 10 tasks are implemented and committed on `main` (commits `a16285a`..`d90297b`, plus the plan/spec revision at `20a7577`). Tests pass (183 total, `npm test`), `npm run build` succeeds cleanly, and `node --check` passes on `content.js` and `sw.js`.

**Not done, and not automatable in this environment:** the manual browser-verification steps in Tasks 2, 3, 8, and 9 (loading the unpacked extension in `chrome://extensions`, clicking through gated features, confirming the grandfathering/upsell/Pro-status UI visually). No live Chrome profile was available to drive here — before shipping 0.6.0, walk through those checklists in a real browser, especially:

- Task 2 Step 4 and Task 8 Step 9's combined verification (grandfathering both branches, all seven gates, then the `grandfathered: true` override unlocking everything).
- Task 3 Step 6 (service worker console loads `ExtPay` without error).
- A real end-to-end purchase against the live `lifetime`/`monthly`/`yearly` Stripe plans (test mode, per ExtensionPay's own docs) — this has not been exercised at all.
- `npm run lint` is currently broken by a pre-existing, unrelated dependency issue (`Cannot find module '@eslint/js'`) — not caused by this work, but worth fixing separately since it silently disabled lint's own pre-release checklist item.

### Task 1: Grandfathering decision logic (pure, unit-tested)

**Files:**
- Create: `src/common/entitlement.js`
- Test: `tests/entitlement.test.mjs`

- [ ] **Step 1: Write the failing test**

```js
// tests/entitlement.test.mjs
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { decideGrandfathered } from "../src/common/entitlement.js";

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `Cannot find module '../src/common/entitlement.js'` (or `decideGrandfathered is not a function`).

- [ ] **Step 3: Write minimal implementation**

```js
// src/common/entitlement.js

/**
 * Decides whether a user should be grandfathered (keep all current
 * features free forever) based on how the extension was loaded.
 *
 * `reason` is `details.reason` from chrome.runtime.onInstalled:
 *   "install"            — brand new installation, no prior data.
 *   "update"              — already installed, updating to a new version.
 *   "chrome_update"       — Chrome itself updated; treat as update (existing user).
 *   "shared_module_update" — treat as update (existing user).
 *
 * `existingValue` is whatever is already stored under the `grandfathered`
 * key, or `undefined` if it has never been set. Once set, it must never
 * change — this guards against a listener firing more than once.
 */
export function decideGrandfathered(reason, existingValue) {
  if (existingValue !== undefined) return existingValue;
  return reason !== "install";
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS (all 4 assertions in `entitlement.test.mjs`, plus every pre-existing test file still passing).

- [ ] **Step 5: Commit**

```bash
git add src/common/entitlement.js tests/entitlement.test.mjs
git commit -m "feat: add grandfathering decision logic for freemium tier"
```

---

### Task 2: Wire grandfathering into the service worker

**Files:**
- Modify: `src/sw.js` (append; do not touch the existing `onInstalled` listeners already in the file)
- Modify: `build.js:56-73` (the `TARGETS` array — add a new entry for `common/entitlement.js`, following the exact pattern of the adjacent `common/quality.js` entry)

- [ ] **Step 1: Add a build target for the new module**

In `build.js`, inside the `TARGETS` array, immediately after the existing `common/quality.js` entry:

```js
  {
    label: 'common/entitlement.js',
    args: ['src/common/entitlement.js', '--bundle=false', '--minify', '--outfile=dist/common/entitlement.js', '--log-level=warning'],
  },
```

- [ ] **Step 2: Run the build to confirm it compiles**

Run: `npm run build`
Expected: `✓ common/entitlement.js` line appears in the output with no error, and `dist/common/entitlement.js` exists.

- [ ] **Step 3: Add the onInstalled listener to sw.js**

Append to the end of `src/sw.js` (after the existing final `chrome.runtime.onInstalled.addListener(async () => { ... })` block — do not merge into it, this is a separate concern):

```js
// Freemium grandfathering: decide once, on install/update, whether this
// user keeps all current features free forever. See src/common/entitlement.js.
chrome.runtime.onInstalled.addListener(async (details) => {
  const { decideGrandfathered } = await import(chrome.runtime.getURL("dist/common/entitlement.js"));
  const { grandfathered } = await chrome.storage.local.get("grandfathered");
  const decided = decideGrandfathered(details.reason, grandfathered);
  if (decided !== grandfathered) {
    await chrome.storage.local.set({ grandfathered: decided });
  }
});
```

- [ ] **Step 4: Manually verify both branches**

1. Build (`npm run build`), load unpacked, open the extension's service worker DevTools console (`chrome://extensions` → the extension card → "service worker" link), run `chrome.storage.local.get('grandfathered', console.log)`. Expected: `{ grandfathered: true }`, because loading it for the first time in this profile fires `onInstalled` with `reason: "install"` — wait, this is a **fresh profile**, so this specific check should show `false`. Confirm the printed value is `false`.
2. Bump `"version"` in `manifest.template.json` to `"0.5.1"` temporarily (do not commit this), run `npm run build`, click the reload icon on the extension card in `chrome://extensions` (this simulates an update, not a fresh install). Re-run the same console command. Expected: still `false` — because Step 3's "never overwrite" guard means the value set during the original install persists.
3. Revert the temporary version bump (`git checkout -- manifest.template.json manifest.json` will also revert the unrelated pre-existing changes — instead manually edit the version field back to `"0.5.0"`).
4. To see the `true` (grandfathered) branch: remove the extension entirely from `chrome://extensions`, reload it as "Load unpacked" again (this is a fresh install from Chrome's perspective and fires `reason: "install"`) — this reproduces the "new user" path, which should print `false`. To see `true`, you cannot easily fake an "existing user updating" on a brand-new profile; trust Task 1's unit tests for the `"update"` branch and use this manual check only to confirm `"install"` behaves correctly end-to-end.

- [ ] **Step 5: Commit**

```bash
git add build.js src/sw.js
git commit -m "feat: wire grandfathering decision into onInstalled"
```

---

### Task 3: Bundle and initialize ExtensionPay in the background

**Files:**
- Create: `src/common/extpay.js` (the ExtensionPay client library — see Step 1)
- Modify: `build.js` (add a build target)
- Modify: `manifest.template.json`
- Modify: `src/sw.js`

- [ ] **Step 1: Download the ExtPay client library**

Download `ExtPay.js` from the official repository and save it as `src/common/extpay.js`:

```bash
curl -sL https://raw.githubusercontent.com/Glench/ExtPay/main/ExtPay.js -o "/Volumes/Extreme SSD/Chrome Scholar Extension/src/common/extpay.js"
```

Verify the file downloaded and looks like a real UMD-style script (starts with something like `function ExtPay(extensionId) {`):

```bash
head -5 "/Volumes/Extreme SSD/Chrome Scholar Extension/src/common/extpay.js"
```

- [ ] **Step 2: Add a build target (unminified — do not minify a third-party library you haven't audited the minified output of)**

In `build.js`'s `TARGETS` array, after the new `common/entitlement.js` entry from Task 2:

```js
  {
    label: 'common/extpay.js',
    args: ['src/common/extpay.js', '--bundle=false', '--minify=false', '--outfile=dist/common/extpay.js', '--log-level=warning'],
  },
```

- [ ] **Step 3: Update manifest.template.json — CSP and the required extensionpay.com content script**

ExtPay needs (a) permission to call out to `extensionpay.com`, and (b) a content script running on `extensionpay.com` itself so its payment-success page can notify the extension. Edit `manifest.template.json`:

Add a `content_security_policy` field (new top-level key, after `"optional_host_permissions"`):

```json
  "content_security_policy": {
    "extension_pages": "script-src 'self'; object-src 'self'; connect-src 'self' https://extensionpay.com"
  },
```

Add a second entry to the `content_scripts` array (after the existing two Scholar-domain entries):

```json
    {
      "matches": ["https://extensionpay.com/*"],
      "js": ["dist/common/extpay.js"],
      "run_at": "document_start"
    }
```

- [ ] **Step 4: Rebuild and confirm the generated manifest.json picked up both changes**

Run: `npm run build`
Then: `grep -A3 content_security_policy manifest.json` and `grep -A4 extensionpay.com manifest.json`
Expected: both new blocks appear verbatim in the generated `manifest.json`.

- [ ] **Step 5: Initialize ExtPay in the background**

Append to `src/sw.js` (after the grandfathering listener added in Task 2):

```js
// ExtensionPay background initialization. Must run exactly once per
// service worker lifecycle and must NOT be called again inside message
// listeners below — re-declare `extpay` locally in each listener instead,
// per ExtPay's own documented Manifest V3 caveat.
importScripts("dist/common/extpay.js");
const EXTPAY_ID = "scholar-utility-belt"; // confirmed 2026-09-20 via extensionpay.com/home
const extpay = ExtPay(EXTPAY_ID);
extpay.startBackground();
```

- [ ] **Step 6: Manually verify the background loads without errors**

`npm run build`, reload the extension, open the service worker console. Expected: no errors. Run `typeof extpay` in that console — expected `"object"`.

- [ ] **Step 7: Commit**

```bash
git add src/common/extpay.js build.js manifest.template.json manifest.json src/sw.js
git commit -m "feat: bundle and initialize ExtensionPay in the background service worker"
```

Note: this commit intentionally includes the placeholder `EXTPAY_ID` string. Do not ship a release build with the placeholder still in place — Task 8's rollout checklist re-confirms this.

---

### Task 4: Entitlement status message handler + query helper

**Files:**
- Modify: `src/sw.js`
- Modify: `src/common/entitlement.js`
- Modify: `tests/entitlement.test.mjs`

- [ ] **Step 1: Write the failing test for the pure status-shaping function**

Append to `tests/entitlement.test.mjs`:

```js
import { describeEntitlement } from "../src/common/entitlement.js";

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `describeEntitlement is not a function`.

- [ ] **Step 3: Implement `describeEntitlement`**

Append to `src/common/entitlement.js`:

```js
/**
 * Shapes the final entitlement status shown across the extension's UI.
 * `grandfathered` always wins (a paying-but-also-grandfathered user is
 * still just "grandfathered" for display purposes — they don't need to
 * see a subscription status for a purchase they didn't need to make).
 */
export function describeEntitlement({ grandfathered, extpayUser }) {
  if (grandfathered) return { paid: true, tier: "grandfathered" };
  if (extpayUser?.paid) return { paid: true, tier: extpayUser.plan?.nickname || "paid" };
  return { paid: false, tier: "free" };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS (all `entitlement.test.mjs` tests, plus the full existing suite).

- [ ] **Step 5: Wire a message handler into sw.js**

Add to the existing `chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => { ... })` block in `src/sw.js` (insert as a new `if` branch alongside the existing `"openTabs"`, `"fetchBib"`, etc. branches — do not create a second listener):

```js
  if (msg?.action === "getEntitlementStatus") {
    (async () => {
      const { describeEntitlement } = await import(chrome.runtime.getURL("dist/common/entitlement.js"));
      const { grandfathered } = await chrome.storage.local.get("grandfathered");
      if (grandfathered) {
        sendResponse(describeEntitlement({ grandfathered: true, extpayUser: null }));
        return;
      }
      // Re-declare extpay locally: per ExtPay's Manifest V3 documentation,
      // the outer `extpay` const can be undefined inside message callbacks.
      const localExtpay = ExtPay(EXTPAY_ID);
      const user = await localExtpay.getUser();
      sendResponse(describeEntitlement({ grandfathered: false, extpayUser: user }));
    })();
    return true;
  }
```

- [ ] **Step 6: Manually verify the message round-trip**

Reload the extension, open the service worker console, run:

```js
chrome.runtime.sendMessage({ action: "getEntitlementStatus" }, console.log)
```

Expected on a fresh profile (not grandfathered, no ExtPay account yet): `{ paid: false, tier: "free" }`.

- [ ] **Step 7: Commit**

```bash
git add src/sw.js src/common/entitlement.js tests/entitlement.test.mjs
git commit -m "feat: add entitlement status message handler"
```

---

### Task 5: Client-side entitlement query helper for content.js / popup / options

**Files:**
- Modify: `src/common/entitlement.js`
- Modify: `tests/entitlement.test.mjs`

- [ ] **Step 1: Write the failing test**

Append to `tests/entitlement.test.mjs`, above the existing mock-chrome setup at the top of the file add (if not already present in this file — check before duplicating) a `chrome.runtime.sendMessage` mock, then:

```js
describe("getEntitlementStatus (client helper)", () => {
  it("resolves with whatever the background sends back", async () => {
    globalThis.chrome.runtime = {
      sendMessage: (msg, cb) => {
        assert.equal(msg.action, "getEntitlementStatus");
        cb({ paid: true, tier: "lifetime" });
      },
    };
    const { getEntitlementStatus } = await import("../src/common/entitlement.js");
    const result = await getEntitlementStatus();
    assert.deepEqual(result, { paid: true, tier: "lifetime" });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test`
Expected: FAIL — `getEntitlementStatus is not a function`.

- [ ] **Step 3: Implement the client helper**

Append to `src/common/entitlement.js`:

```js
/**
 * Callable from any extension context (content script via dynamic import,
 * popup, or options page) to get the current user's entitlement status.
 * Always resolves — never rejects — with { paid, tier }.
 */
export function getEntitlementStatus() {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ action: "getEntitlementStatus" }, (response) => {
      resolve(response || { paid: false, tier: "free" });
    });
  });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/common/entitlement.js tests/entitlement.test.mjs
git commit -m "feat: add client-side getEntitlementStatus helper"
```

---

### Task 6: Locked-state upsell UI (shared renderer, used by Tasks 7-9)

**Files:**
- Modify: `src/content/content.js` (add near the top-level helper functions, alongside other small DOM-building helpers — search for `function createButton(` to find that neighborhood and add this function right after it)
- Modify: `src/content/content.css`

- [ ] **Step 1: Add the locked-state HTML builder**

In `src/content/content.js`, add this function near `createButton` (do not duplicate — place it once, anywhere in the top-level function scope inside the outer `(async () => { ... })()`):

```js
  function renderLockedFeature(featureName, description) {
    return `
      <div class="su-locked-feature">
        <div class="su-locked-feature-badge">Pro</div>
        <div class="su-locked-feature-desc">${description}</div>
        <button type="button" class="su-locked-feature-unlock" data-unlock-feature="${featureName}">Unlock</button>
      </div>
    `;
  }
```

- [ ] **Step 2: Add a delegated click handler for the Unlock button**

Find the existing delegated click-handler block in `content.js` that handles `compareBtn`/`venueTag` clicks (the block containing `const compareBtn = e.target.closest(".su-compare-authors-btn");` around line 7834). Add a new branch immediately before it:

```js
        const unlockBtn = e.target.closest("[data-unlock-feature]");
        if (unlockBtn) {
          e.stopPropagation();
          e.preventDefault();
          chrome.runtime.sendMessage({ action: "openUpsellModal" });
          return;
        }
```

(The actual pricing modal is opened via `extpay.openPaymentPage()`, which must run in the background per Task 3 — Task 7 adds the `"openUpsellModal"` message handler to `sw.js`.)

- [ ] **Step 3: Add CSS for the locked-feature block**

Append to `src/content/content.css`:

```css
.su-locked-feature {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--su-surface-muted, #f4f4f5);
  font-size: 12px;
}
.su-locked-feature-badge {
  font-weight: 600;
  font-size: 10px;
  letter-spacing: 0.03em;
  text-transform: uppercase;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--su-accent, #6d28d9);
  color: #fff;
}
.su-locked-feature-desc {
  flex: 1;
  color: var(--su-text-muted, #52525b);
}
.su-locked-feature-unlock {
  font-size: 12px;
  padding: 4px 10px;
  border-radius: 5px;
  border: 1px solid var(--su-accent, #6d28d9);
  background: transparent;
  color: var(--su-accent, #6d28d9);
  cursor: pointer;
}
.su-locked-feature-unlock:hover {
  background: var(--su-accent, #6d28d9);
  color: #fff;
}
```

(If `content.css` already defines `--su-accent` / `--su-surface-muted` / `--su-text-muted` custom properties elsewhere, these rules will automatically pick up the existing theme values — confirm by running `grep -n "\-\-su-accent" src/content/content.css` before assuming the fallback hex values are what actually renders.)

- [ ] **Step 4: Manually verify**

`npm run build`, reload extension, run in the service worker or a content-script console (via the page's DevTools, "Sources" → content script context): confirm `renderLockedFeature` produces the expected HTML string by temporarily calling it from the console if the content script exposes it on `window` for debugging, or simply proceed to Task 7 where it's used in a real render path and verify visually there.

- [ ] **Step 5: Commit**

```bash
git add src/content/content.js src/content/content.css
git commit -m "feat: add locked-feature upsell UI component"
```

---

### Task 7: Add the upsell-modal message handler to the background

**Files:**
- Modify: `src/sw.js`

- [ ] **Step 1: Add the handler**

Add to the same `chrome.runtime.onMessage.addListener` block in `src/sw.js` used in Task 4:

```js
  if (msg?.action === "openUpsellModal") {
    const localExtpay = ExtPay(EXTPAY_ID);
    localExtpay.openPaymentPage(); // shows all configured plans (lifetime, monthly, yearly) for the user to choose
    return false;
  }
  if (msg?.action === "openLoginPage") {
    const localExtpay = ExtPay(EXTPAY_ID);
    localExtpay.openLoginPage();
    return false;
  }
```

- [ ] **Step 2: Manually verify**

Reload the extension, open the service worker console, run `chrome.runtime.sendMessage({ action: "openUpsellModal" })`. Expected: a new tab opens showing ExtensionPay's payment page (will show a "plan not found" or similar error until Ben has configured real plans in the ExtensionPay dashboard with nicknames `lifetime`/`monthly`/`yearly` — that configuration is Ben's task, not part of this plan's code changes).

- [ ] **Step 3: Commit**

```bash
git add src/sw.js
git commit -m "feat: wire upsell modal and login page message handlers"
```

---

### Task 8: Gate the seven feature render points

**Files:**
- Modify: `src/content/content.js`

**Revision note (2026-09-20):** expanded from 3 to 7 gated entry points per the design spec's revision — added the review workspace, citation-graph overlay, Publish-or-Perish report, and the RCR/Influential-Citations numeric badges (gated alongside p-index/FWCI in Step 4). This task has seven independent sub-steps. Do all seven, then run one combined manual verification.

- [ ] **Step 1: Load the entitlement helper into content.js's module set**

In `ensureModulesLoaded()` (see `src/content/content.js:72-144`), add `entitlement` to the `Promise.all` array and destructure it:

```js
    const [storage, quality, domCache, dataLoader, entitlement] = await Promise.all([
      importModuleWithRetry("dist/common/storage.js"),
      importModuleWithRetry("dist/common/quality.js"),
      importModuleWithRetry("dist/content/dom-cache.js"),
      importModuleWithRetry("dist/content/data-loader.js"),
      importModuleWithRetry("dist/common/entitlement.js"),
    ]);
```

Add a new top-level `let` near the other module-provided bindings (e.g., right after `let compileQualityIndex;` around line 41):

```js
  let getEntitlementStatus;
```

Add the destructuring assignment after the existing `({ ...quality... } = quality);` block:

```js
    ({ getEntitlementStatus } = entitlement);
```

- [ ] **Step 2: Gate "Compare authors"**

Replace the compare-button click handler found at `src/content/content.js:7834-7840`:

```js
        const compareBtn = e.target.closest(".su-compare-authors-btn");
        if (compareBtn) {
          e.stopPropagation();
          e.preventDefault();
          openAuthorCompareOverlay(window.suState);
          return;
        }
```

with:

```js
        const compareBtn = e.target.closest(".su-compare-authors-btn");
        if (compareBtn) {
          e.stopPropagation();
          e.preventDefault();
          const entitlement = await getEntitlementStatus();
          if (!entitlement.paid) {
            chrome.runtime.sendMessage({ action: "openUpsellModal" });
            return;
          }
          openAuthorCompareOverlay(window.suState);
          return;
        }
```

(Verify the enclosing function is already `async` — it must be, since it's already awaiting other things nearby such as `bindIdeaLineageButton`'s own async body at line 5853. If the specific delegated-click-handler function wrapping this branch is not itself declared `async`, add `async` to its signature — check with `grep -n "addEventListener(\"click\"" src/content/content.js` to find and confirm the enclosing function signature before editing.)

- [ ] **Step 3: Gate the "Lineage" button**

In `bindIdeaLineageButton` (starts at `src/content/content.js:3668`), the button's click ultimately calls `openIdeaLineageOverlay(container, p)` (see the call sites at lines 3680 and 5853). Add the gate inside `bindIdeaLineageButton` itself, before its click listener invokes the overlay:

```js
  function bindIdeaLineageButton(btn, container) {
    if (!btn || btn.dataset.suLineageBound === "1") return;
    btn.dataset.suLineageBound = "1";
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      e.preventDefault();
      const entitlement = await getEntitlementStatus();
      if (!entitlement.paid) {
        chrome.runtime.sendMessage({ action: "openUpsellModal" });
        return;
      }
      const p = /* existing paper-extraction logic already here — do not change it */;
      await openIdeaLineageOverlay(container, p);
    });
  }
```

Read the real current body of `bindIdeaLineageButton` first (`sed -n '3668,3700p' src/content/content.js`) and merge this gate into it — **do not blindly overwrite the function**, since the paper-extraction logic (`const p = ...`) between the listener setup and the `openIdeaLineageOverlay` call is real existing code this plan hasn't captured verbatim; insert the entitlement check as the very first lines inside the click listener, before any of that existing logic runs.

- [ ] **Step 4: Gate the narrative/p-index/FWCI display block**

The narrative HTML and the `pIndex`/`fwci` numbers are computed synchronously from `stats` (see `generateResearchNarrative(stats)` and the `stats.pIndex`/`stats.fwci` fields populated by `computePIndex`, `src/content/content.js:7038-7186`). Because this render path (around line 8743) is not itself `async`, gate at render time using a cached status instead of an inline `await`:

Add a module-level cache near the top of the file (alongside `let modulesLoaded = false;`):

```js
  let cachedEntitlement = null;
  async function refreshEntitlementCache() {
    cachedEntitlement = await getEntitlementStatus();
    return cachedEntitlement;
  }
```

Call `await refreshEntitlementCache();` once inside `ensureModulesLoaded()`, right after the `({ getEntitlementStatus } = entitlement);` line added in Step 1.

Then in the render block at `src/content/content.js:8743-8749`, replace:

```js
      const narrativeHtml = generateResearchNarrative(stats);
      const narrativeOpen = !!window.suNarrativeOpen;
      const narrativeBlock = narrativeHtml
        ? `<div id="su-narrative" class="su-narrative${narrativeOpen ? "" : " su-narrative-hidden"}">${narrativeHtml}<div class="su-narrative-note">Auto-generated from profile data${stats.pIndexStatus !== "done" ? " · field-normalized metrics still loading…" : ""}. Copy freely.</div></div>`
        : "";
      const narrativeBtn = narrativeHtml
        ? `<button type="button" class="su-compare-authors-btn" data-narrative-toggle="1">${narrativeOpen ? "Hide narrative" : "Narrative"}</button>`
        : "";
```

with:

```js
      const isPaidUser = !!cachedEntitlement?.paid;
      const narrativeHtml = isPaidUser ? generateResearchNarrative(stats) : "";
      const narrativeOpen = !!window.suNarrativeOpen;
      const narrativeBlock = isPaidUser
        ? (narrativeHtml
            ? `<div id="su-narrative" class="su-narrative${narrativeOpen ? "" : " su-narrative-hidden"}">${narrativeHtml}<div class="su-narrative-note">Auto-generated from profile data${stats.pIndexStatus !== "done" ? " · field-normalized metrics still loading…" : ""}. Copy freely.</div></div>`
            : "")
        : renderLockedFeature("narrative-cv", "Narrative CV, p-index, and FWCI are part of Scholar Utility Belt Pro.");
      const narrativeBtn = isPaidUser && narrativeHtml
        ? `<button type="button" class="su-compare-authors-btn" data-narrative-toggle="1">${narrativeOpen ? "Hide narrative" : "Narrative"}</button>`
        : "";
```

This gates the narrative text, p-index, and FWCI together as one block (they're already generated together by `generateResearchNarrative`), matching the spec's grouping of these three under one "Pro" feature. Because `generateResearchNarrative` also composes RCR and Influential-Citations sentences into the same `bits` array (added in the prior, unrelated bibliometrics commit), gating `narrativeHtml` on `isPaidUser` already covers their prose mentions too — Step 5 below handles their separate numeric badges in the stats row.

- [ ] **Step 5: Gate the RCR / Influential-Citations numeric badges**

These render as standalone stat-row items alongside (but not inside) the p-index/FWCI block, around `src/content/content.js:8528-8536`. Wrap both pushes in the same `isPaidUser` flag computed in Step 4 (compute it once, above both this block and the narrative block, since both live in the same render function):

```js
    if (isPaidUser && stats.influentialCitations != null) {
      const iTip = getAuthorStatTooltipHtml("influential", stats);
      const rate = stats.influentialRate != null ? ` (${stats.influentialRate}%)` : "";
      metricsItems.push(`<span class="${CLS_METRIC}" data-stat-tooltip="influential"><span class="su-stat-label">Influential cites:</span> <strong>${stats.influentialCitations}${rate}</strong><span class="su-author-stat-tooltip">${iTip}</span></span>`);
    }
    if (isPaidUser && stats.rcrMean != null) {
      const rTip = getAuthorStatTooltipHtml("rcr", stats);
      metricsItems.push(`<span class="${CLS_METRIC}" data-stat-tooltip="rcr"><span class="su-stat-label">RCR (mean):</span> <strong>${stats.rcrMean}</strong><span class="su-author-stat-tooltip">${rTip}</span></span>`);
    }
```

Since `isPaidUser` is computed later in the function (near the narrative block), hoist that one line (`const isPaidUser = !!cachedEntitlement?.paid;`) to above this block instead, then reuse it at the narrative block in Step 4.

- [ ] **Step 6: Gate the Publish-or-Perish report**

The popup button handler is at `src/content/content.js:7817-7823`, inside the same delegated async click handler as the compare-authors gate from Step 2 — add the identical gate pattern:

```js
        const popBtn = e.target.closest("[data-pop-report]");
        if (popBtn) {
          e.stopPropagation();
          e.preventDefault();
          const entitlement = await getEntitlementStatus();
          if (!entitlement.paid) {
            chrome.runtime.sendMessage({ action: "openUpsellModal" });
            return;
          }
          openPoPOverlay();
          return;
        }
```

- [ ] **Step 7: Gate the citation-graph overlay**

The "Open map" and "Build" buttons are handled in the `[data-graph-open]` / `[data-graph-build]` delegated click listener at `src/content/content.js:8113-8131`, which is already `async`:

```js
        document.addEventListener("click", async (e) => {
          if (e.target.closest("#su-graph-overlay")) return;
          const openBtn = e.target.closest("[data-graph-open]");
          if (openBtn) {
            e.preventDefault();
            const entitlement = await getEntitlementStatus();
            if (!entitlement.paid) {
              chrome.runtime.sendMessage({ action: "openUpsellModal" });
              return;
            }
            await openGraphOverlay();
            return;
          }
          const buildBtn = e.target.closest("[data-graph-build]");
          if (buildBtn) {
            e.preventDefault();
            const entitlement = await getEntitlementStatus();
            if (!entitlement.paid) {
              chrome.runtime.sendMessage({ action: "openUpsellModal" });
              return;
            }
            const count = Math.min(50, Math.max(3, Number(buildBtn.dataset.graphBuildCount) || 10));
            await openGraphOverlay();
            await buildGraphFromSeeds(count);
          }
        });
```

- [ ] **Step 8: Gate the systematic-review workspace**

The review-workspace button is created and bound around `src/content/content.js:15501-15506`:

```js
    const reviewBtn = document.createElement("button");
    reviewBtn.type = "button";
    reviewBtn.className = "su-filter-clear su-review-workspace";
    reviewBtn.textContent = "Review workspace";
    reviewBtn.title = "Open the systematic review workspace for screening and extraction.";
    reviewBtn.addEventListener("click", async () => {
      const entitlement = await getEntitlementStatus();
      if (!entitlement.paid) {
        chrome.runtime.sendMessage({ action: "openUpsellModal" });
        return;
      }
      openReviewOverlay();
    });
```

- [ ] **Step 9: Combined manual verification**

`npm run build`, reload extension. On a fresh test profile (not grandfathered):
1. Visit a Google Scholar author profile page. Confirm the narrative/p-index/FWCI/RCR/influential area now shows the "Pro" locked block with an "Unlock" button instead of real numbers.
2. Click "Compare authors". Confirm it opens the upsell flow instead of the compare overlay.
3. Click a "Lineage" button on a search result or author page. Confirm it opens the upsell flow instead of the lineage overlay.
4. Click "Publish or Perish" report button. Confirm it opens the upsell flow.
5. Click "Open map" / "Build" on the citation graph. Confirm it opens the upsell flow.
6. Click "Review workspace". Confirm it opens the upsell flow.

Then, in the service worker console, run `chrome.storage.local.set({ grandfathered: true })`, reload the Scholar tab, and confirm all seven features render/open normally again (proving the grandfathering path fully unlocks everything, matching a real existing user).

- [ ] **Step 10: Commit**

```bash
git add src/content/content.js
git commit -m "feat: gate compare/lineage/narrative-CV/PoP/graph/review-workspace behind paid entitlement"
```

---

### Task 9: Options page "Scholar Utility Belt Pro" section

**Files:**
- Modify: `src/options/options.html`
- Modify: `src/options/options.js`

- [ ] **Step 1: Add the HTML section**

In `src/options/options.html`, add a new section (place it near other top-level settings sections — find an existing `<section>` block to match the surrounding structure and heading style):

```html
    <section class="su-options-section">
      <h2>Scholar Utility Belt Pro</h2>
      <p id="su-pro-status">Checking your plan…</p>
      <div id="su-pro-actions"></div>
    </section>
```

- [ ] **Step 2: Add the status/actions logic**

In `src/options/options.js`, add (near the top-level initialization code — this file is a real ES module loaded via `<script type="module">`, so a static `import` is correct here, unlike content.js):

```js
import { getEntitlementStatus } from "../common/entitlement.js";

async function renderProStatus() {
  const statusEl = document.getElementById("su-pro-status");
  const actionsEl = document.getElementById("su-pro-actions");
  if (!statusEl || !actionsEl) return;

  const entitlement = await getEntitlementStatus();
  const labels = {
    grandfathered: "Free forever (existing user) — all features unlocked.",
    lifetime: "Pro — Lifetime unlock.",
    monthly: "Pro — Monthly subscription.",
    yearly: "Pro — Annual subscription.",
    free: "Free plan — compare authors, citation lineage, and narrative CV are part of Pro.",
  };
  statusEl.textContent = labels[entitlement.tier] || labels.free;

  actionsEl.innerHTML = "";
  if (!entitlement.paid) {
    const unlockBtn = document.createElement("button");
    unlockBtn.type = "button";
    unlockBtn.textContent = "Unlock Pro";
    unlockBtn.addEventListener("click", () => chrome.runtime.sendMessage({ action: "openUpsellModal" }));
    actionsEl.appendChild(unlockBtn);
  }
  const loginBtn = document.createElement("button");
  loginBtn.type = "button";
  loginBtn.textContent = "Already purchased on another device?";
  loginBtn.addEventListener("click", () => chrome.runtime.sendMessage({ action: "openLoginPage" }));
  actionsEl.appendChild(loginBtn);
}

renderProStatus();
```

Place the `import` line with the file's other top-of-file imports (check `head -10 src/options/options.js` first — the spec's own Section 3 context showed this file already imports several named functions from `../common/storage.js` at the top; add this new import as an additional line in that same existing import block's neighborhood, not a second disconnected import statement, if a single multi-import from a single module isn't required — `../common/entitlement.js` is a different module so it is correctly its own `import` line either way).

- [ ] **Step 3: Manually verify**

`npm run build`, reload extension, open the options page (right-click the extension icon → Options). Confirm the new "Scholar Utility Belt Pro" section renders, shows "Free plan…" on a fresh profile, and both buttons work (Unlock opens the payment page tab, the login link opens ExtensionPay's login page).

- [ ] **Step 4: Commit**

```bash
git add src/options/options.html src/options/options.js
git commit -m "feat: add Pro status section to options page"
```

---

### Task 10: Version bump, disclosure, and rollout checklist

**Files:**
- Modify: `manifest.template.json`
- Modify: `README.md`
- Create or modify: a `CHANGELOG.md` entry (check `ls CHANGELOG.md` first — if it doesn't exist, check `README.md` for an existing "Changelog" section instead of creating a new file the project doesn't use)

- [ ] **Step 1: Version bump**

In `manifest.template.json`, change `"version": "0.5.0"` to `"version": "0.6.0"`. Run `npm run build` to regenerate `manifest.json`.

- [ ] **Step 2: Disclose the ExtensionPay network call**

Add a short paragraph to `README.md`, near its existing "local-first" / privacy claims (search for "telemetry" or "backend" in `README.md` to find the right spot):

```markdown
### A note on the Pro tier

Scholar Utility Belt Pro (author compare, citation lineage, p-index/FWCI, and narrative CV) is
licensed through [ExtensionPay](https://extensionpay.com), which makes a network request to
`extensionpay.com` to check your entitlement status. This is the one exception to the "no backend,
no telemetry" design above — everything else in the extension still runs entirely from data already
on the page or packaged with the extension. Existing users (installed before this feature shipped)
keep every current feature free, permanently.
```

- [ ] **Step 3: Changelog entry**

Add an entry (matching whatever format the existing changelog/README section uses) stating plainly: "v0.6.0: added an optional Pro tier (author compare, citation lineage, p-index/FWCI, narrative CV). If you already had Scholar Utility Belt installed, nothing changes for you — these features stay free. New installs can try them free via [trial mechanism, if configured in ExtensionPay] or unlock via a one-time or subscription purchase."

- [ ] **Step 4: Final pre-release checklist (manual, not automatable)**

- [x] `EXTPAY_ID` is the real value (`scholar-utility-belt`) — confirmed 2026-09-20, no placeholder was ever committed.
- [x] ExtensionPay dashboard has three plans configured with nicknames exactly `lifetime`, `monthly`, `yearly` — confirmed 2026-09-20. Task 7's `openPaymentPage()` call with no argument (showing all plans) is the desired UX.
- [ ] Run the full test suite once more: `npm test` — expected all green, including every test added in Tasks 1, 4, and 5.
- [ ] Run `npm run lint` — expected no new errors introduced by this plan's changes (pre-existing lint issues from the unrelated uncommitted work are out of scope).
- [ ] Update the Chrome Web Store listing description to mention the new Pro tier before submitting the new version for review.
- [ ] Update the extension's published privacy policy (wherever it's hosted — check the Chrome Web Store listing's privacy tab for the current URL) with the same disclosure added to `README.md` in Step 2.

- [ ] **Step 5: Commit**

```bash
git add manifest.template.json manifest.json README.md
git commit -m "chore: bump to v0.6.0, disclose ExtensionPay network call"
```

---

## Out of Scope (matches the spec)

- No changes to any currently-free feature's behavior.
- No ad network integration.
- No custom backend/licensing server.
- No team/institutional licensing.
- **Live unlock after purchase**: Task 8's `cachedEntitlement` is fetched once per Scholar page load (inside `ensureModulesLoaded()`), not re-checked afterward. A user who completes a purchase in the ExtensionPay tab will need to reload their Scholar tab to see gated features unlock — there's no live `onPaid`-triggered refresh of already-open tabs in this plan. Acceptable for v0.6.0; a future iteration could listen for `extpay.onPaid` in the background and broadcast a `chrome.tabs.sendMessage` to open Scholar tabs to bust the cache.
