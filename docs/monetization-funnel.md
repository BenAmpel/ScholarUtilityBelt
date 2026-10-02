# Where each monetization number lives

The extension has no telemetry by design, so the funnel is read from the services that already see each step. Nothing here needs code in the extension.

| Step | Where to read it | Notes |
|---|---|---|
| Impressions, installs, uninstalls | Chrome Web Store developer dashboard | Weekly installs and users. The public listing and chrome-stats only show a rounded bucket (1,000 / 10,000...), which can sit still for months, so they cannot show growth. |
| Eligible users (can ever pay) | Installs since the Pro release | Everyone who updated from an earlier version is grandfathered for the legacy Pro features and is only reachable through features added later (see `PRO_FEATURES` in `src/common/entitlement.js`). |
| Trial starts | ExtensionPay > your extension > Free trial user dashboard | One row per trial with "Free trial started at". Filter by last 7 / 30 / 90 days. |
| Trial to paid | Same page, "Paid at" column | A row with "Paid at" filled in converted. Conversion = rows with Paid at / total rows. |
| Direct purchases and revenue | Stripe dashboard, product created by ExtensionPay | The product only appears after the first live-mode payment. |
| App Pass users and earnings | joinapppass.com/partner dashboard | Active users and estimated earnings, up to 24 hours stale. Paid by PayPal monthly once the balance passes $50. |

## Reading it

- **Trial starts / eligible installs** says whether the offer is being seen and wanted. Low means the problem is the locked-feature prompt or the Pro features, not pricing.
- **Paid / trial starts** says whether the product converts once tried. Low with healthy starts means price or the features behind the lock.
- A trial ends after `TRIAL_DAYS` (14). Judge cohorts only after that window has passed.
- Trials need an email, which ExtensionPay collects and shows you. Treat those addresses as the trial users' data.

## What to do when a number is bad

- No trial starts: check that the locked-feature block shows "Try free for 14 days" on a fresh install, and that Options shows the trial button.
- Starts but no conversions: the trial worked but the paid features did not earn the purchase. Add value behind the lock (new Pro features are gated for everyone, grandfathered or not) before touching price.
- Installs flat: this is a listing and discovery problem. The extension ranks around #46 for "author" and #174 for "badge" in chrome-stats keyword rankings; reviews (16 so far) feed the store ranking.
