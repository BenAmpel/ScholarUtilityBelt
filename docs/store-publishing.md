# Publishing to the three stores

`scripts/publish_stores.mjs` uploads a built release to the Chrome Web Store, Microsoft Edge Add-ons and Firefox
Add-ons (AMO) through each store's own API. It can also submit the release for review. It runs only on your Mac, with
credentials you create once in each developer console. Nothing in CI calls it.

## What it does

| Store | `--upload` | `--upload ... --submit` |
|---|---|---|
| Chrome | Uploads `output/ScholarUtilityBelt-<v>.zip` as the item's draft (API v2) | Then calls `publish`, which sends the draft to review. It goes live once approved. |
| Edge | Uploads `output/ScholarUtilityBelt-<v>-edge.zip` to the draft submission and waits until Partner Center has processed it | Then publishes the submission, with `output/firefox-store/release-notes.txt` as the certification notes |
| Firefox | Uploads `output/ScholarUtilityBelt-<v>-firefox.zip` to the listed channel and waits for AMO's validator. AMO keeps nothing for review at this step. | Then creates the listed version with `output/firefox-store/ScholarUtilityBelt-<v>-source.zip` as the source code. Creating it submits it for review. It then sets the release notes (`release-notes.txt`) and the notes to reviewer (`SOURCE-NOTES.txt`). |

Without `--upload` it is a dry run and sends nothing to any store. For each store it checks:

- **Package:** the file exists, its `manifest.json` has the release version, and its sha256 matches the table in
  `output/UPLOAD-<v>.md`. The Firefox package must also carry a gecko id.
- **Firefox source zip:** it exists and its manifest has the same version.
- **Store version:** the version must be newer than what the Chrome Web Store and AMO serve now. The script reads their
  public listings through `store_status.mjs`. The Edge listing is not checked.
- **Notes:** the release and reviewer notes files exist and are not empty.
- **Credentials:** every field is present. The script names only the missing fields, never the values.

An upload run does every dry-run check first and uploads nothing if any of them fails. If one store fails during the
upload, the other stores still go ahead, and the exit code is 1.

## Running it

```bash
bash scripts/package_release.sh            # builds the three packages
node scripts/publish_stores.mjs            # dry run: read every line, fix any PROBLEM
node scripts/publish_stores.mjs --upload chrome,edge,firefox             # upload only
node scripts/publish_stores.mjs --upload chrome,edge,firefox --submit    # upload and submit for review
```

Other options: `--only edge` (dry-run one store), `--version 1.1.1`, `--credentials FILE`, `--release-notes FILE`,
`--approval-notes FILE` and `--offline` (skips reading the public listings). `--help` lists them all.

For a first run, upload without `--submit` and look at the Chrome and Edge drafts in their dashboards. You can then
submit from the dashboard, or rerun with `--submit`. On Firefox, an upload without `--submit` only validates the
package, which is a quick way to see AMO's validator result before you commit to a review.

Before running it, read `output/UPLOAD-<v>.md`. The script uploads `release-notes.txt` exactly as it is on disk, so any
extra sentences the checklist mentions must already be in that file.

## Credentials file

Credentials are kept in `~/.config/scholar-belt/store-credentials.json`, outside the repo. The script refuses the file
if any other user can read it.

```bash
mkdir -p ~/.config/scholar-belt && touch ~/.config/scholar-belt/store-credentials.json && chmod 600 ~/.config/scholar-belt/store-credentials.json
```

```json
{
  "chrome":  { "clientId": "…", "clientSecret": "…", "refreshToken": "…", "publisherId": "…" },
  "edge":    { "clientId": "…", "apiKey": "…", "productId": "…" },
  "firefox": { "jwtIssuer": "user:…", "jwtSecret": "…" }
}
```

You only need the blocks for the stores you use. Each field can also come from an environment variable, which overrides
the file: `SUB_CHROME_CLIENT_ID`, `SUB_CHROME_CLIENT_SECRET`, `SUB_CHROME_REFRESH_TOKEN`, `SUB_CHROME_PUBLISHER_ID`,
`SUB_EDGE_CLIENT_ID`, `SUB_EDGE_API_KEY`, `SUB_EDGE_PRODUCT_ID`, `SUB_AMO_JWT_ISSUER`, `SUB_AMO_JWT_SECRET`.

The script sends credentials only in request headers or the OAuth form body, never in a URL. It removes them from
any error text before printing. Never commit this file or paste its contents anywhere.

## One-time setup in each console

### Chrome Web Store (about 15 minutes)

1. In the [Google Cloud console](https://console.cloud.google.com/), create a project (for example "SUB publishing").
2. Go to **APIs & Services > Library**, find **Chrome Web Store API** and click **Enable**.
3. Set up the **OAuth consent screen** (Google Auth Platform): choose External, use your own email as the support and
   developer contact, and add yourself as a test user. Then set the publishing status to **In production**. While the
   app is in Testing, the refresh token expires after 7 days. An unverified app used only by you is fine; Google shows
   a warning screen once.
4. Go to **Credentials > Create credentials > OAuth client ID**. Choose type **Web application** and add the authorized
   redirect URI `https://developers.google.com/oauthplayground`. Copy the client ID and client secret.
5. Open the [OAuth 2.0 Playground](https://developers.google.com/oauthplayground). Click the gear icon, tick **Use your
   own OAuth credentials** and paste the client ID and secret. In Step 1, type the scope
   `https://www.googleapis.com/auth/chromewebstore`, click **Authorize APIs** and sign in with the account that owns the
   extension. That account needs 2-step verification. In Step 2, click **Exchange authorization code for tokens** and
   copy the **refresh token**.
6. In the [Developer Dashboard](https://chrome.google.com/webstore/devconsole), open **Account** (publisher settings)
   and copy the **publisher ID**.
7. Put the four values in the `chrome` block.

If you change the listing's visibility by hand in the dashboard, the API cannot publish until you have published once
by hand with the new setting.

### Microsoft Edge Add-ons (about 5 minutes, after the listing exists)

The API can only update an extension that is already in Partner Center. If Scholar Utility Belt has not been submitted
to Edge yet, make the first submission by hand. Once the listing is public, also replace the search links in
`scripts/make_variant.mjs` with its real URL.

1. In [Partner Center](https://partner.microsoft.com/dashboard/microsoftedge/overview), open **Microsoft Edge > Publish API**.
2. Turn on the new API experience if Partner Center asks, then click **Create API credentials**. Copy the **Client ID**
   and the **API key**. The key is shown once and expires after a period that Partner Center shows next to it. Create
   a new key before then.
3. Open the extension's overview page in Partner Center and copy its **Product ID** (a GUID, not the store's CRX id).
4. Put the three values in the `edge` block.

### Firefox Add-ons (about 2 minutes)

1. Sign in at [addons.mozilla.org](https://addons.mozilla.org/) with the account that owns the add-on, then open
   [Manage API Keys](https://addons.mozilla.org/developers/addon/api/key/).
2. Click **Generate new credentials**. Copy the **JWT issuer** (it looks like `user:12345:67`) and the **JWT secret**.
3. Put both in the `firefox` block.

## Tests

`tests/publish-stores.test.mjs` covers argument parsing, the file, version and credential checks, redaction, the AMO
JWT, and the requests each store client builds. Every `fetch` in it is a mock that fails on any URL it does not
expect, so `npm test` never contacts a store.
