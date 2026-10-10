# Install and uninstall pages

The extension has no telemetry. Installs and uninstalls are still countable without it, because the browser can be
asked to open a page:

- **Fresh install, once:** the service worker opens `https://purplelink.llc/scholar-utility-belt/start/?v=<version>` in a
  tab. It is a getting-started page with the same FAQ as the settings page. Updates, Chrome updates and shared-module
  updates never open it, and a stored flag (`startPageShown`) stops a second tab.
- **Uninstall:** `chrome.runtime.setUninstallURL` is set to
  `https://purplelink.llc/scholar-utility-belt/uninstalled/?v=<version>` on every install, update and browser start.
  Chrome opens it after the extension is removed. It carries an optional one-question form.

The extension makes no request itself and needs no extra permission. The two pages count themselves with the site's
first-party, cookieless analytics (`ext_install` and `ext_uninstall` events, with the version as detail), and those
counts reach the dashboard through the stats function's `otherEvents`.

## What the numbers mean

They are lower bounds. A visit is not counted when the visitor has Do Not Track on, blocks the site's script, or
closes the tab before it loads. Enterprise and synced installs open the start page like any other install. Developers
loading an unpacked copy also trigger both events, so test with a throwaway browser profile and ignore those days.
Compare against the Chrome Web Store CSV exports monthly to see how close the lower bound is.

## Where it lives

- `src/common/install-pages.js`: the two URLs and `onInstalledPlan()` (the decision, unit tested).
- `src/sw.js`: the listeners, at the end of the file.
- `src/options/options.html`: the FAQ card.
- Site repo: `site/scholar-utility-belt/start/` and `site/scholar-utility-belt/uninstalled/`.

## When shipping this

Both the Chrome Web Store listing text (`store-assets/STORE_LISTING.md`) and the Privacy practices tab in the
developer console should match: the extension collects nothing; it opens a page on purplelink.llc once on install and
Chrome opens one on uninstall. The site's privacy page needs the same two sentences.
