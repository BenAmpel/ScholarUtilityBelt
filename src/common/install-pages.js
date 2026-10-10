/**
 * The two pages the extension asks Chrome to open on purplelink.llc, and when.
 *
 * The extension itself sends nothing. On a fresh install it opens a getting-started page; when it is
 * uninstalled, Chrome opens a short page. The visits are counted the way any page on purplelink.llc is
 * (first-party, cookieless), which is how installs and uninstalls become a daily count without any
 * telemetry in the extension. See docs/install-pages.md.
 *
 * Both URLs carry the extension's version as ?v=, only when it looks like a version number, so the page
 * can say which release the visitor has and the count can be split by release.
 */
export const START_PAGE = "https://purplelink.llc/scholar-utility-belt/start/";
export const UNINSTALL_PAGE = "https://purplelink.llc/scholar-utility-belt/uninstalled/";

const VERSION_RE = /^\d{1,3}(\.\d{1,3}){1,3}$/;

function withVersion(base, version) {
  const v = String(version ?? "");
  return VERSION_RE.test(v) ? `${base}?v=${v}` : base;
}

export function startPageUrl(version) {
  return withVersion(START_PAGE, version);
}

export function uninstallPageUrl(version) {
  return withVersion(UNINSTALL_PAGE, version);
}

/**
 * What to do when chrome.runtime.onInstalled fires.
 *
 * `reason` is details.reason. Only a brand-new install opens the start page, and only once:
 * `alreadyShown` is the stored flag, so a listener that fires twice, or a re-install that keeps storage,
 * never opens a second tab. An update, a Chrome update and a shared-module update never open anything.
 * The uninstall URL is set on every call, because Chrome keeps one per extension and it must follow
 * the version.
 */
export function onInstalledPlan(reason, version, alreadyShown) {
  return {
    uninstallUrl: uninstallPageUrl(version),
    startUrl: reason === "install" && !alreadyShown ? startPageUrl(version) : null,
  };
}
