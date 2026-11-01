// The manifest changes make_variant.mjs applies for each store. Pure, so tests/permission-growth.test.mjs can check
// that no variant asks for more than the Chrome manifest does (a variant that adds a permission would disable the
// extension for that store's existing users on update, the 0.6.0 problem).
export function variantManifest(manifest, target) {
  const m = structuredClone(manifest);
  if (target === "firefox") {
    // Firefox runs MV3 backgrounds as event pages (background.scripts), not service workers. These are the files
    // sw.js loads with importScripts in Chrome, in the same order, then sw.js itself.
    m.background = { scripts: [
      "dist/common/extpay.js", "dist/common/entitlement.sw.js", "dist/common/url-policy.sw.js",
      "dist/common/apppass.sw.js", "dist/common/install-pages.sw.js", "dist/common/ask-sw.js", "src/sw.js",
    ] };
    // The downloads API is only used to suggest a file name, behind a typeof check; exports use plain link downloads.
    m.permissions = (m.permissions || []).filter((p) => p !== "downloads");
    m.browser_specific_settings = { gecko: {
      id: "scholar-utility-belt@purplelink.llc",
      strict_min_version: "140.0",
      // Declared for Firefox's install prompt. The default detailed view sends each result's DOI or title to public
      // scholarly services (OpenAlex, Crossref and others), which is website content; watch lists send the search
      // text you add to OpenAlex. Nothing goes to the developer. Review before submitting.
      data_collection_permissions: { required: ["websiteContent"], optional: ["searchTerms"] },
    } };
  }
  return m;
}
