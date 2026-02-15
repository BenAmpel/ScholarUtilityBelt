/**
 * Runs at document_start to prevent white flash when in dark mode.
 * Injects dark background immediately so the first paint is never white for dark-mode users.
 * Then reads theme from storage and switches to light background if the user prefers light.
 */
(function () {
  const DARK_BG = "#202124";
  const DARK_COLOR = "#e8eaed";
  const LIGHT_BG = "#fff";
  const LIGHT_COLOR = "#202124";

  const style = document.createElement("style");
  style.id = "su-early-theme";
  const EARLY_SELECTORS = "html, body, #gs_top, #gs_hdr, #gs_hdr_inn, #gs_hdr_outer, #gs_bdy, #gsc_bdy, #gs_ab, #gs_lnv, #gs_res_ccl, #gs_res_ccl_mid";
  style.textContent = `${EARLY_SELECTORS} { background: ${DARK_BG} !important; color: ${DARK_COLOR} !important; }`;
  const target = document.head || document.documentElement;
  if (target) target.insertBefore(style, target.firstChild);

  function applyLight() {
    const el = document.getElementById("su-early-theme");
    if (el) el.textContent = `${EARLY_SELECTORS} { background: ${LIGHT_BG} !important; color: ${LIGHT_COLOR} !important; }`;
  }

  try {
    if (chrome && chrome.storage && chrome.storage.local && chrome.storage.local.get) {
      chrome.storage.local.get({ settings: {} }, function (result) {
        const settings = result.settings || {};
        const theme = settings.theme || "auto";
        const resolved =
          theme === "dark"
            ? "dark"
            : theme === "light"
              ? "light"
              : typeof window.matchMedia !== "undefined" && window.matchMedia("(prefers-color-scheme: dark)").matches
                ? "dark"
                : "light";
        if (resolved === "light") applyLight();
      });
    }
  } catch (_) {}
})();
