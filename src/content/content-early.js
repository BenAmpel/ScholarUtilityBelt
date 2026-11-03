/**
 * Runs at document_start so the first paint already has the right background.
 * Paints from the cached choice (or the system setting), then repaints from the saved setting.
 */
(function () {
  const DARK_BG = "#202124";
  const DARK_COLOR = "#e8eaed";
  const LIGHT_BG = "#fff";
  const LIGHT_COLOR = "#202124";
  const STORAGE_CONTEXT_ERROR = "access to storage is not allowed from this context";

  const swallowStorageContextError = (event, message) => {
    if (!message || !String(message).toLowerCase().includes(STORAGE_CONTEXT_ERROR)) return;
    try { event.preventDefault(); } catch (_) {}
    try { event.stopImmediatePropagation(); } catch (_) {}
  };

  window.addEventListener("error", (event) => {
    swallowStorageContextError(event, event?.message || event?.error?.message || "");
  }, true);

  window.addEventListener("unhandledrejection", (event) => {
    const reason = event?.reason;
    swallowStorageContextError(event, reason?.message || reason || "");
  }, true);

  // A saved Light or Dark choice always wins; "auto" (Follow system) uses prefers-color-scheme.
  // Keep in step with resolveTheme in src/common/theme.js.
  const systemDark = () => {
    try { return typeof window.matchMedia !== "undefined" && window.matchMedia("(prefers-color-scheme: dark)").matches; } catch (_) { return false; }
  };
  const resolve = (choice) => (choice === "light" || choice === "dark" ? choice : systemDark() ? "dark" : "light");

  // chrome.storage answers asynchronously, so the first paint uses the choice content.js cached in this
  // page's localStorage on the last visit (or the system setting on a first visit). The stored setting
  // replaces it whenever it arrives; the cache never overrides it.
  const CACHE_KEY = "su-theme-choice";
  let cached = null;
  try { cached = window.localStorage.getItem(CACHE_KEY); } catch (_) { /* storage blocked: use the system setting */ }

  const style = document.createElement("style");
  style.id = "su-early-theme";
  const EARLY_SELECTORS = "html, body, #gs_top, #gs_hdr, #gs_hdr_inn, #gs_hdr_outer, #gs_bdy, #gsc_bdy, #gs_ab, #gs_lnv, #gs_res_ccl, #gs_res_ccl_mid";
  function paint(resolved) {
    const [bg, fg] = resolved === "dark" ? [DARK_BG, DARK_COLOR] : [LIGHT_BG, LIGHT_COLOR];
    style.textContent = `${EARLY_SELECTORS} { background: ${bg} !important; color: ${fg} !important; }`;
    try { document.documentElement?.setAttribute("data-su-theme", resolved); } catch (_) { /* no root element yet */ }
  }
  paint(resolve(cached));
  const target = document.head || document.documentElement;
  if (target) target.insertBefore(style, target.firstChild);

  try {
    if (chrome && chrome.storage && chrome.storage.local && chrome.storage.local.get) {
      chrome.storage.local.get({ settings: {} }, function (result) {
        // No late-answer cut-off: however long storage takes, the saved choice is what the page shows.
        const choice = (result && result.settings && result.settings.theme) || "auto";
        paint(resolve(choice));
        try { window.localStorage.setItem(CACHE_KEY, choice); } catch (_) { /* storage blocked: no cache */ }
      });
    }
  } catch (_) {}
})();
