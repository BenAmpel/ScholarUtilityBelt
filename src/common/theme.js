// Theme choice: "auto" (follow the system), "light" or "dark".
// The choice lives in chrome.storage.local under settings.theme. An explicit Light or Dark always wins over
// the system color scheme; only "auto" reads the system. The extension pages (Options, popup, library) set
// data-su-theme on <html> so their CSS follows the saved choice instead of prefers-color-scheme alone.
// content-early.js and content.js are plain content scripts that cannot import this module; they repeat
// resolveTheme inline, and tests/theme.test.mjs runs content-early.js against the same cases.

export const THEME_CHOICES = ["auto", "light", "dark"];

export const THEME_LABELS = { auto: "Follow system", light: "Light", dark: "Dark" };

/** Any stored value that is not "light" or "dark" means follow the system. */
export function normalizeThemeChoice(value) {
  return value === "light" || value === "dark" ? value : "auto";
}

/** The theme to show: an explicit choice wins, "auto" follows the system. */
export function resolveTheme(choice, systemPrefersDark) {
  const c = normalizeThemeChoice(choice);
  if (c !== "auto") return c;
  return systemPrefersDark ? "dark" : "light";
}

export function systemPrefersDark(win = typeof window !== "undefined" ? window : null) {
  try {
    return !!win?.matchMedia?.("(prefers-color-scheme: dark)")?.matches;
  } catch {
    return false;
  }
}

/** Mark <html> with the resolved theme. Returns the resolved value. */
export function applyPageTheme(doc, choice, win = typeof window !== "undefined" ? window : null) {
  const resolved = resolveTheme(choice, systemPrefersDark(win));
  doc?.documentElement?.setAttribute("data-su-theme", resolved);
  return resolved;
}

/**
 * Apply the choice now and keep following it: a system change matters only under "auto", and a change saved
 * from another page arrives through storage.onChanged. getChoice returns the current choice.
 */
export function watchPageTheme(doc, getChoice, win = typeof window !== "undefined" ? window : null) {
  applyPageTheme(doc, getChoice(), win);
  try {
    const mq = win?.matchMedia?.("(prefers-color-scheme: dark)");
    const onSystem = () => applyPageTheme(doc, getChoice(), win);
    if (mq?.addEventListener) mq.addEventListener("change", onSystem);
    else if (mq?.addListener) mq.addListener(onSystem);
  } catch {
    // No matchMedia: the explicit choice still applies.
  }
}
