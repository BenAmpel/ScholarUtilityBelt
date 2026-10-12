/**
 * Banner shown when an open Scholar tab outlives an extension update. From then on every
 * chrome.* call fails, so saves go nowhere; the page must be reloaded. The module is loaded
 * at page start on purpose: after the update, new imports from the extension no longer work.
 * Class-based styling (content.css), text via textContent, no chrome.* calls.
 */
export const RELOAD_MESSAGE = "Scholar Utility Belt was updated or reloaded. Reload this page to keep saving: anything you changed on this page since then was not saved.";

export function showReloadBanner(doc = globalThis.document, reload = () => globalThis.location.reload()) {
  if (!doc || !doc.body) return null;
  if (doc.querySelector(".su-reload-banner")) return null;
  const bar = doc.createElement("div");
  bar.className = "su-reload-banner";
  bar.setAttribute("role", "alert");
  const text = doc.createElement("span");
  text.className = "su-reload-banner-text";
  text.textContent = RELOAD_MESSAGE;
  const go = doc.createElement("button");
  go.type = "button";
  go.className = "su-reload-banner-btn";
  go.textContent = "Reload page";
  go.addEventListener("click", () => reload());
  const dismiss = doc.createElement("button");
  dismiss.type = "button";
  dismiss.className = "su-reload-banner-btn";
  dismiss.textContent = "Dismiss";
  dismiss.addEventListener("click", () => bar.remove());
  bar.append(text, go, dismiss);
  doc.body.appendChild(bar);
  return bar;
}
