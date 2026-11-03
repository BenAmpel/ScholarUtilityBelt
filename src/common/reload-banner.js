/**
 * Notices on the Scholar page. First, the banner shown when an open Scholar tab outlives an extension update. From then on every
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

/**
 * Notice shown while the Scholar cooldown runs (after Scholar answered with a robot check or a rate limit).
 * `notice` comes from scholarCooldownNotice() in scholar-links.js. One notice per page: a second call updates
 * the time left in place, so focus and the screen reader's last announcement are kept. Only the title line is
 * a live region; the minute countdown is not, so it is not read out every update.
 * Retry awaits onRetry() (which clears the cooldown and re-runs the paused work), then removes the notice.
 */
export function showScholarCooldownNotice(doc = globalThis.document, notice, { onRetry = () => {}, onHide = () => {} } = {}) {
  if (!doc || !doc.body || !notice) return null;
  const existing = doc.querySelector(".su-cooldown-notice");
  if (existing) {
    const time = existing.querySelector(".su-cooldown-notice-time");
    if (time) time.textContent = notice.timeLeft;
    return existing;
  }
  const bar = doc.createElement("div");
  bar.className = "su-cooldown-notice";
  bar.setAttribute("role", "region");
  bar.setAttribute("aria-label", notice.title);
  const status = doc.createElement("p");
  status.className = "su-cooldown-notice-title";
  status.setAttribute("role", "status");
  status.textContent = `${notice.title}. ${notice.hint}`;
  const time = doc.createElement("p");
  time.className = "su-cooldown-notice-time";
  time.textContent = notice.timeLeft;
  const pausedLabel = doc.createElement("p");
  pausedLabel.className = "su-cooldown-notice-label";
  pausedLabel.textContent = "Paused until then:";
  const list = doc.createElement("ul");
  list.className = "su-cooldown-notice-list";
  for (const f of notice.features || []) {
    const li = doc.createElement("li");
    li.textContent = f;
    list.appendChild(li);
  }
  const actions = doc.createElement("div");
  actions.className = "su-cooldown-notice-actions";
  const link = doc.createElement("a");
  link.className = "su-cooldown-notice-link";
  link.href = notice.solveUrl;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.textContent = notice.linkText;
  const retry = doc.createElement("button");
  retry.type = "button";
  retry.className = "su-cooldown-notice-btn su-cooldown-notice-retry";
  retry.textContent = "Retry";
  retry.addEventListener("click", async () => {
    if (retry.disabled) return;
    retry.disabled = true;
    try { await onRetry(); } finally { bar.remove(); }
  });
  const hide = doc.createElement("button");
  hide.type = "button";
  hide.className = "su-cooldown-notice-btn";
  hide.textContent = "Hide";
  hide.addEventListener("click", () => { bar.remove(); onHide(); });
  actions.append(link, retry, hide);
  bar.append(status, time, pausedLabel, list, actions);
  doc.body.appendChild(bar);
  return bar;
}

/** Removes the cooldown notice, if one is showing. */
export function hideScholarCooldownNotice(doc = globalThis.document) {
  const bar = doc?.querySelector?.(".su-cooldown-notice");
  if (bar) bar.remove();
}
