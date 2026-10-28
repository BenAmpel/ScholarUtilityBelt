// Service worker. It does the work content scripts cannot: ExtensionPay and App Pass, downloads,
// and cross-origin fetches on their behalf (public scholarly APIs, publisher pages the user opens).
// It makes no request of its own to any host except ExtensionPay, and every fetch it makes for a
// page is checked against src/common/url-policy.js first.

function sanitizeFilenameSegment(s, maxLen) {
  let t = s.replace(/[/\\:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
  if (maxLen && t.length > maxLen) t = t.slice(0, maxLen).trim();
  return t || "Unknown";
}

// URL and caller rules live in src/common/url-policy.js (loaded below as SUUrlPolicy, with tests).
const isFetchableUrl = (url) => SUUrlPolicy.isFetchableUrl(url);
const isScholarUrl = (url) => SUUrlPolicy.isScholarUrl(url);
const isAllowedApiUrl = (url) => SUUrlPolicy.isAllowedApiUrl(url);
const fromOwnContext = (sender) => SUUrlPolicy.isOwnContext(sender, chrome.runtime.id, chrome.runtime.getURL(""));

function normalizeFetchHeaders(headers) {
  if (!headers || typeof headers !== "object") return undefined;
  const out = {};
  for (const [key, value] of Object.entries(headers)) {
    if (!key || value == null) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      out[key] = String(value);
    }
  }
  return Object.keys(out).length ? out : undefined;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const timeout = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 10000;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(timer);
  }
}

// ExtensionPay background initialization. Must run exactly once per
// service worker lifecycle and must NOT be called again inside message
// listeners below — re-declare `extpay` locally in each listener instead,
// per ExtPay's own documented Manifest V3 caveat (it can otherwise become
// undefined inside service-worker event callbacks).
// importScripts() resolves relative URLs against this script's own location
// (src/), not the extension root — chrome.runtime.getURL() is required here.
// Dynamic import() is disallowed in a classic (non-module) service worker
// per the HTML spec, so entitlement.js is loaded the same way, as a global
// (SUEntitlement), rather than via import().
// extpay.js and entitlement.sw.js are required: without them nothing below can work. The others
// are optional features, loaded one at a time so a missing or broken file turns off only that
// feature (each use below already sits in a try/catch or .catch) instead of stopping the worker
// from registering any handler at all.
// Firefox runs this file as an event page, where importScripts does not exist: its manifest lists these files
// before sw.js instead (scripts/make_variant.mjs), so there the call does nothing.
const loadScripts = typeof importScripts === "function" ? importScripts : () => {};
loadScripts(
  chrome.runtime.getURL("dist/common/extpay.js"),
  chrome.runtime.getURL("dist/common/entitlement.sw.js"),
  chrome.runtime.getURL("dist/common/url-policy.sw.js")
);
for (const optional of ["apppass.sw.js", "install-pages.sw.js", "ask-sw.js"]) {
  try {
    loadScripts(chrome.runtime.getURL(`dist/common/${optional}`));
  } catch (e) {
    console.warn(`[SU] optional script ${optional} failed to load; that feature is off`, e);
  }
}
// Content scripts can only use chrome.storage.session after this call (the default is extension pages and this worker
// only). Without it the content script's session caches (the compiled quality index, the PDF-name map read by the
// download helper below) silently failed and every Scholar page rebuilt the index. Still extension-only: web pages
// never see this storage. Must run on every worker start; the setting is not persisted.
try {
  chrome.storage.session?.setAccessLevel?.({ accessLevel: "TRUSTED_AND_UNTRUSTED_CONTEXTS" })?.catch?.(() => {});
} catch (_) { /* older browsers: the caches stay off, as before */ }
const EXTPAY_ID = "scholar-utility-belt";
const extpay = ExtPay(EXTPAY_ID);
extpay.startBackground();

// App Pass (joinapppass.com) is strictly opt-in: nothing below talks to it unless
// the user turned it on from Options. The SDK's check sends this extension's id and
// the user's joinapppass.com cookie, and the server counts those checks as "used
// today" for revenue sharing, so it must never run for someone who didn't ask.
let appPassInFlight = null;

// ExtensionPay lookup that survives being offline or hanging. ExtPay throws when it cannot reach
// its server and has no timeout, which used to leave a paying user on the free plan. A live answer
// is remembered (time only; ExtPay stores the user itself) and, when the lookup fails or takes more
// than 8 s, the last stored user is used for up to EXTPAY_OFFLINE_GRACE_DAYS.
async function getExtpayUserOrCached(localExtpay) {
  try {
    const user = await Promise.race([
      localExtpay.getUser(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("ExtensionPay timed out")), 8000)),
    ]);
    chrome.storage.local.set({ suExtpayOkAt: Date.now() }).catch(() => {});
    return user;
  } catch {
    try {
      const [sync, local] = await Promise.all([
        chrome.storage.sync.get("extensionpay_user").catch(() => ({})),
        chrome.storage.local.get(["extensionpay_user", "suExtpayOkAt"]).catch(() => ({})),
      ]);
      const cached = sync.extensionpay_user || local.extensionpay_user || null;
      let okAt = local.suExtpayOkAt ?? null;
      if (cached && okAt == null) {
        // No live lookup has been recorded yet (first run after this update): start the grace period now.
        okAt = Date.now();
        chrome.storage.local.set({ suExtpayOkAt: okAt }).catch(() => {});
      }
      return SUEntitlement.offlineExtpayUser(cached, okAt);
    } catch {
      return null;
    }
  }
}

async function resolveAppPass({ force = false } = {}) {
  const { appPassOptIn, appPassCache } = await chrome.storage.local.get(["appPassOptIn", "appPassCache"]);
  if (appPassOptIn !== true) return null;
  if (!force && SUEntitlement.isAppPassCacheFresh(appPassCache, Date.now())) return appPassCache;
  // Every Scholar tab asks for entitlement on load; share one in-flight check.
  if (!appPassInFlight) {
    appPassInFlight = (async () => {
      try {
        const res = await SUAppPass.checkAppPass();
        const fresh = { status: String(res?.status || "unknown_error"), checkedAt: Date.now() };
        await chrome.storage.local.set({ appPassCache: fresh });
        return fresh;
      } catch {
        return { status: "unknown_error", checkedAt: Date.now() };
      } finally {
        appPassInFlight = null;
      }
    })();
  }
  return appPassInFlight;
}

// Smart-rename PDF: when a PDF download matches a URL we have metadata for (from Scholar result), suggest [Author] - [Year] - [Title].pdf
if (typeof chrome.downloads !== "undefined" && chrome.downloads.onDeterminingFilename) {
  chrome.downloads.onDeterminingFilename.addListener(function (downloadItem, suggest) {
    (function run() {
      const isPdf =
        (downloadItem.mimeType && downloadItem.mimeType.toLowerCase() === "application/pdf") ||
        (downloadItem.filename && /\.pdf$/i.test(downloadItem.filename));
      if (!isPdf || !downloadItem.url) {
        suggest({ filename: downloadItem.filename || "download" });
        return;
      }
      chrome.storage.session.get("pdfUrlToMetadata").then(function (st) {
        const map = st.pdfUrlToMetadata && typeof st.pdfUrlToMetadata === "object" ? st.pdfUrlToMetadata : {};
        const meta = map[downloadItem.url] || map[downloadItem.finalUrl];
        if (!meta || (meta.author == null && meta.title == null)) {
          suggest({ filename: downloadItem.filename || "download" });
          return;
        }
        const author = sanitizeFilenameSegment(String(meta.author || "Unknown").trim(), 60);
        const year = meta.year ? String(meta.year).replace(/\D/g, "").slice(0, 4) : "";
        const title = sanitizeFilenameSegment(String(meta.title || "Paper").trim(), 80);
        const parts = [author, year, title].filter(Boolean);
        suggest({ filename: (parts.join(" - ") || "download") + ".pdf" });
      }).catch(function () { suggest({ filename: downloadItem.filename || "download" }); });
    })();
    return true;
  });
}

// Messages that make the worker fetch or open something for the caller. Anything that is not from this
// extension (its pages, or its content script on a Scholar page) is refused.
const CALLER_CHECKED_ACTIONS = new Set(["openTabs", "fetchBib", "fetchSnippet", "fetchExternal"]);

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (CALLER_CHECKED_ACTIONS.has(msg?.action) && !fromOwnContext(sender)) {
    sendResponse({ ok: false, error: "not_allowed" });
    return false;
  }
  if (msg?.action === "openTabs" && Array.isArray(msg.urls)) {
    (async () => {
      let opened = 0;
      for (const url of msg.urls) {
        if (typeof url !== "string" || !/^https?:/i.test(url)) continue;
        try {
          await new Promise((resolve) => {
            chrome.tabs.create({ url, active: false }, () => resolve());
          });
          opened += 1;
        } catch (_) {}
      }
      sendResponse({ ok: true, opened });
    })();
    return true;
  }
  if (msg?.action === "fetchBib" && typeof msg.url === "string") {
    if (!isFetchableUrl(msg.url)) {
      sendResponse({ ok: false, error: "unsupported_url" });
      return true;
    }
    // Same 10 s limit as the other fetch handlers; this one had none and could hold the worker open.
    fetchWithTimeout(msg.url, { credentials: "omit" }, 10000)
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((body) => sendResponse({ ok: true, body }))
      .catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "fetchSnippet" && typeof msg.url === "string") {
    const credentials = isScholarUrl(msg.url) ? "include" : "omit";
    fetchWithTimeout(msg.url, { credentials }, 8000)
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((html) => sendResponse({ ok: true, html }))
      .catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "fetchExternal" && typeof msg.url === "string") {
    // Only the public scholarly APIs the extension documents (see ALLOWED_API_HOSTS), over https.
    if (!isAllowedApiUrl(msg.url)) {
      sendResponse({ ok: false, error: "unsupported_url" });
      return true;
    }
    const timeout = Number(msg.timeoutMs) > 0 ? Number(msg.timeoutMs) : 10000;
    const headers = normalizeFetchHeaders(msg.headers);
    const responseType = msg.responseType === "text" ? "text" : "json";
    fetchWithTimeout(msg.url, { credentials: "omit", headers }, timeout)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return responseType === "text" ? r.text() : r.json();
      })
      .then((body) => sendResponse({ ok: true, body }))
      .catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "graphMonitorCheck") {
    const authorId = typeof msg.authorId === "string" ? msg.authorId : "";
    checkGraphMonitors(authorId).then((res) => sendResponse({ ok: true, result: res })).catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "getEntitlementStatus") {
    (async () => {
      const { describeEntitlement, isTrialActive } = SUEntitlement;
      const { grandfathered } = await chrome.storage.local.get("grandfathered");
      // Grandfathered users are answered offline (no ExtensionPay call) unless the
      // caller is gating a "new" Pro feature and needs to know if they truly purchased.
      if (grandfathered && msg.needPurchased !== true) {
        sendResponse(describeEntitlement({ grandfathered: true, extpayUser: null }));
        return;
      }
      try {
        // Re-declare extpay locally: per ExtPay's Manifest V3 documentation,
        // the outer `extpay` const can be undefined inside message callbacks.
        const localExtpay = ExtPay(EXTPAY_ID);
        const user = await getExtpayUserOrCached(localExtpay);
        // Someone who already paid, or is mid-trial, never triggers an App Pass check.
        const appPass = user?.paid || isTrialActive(user) ? null : await resolveAppPass().catch(() => null);
        sendResponse(describeEntitlement({ grandfathered: !!grandfathered, extpayUser: user, appPass }));
      } catch {
        // ExtensionPay unreachable: fall back to what we know offline rather than hang the caller.
        sendResponse(describeEntitlement({ grandfathered: !!grandfathered, extpayUser: null }));
      }
    })();
    return true;
  }
  if (msg?.action === "startTrial") {
    (async () => {
      const localExtpay = ExtPay(EXTPAY_ID);
      const user = await localExtpay.getUser();
      if (user?.paid) { sendResponse({ ok: false, reason: "already_paid" }); return; }
      if (user?.trialStartedAt) { sendResponse({ ok: false, reason: "already_used" }); return; }
      await localExtpay.openTrialPage(`${SUEntitlement.TRIAL_DAYS}-day`);
      sendResponse({ ok: true });
    })().catch(() => sendResponse({ ok: false, reason: "error" }));
    return true;
  }
  if (msg?.action === "appPassState") {
    // Local state only, no network.
    chrome.storage.local.get(["appPassOptIn", "appPassCache"]).then(({ appPassOptIn, appPassCache }) => {
      sendResponse({ optedIn: appPassOptIn === true, status: appPassCache?.status || null });
    }).catch(() => sendResponse({ optedIn: false, status: null }));
    return true;
  }
  if (msg?.action === "appPassSetOptIn") {
    const enabled = msg.enabled === true;
    chrome.storage.local.set({ appPassOptIn: enabled }).then(async () => {
      if (!enabled) await chrome.storage.local.remove("appPassCache");
      sendResponse({ ok: true });
    }).catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg?.action === "appPassRefresh") {
    resolveAppPass({ force: true })
      .then((res) => sendResponse({ ok: true, status: res?.status || null }))
      .catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg?.action === "appPassActivate") {
    (async () => {
      const { appPassOptIn } = await chrome.storage.local.get("appPassOptIn");
      if (appPassOptIn !== true) { sendResponse({ ok: false }); return; }
      await chrome.storage.local.remove("appPassCache"); // a stale "no pass" must not outlive activation
      await SUAppPass.activateAppPass();
      sendResponse({ ok: true });
    })().catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg?.action === "appPassManage") {
    SUAppPass.manageAppPass().then(() => sendResponse({ ok: true })).catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg?.action === "askShortlist") {
    // "Ask my shortlist" (Pro, bring your own key). Only the extension's own pages may call this, because
    // it spends the user's API key. The key is read here from chrome.storage.local and goes nowhere except
    // the provider the user picked. It is never logged, and never put in a response.
    (async () => {
      const fromOwnPage = sender?.id === chrome.runtime.id && typeof sender?.url === "string" && sender.url.startsWith(chrome.runtime.getURL(""));
      if (!fromOwnPage) { sendResponse({ ok: false, error: "Not allowed from this page." }); return; }
      if (msg.confirmed !== true) { sendResponse({ ok: false, error: "Not confirmed. Nothing was sent." }); return; }
      const { grandfathered } = await chrome.storage.local.get("grandfathered");
      const localExtpay = ExtPay(EXTPAY_ID);
      const user = await getExtpayUserOrCached(localExtpay);
      const appPass = user?.paid || SUEntitlement.isTrialActive(user) ? null : await resolveAppPass().catch(() => null);
      const ent = SUEntitlement.describeEntitlement({ grandfathered: !!grandfathered, extpayUser: user, appPass });
      if (!SUEntitlement.canUseFeature("askShortlist", ent)) { sendResponse({ ok: false, error: "Ask my shortlist is a Pro feature." }); return; }
      const { askShortlistConfig: cfg } = await chrome.storage.local.get("askShortlistConfig");
      const provider = SUAsk.getProvider(cfg?.providerId);
      if (!cfg?.enabled || !provider || !cfg.apiKey) { sendResponse({ ok: false, error: "Turn on Ask my shortlist in Options first." }); return; }
      if (!(await chrome.permissions.contains({ origins: [provider.origin] }))) { sendResponse({ ok: false, error: "The host permission for " + provider.host + " is not granted. Turn the feature on again in Options." }); return; }
      const keys = Array.isArray(msg.keys) ? msg.keys.filter((k) => typeof k === "string") : [];
      const { savedPapers } = await chrome.storage.local.get("savedPapers");
      const { papers } = SUAsk.selectPapersForAsk(keys.map((k) => ({ key: k, ...(savedPapers && savedPapers[k]) })));
      const result = await SUAsk.runAsk({
        providerId: cfg.providerId, apiKey: cfg.apiKey, model: cfg.model,
        question: String(msg.question || ""), papers, confirmed: true,
      });
      sendResponse(result);
    })().catch(() => sendResponse({ ok: false, error: "Something went wrong. Nothing more was sent." }));
    return true;
  }
  if (msg?.action === "openUpsellModal") {
    const localExtpay = ExtPay(EXTPAY_ID);
    localExtpay.openPaymentPage(); // shows all configured plans (lifetime, monthly, yearly) for the user to choose
    return false;
  }
  if (msg?.action === "openLoginPage") {
    const localExtpay = ExtPay(EXTPAY_ID);
    localExtpay.openLoginPage();
    return false;
  }
  if (msg?.action === "openTenurePacket") {
    // The packet page reads the snapshot the profile content script saved in storage.
    chrome.tabs.create({ url: chrome.runtime.getURL("src/library/tenure-packet.html") }, () => {
      void chrome.runtime.lastError;
      sendResponse({ ok: true });
    });
    return true;
  }
  if (msg?.action === "openOptionsPage") {
    chrome.runtime.openOptionsPage();
    return false;
  }
});

const GRAPH_ALERTS_KEY = "authorGraphAlerts";
const GRAPH_MONITOR_ALARM = "graphMonitorCheck";
const GRAPH_MONITOR_MINUTES = 24 * 60;

function normalizeOpenAlexId(id) {
  if (!id) return "";
  const raw = String(id).trim();
  const match = raw.match(/W\d+/i);
  if (match) return match[0].toUpperCase();
  return raw;
}

function formatOpenAlexUrl(url) {
  try {
    const u = new URL(url);
    if (!u.searchParams.has("mailto")) u.searchParams.set("mailto", "scholar-extension@local");
    return u.toString();
  } catch {
    return url;
  }
}

async function fetchOpenAlexWork(id) {
  const norm = normalizeOpenAlexId(id);
  if (!norm) return null;
  const url = formatOpenAlexUrl(`https://api.openalex.org/works/${encodeURIComponent(norm)}`);
  try {
    const res = await fetchWithTimeout(url, {}, 10000);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function fetchOpenAlexWorks(ids) {
  const cleaned = Array.from(new Set(ids.map((id) => normalizeOpenAlexId(id)).filter(Boolean)));
  if (!cleaned.length) return [];
  const chunk = cleaned.slice(0, 25);
  const filter = chunk.map((id) => `https://openalex.org/${id}`).join("|");
  const url = formatOpenAlexUrl(`https://api.openalex.org/works?filter=ids:${encodeURIComponent(filter)}&per_page=${chunk.length}`);
  try {
    const res = await fetchWithTimeout(url, {}, 10000);
    if (!res.ok) return [];
    const data = await res.json();
    return Array.isArray(data?.results) ? data.results : [];
  } catch {
    return [];
  }
}

async function checkGraphMonitors(targetAuthorId) {
  const stored = await chrome.storage.local.get({ [GRAPH_ALERTS_KEY]: {} });
  const map = stored[GRAPH_ALERTS_KEY] || {};
  const keys = targetAuthorId ? [targetAuthorId] : Object.keys(map);
  const nowIso = new Date().toISOString();
  // Results are collected here and merged into a fresh read at the end: this loop awaits network
  // calls for a long time, and writing back the map read at the start would erase changes the
  // user made in the meantime (read marks, disabling a monitor).
  const updates = {};
  for (const authorId of keys) {
    const entry = map[authorId];
    if (!entry || !entry.enabled) continue;
    const seedIds = Array.isArray(entry.seedIds) ? entry.seedIds.slice(0, 6) : [];
    if (!seedIds.length) continue;
    const lastCheck = entry.lastCheck || nowIso;
    const parsedLastDate = new Date(lastCheck);
    const lastDate = Number.isFinite(parsedLastDate.getTime()) ? parsedLastDate : new Date(0);
    const candidateIds = new Set();
    const seedWorks = await Promise.all(seedIds.map((seed) => fetchOpenAlexWork(seed)));
    // Every lookup failed (offline, rate limit, server error): leave lastCheck alone. Moving it on
    // would mean works published during an outage are never alerted.
    if (!seedWorks.some(Boolean)) continue;
    for (const work of seedWorks) {
      const related = Array.isArray(work?.related_works) ? work.related_works.slice(0, 8) : [];
      related.forEach((r) => {
        const id = normalizeOpenAlexId(r);
        if (id) candidateIds.add(id);
      });
    }
    const candidates = Array.from(candidateIds);
    const newAlerts = [];
    const workBatches = [];
    for (let i = 0; i < candidates.length; i += 25) {
      workBatches.push(fetchOpenAlexWorks(candidates.slice(i, i + 25)));
    }
    const workResults = await Promise.all(workBatches);
    if (candidates.length > 0 && workResults.every((w) => !Array.isArray(w) || w.length === 0)) continue;
    for (const works of workResults) {
      for (const w of works) {
        const pubDate = w?.publication_date ? new Date(w.publication_date) : null;
        if (pubDate && pubDate > lastDate) {
          const id = normalizeOpenAlexId(w?.id || "");
          newAlerts.push({
            id,
            title: w?.display_name || w?.title || "Untitled",
            year: w?.publication_year || "",
            url: w?.id || "",
            reason: "Related work",
            foundAt: nowIso
          });
        }
      }
    }
    updates[authorId] = { newAlerts, lastCheck: nowIso };
  }
  const fresh = (await chrome.storage.local.get({ [GRAPH_ALERTS_KEY]: {} }))[GRAPH_ALERTS_KEY] || {};
  for (const [authorId, update] of Object.entries(updates)) {
    if (!fresh[authorId]) continue; // removed while the check ran
    const current = Array.isArray(fresh[authorId].alerts) ? fresh[authorId].alerts : [];
    const known = new Set(current.map((a) => a.id));
    const added = update.newAlerts.filter((a) => !known.has(a.id));
    fresh[authorId] = {
      ...fresh[authorId],
      alerts: [...added, ...current].slice(0, 50),
      unreadCount: Math.min(50, (Number(fresh[authorId].unreadCount) || 0) + added.length),
      lastCheck: update.lastCheck
    };
  }
  await chrome.storage.local.set({ [GRAPH_ALERTS_KEY]: fresh });
  return true;
}

function ensureGraphMonitorAlarm() {
  try {
    chrome.alarms?.create?.(GRAPH_MONITOR_ALARM, { periodInMinutes: GRAPH_MONITOR_MINUTES });
  } catch {}
}

chrome.runtime.onInstalled.addListener(() => {
  ensureGraphMonitorAlarm();
});

chrome.runtime.onStartup?.addListener?.(() => {
  ensureGraphMonitorAlarm();
});

chrome.alarms?.onAlarm?.addListener?.((alarm) => {
  if (alarm?.name !== GRAPH_MONITOR_ALARM) return;
  checkGraphMonitors().catch(() => {});
});

// Bundled reference datasets for venue-quality badges (FT50, UTD24, ABDC, VHB,
// CORE, CCF). Each ships inside the extension and gets corrected/extended over
// time (a new CORE edition, a fixed alias — see the 2026-09 NeurIPS fix, which
// this refresh exists to keep from needing a manual "Clear + Load defaults"
// click ever again). A field seeded from these files, and never hand-edited in
// Options, should pick up an update on the next extension update; a field the
// user customized in Options must never be silently overwritten. options.js
// sets the paired "*Custom" flag only when a saved value stops matching the
// bundled content, so untouched fields — including pre-existing installs from
// before this flag existed, where it defaults to false — stay refreshable.
const QUALITY_DATASETS = [
  { key: "qualityFt50List", customKey: "qualityFt50ListCustom", files: ["src/data/ft50.txt"] },
  { key: "qualityUtd24List", customKey: "qualityUtd24ListCustom", files: ["src/data/utd24.txt"] },
  { key: "qualityAbdcRanks", customKey: "qualityAbdcRanksCustom", files: ["src/data/abdc2025.csv"] },
  { key: "qualityVhbRanks", customKey: "qualityVhbRanksCustom", files: ["src/data/vhb2024.csv"] },
  { key: "qualityCoreRanks", customKey: "qualityCoreRanksCustom",
    files: ["src/data/core_icore2026.csv", "src/data/core_portal_ranks.csv"] },
  { key: "qualityCcfRanks", customKey: "qualityCcfRanksCustom", files: ["src/data/ccf_ranks.csv"] }
];

async function fetchBundledDataset(files) {
  const texts = await Promise.all(
    files.map((f) => fetch(chrome.runtime.getURL(f)).then((r) => (r.ok ? r.text() : "")).catch(() => ""))
  );
  const merged = texts.map((t) => (t || "").trim()).filter(Boolean).join("\n");
  return merged ? merged + "\n" : "";
}

const BUNDLED_QUARTILES_FILE = "src/data/scimago_2024_quartiles.json";
// The bundled snapshot's meta.source; Options imports use "SCImago (SJR export)" or "SCImago (download)".
function isBundledQuartilesMeta(meta) {
  return meta?.source === "SCImago Journal Rank (CSV export)";
}

chrome.runtime.onInstalled.addListener(async () => {
  // Initialize storage keys if missing.
  const { savedPapers, settings } = await chrome.storage.local.get({ savedPapers: {}, settings: {} });
  const s = settings || {};
  const hadAnyQuality = QUALITY_DATASETS.some((d) => !!String(s[d.key] || "").trim());
  const updatedSettings = { ...s };

  for (const { key, customKey, files } of QUALITY_DATASETS) {
    const current = String(updatedSettings[key] || "").trim();
    if (current && updatedSettings[customKey] === true) continue; // hand-edited in Options — never overwrite
    try {
      const bundled = await fetchBundledDataset(files);
      if (bundled && bundled.trim() !== current) updatedSettings[key] = bundled;
    } catch {
      // Ignore; user can reload from Options ("Load built-in lists").
    }
  }
  if (!hadAnyQuality) updatedSettings.showQualityBadges = true;

  await chrome.storage.local.set({ savedPapers, settings: updatedSettings });

  // Seed the bundled SJR quartiles, and replace an older bundled snapshot when a newer one ships. A file the
  // user imported in Options (its meta.source differs) is never replaced.
  const { qualityQuartilesIndex, qualityQuartilesMeta } = await chrome.storage.local.get({ qualityQuartilesIndex: {}, qualityQuartilesMeta: null });
  const hasIndex = qualityQuartilesIndex && Object.keys(qualityQuartilesIndex).length > 0;
  if (!hasIndex || isBundledQuartilesMeta(qualityQuartilesMeta)) {
    try {
      const url = chrome.runtime.getURL(BUNDLED_QUARTILES_FILE);
      const r = await fetch(url);
      if (r.ok) {
        const data = await r.json();
        const index = data?.index || {};
        const meta = data?.meta || null;
        const newer = !hasIndex || Number(meta?.year) > Number(qualityQuartilesMeta?.year || 0);
        if (newer && index && typeof index === "object" && Object.keys(index).length > 1000) {
          await chrome.storage.local.set({ qualityQuartilesIndex: index, qualityQuartilesMeta: meta });
        }
      }
    } catch {
      // Ignore; user can import manually from Options.
    }
  }
});

// Freemium grandfathering: decide once, on install/update, whether this
// user keeps all current features free forever. See src/common/entitlement.js.
chrome.runtime.onInstalled.addListener(async (details) => {
  const { decideGrandfathered } = SUEntitlement;
  const { grandfathered } = await chrome.storage.local.get("grandfathered");
  const decided = decideGrandfathered(details.reason, grandfathered);
  if (decided !== grandfathered) {
    await chrome.storage.local.set({ grandfathered: decided });
  }
});

// What's-new page: a local extension page (src/whatsnew/whatsnew.html) opened
// at most once, and only when an update lands on WHATS_NEW_VERSION. It never
// opens on a fresh install, on browser start, or on a reload of the same
// version, and the stored flag stops a repeat. Bump WHATS_NEW_VERSION only
// when a release ships a new what's-new page; otherwise leave it and nothing
// opens. No network request is involved.
const WHATS_NEW_VERSION = "1.0.0";
const WHATS_NEW_FLAG = "whatsNewShownVersion";

chrome.runtime.onInstalled.addListener(async (details) => {
  try {
    if (details?.reason !== "update") return;
    // PDF page counts were stored for a reading-load badge that never shipped (removed in 1.1.0).
    chrome.storage.local.remove("readingLoadPageCounts").catch(() => {});
    const current = chrome.runtime.getManifest().version;
    if (current !== WHATS_NEW_VERSION || details.previousVersion === current) return;
    const stored = await chrome.storage.local.get(WHATS_NEW_FLAG);
    if (stored[WHATS_NEW_FLAG] === current) return;
    await chrome.storage.local.set({ [WHATS_NEW_FLAG]: current });
    chrome.tabs.create({ url: chrome.runtime.getURL("src/whatsnew/whatsnew.html") });
  } catch {
    // Never let the what's-new page break install/update handling.
  }
});

// Install and uninstall pages on purplelink.llc (src/common/install-pages.js, docs/install-pages.md).
// The extension itself sends nothing. On a fresh install it opens one getting-started tab, once; Chrome opens the
// uninstall page after the extension is removed. Opening a tab on another site needs no extra permission. The site
// counts the two visits like any other page, which is how installs and uninstalls get a daily count without telemetry.
const START_SHOWN_FLAG = "startPageShown";

function setUninstallPage() {
  try {
    chrome.runtime.setUninstallURL?.(SUInstallPages.uninstallPageUrl(chrome.runtime.getManifest().version));
  } catch {
    // Never let this break startup.
  }
}

chrome.runtime.onStartup?.addListener?.(setUninstallPage);

chrome.runtime.onInstalled.addListener(async (details) => {
  try {
    const version = chrome.runtime.getManifest().version;
    const stored = await chrome.storage.local.get(START_SHOWN_FLAG);
    const plan = SUInstallPages.onInstalledPlan(details?.reason, version, stored[START_SHOWN_FLAG] === true);
    chrome.runtime.setUninstallURL?.(plan.uninstallUrl);
    if (plan.startUrl) {
      await chrome.storage.local.set({ [START_SHOWN_FLAG]: true });
      chrome.tabs.create({ url: plan.startUrl });
    }
  } catch {
    // Never let the install pages break install/update handling.
  }
});
