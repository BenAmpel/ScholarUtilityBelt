// Service worker kept intentionally minimal.
// We avoid extra network requests to Scholar by default.

function sanitizeFilenameSegment(s, maxLen) {
  let t = s.replace(/[/\\:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
  if (maxLen && t.length > maxLen) t = t.slice(0, maxLen).trim();
  return t || "Unknown";
}

function isFetchableUrl(url) {
  try {
    const u = new URL(url);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function isScholarUrl(url) {
  try {
    const u = new URL(url);
    const host = String(u.hostname || "").toLowerCase();
    return host === "scholar.google.com" || host.startsWith("scholar.google.");
  } catch {
    return false;
  }
}

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
importScripts(
  chrome.runtime.getURL("dist/common/extpay.js"),
  chrome.runtime.getURL("dist/common/entitlement.sw.js")
);
const EXTPAY_ID = "scholar-utility-belt";
const extpay = ExtPay(EXTPAY_ID);
extpay.startBackground();

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

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
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
    fetch(msg.url, { credentials: "omit" })
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
    if (!isFetchableUrl(msg.url)) {
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
  if (msg?.action === "fetchPdfPageCount" && typeof msg.url === "string" && typeof msg.paperKey === "string") {
    if (!isFetchableUrl(msg.url)) {
      sendResponse({ ok: false, error: "unsupported_url" });
      return true;
    }
    const tailBytes = 8192;
    fetchWithTimeout(msg.url, {
      credentials: "omit",
      headers: { Range: `bytes=-${tailBytes}` }
    }, 10000)
      .then((r) => {
        if (!r.ok && r.status !== 206) throw new Error(`HTTP ${r.status}`);
        return r.arrayBuffer();
      })
      .then((buf) => {
        const bytes = new Uint8Array(buf);
        const decoder = new TextDecoder("latin1");
        const tail = decoder.decode(bytes);
        const matches = tail.match(/\/Count\s+(\d+)/g);
        if (!matches || matches.length === 0) {
          sendResponse({ ok: false });
          return;
        }
        const counts = matches.map((m) => parseInt(m.replace(/\D/g, ""), 10));
        const pages = Math.max(...counts);
        if (pages < 1 || pages > 50000) {
          sendResponse({ ok: false });
          return;
        }
        sendResponse({ ok: true, pages });
      })
      .catch(() => sendResponse({ ok: false }));
    return true;
  }
  if (msg?.action === "graphMonitorCheck") {
    const authorId = typeof msg.authorId === "string" ? msg.authorId : "";
    checkGraphMonitors(authorId).then((res) => sendResponse({ ok: true, result: res })).catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "getEntitlementStatus") {
    (async () => {
      const { describeEntitlement } = SUEntitlement;
      const { grandfathered } = await chrome.storage.local.get("grandfathered");
      if (grandfathered) {
        sendResponse(describeEntitlement({ grandfathered: true, extpayUser: null }));
        return;
      }
      // Re-declare extpay locally: per ExtPay's Manifest V3 documentation,
      // the outer `extpay` const can be undefined inside message callbacks.
      const localExtpay = ExtPay(EXTPAY_ID);
      const user = await localExtpay.getUser();
      sendResponse(describeEntitlement({ grandfathered: false, extpayUser: user }));
    })();
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
    const existing = Array.isArray(entry.alerts) ? entry.alerts : [];
    const existingIds = new Set(existing.map((a) => a.id));
    const merged = [...newAlerts.filter((a) => !existingIds.has(a.id)), ...existing].slice(0, 50);
    entry.alerts = merged;
    entry.unreadCount = Math.min(50, (Number(entry.unreadCount) || 0) + newAlerts.length);
    entry.lastCheck = nowIso;
    map[authorId] = entry;
  }
  await chrome.storage.local.set({ [GRAPH_ALERTS_KEY]: map });
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
  { key: "qualityAbdcRanks", customKey: "qualityAbdcRanksCustom", files: ["src/data/abdc2022.csv"] },
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

  // Seed SJR quartiles snapshot if the user hasn't imported one yet.
  const { qualityQuartilesIndex } = await chrome.storage.local.get({ qualityQuartilesIndex: {} });
  if (!qualityQuartilesIndex || Object.keys(qualityQuartilesIndex).length === 0) {
    try {
      const url = chrome.runtime.getURL("src/data/scimago_2024_quartiles.json");
      const r = await fetch(url);
      if (r.ok) {
        const data = await r.json();
        const index = data?.index || {};
        const meta = data?.meta || null;
        if (index && typeof index === "object" && Object.keys(index).length > 1000) {
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
