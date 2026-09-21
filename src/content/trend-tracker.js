export function normalizeCacheKey(query) {
  const s = String(query || "").toLowerCase().trim();
  if (!s) return "";
  const words = s.split(/\s+/).filter(Boolean);
  return [...new Set(words)].sort().join(" ");
}

export function extractYearsFromResults(resultElements) {
  const counts = {};
  for (const el of resultElements) {
    const meta = el?.querySelector?.(".gs_a");
    if (!meta) continue;
    const text = meta.textContent || "";
    const m = text.match(/\b(19\d{2}|20\d{2})\b/);
    if (m) {
      const year = m[1];
      const y = parseInt(year, 10);
      if (y >= 1900 && y <= 2100) {
        counts[year] = (counts[year] || 0) + 1;
      }
    }
  }
  return counts;
}

export function computeTrend(yearlyCountsArray) {
  const arr = yearlyCountsArray || [];
  if (arr.length < 2) return "flat";
  const n = arr.length;
  const mean = arr.reduce((a, b) => a + b, 0) / n;
  if (mean === 0) return "flat";
  let sumXY = 0;
  let sumX2 = 0;
  const xMean = (n - 1) / 2;
  for (let i = 0; i < n; i++) {
    const dx = i - xMean;
    sumXY += dx * arr[i];
    sumX2 += dx * dx;
  }
  const slope = sumX2 === 0 ? 0 : sumXY / sumX2;
  const threshold = mean * 0.05;
  if (slope > threshold) return "up";
  if (slope < -threshold) return "down";
  return "flat";
}

export const CACHE_MAX = 200;
export const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

async function getCache() {
  try {
    const data = await chrome.storage.local.get({ trendCache: {} });
    return data.trendCache || {};
  } catch {
    return {};
  }
}

async function setCache(cache) {
  try {
    await chrome.storage.local.set({ trendCache: cache });
  } catch {}
}

export async function readCache(query) {
  const key = normalizeCacheKey(query);
  if (!key) return null;
  const cache = await getCache();
  const entry = cache[key];
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) return null;
  entry.lastAccess = Date.now();
  cache[key] = entry;
  await setCache(cache);
  return entry;
}

export async function writeCache(query, data) {
  const key = normalizeCacheKey(query);
  if (!key) return;
  const cache = await getCache();
  cache[key] = {
    ts: Date.now(),
    lastAccess: Date.now(),
    concepts: data.concepts || [],
    yearlyWorks: data.yearlyWorks || {}
  };
  await setCache(cache);
  await evictCache();
}

export async function evictCache() {
  const cache = await getCache();
  const keys = Object.keys(cache);
  if (keys.length <= CACHE_MAX) return;
  const sorted = keys.sort((a, b) => (cache[a].lastAccess || 0) - (cache[b].lastAccess || 0));
  const toRemove = sorted.slice(0, keys.length - CACHE_MAX);
  for (const k of toRemove) delete cache[k];
  await setCache(cache);
}

export const OPENALEX_RATE_LIMIT_MS = 1000;

let _oaKeyPromise = null;
function getOpenAlexKeySuffix() {
  if (!_oaKeyPromise) {
    _oaKeyPromise = (async () => {
      try {
        const { settings } = await chrome.storage.local.get({ settings: {} });
        const key = settings?.openalexApiKey || "";
        return key ? `&api_key=${encodeURIComponent(key)}` : "";
      } catch { return ""; }
    })();
  }
  return _oaKeyPromise;
}
let lastApiCallTs = 0;

async function rateLimitedFetch(url) {
  const now = Date.now();
  const wait = OPENALEX_RATE_LIMIT_MS - (now - lastApiCallTs);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  lastApiCallTs = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeout);
    return res;
  } catch (e) {
    clearTimeout(timeout);
    throw e;
  }
}

import { detectMethods } from "./trend-methods.js";

export async function fetchConceptTrends(query) {
  try {
    const encoded = encodeURIComponent(String(query || "").trim());
    if (!encoded) return [];
    const url = `https://api.openalex.org/concepts?search=${encoded}&per_page=8&mailto=scholar-extension@local${await getOpenAlexKeySuffix()}`;
    const res = await rateLimitedFetch(url);
    if (!res.ok) return [];
    const data = await res.json();
    const results = data?.results || [];
    const currentYear = new Date().getFullYear();
    return results.slice(0, 8).map(concept => {
      const name = concept.display_name || "Unknown";
      const countsByYear = concept.counts_by_year || [];
      const last5 = [];
      for (let y = currentYear - 4; y <= currentYear; y++) {
        const entry = countsByYear.find(c => c.year === y);
        last5.push(entry ? entry.works_count : 0);
      }
      const totalWorks = last5.reduce((a, b) => a + b, 0);
      return { name, trend: computeTrend(last5), worksCount: totalWorks };
    });
  } catch {
    return [];
  }
}

let panelEl = null;

function buildYearChart(yearCounts) {
  const years = Object.keys(yearCounts).sort();
  if (years.length === 0) return null;
  const maxCount = Math.max(...Object.values(yearCounts));
  const currentYear = new Date().getFullYear();
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-years";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Year Distribution";
  section.appendChild(heading);
  for (const year of years) {
    const count = yearCounts[year];
    const pct = maxCount > 0 ? Math.round((count / maxCount) * 100) : 0;
    const row = document.createElement("div");
    row.className = "su-trend-bar-row";
    const label = document.createElement("span");
    label.className = "su-trend-bar-label";
    label.textContent = year;
    const barWrap = document.createElement("div");
    barWrap.className = "su-trend-bar-wrap";
    const bar = document.createElement("div");
    bar.className = "su-trend-bar";
    const y = parseInt(year, 10);
    if (y >= currentYear - 1) bar.classList.add("su-trend-bar-recent");
    bar.style.width = pct + "%";
    barWrap.appendChild(bar);
    const countSpan = document.createElement("span");
    countSpan.className = "su-trend-bar-count";
    countSpan.textContent = count;
    row.appendChild(label);
    row.appendChild(barWrap);
    row.appendChild(countSpan);
    section.appendChild(row);
  }
  return section;
}

function buildConceptChips(concepts) {
  if (!concepts || concepts.length === 0) return null;
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-concepts";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Rising Concepts";
  section.appendChild(heading);
  const chipWrap = document.createElement("div");
  chipWrap.className = "su-trend-chip-wrap";
  for (const c of concepts) {
    const chip = document.createElement("button");
    chip.className = "su-trend-chip";
    chip.type = "button";
    const arrow = c.trend === "up" ? "↑" : c.trend === "down" ? "↓" : "→";
    const arrowClass = "su-trend-arrow-" + c.trend;
    chip.innerHTML = '<span class="su-trend-chip-name">' + escapeHtml(c.name) + '</span> <span class="' + arrowClass + '">' + arrow + '</span>';
    chip.title = c.name + " (" + c.worksCount + " works, trending " + c.trend + ")";
    chip.addEventListener("click", () => {
      const searchBox = document.querySelector("#gs_hdr_tsi") || document.querySelector("input[name='q']");
      if (searchBox) {
        const current = searchBox.value.trim();
        if (!current.toLowerCase().includes(c.name.toLowerCase())) {
          searchBox.value = current + " " + c.name;
          searchBox.focus();
        }
      }
    });
    chipWrap.appendChild(chip);
  }
  section.appendChild(chipWrap);
  return section;
}

function buildMethodPills(methodCounts) {
  if (!methodCounts || methodCounts.size === 0) return null;
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-methods";
  const heading = document.createElement("div");
  heading.className = "su-trend-section-title";
  heading.textContent = "Method Signals";
  section.appendChild(heading);
  const pillWrap = document.createElement("div");
  pillWrap.className = "su-trend-pill-wrap";
  for (const [method, count] of methodCounts) {
    const pill = document.createElement("span");
    pill.className = "su-trend-pill";
    pill.textContent = method + " (" + count + ")";
    pillWrap.appendChild(pill);
  }
  section.appendChild(pillWrap);
  return section;
}

function buildUnavailableMessage() {
  const section = document.createElement("div");
  section.className = "su-trend-section su-trend-unavailable";
  section.textContent = "Trend data unavailable";
  const retry = document.createElement("button");
  retry.className = "su-trend-retry";
  retry.type = "button";
  retry.textContent = "Retry";
  section.appendChild(retry);
  return section;
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

export async function initTrendPanel(resultsContainer, query, settings) {
  if (!resultsContainer || !query) return;
  destroyTrendPanel();

  const panel = document.createElement("div");
  panel.id = "su-trend-panel";
  const collapsed = await isPanelCollapsed();
  panel.classList.toggle("su-trend-collapsed", collapsed);

  const toggle = document.createElement("button");
  toggle.id = "su-trend-toggle";
  toggle.type = "button";
  toggle.className = "su-trend-toggle-btn";
  toggle.title = "Toggle trend tracker";
  toggle.innerHTML = '<span class="su-trend-toggle-icon">\u{1F4C8}</span>';
  toggle.addEventListener("click", async () => {
    const isCollapsed = panel.classList.toggle("su-trend-collapsed");
    await savePanelCollapsed(isCollapsed);
    if (!isCollapsed && !panel.dataset.loaded) {
      await loadPanelData(panel, resultsContainer, query);
    }
  });

  const content = document.createElement("div");
  content.className = "su-trend-content";

  const header = document.createElement("div");
  header.className = "su-trend-header";
  header.textContent = "Trend Tracker";

  content.appendChild(header);
  panel.appendChild(toggle);
  panel.appendChild(content);

  const gs_res = document.getElementById("gs_res_ccl") || document.getElementById("gs_res_ccl_mid") || resultsContainer;
  if (gs_res && gs_res.parentNode) {
    gs_res.parentNode.insertBefore(panel, gs_res.nextSibling);
  } else {
    document.body.appendChild(panel);
  }
  panelEl = panel;

  if (!collapsed) {
    await loadPanelData(panel, resultsContainer, query);
  }
}

async function loadPanelData(panel, resultsContainer, query) {
  const content = panel.querySelector(".su-trend-content");
  if (!content) return;
  panel.dataset.loaded = "1";

  const resultEls = Array.from(resultsContainer.querySelectorAll(".gs_r"));
  const yearCounts = extractYearsFromResults(resultEls);

  const yearChart = buildYearChart(yearCounts);
  if (yearChart) content.appendChild(yearChart);

  const methodCounts = detectMethods(resultEls);
  const methodPills = buildMethodPills(methodCounts);
  if (methodPills) content.appendChild(methodPills);

  let cached = await readCache(query);
  if (cached && cached.concepts) {
    const chips = buildConceptChips(cached.concepts);
    if (chips) content.appendChild(chips);
  } else {
    const concepts = await fetchConceptTrends(query);
    if (concepts.length > 0) {
      await writeCache(query, { concepts, yearlyWorks: yearCounts });
      const chips = buildConceptChips(concepts);
      if (chips) content.appendChild(chips);
    } else {
      const msg = buildUnavailableMessage();
      const retryBtn = msg.querySelector(".su-trend-retry");
      if (retryBtn) {
        retryBtn.addEventListener("click", async () => {
          msg.remove();
          const retryConcepts = await fetchConceptTrends(query);
          if (retryConcepts.length > 0) {
            await writeCache(query, { concepts: retryConcepts, yearlyWorks: yearCounts });
            const chips = buildConceptChips(retryConcepts);
            if (chips) content.appendChild(chips);
          } else {
            content.appendChild(buildUnavailableMessage());
          }
        });
      }
      content.appendChild(msg);
    }
  }
}

async function isPanelCollapsed() {
  try {
    const data = await chrome.storage.local.get({ trendPanelCollapsed: true });
    return !!data.trendPanelCollapsed;
  } catch {
    return true;
  }
}

async function savePanelCollapsed(collapsed) {
  try {
    await chrome.storage.local.set({ trendPanelCollapsed: !!collapsed });
  } catch {}
}

export function destroyTrendPanel() {
  if (panelEl) {
    panelEl.remove();
    panelEl = null;
  }
  const existing = document.getElementById("su-trend-panel");
  if (existing) existing.remove();
}
