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
