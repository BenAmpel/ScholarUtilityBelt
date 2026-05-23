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
