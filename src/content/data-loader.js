import { normalizeVenueName, normalizeVhbRank } from "../common/quality.js";

function parseCsvLine(line, delim = ";") {
  const s = String(line || "");
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (inQ) {
      if (ch === '"') {
        if (s[i + 1] === '"') { cur += '"'; i++; }
        else inQ = false;
      } else cur += ch;
    } else {
      if (ch === '"') inQ = true;
      else if (ch === delim) { out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim()); cur = ""; }
      else cur += ch;
    }
  }
  out.push(cur.replace(/^"|"$/g, "").replace(/""/g, '"').trim());
  return out;
}

export async function loadEraAndNorwegian() {
  if (window.__suEraNorwegian) return window.__suEraNorwegian;
  let eraSet = new Set();
  let norwegianMap = new Map();
  let absIndex = new Map();
  try {
    const base = chrome.runtime.getURL("src/data/");
    const [eraText, absText] = await Promise.all([
      fetch(base + "era2023.txt").then((r) => (r.ok ? r.text() : "")).catch(() => ""),
      fetch(base + "abs2024.csv").then((r) => (r.ok ? r.text() : "")).catch(() => "")
    ]);

    for (const line of (eraText || "").split(/\r?\n/)) {
      const t = line.trim();
      if (!t || t.startsWith("#")) continue;
      const n = normalizeVenueName(t);
      if (n) eraSet.add(n);
    }

    let norwegianLoaded = false;
    try {
      const norJson = await fetch(base + "norwegian_compact.json").then((r) => r.ok ? r.json() : null).catch(() => null);
      if (norJson && typeof norJson === "object") {
        for (const [key, level] of Object.entries(norJson)) {
          if (level === "1" || level === "2") norwegianMap.set(key, level);
        }
        norwegianLoaded = true;
      }
    } catch (_) {}

    if (!norwegianLoaded) {
      const norwegianText = await fetch(base + "norwegian_register.csv").then((r) => (r.ok ? r.text() : "")).catch(() => "");
      const norLines = (norwegianText || "").split(/\r?\n/).filter((l) => l.trim());
      if (norLines.length > 0) {
        const header = parseCsvLine(norLines[0], ";");
        const titleIdx = header.findIndex((h) => /International Title|Original Title/i.test(String(h)));
        const levelIdx = header.findIndex((h) => /^Level 20\d{2}$/.test(String(h).trim()));
        const useTitleIdx = titleIdx >= 0 ? titleIdx : 2;
        const useLevelIdx = levelIdx >= 0 ? levelIdx : 9;
        for (let i = 1; i < norLines.length; i++) {
          const cells = parseCsvLine(norLines[i], ";");
          const name = (cells[useTitleIdx] || "").trim();
          const level = String(cells[useLevelIdx] || "").replace(/\D/g, "").slice(0, 1);
          if (name && (level === "1" || level === "2")) {
            const n = normalizeVenueName(name);
            if (n) norwegianMap.set(n, level);
          }
        }
      }
    }

    for (const line of (absText || "").split(/\r?\n/)) {
      const idx = line.lastIndexOf(",");
      if (idx <= 0 || idx >= line.length - 1) continue;
      const name = line.slice(0, idx).trim().replace(/^"|"$/g, "");
      const rank = line.slice(idx + 1).trim();
      if (!name || !rank) continue;
      const n = normalizeVenueName(name);
      if (n && /^4\*?$|^[1234]$/i.test(rank)) absIndex.set(n, rank);
    }
  } catch (_) {
    eraSet = new Set();
    norwegianMap = new Map();
    absIndex = new Map();
  }
  window.__suEraNorwegian = { eraSet, norwegianMap, absIndex };
  return window.__suEraNorwegian;
}

export async function loadH5Index() {
  if (window.__suH5Index) return window.__suH5Index;
  try {
    const url = chrome.runtime.getURL("src/data/venue_h5_index.json");
    const r = await fetch(url);
    const data = (r.ok ? await r.json() : null) || {};
    window.__suH5Index = data;
    return data;
  } catch (_) {
    window.__suH5Index = {};
    return {};
  }
}

export async function loadVhbIndex() {
  if (window.__suVhbIndex) return window.__suVhbIndex;
  const map = new Map();
  try {
    const url = chrome.runtime.getURL("src/data/vhb2024.csv");
    const r = await fetch(url);
    const text = r.ok ? await r.text() : "";
    for (const line of (text || "").split(/\r?\n/)) {
      const idx = line.lastIndexOf(",");
      if (idx <= 0 || idx >= line.length - 1) continue;
      const name = line.slice(0, idx).trim();
      const rank = normalizeVhbRank(line.slice(idx + 1).trim());
      if (!name || !rank) continue;
      for (const syn of String(name).split("|")) {
        const n = normalizeVenueName(syn);
        if (n) map.set(n, rank);
      }
    }
  } catch (_) {}
  window.__suVhbIndex = map;
  return map;
}

export async function loadImpactIndex() {
  if (window.__suImpactIndex) return window.__suImpactIndex;
  const map = new Map();
  try {
    const url = chrome.runtime.getURL("src/data/journal_impact_2024.csv");
    const r = await fetch(url);
    const text = r.ok ? await r.text() : "";
    for (const line of (text || "").split(/\r?\n/)) {
      if (!line || /^\s*Journal\s*Name\s*,/i.test(line)) continue;
      const idx = line.lastIndexOf(",");
      if (idx <= 0 || idx >= line.length - 1) continue;
      const name = line.slice(0, idx).trim().replace(/^"|"$/g, "");
      const raw = line.slice(idx + 1).trim();
      const val = parseFloat(String(raw || "").replace(/[^0-9.]/g, ""));
      if (!name || !Number.isFinite(val) || val <= 0) continue;
      const n = normalizeVenueName(name);
      if (n) map.set(n, val);
    }
  } catch (_) {}
  window.__suImpactIndex = map;
  return map;
}

function fnv1a32(str) {
  let h = 2166136261;
  const prime = 16777619;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, prime);
  }
  return h >>> 0;
}

function bloomIndices(doi, m, k) {
  const s = String(doi).toLowerCase().trim();
  const h1 = fnv1a32(s);
  const h2 = (fnv1a32(s + "salt") | 1) >>> 0;
  const indices = [];
  for (let i = 0; i < k; i++) {
    indices.push(((h1 + i * h2) >>> 0) % m);
  }
  return indices;
}

function decodeBloomBits(bitsBase64) {
  const bin = atob(String(bitsBase64 || ""));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export async function loadRetractionBloom() {
  if (window.__suRetractionBloom) return window.__suRetractionBloom;
  try {
    const url = chrome.runtime.getURL("src/data/retraction_bloom.json");
    const r = await fetch(url);
    const data = r.ok ? await r.json() : null;
    if (!data || !data.bits || !data.m || !data.k) throw new Error("Invalid bloom data");
    const bits = decodeBloomBits(data.bits);
    window.__suRetractionBloom = { m: data.m, k: data.k, bits, source: data.source, built: data.built, count: data.count };
    return window.__suRetractionBloom;
  } catch (_) {
    window.__suRetractionBloom = null;
    return null;
  }
}

export async function loadTorturedPhrases() {
  if (window.__suTorturedPhrases !== undefined) return window.__suTorturedPhrases;
  try {
    const url = chrome.runtime.getURL("src/data/tortured_phrases.txt");
    const r = await fetch(url);
    const text = r.ok ? await r.text() : "";
    const phrases = text.split(/\r?\n/)
      .map((l) => l.trim().toLowerCase())
      .filter((l) => l && !l.startsWith("#"));
    window.__suTorturedPhrases = phrases.length ? phrases : null;
  } catch {
    window.__suTorturedPhrases = null;
  }
  return window.__suTorturedPhrases;
}

export function bloomHasDoi(doi, bloom) {
  if (!doi || !bloom) return false;
  const { m, k, bits } = bloom;
  if (!m || !k || !bits) return false;
  for (const idx of bloomIndices(doi, m, k)) {
    const byteIdx = idx >> 3;
    const mask = 1 << (idx & 7);
    if ((bits[byteIdx] & mask) === 0) return false;
  }
  return true;
}

const retractionCheckCache = new Map();

export async function checkRetractionStatus(doi) {
  const key = String(doi).toLowerCase().trim();
  if (!key || key.length < 10) return false;
  if (retractionCheckCache.has(key)) return retractionCheckCache.get(key);
  try {
    const url = `https://api.crossref.org/works/${encodeURIComponent(key)}?mailto=scholar-extension@local`;
    const r = await fetch(url);
    if (!r.ok) {
      retractionCheckCache.set(key, false);
      return false;
    }
    const data = await r.json();
    const updatedBy = data?.message?.["updated-by"];
    const isRetracted = Array.isArray(updatedBy) && updatedBy.length > 0;
    retractionCheckCache.set(key, isRetracted);
    return isRetracted;
  } catch {
    retractionCheckCache.set(key, false);
    return false;
  }
}
