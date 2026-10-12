// Merge-safe library import. Pure functions, no chrome.* calls.
//
// Rules (nothing in the existing library is ever deleted):
//  - Papers are matched by paperFingerprint (DOI, then normalized title), then by storage key.
//  - tags, collections and highlights are unioned.
//  - notes and the PDF summary are concatenated with a separator when they differ.
//  - for every other field, the newer non-empty value wins; an empty value never replaces a filled one.
//  - when url or pdfUrl differ, the value that loses is kept in altUrls.

const UNSAFE_KEYS = new Set(["__proto__", "constructor", "prototype"]);
export const NOTE_SEPARATOR = "\n\n---\n\n";

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

export function normalizeTitleForMatch(title) {
  return String(title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function normalizeDoi(doi) {
  return String(doi || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\/(dx\.)?doi\.org\//, "")
    .replace(/^doi:\s*/, "");
}

export function paperFingerprint(p) {
  const doi = normalizeDoi(p?.doi);
  if (doi) return `doi:${doi}`;
  const t = normalizeTitleForMatch(p?.title || "");
  return t ? `title:${t}` : `key:${p?.key || ""}`;
}

function isEmpty(v) {
  if (v === null || v === undefined) return true;
  if (typeof v === "string") return v.trim() === "";
  if (Array.isArray(v)) return v.length === 0;
  if (isPlainObject(v)) return Object.keys(v).length === 0;
  return false;
}

function sameJson(a, b) {
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}

function timeOf(p) {
  const t = Date.parse(p?.updatedAt || p?.savedAt || "");
  return Number.isFinite(t) ? t : 0;
}

function unionStrings(a, b, caseInsensitive) {
  const out = [];
  const seen = new Set();
  for (const v of [...(Array.isArray(a) ? a : []), ...(Array.isArray(b) ? b : [])]) {
    if (typeof v !== "string") continue;
    const text = v.trim();
    if (!text) continue;
    const k = caseInsensitive ? text.toLowerCase() : text;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(text);
  }
  return out;
}

function unionByJson(a, b) {
  const out = [];
  const seen = new Set();
  for (const v of [...(Array.isArray(a) ? a : []), ...(Array.isArray(b) ? b : [])]) {
    const k = JSON.stringify(v);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(v);
  }
  return out;
}

/** Concatenate two free-text fields without losing either; idempotent when one already contains the other. */
export function joinText(a, b) {
  const x = typeof a === "string" ? a : a == null ? "" : String(a);
  const y = typeof b === "string" ? b : b == null ? "" : String(b);
  const tx = x.trim();
  const ty = y.trim();
  if (!tx) return y;
  if (!ty) return x;
  if (tx === ty) return x;
  if (tx.includes(ty)) return x;
  if (ty.includes(tx)) return y;
  return `${x.replace(/\s+$/, "")}${NOTE_SEPARATOR}${y.replace(/^\s+/, "")}`;
}

function highlightKey(h) {
  if (!isPlainObject(h)) return JSON.stringify(h);
  const n = (s) => String(s ?? "").trim().toLowerCase();
  return `${n(h.page)}|${n(h.quote)}|${n(h.note)}`;
}

function mergeHighlights(a, b) {
  const out = [];
  const seen = new Set();
  for (const h of [...(Array.isArray(a) ? a : []), ...(Array.isArray(b) ? b : [])]) {
    const k = highlightKey(h);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(h);
  }
  return out;
}

function mergePdfNotes(a, b, incomingNewer) {
  if (!isPlainObject(a)) return isPlainObject(b) ? b : a;
  if (!isPlainObject(b)) return a;
  const out = {};
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    if (UNSAFE_KEYS.has(k)) continue;
    if (k === "highlights") out.highlights = mergeHighlights(a.highlights, b.highlights);
    else if (k === "summary") out.summary = joinText(a.summary, b.summary);
    else out[k] = pickScalar(a[k], b[k], incomingNewer);
  }
  return out;
}

function pickScalar(a, b, incomingNewer) {
  if (isEmpty(a)) return isEmpty(b) ? a : b;
  if (isEmpty(b)) return a;
  if (sameJson(a, b)) return a;
  return incomingNewer ? b : a;
}

/**
 * Merge two records for the same paper. `existing` keeps its storage key.
 * Returns a new object; inputs are not modified. updatedAt is the later of the two (not "now"),
 * so merging identical data is a no-op.
 */
export function mergePaperRecords(existing, incoming) {
  const a = isPlainObject(existing) ? existing : {};
  const b = isPlainObject(incoming) ? incoming : {};
  const incomingNewer = timeOf(b) > timeOf(a);
  const out = {};
  const altUrls = [];
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    if (UNSAFE_KEYS.has(k)) continue;
    const av = a[k];
    const bv = b[k];
    switch (k) {
      case "key":
        out.key = a.key ?? b.key;
        break;
      case "tags":
        out.tags = unionStrings(av, bv, true);
        break;
      case "collections":
        out.collections = unionStrings(av, bv, false);
        break;
      case "altUrls":
        altUrls.push(...(Array.isArray(av) ? av : []), ...(Array.isArray(bv) ? bv : []));
        break;
      case "notes":
        out.notes = joinText(av, bv);
        break;
      case "pdfNotes":
        out.pdfNotes = mergePdfNotes(av, bv, incomingNewer);
        break;
      case "citations": {
        const x = Number(av);
        const y = Number(bv);
        if (Number.isFinite(x) && Number.isFinite(y)) out.citations = Math.max(x, y);
        else out.citations = pickScalar(av, bv, incomingNewer);
        break;
      }
      case "savedAt": {
        const cands = [av, bv].filter((v) => v && Number.isFinite(Date.parse(v)));
        cands.sort((p, q) => Date.parse(p) - Date.parse(q));
        out.savedAt = cands[0] ?? pickScalar(av, bv, incomingNewer);
        break;
      }
      case "updatedAt": {
        const cands = [av, bv].filter((v) => v && Number.isFinite(Date.parse(v)));
        cands.sort((p, q) => Date.parse(q) - Date.parse(p));
        out.updatedAt = cands[0] ?? pickScalar(av, bv, incomingNewer);
        break;
      }
      case "url":
      case "pdfUrl": {
        const picked = pickScalar(av, bv, incomingNewer);
        out[k] = picked;
        for (const v of [av, bv]) {
          if (typeof v === "string" && v.trim() && v !== picked) altUrls.push(v);
        }
        break;
      }
      default:
        if (Array.isArray(av) || Array.isArray(bv)) {
          if (Array.isArray(av) && Array.isArray(bv)) out[k] = unionByJson(av, bv);
          else out[k] = pickScalar(av, bv, incomingNewer);
        } else {
          out[k] = pickScalar(av, bv, incomingNewer);
        }
    }
  }
  if (altUrls.length) {
    const taken = new Set([out.url, out.pdfUrl].filter(Boolean));
    const list = unionStrings(altUrls, [], false).filter((u) => !taken.has(u));
    if (list.length) out.altUrls = list;
  }
  return out;
}

function stripVolatile(p) {
  const { updatedAt, ...rest } = p || {};
  void updatedAt;
  return rest;
}

/** True when merging `incoming` into `existing` would change nothing but the updatedAt stamp. */
export function isNoopMerge(existing, merged) {
  return sameJson(sortKeys(stripVolatile(existing)), sortKeys(stripVolatile(merged)));
}

function sortKeys(v) {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (isPlainObject(v)) {
    const o = {};
    for (const k of Object.keys(v).sort()) o[k] = sortKeys(v[k]);
    return o;
  }
  return v;
}

function normName(s) {
  return String(s || "").trim().toLowerCase();
}

/**
 * Merge the collections, saved searches and watched folders of an imported library state into the
 * current one. Collections are matched by name, because ids are generated per install.
 * Returns { state, idMap } where idMap maps imported collection ids to ids in the merged state.
 */
export function mergeLibraryState(current, incoming) {
  const cur = isPlainObject(current) ? current : {};
  const inc = isPlainObject(incoming) ? incoming : {};
  const collections = (Array.isArray(cur.collections) ? cur.collections : []).map((c) => ({ ...c }));
  const savedSearches = (Array.isArray(cur.savedSearches) ? cur.savedSearches : []).map((s) => ({ ...s }));
  const watchedFolders = (Array.isArray(cur.watchedFolders) ? cur.watchedFolders : []).map((f) => ({ ...f }));
  const idMap = new Map();
  const usedIds = new Set(collections.map((c) => c.id));
  let added = 0;

  for (const c of Array.isArray(inc.collections) ? inc.collections : []) {
    if (!isPlainObject(c) || !c.id || UNSAFE_KEYS.has(String(c.id))) continue;
    const name = String(c.name || "").trim();
    if (!name) continue;
    const byName = collections.find((x) => normName(x.name) === normName(name));
    if (byName) {
      idMap.set(c.id, byName.id);
      continue;
    }
    let id = String(c.id);
    let n = 1;
    while (usedIds.has(id)) id = `${c.id}_i${n++}`;
    usedIds.add(id);
    collections.push({ ...c, id, name });
    idMap.set(c.id, id);
    added++;
  }

  let searchesAdded = 0;
  for (const s of Array.isArray(inc.savedSearches) ? inc.savedSearches : []) {
    if (!isPlainObject(s) || !s.id) continue;
    const collectionId = s.collectionId ? idMap.get(s.collectionId) || "" : "";
    const dup = savedSearches.some(
      (x) => x.id === s.id || (normName(x.name) === normName(s.name) && normName(x.query) === normName(s.query))
    );
    if (dup) continue;
    savedSearches.push({ ...s, collectionId });
    searchesAdded++;
  }

  for (const f of Array.isArray(inc.watchedFolders) ? inc.watchedFolders : []) {
    if (!isPlainObject(f) || !f.id) continue;
    const existing = watchedFolders.find((x) => x.id === f.id);
    if (!existing) {
      watchedFolders.push({ ...f });
      continue;
    }
    const seen = new Set((existing.files || []).map((x) => x.relativePath));
    const extra = (Array.isArray(f.files) ? f.files : []).filter((x) => x && !seen.has(x.relativePath));
    if (extra.length) existing.files = [...(existing.files || []), ...extra];
  }

  return {
    state: { ...cur, collections, savedSearches, watchedFolders },
    idMap,
    collectionsAdded: added,
    searchesAdded
  };
}

function cloneJson(v) {
  return JSON.parse(JSON.stringify(v));
}

/**
 * Merge an imported library payload ({ savedPapers, libraryState }) into the current library.
 *
 * opts.extraTags: tags added to every imported paper (used by packet import).
 * opts.now: ISO string stamped on papers that are new to the library and lack savedAt.
 *
 * Returns { ok:false, error } for a payload that cannot be read, else
 * { ok:true, papers, state, summary:{ added, merged, unchanged, skipped, collectionsAdded } }.
 */
export function mergeLibraryImport(currentPapers, currentState, payload, opts = {}) {
  if (!isPlainObject(payload)) return { ok: false, error: "The file is not a library export." };
  const incomingPapers = payload.savedPapers;
  if (!isPlainObject(incomingPapers)) return { ok: false, error: "The file has no savedPapers section." };

  const papers = {};
  for (const [k, v] of Object.entries(isPlainObject(currentPapers) ? currentPapers : {})) {
    if (!UNSAFE_KEYS.has(k)) papers[k] = cloneJson(v);
  }
  const mergedState = mergeLibraryState(currentState, payload.libraryState);
  const currentCollectionIds = new Set((Array.isArray(currentState?.collections) ? currentState.collections : []).map((c) => c.id));

  // fingerprint -> storage key. The first record seen for a fingerprint wins the slot.
  const byFingerprint = new Map();
  for (const [k, p] of Object.entries(papers)) {
    const fp = paperFingerprint({ ...p, key: p?.key || k });
    if (!byFingerprint.has(fp)) byFingerprint.set(fp, k);
  }

  const summary = { added: 0, merged: 0, unchanged: 0, skipped: 0, collectionsAdded: mergedState.collectionsAdded };
  const extraTags = Array.isArray(opts.extraTags) ? opts.extraTags : [];
  const now = opts.now || new Date().toISOString();

  for (const [rawKey, rawPaper] of Object.entries(incomingPapers)) {
    if (UNSAFE_KEYS.has(rawKey) || !isPlainObject(rawPaper)) {
      summary.skipped++;
      continue;
    }
    const incoming = cloneJson(rawPaper);
    incoming.key = typeof incoming.key === "string" && incoming.key ? incoming.key : rawKey;
    if (UNSAFE_KEYS.has(incoming.key)) {
      summary.skipped++;
      continue;
    }
    if (Array.isArray(incoming.collections)) {
      incoming.collections = incoming.collections
        .map((id) => (mergedState.idMap.has(id) ? mergedState.idMap.get(id) : currentCollectionIds.has(id) ? id : null))
        .filter(Boolean);
      incoming.collections = unionStrings(incoming.collections, [], false);
    }
    if (extraTags.length) incoming.tags = unionStrings(incoming.tags, extraTags, true);

    const fp = paperFingerprint(incoming);
    let targetKey = byFingerprint.get(fp);
    if (targetKey === undefined && Object.prototype.hasOwnProperty.call(papers, incoming.key)) targetKey = incoming.key;

    if (targetKey === undefined) {
      let key = incoming.key;
      let n = 1;
      while (Object.prototype.hasOwnProperty.call(papers, key)) key = `${incoming.key}_i${n++}`;
      incoming.key = key;
      if (!incoming.savedAt) incoming.savedAt = now;
      papers[key] = incoming;
      byFingerprint.set(fp, key);
      summary.added++;
      continue;
    }

    const existing = papers[targetKey];
    const merged = mergePaperRecords(existing, incoming);
    merged.key = existing.key || targetKey;
    if (isNoopMerge(existing, merged)) {
      summary.unchanged++;
      continue;
    }
    papers[targetKey] = merged;
    // The merged record may now carry a DOI it lacked before: index it too.
    const fp2 = paperFingerprint(merged);
    if (!byFingerprint.has(fp2)) byFingerprint.set(fp2, targetKey);
    summary.merged++;
  }

  return { ok: true, papers, state: mergedState.state, summary };
}

/**
 * Merge paper `dropKey` into paper `keepKey` inside one library (the duplicate-finder action).
 * Both URLs are kept (url / altUrls). Returns { ok, papers } with dropKey removed, or { ok:false, error }.
 */
export function mergeDuplicatePair(papers, keepKey, dropKey) {
  if (!isPlainObject(papers) || keepKey === dropKey) return { ok: false, error: "Choose two different papers." };
  if (!Object.prototype.hasOwnProperty.call(papers, keepKey) || !Object.prototype.hasOwnProperty.call(papers, dropKey)) {
    return { ok: false, error: "One of the papers is no longer in the library." };
  }
  const next = {};
  for (const [k, v] of Object.entries(papers)) if (!UNSAFE_KEYS.has(k)) next[k] = v;
  const keep = papers[keepKey];
  const drop = papers[dropKey];
  const merged = mergePaperRecords(keep, drop);
  merged.key = keepKey;
  // Prefer the keeper's own url as primary even if the dropped record is newer.
  if (keep.url && merged.url !== keep.url) {
    const alt = new Set(merged.altUrls || []);
    if (merged.url) alt.add(merged.url);
    alt.delete(keep.url);
    merged.url = keep.url;
    if (alt.size) merged.altUrls = Array.from(alt);
    else delete merged.altUrls;
  }
  merged.updatedAt = new Date().toISOString();
  next[keepKey] = merged;
  delete next[dropKey];
  return { ok: true, papers: next };
}

/** Human-readable one-liner for the import summary. */
export function describeImportSummary(s) {
  const parts = [`${s.added} added`, `${s.merged} merged`, `${s.unchanged} unchanged`];
  if (s.skipped) parts.push(`${s.skipped} skipped (unreadable)`);
  if (s.collectionsAdded) parts.push(`${s.collectionsAdded} collection${s.collectionsAdded === 1 ? "" : "s"} added`);
  return parts.join(", ");
}
