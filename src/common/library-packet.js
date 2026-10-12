// Collection packets: one collection exported as a small .sub.json file that someone else can import.
//
// A packet is NOT signed. It carries a SHA-256 checksum of its contents, which only detects accidental
// damage or hand edits; anyone can edit a packet and recompute the checksum.

export const PACKET_FORMAT = "scholar-utility-belt-packet";
export const PACKET_VERSION = 1;
export const PACKET_EXTENSION = ".sub.json";
export const PACKET_LIMITS = {
  maxBytes: 5 * 1024 * 1024,
  maxPapers: 2000,
  maxName: 80,
  maxFrom: 60,
  maxTitle: 1000,
  maxShort: 500,
  maxUrl: 2000,
  maxText: 10000,
  maxTags: 50,
  maxHighlights: 200
};

const PAPER_FIELDS = ["key", "title", "authorsVenue", "venue", "year", "doi", "citations", "snippet", "abstract"];
const URL_FIELDS = ["url", "sourcePageUrl", "pdfUrl"];

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

function cleanLine(value, max) {
  // Strip control characters and collapse whitespace so labels and titles stay single-line.
  return String(value ?? "")
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max);
}

function cleanText(value, max) {
  return String(value ?? "")
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "")
    .slice(0, max);
}

function cleanUrl(value) {
  const s = String(value ?? "").trim();
  if (!s || s.length > PACKET_LIMITS.maxUrl) return "";
  try {
    const u = new URL(s);
    return u.protocol === "https:" || u.protocol === "http:" ? s : "";
  } catch {
    return "";
  }
}

export function cleanLabel(value, max = PACKET_LIMITS.maxFrom) {
  return cleanLine(value, max);
}

/** Reduce a paper to the packet whitelist. Private notes and highlights only when includeNotes is true. */
export function sanitizePacketPaper(p, { includeNotes = false } = {}) {
  if (!isPlainObject(p)) return null;
  const out = {};
  for (const f of PAPER_FIELDS) {
    const v = p[f];
    if (v === undefined || v === null || v === "") continue;
    if (f === "year" || f === "citations") {
      const n = Number(v);
      if (Number.isFinite(n)) out[f] = n;
      else if (f === "year") out.year = cleanLine(v, 16);
      continue;
    }
    out[f] = f === "title" ? cleanLine(v, PACKET_LIMITS.maxTitle) : f === "snippet" || f === "abstract" ? cleanText(v, PACKET_LIMITS.maxText) : cleanLine(v, PACKET_LIMITS.maxShort);
  }
  for (const f of URL_FIELDS) {
    const u = cleanUrl(p[f]);
    if (u) out[f] = u;
  }
  if (Array.isArray(p.tags)) {
    const tags = [];
    for (const t of p.tags) {
      const c = cleanLine(t, 60);
      if (c && !tags.includes(c)) tags.push(c);
      if (tags.length >= PACKET_LIMITS.maxTags) break;
    }
    if (tags.length) out.tags = tags;
  }
  if (includeNotes) {
    const notes = cleanText(p.notes, PACKET_LIMITS.maxText);
    if (notes.trim()) out.notes = notes;
    const pn = p.pdfNotes;
    if (isPlainObject(pn)) {
      const clean = {};
      const summary = cleanText(pn.summary, PACKET_LIMITS.maxText);
      if (summary.trim()) clean.summary = summary;
      if (Array.isArray(pn.highlights)) {
        const hs = pn.highlights
          .filter(isPlainObject)
          .slice(0, PACKET_LIMITS.maxHighlights)
          .map((h) => ({ page: cleanLine(h.page, 16), quote: cleanText(h.quote, 2000), note: cleanText(h.note, 2000) }))
          .filter((h) => h.quote || h.note);
        if (hs.length) clean.highlights = hs;
      }
      if (Object.keys(clean).length) out.pdfNotes = clean;
    }
  }
  if (typeof out.key !== "string" || ["__proto__", "constructor", "prototype"].includes(out.key)) out.key = "";
  if (!out.title && !out.doi && !out.key) return null;
  return out;
}

function canonical(v) {
  if (Array.isArray(v)) return v.map(canonical);
  if (isPlainObject(v)) {
    const o = {};
    for (const k of Object.keys(v).sort()) o[k] = canonical(v[k]);
    return o;
  }
  return v;
}

function checksumBody(packet) {
  const { checksum, ...rest } = packet;
  void checksum;
  return JSON.stringify(canonical(rest));
}

/** SHA-256 of the packet contents, as lowercase hex. */
export async function computeChecksum(packet) {
  const bytes = new TextEncoder().encode(checksumBody(packet));
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Build a packet. `papers` are the papers of the collection already selected by the caller.
 * Returns { ok:true, packet } or { ok:false, error }.
 */
export async function buildPacket({ papers, name, from, collectionName = "", includeNotes = false, now = new Date().toISOString() }) {
  const packetName = cleanLine(name, PACKET_LIMITS.maxName);
  const fromLabel = cleanLine(from, PACKET_LIMITS.maxFrom);
  if (!packetName) return { ok: false, error: "Enter a packet name." };
  if (!fromLabel) return { ok: false, error: "Enter a 'from' label, for example your name or lab." };
  const list = (Array.isArray(papers) ? papers : [])
    .map((p) => sanitizePacketPaper(p, { includeNotes }))
    .filter(Boolean);
  if (!list.length) return { ok: false, error: "This collection has no papers to export." };
  if (list.length > PACKET_LIMITS.maxPapers) return { ok: false, error: `A packet holds at most ${PACKET_LIMITS.maxPapers} papers.` };
  const packet = {
    format: PACKET_FORMAT,
    version: PACKET_VERSION,
    name: packetName,
    from: fromLabel,
    collection: cleanLine(collectionName || packetName, PACKET_LIMITS.maxName),
    createdAt: now,
    includesNotes: !!includeNotes,
    papers: list
  };
  packet.checksum = { algorithm: "sha-256", value: await computeChecksum(packet) };
  return { ok: true, packet };
}

/**
 * Check packet text from a file. Returns { ok:false, error } or
 * { ok:true, packet (sanitized), warnings: string[], checksum: "ok"|"mismatch"|"missing" }.
 */
export async function validatePacketText(text) {
  if (typeof text !== "string" || !text.trim()) return { ok: false, error: "The file is empty." };
  if (text.length > PACKET_LIMITS.maxBytes) return { ok: false, error: "The file is larger than 5 MB, which is more than a packet holds." };
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    return { ok: false, error: "The file is not valid JSON, so it is not a collection packet." };
  }
  return validatePacket(data);
}

export async function validatePacket(data) {
  if (!isPlainObject(data)) return { ok: false, error: "The file is not a collection packet." };
  if (data.format !== PACKET_FORMAT) {
    return {
      ok: false,
      error: data.savedPapers
        ? "This looks like a full library export. Use Import library for it."
        : "The file is not a Scholar Utility Belt collection packet."
    };
  }
  if (!Number.isInteger(data.version)) return { ok: false, error: "The packet has no version number." };
  if (data.version > PACKET_VERSION) {
    return { ok: false, error: `This packet is version ${data.version}. This copy of the extension reads version ${PACKET_VERSION}; update the extension to open it.` };
  }
  if (data.version < 1) return { ok: false, error: "The packet version is not valid." };
  if (!Array.isArray(data.papers)) return { ok: false, error: "The packet has no papers list." };
  if (data.papers.length === 0) return { ok: false, error: "The packet contains no papers." };
  if (data.papers.length > PACKET_LIMITS.maxPapers) {
    return { ok: false, error: `The packet lists ${data.papers.length} papers; the limit is ${PACKET_LIMITS.maxPapers}.` };
  }
  const name = cleanLine(data.name, PACKET_LIMITS.maxName);
  const from = cleanLine(data.from, PACKET_LIMITS.maxFrom);
  if (!name) return { ok: false, error: "The packet has no name." };
  if (!from) return { ok: false, error: "The packet has no 'from' label." };

  const includeNotes = data.includesNotes === true;
  const papers = [];
  let dropped = 0;
  for (const p of data.papers) {
    const clean = sanitizePacketPaper(p, { includeNotes });
    if (clean) papers.push(clean);
    else dropped++;
  }
  if (!papers.length) return { ok: false, error: "None of the papers in the packet could be read." };

  const warnings = [];
  if (dropped) warnings.push(`${dropped} unreadable entr${dropped === 1 ? "y was" : "ies were"} skipped.`);

  let checksum = "missing";
  const stated = data.checksum;
  if (isPlainObject(stated) && stated.algorithm === "sha-256" && typeof stated.value === "string") {
    try {
      checksum = (await computeChecksum(data)) === stated.value.toLowerCase() ? "ok" : "mismatch";
    } catch {
      checksum = "missing";
    }
  }
  if (checksum === "mismatch") warnings.push("The checksum does not match, so the file was changed or damaged after it was exported.");
  if (checksum === "missing") warnings.push("The packet has no usable checksum, so damage cannot be detected.");

  return {
    ok: true,
    checksum,
    warnings,
    packet: {
      format: PACKET_FORMAT,
      version: data.version,
      name,
      from,
      collection: cleanLine(data.collection || name, PACKET_LIMITS.maxName),
      createdAt: typeof data.createdAt === "string" ? data.createdAt.slice(0, 40) : "",
      includesNotes: includeNotes,
      papers
    }
  };
}

/** Convert a validated packet to the payload and options mergeLibraryImport expects. */
export function packetToImport(packet, { now = new Date().toISOString() } = {}) {
  const savedPapers = {};
  packet.papers.forEach((p, i) => {
    const key = p.key || `packet_${i}_${Math.abs(hash(p.doi || p.title || String(i))).toString(36)}`;
    // Packet papers carry no savedAt or collection ids: the importer stamps them and the collection below applies.
    savedPapers[key] = { ...p, key, collections: ["packet_collection"] };
  });
  return {
    payload: {
      savedPapers,
      libraryState: { collections: [{ id: "packet_collection", name: packet.collection || packet.name }] }
    },
    options: { extraTags: [`from ${packet.from}`], now, reason: "packet import" }
  };
}

function hash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h << 5) - h + str.charCodeAt(i);
    h |= 0;
  }
  return h;
}

export function packetFileName(name, now = new Date()) {
  const slug = cleanLine(name, 40).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "collection";
  return `${slug}-${now.toISOString().slice(0, 10)}${PACKET_EXTENSION}`;
}
