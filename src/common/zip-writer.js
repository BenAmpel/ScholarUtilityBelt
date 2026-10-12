/**
 * Minimal store-only (no compression) zip writer. Pure: no DOM, no chrome.* APIs.
 *
 * Writes the classic zip layout (local headers, central directory, end record), UTF-8 file names
 * (general purpose flag bit 11), a correct CRC-32 and DOS date/time. No zip64, so it refuses more than
 * 65535 entries or more than 4 GiB in total; a paper library export is far below both.
 */

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

/** CRC-32 (IEEE 802.3, the one zip uses) of a Uint8Array. Returns an unsigned 32-bit number. */
export function crc32(bytes) {
  let c = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

/** DOS date and time words for a Date, in local time. Years before 1980 clamp to 1980-01-01. */
export function dosDateTime(date) {
  const d = date instanceof Date && !Number.isNaN(date.getTime()) ? date : new Date();
  if (d.getFullYear() < 1980) return { time: 0, date: (0 << 9) | (1 << 5) | 1 };
  const year = Math.min(d.getFullYear(), 2107) - 1980;
  return {
    time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
    date: (year << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  };
}

/** Zip entry names use forward slashes and are relative: no drive letters, leading slashes or ".." parts. */
export function cleanZipName(name) {
  const parts = String(name ?? "")
    .replace(/\\/g, "/")
    .split("/")
    .filter((p) => p && p !== "." && p !== "..");
  return parts.join("/");
}

function toBytes(data) {
  if (data instanceof Uint8Array) return data;
  return new TextEncoder().encode(String(data ?? ""));
}

/**
 * @param {{name: string, data: string|Uint8Array}[]} files
 * @param {{date?: Date}} [opts] modification time stamped on every entry (default: now)
 * @returns {Uint8Array}
 */
export function buildZip(files, opts = {}) {
  if (!Array.isArray(files)) throw new TypeError("files must be an array");
  if (files.length > 0xffff) throw new RangeError("too many files for a non-zip64 archive");
  const stamp = dosDateTime(opts.date);
  const enc = new TextEncoder();
  const seen = new Set();
  const entries = [];
  let offset = 0;
  const chunks = [];

  for (const f of files) {
    const name = cleanZipName(f && f.name);
    if (!name) throw new Error("zip entry needs a file name");
    if (seen.has(name)) throw new Error(`duplicate zip entry: ${name}`);
    seen.add(name);
    const nameBytes = enc.encode(name);
    if (nameBytes.length > 0xffff) throw new RangeError("file name too long");
    const data = toBytes(f.data);
    const crc = crc32(data);

    const local = new Uint8Array(30 + nameBytes.length);
    const dv = new DataView(local.buffer);
    dv.setUint32(0, 0x04034b50, true);
    dv.setUint16(4, 20, true); // version needed
    dv.setUint16(6, 0x0800, true); // UTF-8 names
    dv.setUint16(8, 0, true); // method: stored
    dv.setUint16(10, stamp.time, true);
    dv.setUint16(12, stamp.date, true);
    dv.setUint32(14, crc, true);
    dv.setUint32(18, data.length, true);
    dv.setUint32(22, data.length, true);
    dv.setUint16(26, nameBytes.length, true);
    dv.setUint16(28, 0, true);
    local.set(nameBytes, 30);

    entries.push({ nameBytes, crc, size: data.length, offset });
    chunks.push(local, data);
    offset += local.length + data.length;
    if (offset > 0xffffffff) throw new RangeError("archive too large for a non-zip64 archive");
  }

  const centralStart = offset;
  for (const e of entries) {
    const rec = new Uint8Array(46 + e.nameBytes.length);
    const dv = new DataView(rec.buffer);
    dv.setUint32(0, 0x02014b50, true);
    dv.setUint16(4, 20, true); // version made by
    dv.setUint16(6, 20, true); // version needed
    dv.setUint16(8, 0x0800, true);
    dv.setUint16(10, 0, true);
    dv.setUint16(12, stamp.time, true);
    dv.setUint16(14, stamp.date, true);
    dv.setUint32(16, e.crc, true);
    dv.setUint32(20, e.size, true);
    dv.setUint32(24, e.size, true);
    dv.setUint16(28, e.nameBytes.length, true);
    dv.setUint32(42, e.offset, true);
    rec.set(e.nameBytes, 46);
    chunks.push(rec);
    offset += rec.length;
  }
  const end = new Uint8Array(22);
  const ev = new DataView(end.buffer);
  ev.setUint32(0, 0x06054b50, true);
  ev.setUint16(8, entries.length, true);
  ev.setUint16(10, entries.length, true);
  ev.setUint32(12, offset - centralStart, true);
  ev.setUint32(16, centralStart, true);
  chunks.push(end);

  const out = new Uint8Array(offset + end.length);
  let pos = 0;
  for (const c of chunks) {
    out.set(c, pos);
    pos += c.length;
  }
  return out;
}
