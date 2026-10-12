// Minimal .docx writer: headings, paragraphs and simple tables, packed with a
// store-only (no compression) zip. Pure functions, no DOM, no chrome.* APIs.
//
// NOTE ON DUPLICATION: another branch is adding src/common/zip-writer.js. This
// file carries its own small store-only zip (crc32 + zipStore below) so the two
// branches do not depend on each other. After both are merged, replace zipStore
// here with an import from zip-writer.js.

// ---------------------------------------------------------------------------
// Store-only zip
// ---------------------------------------------------------------------------

let CRC_TABLE = null;
function crcTable() {
  if (CRC_TABLE) return CRC_TABLE;
  CRC_TABLE = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    CRC_TABLE[n] = c >>> 0;
  }
  return CRC_TABLE;
}

export function crc32(bytes) {
  const t = crcTable();
  let c = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) c = t[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

const utf8 = new TextEncoder();

/**
 * Build a zip archive with no compression.
 * files: [{ name: string, data: string | Uint8Array }]. Order is preserved.
 * Entries are stamped 1980-01-01 so the output is reproducible.
 */
export function zipStore(files) {
  const parts = [];
  const central = [];
  let offset = 0;
  const u16 = (v) => [v & 0xff, (v >>> 8) & 0xff];
  const u32 = (v) => [v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff];
  const DOS_TIME = 0;
  const DOS_DATE = (0 << 9) | (1 << 5) | 1; // 1980-01-01
  const FLAGS = 0x0800; // file names are UTF-8
  for (const f of files) {
    const name = utf8.encode(String(f.name));
    const data = typeof f.data === "string" ? utf8.encode(f.data) : f.data;
    const crc = crc32(data);
    const header = new Uint8Array([
      ...u32(0x04034b50), ...u16(20), ...u16(FLAGS), ...u16(0), ...u16(DOS_TIME), ...u16(DOS_DATE),
      ...u32(crc), ...u32(data.length), ...u32(data.length), ...u16(name.length), ...u16(0)
    ]);
    parts.push(header, name, data);
    central.push({ name, crc, size: data.length, offset });
    offset += header.length + name.length + data.length;
  }
  const centralStart = offset;
  for (const c of central) {
    const rec = new Uint8Array([
      ...u32(0x02014b50), ...u16(20), ...u16(20), ...u16(FLAGS), ...u16(0), ...u16(DOS_TIME), ...u16(DOS_DATE),
      ...u32(c.crc), ...u32(c.size), ...u32(c.size), ...u16(c.name.length), ...u16(0), ...u16(0),
      ...u16(0), ...u16(0), ...u32(0), ...u32(c.offset)
    ]);
    parts.push(rec, c.name);
    offset += rec.length + c.name.length;
  }
  const centralSize = offset - centralStart;
  parts.push(new Uint8Array([
    ...u32(0x06054b50), ...u16(0), ...u16(0), ...u16(central.length), ...u16(central.length),
    ...u32(centralSize), ...u32(centralStart), ...u16(0)
  ]));
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let pos = 0;
  for (const p of parts) { out.set(p, pos); pos += p.length; }
  return out;
}

// ---------------------------------------------------------------------------
// XML helpers
// ---------------------------------------------------------------------------

/** Escape text for XML and drop characters XML 1.0 cannot contain. */
export function escapeXml(value) {
  let s = String(value == null ? "" : value);
  // eslint-disable-next-line no-control-regex
  s = s.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F￾￿]/g, "");
  // lone surrogates are not valid XML characters either
  s = s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "");
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

function run(text, { bold = false, italic = false, size = 0 } = {}) {
  const props = `${bold ? "<w:b/>" : ""}${italic ? "<w:i/>" : ""}${size ? `<w:sz w:val="${size}"/>` : ""}`;
  const lines = String(text == null ? "" : text).split(/\r?\n/);
  const body = lines.map((ln, i) => `${i ? "<w:br/>" : ""}<w:t xml:space="preserve">${escapeXml(ln)}</w:t>`).join("");
  return `<w:r>${props ? `<w:rPr>${props}</w:rPr>` : ""}${body}</w:r>`;
}

function paragraph(text, { style = "", bold = false, italic = false, size = 0, align = "", boxed = false } = {}) {
  const box = boxed ? `<w:pBdr>${["top", "left", "bottom", "right"].map((n) => `<w:${n} w:val="single" w:sz="4" w:space="4" w:color="808080"/>`).join("")}</w:pBdr>` : "";
  const ppr = `${style ? `<w:pStyle w:val="${style}"/>` : ""}${box}${align ? `<w:jc w:val="${align}"/>` : ""}`;
  return `<w:p>${ppr ? `<w:pPr>${ppr}</w:pPr>` : ""}${run(text, { bold, italic, size })}</w:p>`;
}

function cell(text, { header = false, align = "", width = 0 } = {}) {
  const tcPr = `${width ? `<w:tcW w:w="${width}" w:type="dxa"/>` : ""}${header ? '<w:shd w:val="clear" w:color="auto" w:fill="E7E6E6"/>' : ""}`;
  return `<w:tc>${tcPr ? `<w:tcPr>${tcPr}</w:tcPr>` : ""}${paragraph(text, { bold: header, align, size: 20 })}</w:tc>`;
}

function table(block) {
  const header = Array.isArray(block.header) ? block.header : [];
  const rows = Array.isArray(block.rows) ? block.rows : [];
  const ncols = Math.max(header.length, ...rows.map((r) => r.length), 1);
  const numeric = new Set(block.numericCols || []);
  const total = 9360; // US Letter, 1 inch margins, in twentieths of a point
  const widths = Array.isArray(block.colWidths) && block.colWidths.length === ncols
    ? block.colWidths.map((w) => Math.round((w / block.colWidths.reduce((a, b) => a + b, 0)) * total))
    : Array.from({ length: ncols }, () => Math.floor(total / ncols));
  const border = (n) => `<w:${n} w:val="single" w:sz="4" w:space="0" w:color="808080"/>`;
  const tblPr = `<w:tblPr><w:tblW w:w="${total}" w:type="dxa"/><w:tblBorders>${["top", "left", "bottom", "right", "insideH", "insideV"].map(border).join("")}</w:tblBorders><w:tblLayout w:type="fixed"/><w:tblCellMar><w:left w:w="80" w:type="dxa"/><w:right w:w="80" w:type="dxa"/></w:tblCellMar></w:tblPr>`;
  const grid = `<w:tblGrid>${widths.map((w) => `<w:gridCol w:w="${w}"/>`).join("")}</w:tblGrid>`;
  const mkRow = (cells, isHeader) => {
    const padded = Array.from({ length: ncols }, (_, i) => cells[i] == null ? "" : cells[i]);
    return `<w:tr>${isHeader ? "<w:trPr><w:tblHeader/></w:trPr>" : "<w:trPr><w:cantSplit/></w:trPr>"}${padded.map((c, i) => cell(c, { header: isHeader, align: numeric.has(i) && !isHeader ? "right" : "", width: widths[i] })).join("")}</w:tr>`;
  };
  const trs = (header.length ? [mkRow(header, true)] : []).concat(rows.map((r) => mkRow(r, false))).join("");
  // An empty paragraph after a table keeps Word from merging adjacent tables.
  return `<w:tbl>${tblPr}${grid}${trs}</w:tbl><w:p/>`;
}

/**
 * Block vocabulary:
 *  { type: "title", text }
 *  { type: "heading", level: 1|2|3, text }
 *  { type: "paragraph", text, bold?, italic? }
 *  { type: "note", text }                      (small italic paragraph)
 *  { type: "callout", text }                   (boxed paragraph)
 *  { type: "table", header: [], rows: [[]], numericCols?: [], colWidths?: [] }
 */
export function blocksToBodyXml(blocks) {
  const out = [];
  for (const b of blocks || []) {
    if (!b || typeof b !== "object") continue;
    if (b.type === "title") out.push(paragraph(b.text, { style: "Title" }));
    else if (b.type === "heading") out.push(paragraph(b.text, { style: `Heading${Math.min(3, Math.max(1, Number(b.level) || 1))}` }));
    else if (b.type === "paragraph") out.push(paragraph(b.text, { bold: !!b.bold, italic: !!b.italic }));
    else if (b.type === "callout") out.push(paragraph(b.text, { boxed: true, size: 20 }));
    else if (b.type === "note") out.push(paragraph(b.text, { italic: true, size: 19 }));
    else if (b.type === "table") out.push(table(b));
  }
  return out.join("");
}

const NS_W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main";
const XML_HEAD = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n';

function documentXml(blocks) {
  return `${XML_HEAD}<w:document xmlns:w="${NS_W}"><w:body>${blocksToBodyXml(blocks)}<w:sectPr><w:pgSz w:w="12240" w:h="15840"/><w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="720" w:footer="720" w:gutter="0"/></w:sectPr></w:body></w:document>`;
}

function stylesXml() {
  const heading = (n, size, before) =>
    `<w:style w:type="paragraph" w:styleId="Heading${n}"><w:name w:val="heading ${n}"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/><w:pPr><w:keepNext/><w:spacing w:before="${before}" w:after="80"/><w:outlineLvl w:val="${n - 1}"/></w:pPr><w:rPr><w:b/><w:sz w:val="${size}"/></w:rPr></w:style>`;
  return `${XML_HEAD}<w:styles xmlns:w="${NS_W}"><w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Calibri" w:hAnsi="Calibri" w:cs="Calibri"/><w:sz w:val="22"/><w:szCs w:val="22"/><w:lang w:val="en-US"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="120" w:line="264" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>`
    + `<w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:qFormat/></w:style>`
    + `<w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:qFormat/><w:pPr><w:spacing w:after="120"/></w:pPr><w:rPr><w:b/><w:sz w:val="40"/></w:rPr></w:style>`
    + heading(1, 30, 280) + heading(2, 26, 200) + heading(3, 23, 160)
    + `</w:styles>`;
}

function isoStamp(d) {
  const t = d instanceof Date && !Number.isNaN(d.getTime()) ? d : new Date(0);
  return t.toISOString().replace(/\.\d{3}Z$/, "Z");
}

/**
 * Build a .docx file as a Uint8Array.
 * meta: { title?, author?, created?: Date }
 */
export function buildDocx(blocks, meta = {}) {
  const title = meta.title || "Document";
  const author = meta.author || "";
  const NS_CT = "http://schemas.openxmlformats.org/package/2006/content-types";
  const NS_REL = "http://schemas.openxmlformats.org/package/2006/relationships";
  const REL_DOC = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
  const REL_STYLES = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles";
  const REL_CORE = "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties";
  const contentTypes = `${XML_HEAD}<Types xmlns="${NS_CT}"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/>`
    + `<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>`
    + `<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>`
    + `<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/></Types>`;
  const rootRels = `${XML_HEAD}<Relationships xmlns="${NS_REL}"><Relationship Id="rId1" Type="${REL_DOC}" Target="word/document.xml"/><Relationship Id="rId2" Type="${REL_CORE}" Target="docProps/core.xml"/></Relationships>`;
  const docRels = `${XML_HEAD}<Relationships xmlns="${NS_REL}"><Relationship Id="rId1" Type="${REL_STYLES}" Target="styles.xml"/></Relationships>`;
  const core = `${XML_HEAD}<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><dc:title>${escapeXml(title)}</dc:title><dc:creator>${escapeXml(author)}</dc:creator><dcterms:created xsi:type="dcterms:W3CDTF">${isoStamp(meta.created)}</dcterms:created></cp:coreProperties>`;
  return zipStore([
    { name: "[Content_Types].xml", data: contentTypes },
    { name: "_rels/.rels", data: rootRels },
    { name: "word/document.xml", data: documentXml(blocks) },
    { name: "word/_rels/document.xml.rels", data: docRels },
    { name: "word/styles.xml", data: stylesXml() },
    { name: "docProps/core.xml", data: core }
  ]);
}
