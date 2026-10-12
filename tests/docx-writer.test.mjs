import { describe, it } from "node:test";
import assert from "node:assert/strict";
import zlib from "node:zlib";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const { buildDocx, escapeXml, zipStore, crc32, blocksToBodyXml } = await import("../src/common/docx-writer.js");

// Independent zip reader (central directory), used to check the writer's output.
function readZip(bytes) {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let eocd = -1;
  for (let i = bytes.length - 22; i >= 0; i--) if (dv.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  assert.ok(eocd >= 0, "end of central directory record");
  const count = dv.getUint16(eocd + 10, true);
  let p = dv.getUint32(eocd + 16, true);
  const files = {};
  for (let n = 0; n < count; n++) {
    assert.equal(dv.getUint32(p, true), 0x02014b50);
    const method = dv.getUint16(p + 10, true);
    const crc = dv.getUint32(p + 16, true);
    const csize = dv.getUint32(p + 20, true);
    const usize = dv.getUint32(p + 24, true);
    const nlen = dv.getUint16(p + 28, true);
    const elen = dv.getUint16(p + 30, true);
    const clen = dv.getUint16(p + 32, true);
    const off = dv.getUint32(p + 42, true);
    const name = new TextDecoder().decode(bytes.subarray(p + 46, p + 46 + nlen));
    assert.equal(dv.getUint32(off, true), 0x04034b50, "local header");
    const lnlen = dv.getUint16(off + 26, true);
    const lelen = dv.getUint16(off + 28, true);
    const start = off + 30 + lnlen + lelen;
    const data = bytes.subarray(start, start + csize);
    assert.equal(method, 0, "stored");
    assert.equal(csize, usize);
    assert.equal(zlib.crc32(data) >>> 0, crc, `crc of ${name}`);
    files[name] = new TextDecoder().decode(data);
    p += 46 + nlen + elen + clen;
  }
  return files;
}

// Tiny well-formedness check: balanced tags, one root, legal attribute quoting.
function assertWellFormed(xml, label) {
  const body = xml.replace(/^<\?xml[^>]*\?>\s*/, "");
  const stack = [];
  const re = /<(\/?)([A-Za-z_][\w:.-]*)((?:\s+[\w:.-]+="[^"<]*")*)\s*(\/?)>|<([^>]*)>|[^<]+/g;
  let m;
  let roots = 0;
  while ((m = re.exec(body))) {
    if (m[5] !== undefined) assert.fail(`${label}: malformed tag <${m[5]}>`);
    if (m[2] === undefined) {
      assert.equal(/[<>]|&(?!(?:amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);)/.test(m[0]), false, `${label}: bad text "${m[0].slice(0, 40)}"`);
      continue;
    }
    if (m[1]) {
      assert.equal(stack.pop(), m[2], `${label}: closing ${m[2]}`);
    } else if (!m[4]) {
      if (stack.length === 0) roots++;
      stack.push(m[2]);
    } else if (stack.length === 0) roots++;
  }
  assert.equal(stack.length, 0, `${label}: unclosed tags`);
  assert.equal(roots, 1, `${label}: exactly one root`);
}

describe("crc32 and zipStore", () => {
  it("matches known CRC-32 values", () => {
    assert.equal(crc32(new TextEncoder().encode("123456789")), 0xcbf43926);
    assert.equal(crc32(new Uint8Array(0)), 0);
  });
  it("round-trips files, including non-ASCII names and binary data", () => {
    const bin = new Uint8Array([0, 1, 2, 255, 254]);
    const z = zipStore([{ name: "a.txt", data: "hello" }, { name: "dir/café.bin", data: bin }]);
    const out = readZip(z);
    assert.equal(out["a.txt"], "hello");
    assert.deepEqual(Object.keys(out), ["a.txt", "dir/café.bin"]);
  });
  it("is reproducible", () => {
    const a = zipStore([{ name: "x", data: "1" }]);
    const b = zipStore([{ name: "x", data: "1" }]);
    assert.deepEqual(a, b);
  });
});

describe("escapeXml", () => {
  it("escapes the five XML specials", () => {
    assert.equal(escapeXml(`<a href="x">Tom & 'Jerry'</a>`), "&lt;a href=&quot;x&quot;&gt;Tom &amp; &apos;Jerry&apos;&lt;/a&gt;");
  });
  it("drops characters XML 1.0 cannot hold but keeps tab, newline and astral characters", () => {
    assert.equal(escapeXml("a\u0000b\u0008c\u000Bd\u001Fe￾f"), "abcdef");
    assert.equal(escapeXml("t\tn\nok \u{1F4DA}"), "t\tn\nok \u{1F4DA}");
    assert.equal(escapeXml("lone\uD800surrogate"), "lonesurrogate");
  });
  it("handles null and numbers", () => {
    assert.equal(escapeXml(null), "");
    assert.equal(escapeXml(42), "42");
  });
});

describe("buildDocx", () => {
  const blocks = [
    { type: "title", text: "Profile summary <draft> & notes" },
    { type: "heading", level: 1, text: "Indices" },
    { type: "paragraph", text: "Line one\nLine two with \"quotes\" and 'apostrophes'." },
    { type: "note", text: "Source: Google Scholar" },
    { type: "table", header: ["Measure", "Value"], rows: [["h-index", 21], ["Title with <b>tags</b> & ampersand", "n/a"]], numericCols: [1] },
    { type: "heading", level: 2, text: "Second level" },
    { type: "unknown" },
    null
  ];
  const bytes = buildDocx(blocks, { title: "A & B", author: "Ana <Test>", created: new Date(Date.UTC(2026, 9, 12, 8, 30, 0)) });
  const files = readZip(bytes);

  it("is a valid zip with the required parts", () => {
    for (const name of ["[Content_Types].xml", "_rels/.rels", "word/document.xml"]) assert.ok(name in files, name);
    assert.equal(Object.keys(files)[0], "[Content_Types].xml");
    assert.ok("word/styles.xml" in files);
    assert.ok("word/_rels/document.xml.rels" in files);
  });
  it("declares the main document part and wires the relationships", () => {
    assert.match(files["[Content_Types].xml"], /PartName="\/word\/document\.xml" ContentType="application\/vnd\.openxmlformats-officedocument\.wordprocessingml\.document\.main\+xml"/);
    assert.match(files["_rels/.rels"], /Target="word\/document\.xml"/);
    assert.match(files["word/_rels/document.xml.rels"], /Target="styles\.xml"/);
  });
  it("produces well-formed XML in every part", () => {
    for (const [name, xml] of Object.entries(files)) assertWellFormed(xml, name);
  });
  it("escapes text in the document and in the core properties", () => {
    const doc = files["word/document.xml"];
    assert.ok(doc.includes("Profile summary &lt;draft&gt; &amp; notes"));
    assert.ok(doc.includes("Title with &lt;b&gt;tags&lt;/b&gt; &amp; ampersand"));
    assert.ok(doc.includes("&quot;quotes&quot; and &apos;apostrophes&apos;"));
    assert.equal(doc.includes("<draft>"), false);
    assert.equal(doc.includes("<b>tags</b>"), false);
    assert.ok(files["docProps/core.xml"].includes("<dc:title>A &amp; B</dc:title>"));
    assert.ok(files["docProps/core.xml"].includes("Ana &lt;Test&gt;"));
    assert.ok(files["docProps/core.xml"].includes("2026-10-12T08:30:00Z"));
  });
  it("uses heading styles, a header row, right-aligned numeric cells and line breaks", () => {
    const doc = files["word/document.xml"];
    assert.ok(doc.includes('<w:pStyle w:val="Title"/>'));
    assert.ok(doc.includes('<w:pStyle w:val="Heading1"/>'));
    assert.ok(doc.includes('<w:pStyle w:val="Heading2"/>'));
    assert.ok(doc.includes("<w:tblHeader/>"));
    assert.ok(doc.includes('<w:jc w:val="right"/>'));
    assert.ok(doc.includes("<w:br/>"));
    assert.equal((doc.match(/<w:tbl>/g) || []).length, 1);
    assert.equal((doc.match(/<w:tr>/g) || []).length, 3);
  });
  it("defines the styles it references", () => {
    for (const id of ["Normal", "Title", "Heading1", "Heading2", "Heading3"]) assert.ok(files["word/styles.xml"].includes(`w:styleId="${id}"`), id);
  });
  it("handles empty input, ragged tables and stripped control characters", () => {
    const empty = readZip(buildDocx([]));
    assertWellFormed(empty["word/document.xml"], "empty");
    const ragged = readZip(buildDocx([{ type: "table", header: ["A", "B", "C"], rows: [["1"], ["1", "2", "3", "4"]] }, { type: "paragraph", text: "bad\u0000char" }]));
    assertWellFormed(ragged["word/document.xml"], "ragged");
    assert.ok(ragged["word/document.xml"].includes("badchar"));
  });
  it("blocksToBodyXml ignores unknown block types", () => {
    assert.equal(blocksToBodyXml([{ type: "nope" }, null, 5]), "");
  });

  it("passes unzip -t when unzip is installed", (t) => {
    const probe = spawnSync("unzip", ["-v"], { encoding: "utf8" });
    if (probe.error || probe.status !== 0) return t.skip("unzip not available");
    const dir = mkdtempSync(path.join(tmpdir(), "docx-test-"));
    try {
      const file = path.join(dir, "t.docx");
      writeFileSync(file, bytes);
      const r = spawnSync("unzip", ["-t", file], { encoding: "utf8" });
      assert.equal(r.status, 0, r.stdout + r.stderr);
      assert.match(r.stdout, /No errors detected/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
