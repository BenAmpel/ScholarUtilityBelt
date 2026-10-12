import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { crc32, buildZip, dosDateTime, cleanZipName } from "../src/common/zip-writer.js";

const enc = (s) => new TextEncoder().encode(s);
const have = (cmd, args) => { try { return spawnSync(cmd, args).status === 0; } catch { return false; } };

describe("crc32", () => {
  it("matches published check values", () => {
    assert.equal(crc32(enc("")), 0);
    assert.equal(crc32(enc("a")), 0xe8b7be43);
    assert.equal(crc32(enc("123456789")), 0xcbf43926);
    assert.equal(crc32(enc("The quick brown fox jumps over the lazy dog")), 0x414fa339);
  });
  it("returns an unsigned number for high-bit results", () => {
    assert.ok(crc32(enc("123456789")) > 0);
  });
});

describe("dosDateTime", () => {
  it("packs local date and time", () => {
    const r = dosDateTime(new Date(2026, 9, 12, 13, 45, 31)); // 12 Oct 2026 13:45:31
    assert.equal(r.date, ((2026 - 1980) << 9) | (10 << 5) | 12);
    assert.equal(r.time, (13 << 11) | (45 << 5) | 15);
  });
  it("clamps years before 1980", () => {
    assert.equal(dosDateTime(new Date(1970, 0, 1)).date, (1 << 5) | 1);
  });
});

describe("cleanZipName", () => {
  it("removes unsafe parts", () => {
    assert.equal(cleanZipName("/a/../b\\c.md"), "a/b/c.md");
    assert.equal(cleanZipName("../../x"), "x");
    assert.equal(cleanZipName(""), "");
  });
});

describe("buildZip", () => {
  const files = [
    { name: "hello.txt", data: "hello world\n" },
    { name: "sub/Ünïcode – ö.md", data: "# Title\n\nbody with ✓ and “quotes”\n" },
    { name: "empty.txt", data: "" },
    { name: "bytes.bin", data: new Uint8Array([0, 1, 2, 255, 254]) },
  ];
  const zip = buildZip(files, { date: new Date(2026, 9, 12, 10, 0, 0) });

  it("has the right record counts and signature", () => {
    const dv = new DataView(zip.buffer, zip.byteOffset, zip.byteLength);
    assert.equal(dv.getUint32(0, true), 0x04034b50);
    assert.equal(dv.getUint32(zip.length - 22, true), 0x06054b50);
    assert.equal(dv.getUint16(zip.length - 22 + 10, true), 4);
  });
  it("rejects duplicates and empty names", () => {
    assert.throws(() => buildZip([{ name: "a", data: "" }, { name: "a", data: "" }]), /duplicate/);
    assert.throws(() => buildZip([{ name: "", data: "" }]), /file name/);
  });
  it("builds an empty archive", () => {
    const z = buildZip([]);
    assert.equal(z.length, 22);
  });

  it("passes unzip -t and zipinfo, with UTF-8 names intact", { skip: !have("unzip", ["-v"]) }, () => {
    const dir = mkdtempSync(join(tmpdir(), "zipw-"));
    try {
      const p = join(dir, "t.zip");
      writeFileSync(p, zip);
      const t = spawnSync("unzip", ["-t", p], { encoding: "utf8" });
      assert.equal(t.status, 0, t.stdout + t.stderr);
      assert.match(t.stdout, /No errors detected/);
      const info = spawnSync("zipinfo", [p], { encoding: "utf8" });
      assert.equal(info.status, 0, info.stderr);
      assert.match(info.stdout, /4 files/);
      // zipinfo escapes non-ASCII names by locale; the Python test checks the exact UTF-8 name.
      assert.match(info.stdout, /sub\//);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("round-trips with Python zipfile (CRC checked)", { skip: !have("python3", ["--version"]) }, () => {
    const dir = mkdtempSync(join(tmpdir(), "zipw-"));
    try {
      const p = join(dir, "t.zip");
      writeFileSync(p, zip);
      const py = "import sys,zipfile\nz=zipfile.ZipFile(sys.argv[1])\nassert z.testzip() is None\nprint(z.read('hello.txt').decode(), end='')\nprint('|'+'|'.join(sorted(z.namelist())))\ni=z.getinfo('hello.txt');print(i.date_time)";
      const r = spawnSync("python3", ["-I", "-c", py, p], { encoding: "utf8" });
      assert.equal(r.status, 0, r.stderr);
      assert.match(r.stdout, /hello world/);
      assert.match(r.stdout, /Ünïcode – ö\.md/);
      assert.match(r.stdout, /\(2026, 10, 12, 10, 0, 0\)/);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
});
