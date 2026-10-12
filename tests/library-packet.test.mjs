import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  PACKET_FORMAT,
  PACKET_LIMITS,
  buildPacket,
  computeChecksum,
  packetFileName,
  packetToImport,
  sanitizePacketPaper,
  validatePacket,
  validatePacketText
} from "../src/common/library-packet.js";
import { mergeLibraryImport } from "../src/common/library-merge.js";

const papers = [
  {
    key: "a", title: "First paper", authorsVenue: "A Smith - J Things, 2020", year: 2020, doi: "10.1/a",
    url: "https://example.org/a", pdfUrl: "https://example.org/a.pdf", tags: ["nlp", "nlp", "read"],
    notes: "PRIVATE note", pdfNotes: { summary: "private summary", highlights: [{ page: "2", quote: "q", note: "n" }] },
    localFile: { name: "secret.pdf", relativePath: "/Users/me/secret.pdf" }, collections: ["c1"], savedAt: "2024-01-01"
  },
  { key: "b", title: "Second paper", url: "javascript:alert(1)", tags: [] }
];

describe("sanitizePacketPaper", () => {
  it("drops private notes by default, plus local files, collection ids and unsafe URLs", () => {
    const p = sanitizePacketPaper(papers[0]);
    assert.equal(p.notes, undefined);
    assert.equal(p.pdfNotes, undefined);
    assert.equal(p.localFile, undefined);
    assert.equal(p.collections, undefined);
    assert.equal(p.savedAt, undefined);
    assert.deepEqual(p.tags, ["nlp", "read"]);
    assert.equal(sanitizePacketPaper(papers[1]).url, undefined);
  });
  it("keeps notes and highlights only when asked", () => {
    const p = sanitizePacketPaper(papers[0], { includeNotes: true });
    assert.equal(p.notes, "PRIVATE note");
    assert.equal(p.pdfNotes.summary, "private summary");
    assert.equal(p.pdfNotes.highlights.length, 1);
  });
  it("rejects non-objects and empty records", () => {
    assert.equal(sanitizePacketPaper(null), null);
    assert.equal(sanitizePacketPaper("x"), null);
    assert.equal(sanitizePacketPaper({}), null);
  });
  it("strips control characters and caps lengths", () => {
    const p = sanitizePacketPaper({ key: "k", title: "A\u0000\nB" + "x".repeat(5000) });
    assert.ok(!/[\u0000\n]/.test(p.title));
    assert.equal(p.title.length, PACKET_LIMITS.maxTitle);
  });
});

describe("buildPacket", () => {
  it("builds a v1 packet that excludes notes by default and carries a checksum", async () => {
    const r = await buildPacket({ papers, name: "Reading list", from: "Ben", collectionName: "Thesis" });
    assert.equal(r.ok, true);
    assert.equal(r.packet.format, PACKET_FORMAT);
    assert.equal(r.packet.version, 1);
    assert.equal(r.packet.includesNotes, false);
    assert.equal(JSON.stringify(r.packet).includes("PRIVATE"), false);
    assert.equal(r.packet.checksum.algorithm, "sha-256");
    assert.match(r.packet.checksum.value, /^[0-9a-f]{64}$/);
    assert.equal(JSON.stringify(r.packet).toLowerCase().includes("signature"), false);
  });
  it("requires a name, a from label and at least one paper", async () => {
    assert.equal((await buildPacket({ papers, name: "", from: "x" })).ok, false);
    assert.equal((await buildPacket({ papers, name: "x", from: "  " })).ok, false);
    assert.equal((await buildPacket({ papers: [], name: "x", from: "y" })).ok, false);
  });
  it("includes notes when requested", async () => {
    const r = await buildPacket({ papers, name: "n", from: "f", includeNotes: true });
    assert.equal(r.packet.includesNotes, true);
    assert.ok(JSON.stringify(r.packet).includes("PRIVATE"));
  });
  it("refuses more papers than the limit", async () => {
    const many = Array.from({ length: PACKET_LIMITS.maxPapers + 1 }, (_, i) => ({ key: `k${i}`, title: `T${i}` }));
    assert.equal((await buildPacket({ papers: many, name: "n", from: "f" })).ok, false);
  });
});

describe("validatePacket", () => {
  const good = async () => (await buildPacket({ papers, name: "Reading list", from: "Ben" })).packet;

  it("accepts a built packet round trip with a matching checksum", async () => {
    const r = await validatePacketText(JSON.stringify(await good()));
    assert.equal(r.ok, true);
    assert.equal(r.checksum, "ok");
    assert.deepEqual(r.warnings, []);
    assert.equal(r.packet.papers.length, 2);
  });

  it("warns, but does not reject, when the checksum no longer matches", async () => {
    const p = await good();
    p.papers[0].title = "Edited by hand";
    const r = await validatePacket(p);
    assert.equal(r.ok, true);
    assert.equal(r.checksum, "mismatch");
    assert.match(r.warnings.join(" "), /checksum does not match/);
  });

  it("warns when the checksum is missing", async () => {
    const p = await good();
    delete p.checksum;
    const r = await validatePacket(p);
    assert.equal(r.checksum, "missing");
  });

  it("rejects malformed files with a clear message", async () => {
    assert.match((await validatePacketText("")).error, /empty/);
    assert.match((await validatePacketText("{oops")).error, /not valid JSON/);
    assert.match((await validatePacket([])).error, /not a collection packet/);
    assert.match((await validatePacket({ format: "other" })).error, /not a Scholar Utility Belt/);
    assert.match((await validatePacket({ savedPapers: {} })).error, /not a Scholar|library export/);
    assert.match((await validatePacket({ format: PACKET_FORMAT })).error, /version/);
    assert.match((await validatePacket({ format: PACKET_FORMAT, version: 1 })).error, /papers list/);
    assert.match((await validatePacket({ format: PACKET_FORMAT, version: 1, papers: [] })).error, /no papers/);
    assert.match((await validatePacket({ format: PACKET_FORMAT, version: 1, papers: [{ title: "t" }] })).error, /no name/);
    assert.match((await validatePacket({ format: PACKET_FORMAT, version: 1, name: "n", papers: [{ title: "t" }] })).error, /from/);
    assert.match((await validatePacket({ format: PACKET_FORMAT, version: 1, name: "n", from: "f", papers: [1, null] })).error, /None of the papers/);
  });

  it("rejects a newer version and a nonsense version", async () => {
    const p = await good();
    assert.match((await validatePacket({ ...p, version: 2 })).error, /version 2/);
    assert.match((await validatePacket({ ...p, version: "1" })).error, /version/);
    assert.match((await validatePacket({ ...p, version: 0 })).error, /not valid/);
  });

  it("rejects oversize files and too many papers", async () => {
    const big = "x".repeat(PACKET_LIMITS.maxBytes + 1);
    assert.match((await validatePacketText(big)).error, /larger than 5 MB/);
    const many = { format: PACKET_FORMAT, version: 1, name: "n", from: "f", papers: new Array(PACKET_LIMITS.maxPapers + 1).fill({ title: "t" }) };
    assert.match((await validatePacket(many)).error, /limit/);
  });

  it("ignores notes in a packet that says it has none, and counts unreadable entries", async () => {
    const r = await validatePacket({
      format: PACKET_FORMAT, version: 1, name: "n", from: "f", includesNotes: false,
      papers: [{ key: "a", title: "T", notes: "smuggled" }, null]
    });
    assert.equal(r.ok, true);
    assert.equal(r.packet.papers[0].notes, undefined);
    assert.match(r.warnings[0], /1 unreadable entry was skipped/);
  });

  it("neutralises hostile strings: unsafe URLs, long fields, prototype keys", async () => {
    const hostile = JSON.parse(
      '{"format":"' + PACKET_FORMAT + '","version":1,"name":"n","from":"f","papers":[{"key":"__proto__","title":"T","url":"javascript:x","__proto__":{"polluted":1},"localFile":{"name":"x"}}]}'
    );
    const r = await validatePacket(hostile);
    assert.equal(r.ok, true);
    assert.equal(r.packet.papers[0].url, undefined);
    assert.equal(r.packet.papers[0].localFile, undefined);
    assert.equal({}.polluted, undefined);
  });
});

describe("packet import through the merge-safe path", () => {
  it("tags imported papers 'from <label>', merges into an existing paper and adds the collection", async () => {
    const built = await buildPacket({ papers, name: "Reading list", from: "Ben's lab", collectionName: "Thesis" });
    const v = await validatePacket(built.packet);
    const { payload, options } = packetToImport(v.packet, { now: "2026-01-01T00:00:00Z" });
    const current = { mine: { key: "mine", title: "First paper", doi: "10.1/a", notes: "my own note", tags: ["keep"] } };
    const r = mergeLibraryImport(current, { collections: [] }, payload, options);
    assert.equal(r.ok, true);
    assert.equal(r.summary.merged, 1);
    assert.equal(r.summary.added, 1);
    assert.equal(r.papers.mine.notes, "my own note");
    assert.ok(r.papers.mine.tags.includes("keep"));
    assert.ok(r.papers.mine.tags.includes("from Ben's lab"));
    assert.equal(r.state.collections[0].name, "Thesis");
    assert.deepEqual(r.papers.mine.collections, [r.state.collections[0].id]);
    const added = Object.values(r.papers).find((p) => p.title === "Second paper");
    assert.ok(added.tags.includes("from Ben's lab"));
    assert.equal(added.savedAt, "2026-01-01T00:00:00Z");
  });

  it("is idempotent when the same packet is imported twice", async () => {
    const built = await buildPacket({ papers, name: "n", from: "f" });
    const { payload, options } = packetToImport((await validatePacket(built.packet)).packet, { now: "2026-01-01T00:00:00Z" });
    const first = mergeLibraryImport({}, {}, payload, options);
    const second = mergeLibraryImport(first.papers, first.state, payload, options);
    assert.equal(second.summary.added, 0);
    assert.equal(second.summary.merged, 0);
  });
});

describe("helpers", () => {
  it("checksum is stable under key order", async () => {
    const a = { format: "x", papers: [{ b: 1, a: 2 }] };
    const b = { papers: [{ a: 2, b: 1 }], format: "x" };
    assert.equal(await computeChecksum(a), await computeChecksum(b));
  });
  it("builds a safe file name", () => {
    assert.equal(packetFileName("My Reading/List!", new Date("2026-03-04T00:00:00Z")), "my-reading-list-2026-03-04.sub.json");
    assert.equal(packetFileName("***", new Date("2026-03-04T00:00:00Z")), "collection-2026-03-04.sub.json");
  });
});
