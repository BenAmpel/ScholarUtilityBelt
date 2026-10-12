import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

const store = {};
globalThis.chrome = {
  storage: {
    local: {
      get(defaults) {
        const out = {};
        for (const [k, d] of Object.entries(defaults || {})) out[k] = k in store ? JSON.parse(JSON.stringify(store[k])) : d;
        return Promise.resolve(out);
      },
      set(items) {
        for (const [k, v] of Object.entries(items)) store[k] = JSON.parse(JSON.stringify(v));
        return Promise.resolve();
      },
      remove(keys) {
        for (const k of Array.isArray(keys) ? keys : [keys]) delete store[k];
        return Promise.resolve();
      }
    }
  }
};

const { importLibraryPayload, mergeDuplicatesInStorage, undoLastImport, parseLibraryFileText } = await import("../src/common/library-import.js");
const { getLibraryBackup, getSavedPapers, getLibraryState } = await import("../src/common/storage.js");

beforeEach(() => {
  for (const k of Object.keys(store)) delete store[k];
});

const seed = () => {
  store.savedPapers = {
    a: { key: "a", title: "Alpha", doi: "10.1/a", notes: "mine", tags: ["t"], savedAt: "2024-01-01T00:00:00Z" },
    b: { key: "b", title: "Alpha preprint", url: "https://arxiv.org/abs/1", savedAt: "2024-02-01T00:00:00Z" }
  };
  store.libraryState = { collections: [{ id: "c1", name: "Thesis" }] };
};

describe("parseLibraryFileText", () => {
  it("handles empty, invalid and valid text", () => {
    assert.equal(parseLibraryFileText("").ok, false);
    assert.equal(parseLibraryFileText("{nope").ok, false);
    assert.deepEqual(parseLibraryFileText('{"a":1}'), { ok: true, data: { a: 1 } });
  });
});

describe("importLibraryPayload", () => {
  it("merges, writes one backup holding the old library, and undo restores it exactly", async () => {
    seed();
    const before = JSON.parse(JSON.stringify(store.savedPapers));
    const r = await importLibraryPayload({
      savedPapers: { a2: { key: "a2", title: "Alpha", doi: "10.1/a", notes: "theirs" }, n: { key: "n", title: "New" } },
      libraryState: { collections: [{ id: "x", name: "Imported" }] }
    });
    assert.equal(r.ok, true);
    assert.deepEqual(r.summary, { added: 1, merged: 1, unchanged: 0, skipped: 0, collectionsAdded: 1 });
    const papers = await getSavedPapers();
    assert.equal(Object.keys(papers).length, 3);
    assert.match(papers.a.notes, /mine[\s\S]*theirs/);
    const backup = await getLibraryBackup();
    assert.deepEqual(backup.savedPapers, before);
    assert.equal(backup.summary.added, 1);

    const undo = await undoLastImport();
    assert.equal(undo.ok, true);
    assert.deepEqual(await getSavedPapers(), before);
    assert.equal((await getLibraryState()).collections.length, 1);
    assert.equal(await getLibraryBackup(), null);
    assert.equal((await undoLastImport()).ok, false);
  });

  it("keeps only the most recent backup", async () => {
    seed();
    await importLibraryPayload({ savedPapers: { n1: { key: "n1", title: "One" } } });
    await importLibraryPayload({ savedPapers: { n2: { key: "n2", title: "Two" } } });
    const backup = await getLibraryBackup();
    assert.deepEqual(Object.keys(backup.savedPapers).sort(), ["a", "b", "n1"]);
  });

  it("writes nothing and keeps the old backup when the payload is unreadable", async () => {
    seed();
    const r = await importLibraryPayload({ nope: true });
    assert.equal(r.ok, false);
    assert.equal(await getLibraryBackup(), null);
    assert.equal(Object.keys(store.savedPapers).length, 2);
  });
});

describe("mergeDuplicatesInStorage", () => {
  it("merges two papers, keeps both URLs, and can be undone", async () => {
    seed();
    store.savedPapers.a.url = "https://journal.org/a";
    const r = await mergeDuplicatesInStorage("a", "b");
    assert.equal(r.ok, true);
    const papers = await getSavedPapers();
    assert.equal(papers.b, undefined);
    assert.equal(papers.a.url, "https://journal.org/a");
    assert.deepEqual(papers.a.altUrls, ["https://arxiv.org/abs/1"]);
    await undoLastImport();
    assert.ok((await getSavedPapers()).b);
  });
});
