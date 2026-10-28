import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  paperFingerprint,
  joinText,
  mergePaperRecords,
  mergeLibraryState,
  mergeLibraryImport,
  mergeDuplicatePair,
  describeImportSummary,
  NOTE_SEPARATOR
} from "../src/common/library-merge.js";

const paper = (over = {}) => ({
  key: "k1",
  title: "Deep learning for citation analysis",
  authorsVenue: "A Smith, B Jones - Journal of Things, 2021",
  year: 2021,
  url: "https://example.org/a",
  doi: "10.1000/abc",
  tags: [],
  collections: [],
  savedAt: "2024-01-01T00:00:00.000Z",
  updatedAt: "2024-01-01T00:00:00.000Z",
  ...over
});

describe("paperFingerprint", () => {
  it("uses the DOI first, normalized", () => {
    assert.equal(paperFingerprint({ doi: "https://doi.org/10.1000/ABC" }), "doi:10.1000/abc");
    assert.equal(paperFingerprint({ doi: "DOI: 10.1000/abc" }), "doi:10.1000/abc");
  });
  it("falls back to a normalized title, then the key", () => {
    assert.equal(paperFingerprint({ title: "  Hello, World! " }), "title:hello world");
    assert.equal(paperFingerprint({ key: "zz" }), "key:zz");
  });
});

describe("joinText", () => {
  it("keeps both texts and is idempotent", () => {
    const once = joinText("mine", "theirs");
    assert.equal(once, `mine${NOTE_SEPARATOR}theirs`);
    assert.equal(joinText(once, "theirs"), once);
    assert.equal(joinText(once, "mine"), once);
  });
  it("handles empty and identical values", () => {
    assert.equal(joinText("", "x"), "x");
    assert.equal(joinText("x", ""), "x");
    assert.equal(joinText("x", " x "), "x");
    assert.equal(joinText(undefined, undefined), "");
  });
});

describe("mergePaperRecords", () => {
  it("unions tags case-insensitively and collections exactly", () => {
    const m = mergePaperRecords(
      paper({ tags: ["NLP", "read"], collections: ["c1"] }),
      paper({ tags: ["nlp", "to-cite"], collections: ["c1", "c2"] })
    );
    assert.deepEqual(m.tags, ["NLP", "read", "to-cite"]);
    assert.deepEqual(m.collections, ["c1", "c2"]);
  });

  it("concatenates differing notes and PDF summaries instead of losing one", () => {
    const m = mergePaperRecords(
      paper({ notes: "my note", pdfNotes: { summary: "my summary" } }),
      paper({ notes: "their note", pdfNotes: { summary: "their summary" } })
    );
    assert.equal(m.notes, `my note${NOTE_SEPARATOR}their note`);
    assert.equal(m.pdfNotes.summary, `my summary${NOTE_SEPARATOR}their summary`);
  });

  it("unions highlights and drops only exact repeats", () => {
    const h1 = { page: "3", quote: "q1", note: "n1", createdAt: "x" };
    const h1again = { page: "3", quote: "Q1 ", note: "n1", createdAt: "y" };
    const h2 = { page: "5", quote: "q2", note: "" };
    const m = mergePaperRecords(
      paper({ pdfNotes: { highlights: [h1] } }),
      paper({ pdfNotes: { highlights: [h1again, h2] } })
    );
    assert.equal(m.pdfNotes.highlights.length, 2);
    assert.deepEqual(m.pdfNotes.highlights[0], h1);
    assert.deepEqual(m.pdfNotes.highlights[1], h2);
  });

  it("newer non-empty scalar wins, but an empty value never overwrites", () => {
    const old = paper({ snippet: "old snippet", venue: "Venue A", updatedAt: "2024-01-01T00:00:00Z" });
    const fresh = paper({ snippet: "new snippet", venue: "", abstract: "abs", updatedAt: "2025-01-01T00:00:00Z" });
    const m = mergePaperRecords(old, fresh);
    assert.equal(m.snippet, "new snippet");
    assert.equal(m.venue, "Venue A");
    assert.equal(m.abstract, "abs");
    // reversed: the older incoming record loses a conflict
    const m2 = mergePaperRecords(fresh, old);
    assert.equal(m2.snippet, "new snippet");
    assert.equal(m2.venue, "Venue A");
  });

  it("takes the higher citation count, earliest savedAt, latest updatedAt", () => {
    const m = mergePaperRecords(
      paper({ citations: 10, savedAt: "2024-03-01T00:00:00Z", updatedAt: "2024-03-01T00:00:00Z" }),
      paper({ citations: 25, savedAt: "2023-01-01T00:00:00Z", updatedAt: "2025-05-05T00:00:00Z" })
    );
    assert.equal(m.citations, 25);
    assert.equal(m.savedAt, "2023-01-01T00:00:00Z");
    assert.equal(m.updatedAt, "2025-05-05T00:00:00Z");
    const zero = mergePaperRecords(paper({ citations: 12 }), paper({ citations: 0, updatedAt: "2030-01-01T00:00:00Z" }));
    assert.equal(zero.citations, 12);
  });

  it("keeps the existing key and both URLs when they differ", () => {
    const m = mergePaperRecords(
      paper({ key: "mine", url: "https://arxiv.org/abs/1", pdfUrl: "https://arxiv.org/pdf/1" }),
      paper({ key: "theirs", url: "https://journal.org/1", pdfUrl: "", updatedAt: "2025-01-01T00:00:00Z" })
    );
    assert.equal(m.key, "mine");
    assert.equal(m.url, "https://journal.org/1");
    assert.deepEqual(m.altUrls, ["https://arxiv.org/abs/1"]);
    assert.equal(m.pdfUrl, "https://arxiv.org/pdf/1");
  });

  it("does not mutate its inputs and ignores prototype-pollution keys", () => {
    const a = paper({ tags: ["a"] });
    const b = JSON.parse('{"key":"k1","title":"t","__proto__":{"polluted":true},"tags":["b"]}');
    const snapshot = JSON.stringify(a);
    const m = mergePaperRecords(a, b);
    assert.equal(JSON.stringify(a), snapshot);
    assert.equal({}.polluted, undefined);
    assert.equal(m.polluted, undefined);
  });

  it("copes with non-object input", () => {
    assert.deepEqual(mergePaperRecords(null, { key: "x", title: "T" }), { key: "x", title: "T" });
    assert.deepEqual(mergePaperRecords({ key: "x" }, "garbage"), { key: "x" });
  });
});

describe("mergeLibraryState", () => {
  it("matches collections by name and maps ids", () => {
    const { state, idMap, collectionsAdded } = mergeLibraryState(
      { collections: [{ id: "col_a", name: "Thesis" }] },
      { collections: [{ id: "col_z", name: "thesis" }, { id: "col_new", name: "Reading" }] }
    );
    assert.equal(idMap.get("col_z"), "col_a");
    assert.equal(idMap.get("col_new"), "col_new");
    assert.equal(collectionsAdded, 1);
    assert.deepEqual(state.collections.map((c) => c.name), ["Thesis", "Reading"]);
  });

  it("gives a new id when an imported id is taken by a differently named collection", () => {
    const { state, idMap } = mergeLibraryState(
      { collections: [{ id: "col_1", name: "Mine" }] },
      { collections: [{ id: "col_1", name: "Theirs" }] }
    );
    assert.equal(state.collections.length, 2);
    assert.notEqual(idMap.get("col_1"), "col_1");
    assert.equal(state.collections.find((c) => c.id === idMap.get("col_1")).name, "Theirs");
  });

  it("adds saved searches once, remaps their collection, and never removes folders", () => {
    const cur = {
      collections: [{ id: "c1", name: "A" }],
      savedSearches: [{ id: "s1", name: "x", query: "q" }],
      watchedFolders: [{ id: "f1", name: "F", files: [{ relativePath: "a.pdf" }] }],
      activeCollectionId: "c1"
    };
    const inc = {
      collections: [{ id: "zz", name: "A" }],
      savedSearches: [
        { id: "s1", name: "other", query: "other" },
        { id: "s9", name: "n", query: "m", collectionId: "zz" }
      ],
      watchedFolders: [{ id: "f1", name: "F", files: [{ relativePath: "a.pdf" }, { relativePath: "b.pdf" }] }],
      activeCollectionId: "zz"
    };
    const { state } = mergeLibraryState(cur, inc);
    assert.equal(state.savedSearches.length, 2);
    assert.equal(state.savedSearches[1].collectionId, "c1");
    assert.equal(state.watchedFolders[0].files.length, 2);
    assert.equal(state.activeCollectionId, "c1");
  });

  it("tolerates garbage", () => {
    const { state } = mergeLibraryState(undefined, { collections: "no", savedSearches: [null, 3], watchedFolders: [{}] });
    assert.deepEqual(state.collections, []);
    assert.deepEqual(state.savedSearches, []);
    assert.deepEqual(state.watchedFolders, []);
  });
});

describe("mergeLibraryImport", () => {
  const current = {
    k1: paper({ key: "k1", tags: ["mine"], notes: "my private note", collections: ["c1"] }),
    k2: paper({ key: "k2", title: "Another paper", doi: "", tags: [] })
  };
  const state = { collections: [{ id: "c1", name: "Thesis" }], savedSearches: [], watchedFolders: [] };

  it("rejects payloads that are not library exports", () => {
    assert.equal(mergeLibraryImport(current, state, null).ok, false);
    assert.equal(mergeLibraryImport(current, state, []).ok, false);
    assert.equal(mergeLibraryImport(current, state, { savedPapers: [] }).ok, false);
    assert.equal(mergeLibraryImport(current, state, { foo: 1 }).ok, false);
  });

  it("does not overwrite an existing paper with the incoming record (the old {...a,...b} bug)", () => {
    const incoming = {
      savedPapers: {
        k1: { key: "k1", title: "Deep learning for citation analysis", doi: "10.1000/abc", notes: "their note", tags: ["theirs"], updatedAt: "2026-01-01T00:00:00Z" }
      }
    };
    const r = mergeLibraryImport(current, state, incoming);
    assert.equal(r.ok, true);
    const p = r.papers.k1;
    assert.equal(p.notes, `my private note${NOTE_SEPARATOR}their note`);
    assert.deepEqual(p.tags, ["mine", "theirs"]);
    assert.deepEqual(p.collections, ["c1"]);
    assert.equal(p.authorsVenue, current.k1.authorsVenue);
    assert.deepEqual(r.summary, { added: 0, merged: 1, unchanged: 0, skipped: 0, collectionsAdded: 0 });
  });

  it("matches by DOI even when the storage keys differ", () => {
    const r = mergeLibraryImport(current, state, {
      savedPapers: { other_key: { key: "other_key", title: "Slightly different title", doi: "https://doi.org/10.1000/ABC", tags: ["x"] } }
    });
    assert.equal(Object.keys(r.papers).length, 2);
    assert.deepEqual(r.papers.k1.tags, ["mine", "x"]);
    assert.equal(r.summary.merged, 1);
  });

  it("matches by normalized title when there is no DOI", () => {
    const r = mergeLibraryImport(current, state, {
      savedPapers: { zz: { key: "zz", title: "ANOTHER paper!", tags: ["t"] } }
    });
    assert.equal(Object.keys(r.papers).length, 2);
    assert.deepEqual(r.papers.k2.tags, ["t"]);
  });

  it("adds new papers, avoiding key collisions with different papers", () => {
    const r = mergeLibraryImport(current, state, {
      savedPapers: { k1: { key: "k1", title: "A completely different paper", doi: "10.9/zzz" } }
    });
    // same key but different DOI and title: matched by key (same Scholar cluster), so merged, not duplicated
    assert.equal(Object.keys(r.papers).length, 2);
    const r2 = mergeLibraryImport(current, state, {
      savedPapers: { new1: { key: "new1", title: "Brand new", doi: "10.9/new" } }
    });
    assert.equal(r2.summary.added, 1);
    assert.equal(r2.papers.new1.title, "Brand new");
    assert.ok(r2.papers.new1.savedAt);
  });

  it("is idempotent: importing the same file twice changes nothing the second time", () => {
    const incoming = {
      savedPapers: {
        n1: { key: "n1", title: "New one", doi: "10.1/n1", notes: "hello", tags: ["a"], pdfNotes: { highlights: [{ page: "1", quote: "q", note: "" }], summary: "s" } },
        k1: { key: "k1", title: "Deep learning for citation analysis", doi: "10.1000/abc", notes: "their note", tags: ["theirs"] }
      },
      libraryState: { collections: [{ id: "cc", name: "Imported" }] }
    };
    const first = mergeLibraryImport(current, state, incoming);
    const second = mergeLibraryImport(first.papers, first.state, incoming);
    assert.equal(second.summary.added, 0);
    assert.equal(second.summary.merged, 0);
    assert.equal(second.summary.unchanged, 2);
    assert.deepEqual(second.papers, first.papers);
    assert.deepEqual(second.state, first.state);
  });

  it("never deletes: every current paper, tag, note and collection survives", () => {
    const r = mergeLibraryImport(current, state, {
      savedPapers: { a: { key: "a", title: "x" }, k1: { key: "k1", title: "Deep learning for citation analysis", doi: "10.1000/abc", tags: [], notes: "", collections: [] } },
      libraryState: { collections: [] }
    });
    assert.ok(r.papers.k1 && r.papers.k2);
    assert.deepEqual(r.papers.k1.tags, ["mine"]);
    assert.equal(r.papers.k1.notes, "my private note");
    assert.deepEqual(r.papers.k1.collections, ["c1"]);
    assert.equal(r.state.collections.length, 1);
  });

  it("remaps imported collection ids, drops dangling ones, and reports new collections", () => {
    const r = mergeLibraryImport(current, state, {
      savedPapers: {
        n1: { key: "n1", title: "N", doi: "10.1/n", collections: ["zz", "new", "ghost", "c1"] }
      },
      libraryState: { collections: [{ id: "zz", name: "thesis" }, { id: "new", name: "Fresh" }] }
    });
    assert.deepEqual(r.papers.n1.collections, ["c1", "new"]);
    assert.equal(r.summary.collectionsAdded, 1);
  });

  it("merges two incoming records of the same paper with each other", () => {
    const r = mergeLibraryImport({}, {}, {
      savedPapers: {
        a: { key: "a", title: "Same paper", tags: ["x"] },
        b: { key: "b", title: "same  paper", tags: ["y"] }
      }
    });
    assert.equal(Object.keys(r.papers).length, 1);
    assert.deepEqual(r.papers.a.tags, ["x", "y"]);
    assert.equal(r.summary.added, 1);
    assert.equal(r.summary.merged, 1);
  });

  it("skips unreadable records and prototype-pollution keys", () => {
    const payload = JSON.parse(
      '{"savedPapers":{"__proto__":{"key":"p","title":"evil"},"bad":"string","nul":null,"ok":{"key":"ok","title":"Fine"}}}'
    );
    const r = mergeLibraryImport({}, {}, payload);
    assert.equal(Object.keys(r.papers).length, 1);
    assert.equal(r.summary.skipped >= 2, true);
    assert.equal({}.title, undefined);
  });

  it("does not modify its inputs", () => {
    const before = JSON.stringify([current, state]);
    mergeLibraryImport(current, state, { savedPapers: { k1: { key: "k1", doi: "10.1000/abc", title: "t", notes: "n" } } });
    assert.equal(JSON.stringify([current, state]), before);
  });

  it("adds extraTags to every imported paper (packet import)", () => {
    const r = mergeLibraryImport(current, state, { savedPapers: { n: { key: "n", title: "N", tags: ["a"] } } }, { extraTags: ["from Lab"] });
    assert.deepEqual(r.papers.n.tags, ["a", "from Lab"]);
  });

  it("handles conflicting records across many fields at once", () => {
    const mine = {
      m: paper({
        key: "m", notes: "A", tags: ["x"], snippet: "old", citations: 5, url: "https://a",
        pdfNotes: { summary: "S1", highlights: [{ page: "1", quote: "q", note: "n" }] },
        updatedAt: "2024-01-01T00:00:00Z"
      })
    };
    const theirs = {
      savedPapers: {
        t: {
          key: "t", title: paper().title, doi: "10.1000/abc", notes: "B", tags: ["Y", "X"], snippet: "newer", citations: 3,
          url: "https://b", pdfNotes: { summary: "S2", highlights: [{ page: "2", quote: "q2", note: "" }] },
          updatedAt: "2026-06-01T00:00:00Z"
        }
      }
    };
    const r = mergeLibraryImport(mine, {}, theirs);
    const p = r.papers.m;
    assert.equal(p.key, "m");
    assert.equal(p.notes, `A${NOTE_SEPARATOR}B`);
    assert.deepEqual(p.tags, ["x", "Y"]);
    assert.equal(p.snippet, "newer");
    assert.equal(p.citations, 5);
    assert.equal(p.url, "https://b");
    assert.deepEqual(p.altUrls, ["https://a"]);
    assert.equal(p.pdfNotes.highlights.length, 2);
    assert.equal(p.pdfNotes.summary, `S1${NOTE_SEPARATOR}S2`);
  });
});

describe("mergeDuplicatePair", () => {
  const lib = {
    pre: paper({ key: "pre", url: "https://arxiv.org/abs/1", doi: "", notes: "preprint note", tags: ["arxiv"], savedAt: "2023-01-01T00:00:00Z" }),
    pub: paper({ key: "pub", url: "https://journal.org/1", doi: "10.1/x", tags: ["journal"], updatedAt: "2026-01-01T00:00:00Z" }),
    other: paper({ key: "other", title: "Unrelated", doi: "10.2/y" })
  };

  it("merges, removes the dropped record and keeps both URLs", () => {
    const r = mergeDuplicatePair(lib, "pre", "pub");
    assert.equal(r.ok, true);
    assert.equal(Object.keys(r.papers).length, 2);
    assert.equal(r.papers.pub, undefined);
    const p = r.papers.pre;
    assert.equal(p.url, "https://arxiv.org/abs/1");
    assert.deepEqual(p.altUrls, ["https://journal.org/1"]);
    assert.equal(p.doi, "10.1/x");
    assert.deepEqual(p.tags, ["arxiv", "journal"]);
    assert.equal(p.notes, "preprint note");
    // input untouched
    assert.ok(lib.pub);
  });

  it("refuses bad input", () => {
    assert.equal(mergeDuplicatePair(lib, "pre", "pre").ok, false);
    assert.equal(mergeDuplicatePair(lib, "pre", "missing").ok, false);
    assert.equal(mergeDuplicatePair(null, "a", "b").ok, false);
  });
});

describe("describeImportSummary", () => {
  it("lists counts", () => {
    assert.equal(
      describeImportSummary({ added: 2, merged: 1, unchanged: 5, skipped: 0, collectionsAdded: 1 }),
      "2 added, 1 merged, 5 unchanged, 1 collection added"
    );
  });
});

describe("normalizeTitleForMatch: accents", async () => {
  const { normalizeTitleForMatch } = await import("../src/common/library-merge.js");
  it("matches a title with and without its accents", () => {
    assert.equal(normalizeTitleForMatch("Pokémon: a study"), "pokemon a study");
    assert.equal(normalizeTitleForMatch("Gómez–Peña effects"), "gomez pena effects");
  });
});
