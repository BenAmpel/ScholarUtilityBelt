import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  tokenize,
  seriesMarkers,
  authorSurnames,
  cosine,
  buildIndex,
  similarTo,
  findDuplicatePairs,
  comparePapers,
  librarySignature,
  createSimilarityCache,
  runChunked
} from "../src/common/similarity.js";

const P = (key, title, extra = {}) => ({ key, title, authorsVenue: "A Smith, B Jones - Journal, 2020", year: 2020, ...extra });

describe("tokenize", () => {
  it("lowercases, folds accents, drops stop words and short tokens, and stems plurals", () => {
    assert.deepEqual(tokenize("The Effects of Naïve Models on Citations"), ["effect", "naive", "model", "citation"]);
    assert.deepEqual(tokenize("Policies"), ["policy"]);
    assert.deepEqual(tokenize(""), []);
    assert.deepEqual(tokenize(null), []);
  });
  it("keeps 3-digit numbers, drops 1-2 digit ones", () => {
    assert.deepEqual(tokenize("GPT 4 and 2024 models"), ["gpt", "2024", "model"]);
  });
});

describe("seriesMarkers and authorSurnames", () => {
  it("detects part numbers and roman numerals", () => {
    assert.deepEqual([...seriesMarkers("Learning to rank, Part II")], ["ii"]);
    assert.deepEqual([...seriesMarkers("Learning to rank: experiment 2")], ["2"]);
    assert.deepEqual([...seriesMarkers("Learning to rank")], []);
  });
  it("extracts surnames from a Scholar line", () => {
    assert.deepEqual(authorSurnames("A Smith, B Jones, C de la Vega - Journal of X, 2020 - Elsevier"), ["smith", "jones", "vega"]);
    assert.deepEqual(authorSurnames("Müller, J García… - arXiv, 2019"), ["muller", "garcia"]);
    assert.deepEqual(authorSurnames(""), []);
    assert.deepEqual(authorSurnames("et al. - X"), []);
  });
});

describe("cosine and similarTo", () => {
  const lib = [
    P("a", "Graph neural networks for molecule property prediction", { abstract: "We predict molecular properties with message passing neural networks." }),
    P("b", "Message passing networks for predicting molecular properties", { abstract: "Neural message passing predicts molecular properties." }),
    P("c", "A survey of medieval trade routes", { abstract: "Trade routes across the Mediterranean in the twelfth century." }),
    P("d", "Medieval trade in the Mediterranean", { abstract: "Commerce and trade routes of the twelfth century." })
  ];
  it("ranks related papers above unrelated ones", () => {
    const idx = buildIndex(lib);
    const near = similarTo(idx, "a", { limit: 3, minScore: 0 });
    assert.equal(near[0].key, "b");
    const near2 = similarTo(idx, "c", { limit: 3, minScore: 0 });
    assert.equal(near2[0].key, "d");
    assert.ok(near[0].score > 0.2 && near[0].score <= 1);
  });
  it("does not return the paper itself, respects limit, handles unknown keys", () => {
    const idx = buildIndex(lib);
    const r = similarTo(idx, "a", { limit: 1, minScore: 0 });
    assert.equal(r.length, 1);
    assert.notEqual(r[0].key, "a");
    assert.deepEqual(similarTo(idx, "missing"), []);
  });
  it("cosine of identical normalized vectors is about 1", () => {
    const idx = buildIndex([P("x", "quantum error correction codes")]);
    assert.ok(Math.abs(cosine(idx.titleVecs[0], idx.titleVecs[0]) - 1) < 1e-9);
    assert.equal(cosine(null, idx.titleVecs[0]), 0);
  });
  it("copes with an empty library and papers with no text", () => {
    const idx = buildIndex([]);
    assert.equal(idx.n, 0);
    const idx2 = buildIndex([{ key: "1" }, { key: "2", title: "" }]);
    assert.deepEqual(similarTo(idx2, "1"), []);
    assert.deepEqual(findDuplicatePairs(idx2), []);
  });
});

describe("findDuplicatePairs", () => {
  it("finds a preprint and its published version", () => {
    const lib = [
      P("pre", "Attention is all you need: a transformer architecture for translation", { authorsVenue: "A Vaswani, N Shazeer, N Parmar - arXiv preprint, 2017", year: 2017 }),
      P("pub", "Attention is all you need", { authorsVenue: "A Vaswani, N Shazeer, N Parmar, J Uszkoreit - NeurIPS, 2017", year: 2018 }),
      P("x", "Unrelated study of soil microbes", { authorsVenue: "Q Lee - Soil Biology, 2018", year: 2018 })
    ];
    const pairs = findDuplicatePairs(buildIndex(lib));
    assert.equal(pairs.length, 1);
    assert.deepEqual([pairs[0].a, pairs[0].b].sort(), ["pre", "pub"]);
    assert.ok(pairs[0].reasons.some((r) => /title match/.test(r)));
    assert.ok(pairs[0].reasons.some((r) => /vaswani/.test(r)));
  });

  it("does not pair the same title written by different authors", () => {
    const lib = [
      P("a", "Introduction to machine learning", { authorsVenue: "E Alpaydin - MIT Press, 2020" }),
      P("b", "Introduction to machine learning", { authorsVenue: "R Kumar, S Patel - Springer, 2020" })
    ];
    assert.deepEqual(findDuplicatePairs(buildIndex(lib)), []);
  });

  it("does not pair parts of a series", () => {
    const lib = [
      P("a", "Robust estimation of treatment effects, Part I", { authorsVenue: "A Smith, B Jones - J Stat, 2020" }),
      P("b", "Robust estimation of treatment effects, Part II", { authorsVenue: "A Smith, B Jones - J Stat, 2020" })
    ];
    assert.deepEqual(findDuplicatePairs(buildIndex(lib)), []);
  });

  it("does not pair papers more than three years apart", () => {
    const lib = [
      P("a", "Citation analysis with transformers", { year: 2015 }),
      P("b", "Citation analysis with transformers", { year: 2021 })
    ];
    assert.deepEqual(findDuplicatePairs(buildIndex(lib)), []);
  });

  it("finds exact duplicates and tolerates missing authors on one side", () => {
    const lib = [P("a", "Exactly the same title here", { authorsVenue: "" }), P("b", "Exactly the same title here")];
    const pairs = findDuplicatePairs(buildIndex(lib));
    assert.equal(pairs.length, 1);
    assert.match(pairs[0].reasons.join(" "), /not available/);
  });

  it("needs author overlap for a merely similar (not near-identical) title", () => {
    const base = "Deep learning approaches to predicting citation counts of academic papers";
    const reworded = "Predicting citation counts of academic papers with deep learning";
    const same = findDuplicatePairs(buildIndex([P("a", base), P("b", reworded)]));
    assert.equal(same.length, 1);
    const different = findDuplicatePairs(
      buildIndex([P("a", base), P("b", reworded, { authorsVenue: "X Wang, Y Chen - Elsevier, 2020" })])
    );
    assert.deepEqual(different, []);
  });

  it("comparePapers returns null for weak matches", () => {
    assert.equal(comparePapers(P("a", "alpha beta"), P("b", "gamma delta"), 0.1), null);
  });
});

describe("cache and chunking", () => {
  const alpha = (i) => String.fromCharCode(97 + Math.floor(i / 26)) + String.fromCharCode(97 + (i % 26));
  const makeLibrary = (n) => {
    const topics = ["graph", "protein", "market", "climate", "language", "vision", "policy", "network", "quantum", "survey"];
    const lib = [];
    for (let i = 0; i < n; i++) {
      const a = topics[i % topics.length];
      const b = topics[(i * 7 + 3) % topics.length];
      lib.push({
        key: `k${i}`,
        title: `${a} ${b} study number ${i} on ${topics[(i * 3) % 10]} dynamics case ${i % 97}`,
        authorsVenue: `Author${i % 300} Name${i % 50}, Other${i % 123} Person${i % 31} - Journal ${i % 17}, ${2000 + (i % 24)}`,
        year: 2000 + (i % 24),
        abstract: `This work examines ${a} and ${b} with ${topics[(i * 5) % 10]} methods across settings ${i}.`
      });
    }
    // plant 20 preprint/published pairs
    for (let i = 0; i < 20; i++) {
      lib.push({
        key: `pre${i}`,
        title: `Zorblatt${alpha(i)} calibration of widgetry under uncertainty`,
        authorsVenue: `Zed Quux${alpha(i)}, Yan Vorn${alpha(i)} - arXiv, 2019`,
        year: 2019
      });
      lib.push({
        key: `pub${i}`,
        title: `Zorblatt${alpha(i)} calibration of widgetry under uncertainty: a revised account`,
        authorsVenue: `Z Quux${alpha(i)}, Y Vorn${alpha(i)} - Journal of Widgets, 2020`,
        year: 2020
      });
    }
    return lib;
  };

  it("handles a 2,000-paper library in chunks without a long synchronous block", async () => {
    const lib = makeLibrary(2000);
    let yields = 0;
    const cache = createSimilarityCache({ yieldFn: async () => { yields++; }, chunkSize: 100 });
    const t0 = Date.now();
    const pairs = await cache.getPairs(lib);
    const elapsed = Date.now() - t0;
    assert.ok(yields >= 20, `expected many yields, got ${yields}`);
    assert.ok(elapsed < 8000, `took ${elapsed} ms`);
    const planted = pairs.filter((p) => p.a.startsWith("pre") && p.b.startsWith("pub") && p.a.slice(3) === p.b.slice(3));
    assert.equal(planted.length, 20);
    const idx = await cache.getIndex(lib);
    const t1 = Date.now();
    const sim = similarTo(idx, "k5", { limit: 10, minScore: 0 });
    assert.ok(Date.now() - t1 < 200);
    assert.equal(sim.length, 10);
  });

  it("builds once per library version and rebuilds when the library changes", async () => {
    const lib = makeLibrary(50);
    const cache = createSimilarityCache({ yieldFn: async () => {} });
    await Promise.all([cache.getIndex(lib), cache.getIndex(lib), cache.getPairs(lib)]);
    await cache.getPairs(lib);
    assert.equal(cache.stats.indexBuilds, 1);
    assert.equal(cache.stats.pairBuilds, 1);
    const changed = lib.map((p, i) => (i === 0 ? { ...p, title: "A new title" } : p));
    await cache.getIndex(changed);
    assert.equal(cache.stats.indexBuilds, 2);
    cache.invalidate();
    await cache.getIndex(changed);
    assert.equal(cache.stats.indexBuilds, 3);
  });

  it("librarySignature changes with content and ignores order of identical content", () => {
    const a = [{ key: "1", title: "x" }];
    assert.equal(librarySignature(a), librarySignature([{ key: "1", title: "x" }]));
    assert.notEqual(librarySignature(a), librarySignature([{ key: "1", title: "y" }]));
    assert.notEqual(librarySignature(a), librarySignature([]));
  });

  it("runChunked reports progress and awaits yields", async () => {
    function* gen() {
      yield { phase: "x", done: 1, total: 2 };
      yield { phase: "x", done: 2, total: 2 };
      return "result";
    }
    const seen = [];
    let waits = 0;
    const r = await runChunked(gen(), async () => { waits++; }, (p) => seen.push(p.done));
    assert.equal(r, "result");
    assert.deepEqual(seen, [1, 2]);
    assert.equal(waits, 2);
  });
});
