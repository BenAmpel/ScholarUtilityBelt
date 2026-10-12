import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { toRecord } from "../src/common/export-model.js";
import { baseCiteKey, assignCitekeys, buildBib, buildCiteList, stripLatexComments, extractCiteKeys, checkCitations } from "../src/common/latex-pack.js";

const paper = (key, title, authorsVenue, year, extra = {}) => ({ key, title, authorsVenue, year, ...extra });

describe("baseCiteKey", () => {
  it("uses family, year and first meaningful title word", () => {
    assert.equal(baseCiteKey(toRecord(paper("1", "The attention mechanism revisited", "A Vaswani", 2017))), "vaswani2017attention");
  });
  it("handles accents, missing authors and missing years", () => {
    assert.equal(baseCiteKey(toRecord(paper("1", "Über Größe", "J Müller", 2020))), "muller2020uber");
    assert.equal(baseCiteKey(toRecord(paper("1", "On the", "", ""))), "anonnd");
    assert.equal(baseCiteKey(toRecord(paper("1", "", "Plato", 2001))), "plato2001");
  });
});

describe("assignCitekeys", () => {
  const a = paper("a", "Attention is all", "A Vaswani", 2017);
  const b = paper("b", "Attention is key", "B Vaswani", 2017);
  it("makes unique keys with letter suffixes", () => {
    const { keys, added } = assignCitekeys([b, a], toRecord);
    assert.equal(keys.get("a"), "vaswani2017attention");
    assert.equal(keys.get("b"), "vaswani2017attentiona");
    assert.equal(added.length, 2);
  });
  it("never changes an existing key and avoids it", () => {
    const first = assignCitekeys([a], toRecord);
    const saved = { ...a, citekey: first.keys.get("a") };
    const again = assignCitekeys([saved, b], toRecord);
    assert.equal(again.keys.get("a"), "vaswani2017attention");
    assert.deepEqual(again.added.map((x) => x.key), ["b"]);
    assert.equal(again.keys.get("b"), "vaswani2017attentiona");
    // even if the title is later edited
    const edited = { ...saved, title: "Something else entirely" };
    assert.equal(assignCitekeys([edited], toRecord).keys.get("a"), "vaswani2017attention");
    assert.equal(assignCitekeys([edited], toRecord).added.length, 0);
  });
  it("avoids keys held by papers outside the exported scope, case-insensitively", () => {
    const { keys } = assignCitekeys([a], toRecord, ["Vaswani2017Attention"]);
    assert.equal(keys.get("a"), "vaswani2017attentiona");
  });
  it("is independent of input order", () => {
    const x = assignCitekeys([a, b], toRecord).keys;
    const y = assignCitekeys([b, a], toRecord).keys;
    assert.deepEqual([...x].sort(), [...y].sort());
  });
});

describe("bib and cite list", () => {
  const recs = [
    { ...toRecord(paper("b", "Second & more", "B Jones, C van Lee", 2019, { doi: "10.1/x", tags: ["t"], notes: "PRIVATE" })), citekey: "jones2019second" },
    { ...toRecord(paper("a", "First", "A Smith", 2018)), citekey: "smith2018first" },
    toRecord(paper("c", "No key", "Z Zed", 2000)),
  ];
  it("writes sorted entries with the stored keys and no private notes", () => {
    const bib = buildBib(recs);
    assert.ok(bib.indexOf("@misc{jones2019second") < bib.indexOf("@misc{smith2018first"));
    assert.match(bib, /author=\{Jones, B and van Lee, C\}/);
    assert.match(bib, /title=\{Second \\& more\}/);
    assert.ok(!bib.includes("PRIVATE"));
    assert.ok(!bib.includes("No key"));
    assert.equal(buildBib([]), "");
  });
  it("lists keys", () => {
    const list = buildCiteList(recs);
    assert.match(list, /^\\cite\{jones2019second,smith2018first\}\n/);
    assert.match(list, /% \\cite\{smith2018first\}  Smith \(2018\)\. First/);
    assert.equal(buildCiteList([]), "");
  });
});

describe("stripLatexComments", () => {
  it("drops comments but keeps escaped percent signs", () => {
    assert.equal(stripLatexComments("50\\% of x % \\cite{no}\nnext"), "50\\% of x \nnext");
    assert.equal(stripLatexComments("a\n\\begin{comment}\n\\cite{x}\n\\end{comment}\nb"), "a\n\nb");
  });
});

describe("extractCiteKeys", () => {
  it("finds the supported commands, multiple keys and optional arguments", () => {
    const tex = String.raw`Text \cite{a,b} and \citep[see][p.~3]{c} then \citet*{d} and \parencite[12]{e, f}
\textcite{g}\autocite{h}\footcite{i} \Citep{j} \citealp{k} \nocite{l}`;
    assert.deepEqual(extractCiteKeys(tex).keys, "abcdefghijkl".split(""));
  });
  it("ignores commented lines and trailing comments", () => {
    const tex = "% \\cite{hidden}\nreal \\cite{shown} % \\cite{alsohidden}\n50\\% \\cite{after}";
    assert.deepEqual(extractCiteKeys(tex).keys, ["shown", "after"]);
  });
  it("tolerates whitespace, newlines inside the list and duplicates", () => {
    assert.deepEqual(extractCiteKeys("\\cite{ a ,\n b , a,, }").keys, ["a", "b"]);
  });
  it("does not treat \\citation-like or unrelated commands as cites", () => {
    assert.deepEqual(extractCiteKeys("\\ref{x} \\label{y} \\includegraphics{z}").keys, []);
  });
  it("reads a plain key list", () => {
    assert.deepEqual(extractCiteKeys("a, b;c\n% skip\nd").keys, ["a", "b", "c", "d"]);
    assert.deepEqual(extractCiteKeys("").keys, []);
  });
  it("flags \\nocite{*}", () => {
    const r = extractCiteKeys("\\nocite{*} \\cite{a}");
    assert.equal(r.wildcard, true);
    assert.deepEqual(r.keys, ["a"]);
  });
});

describe("checkCitations", () => {
  const recs = [
    { ...toRecord(paper("a", "A", "A One", 2001)), citekey: "one2001a" },
    { ...toRecord(paper("b", "B", "B Two", 2002)), citekey: "two2002b" },
    { ...toRecord(paper("c", "C", "C Three", 2003)), citekey: "three2003c" },
  ];
  it("reports uncited saved papers and keys with no saved entry", () => {
    const r = checkCitations("\\cite{one2001a, nope2020x} \\citep{Two2002B}", recs);
    assert.deepEqual(r.missing, [{ key: "nope2020x", suggestion: "" }, { key: "Two2002B", suggestion: "two2002b" }]);
    assert.deepEqual(r.uncited.map((x) => x.citekey), ["two2002b", "three2003c"]);
  });
  it("reports nothing when everything matches", () => {
    const r = checkCitations("\\cite{one2001a,two2002b,three2003c}", recs);
    assert.equal(r.missing.length, 0);
    assert.equal(r.uncited.length, 0);
  });
  it("treats \\nocite{*} as citing everything", () => {
    assert.equal(checkCitations("\\nocite{*}", recs).uncited.length, 0);
  });
  it("ignores records without a key", () => {
    assert.equal(checkCitations("", [toRecord(paper("z", "Z", "Z Z", 2000))]).uncited.length, 0);
  });
});
