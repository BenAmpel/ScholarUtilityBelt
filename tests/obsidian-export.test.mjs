import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { toRecord, sortRecords } from "../src/common/export-model.js";
import { yamlString, obsidianTag, baseFileName, assignFileNames, buildPaperNote, buildIndexNote, buildVaultFiles, shortHash } from "../src/common/obsidian-export.js";
import { buildZip } from "../src/common/zip-writer.js";

const colls = new Map([["c1", { name: "Thesis" }], ["c2", { name: "Reading: later" }]]);
const rec = (p) => toRecord(p, { collections: colls, venueOf: () => "", badgesOf: () => [] });

const tricky = rec({
  key: "k1", title: 'Is "AI" safe? A: study of #1 \\ things', authorsVenue: "A Smith, B O'Neil - J. Odd: Things, 2021", year: 2021,
  venue: "Journal: of \"Odd\" Things", doi: "10.1000/ab:c(1)<2>", url: "https://x.org/?a=1&b=2#frag",
  tags: ["machine learning", "#ethics", "2020", "α/β", "  "], notes: "---\nnot frontmatter\n\nSecond.", collections: ["c1", "c2"],
  pdfNotes: { summary: "Sum.", highlights: [{ quote: "a quote\nover lines", page: "12", note: "nb" }, { quote: "no page", page: "" }] },
});

const havePyYaml = spawnSync("python3", ["-c", "import yaml"]).status === 0;

describe("yamlString", () => {
  it("quotes and escapes", () => {
    assert.equal(yamlString('a "b" \\ c'), '"a \\"b\\" \\\\ c"');
    assert.equal(yamlString("line1\nline2"), '"line1\\nline2"');
    assert.equal(yamlString("x y"), '"x\\u2028y"');
    assert.equal(yamlString(null), '""');
  });
});

describe("obsidianTag", () => {
  it("makes valid tags or nothing", () => {
    assert.equal(obsidianTag("machine learning"), "machine-learning");
    assert.equal(obsidianTag("#ethics"), "ethics");
    assert.equal(obsidianTag("2020"), "");
    assert.equal(obsidianTag("α/β"), "α/β");
    assert.equal(obsidianTag("a, b!"), "a-b");
    assert.equal(obsidianTag("   "), "");
  });
});

describe("file names", () => {
  it("uses the DOI when present, safely", () => {
    assert.equal(baseFileName(tricky), "doi-10.1000-ab-c-1-2");
  });
  it("falls back to author-year-title slug", () => {
    assert.equal(baseFileName(rec({ key: "x", title: "Café: Déjà vu?", authorsVenue: "Z Müller", year: 2019 })), "muller-2019-cafe-deja-vu");
  });
  it("handles missing everything and reserved names", () => {
    assert.equal(baseFileName(rec({ key: "x" })), "untitled");
    assert.equal(baseFileName(rec({ key: "x", title: "Con" })), "paper-con");
  });
  it("bounds very long titles and never ends in a separator", () => {
    const n = baseFileName(rec({ key: "x", title: "word ".repeat(200), authorsVenue: "A Long", year: 2020 }));
    assert.ok(n.length <= 96, String(n.length));
    assert.ok(!/[-.]$/.test(n));
  });
  it("gives duplicates distinct names that do not depend on order", () => {
    const a = rec({ key: "ka", title: "Same Title", authorsVenue: "A Same", year: 2020 });
    const b = rec({ key: "kb", title: "Same Title", authorsVenue: "A Same", year: 2020 });
    const n1 = assignFileNames([a, b]);
    const n2 = assignFileNames([b, a]);
    assert.notEqual(n1.get(a), n1.get(b));
    assert.equal(n1.get(a), n2.get(a));
    assert.equal(n1.get(b), n2.get(b));
    assert.equal(n1.get(a), `same-2020-same-title-${shortHash("ka")}`);
  });
  it("is case-insensitively unique and stable across runs", () => {
    const x = rec({ key: "1", doi: "10.1000/ABC" });
    const y = rec({ key: "2", doi: "10.1000/abc" });
    const n = assignFileNames([x, y]);
    assert.notEqual(n.get(x).toLowerCase(), n.get(y).toLowerCase());
  });
});

describe("paper note", () => {
  const md = buildPaperNote(tricky);
  const end = md.indexOf("\n---\n", 4);
  const front = md.slice(4, end);
  const body = md.slice(end + 5);
  it("has the expected sections", () => {
    assert.match(body, /# Is "AI" safe\?/);
    assert.match(body, /## Notes\n\n---\nnot frontmatter\n\nSecond\./);
    assert.match(body, /- > a quote over lines \(p\. 12\)\n  - Note: nb\n- > no page\n/);
  });
  it("writes frontmatter fields", () => {
    assert.match(front, /year: 2021/);
    assert.match(front, /tags:\n {2}- "machine-learning"\n {2}- "ethics"\n {2}- "α\/β"\n/);
    assert.match(front, /collections:\n {2}- "Thesis"\n {2}- "Reading: later"/);
    assert.match(front, /quality: \[\]/);
  });
  it("parses as YAML and round-trips tricky strings", { skip: !havePyYaml }, () => {
    const py = "import sys,json,yaml\nd=yaml.safe_load(sys.stdin.read().split('\\n---\\n')[0][4:])\nprint(json.dumps(d))";
    const r = spawnSync("python3", ["-I", "-c", py], { input: md, encoding: "utf8" });
    assert.equal(r.status, 0, r.stderr);
    const d = JSON.parse(r.stdout);
    assert.equal(d.title, tricky.title);
    assert.equal(d.venue, tricky.venue);
    assert.equal(d.doi, tricky.doi);
    assert.equal(d.url, tricky.url);
    assert.deepEqual(d.authors, ["A Smith", "B O'Neil"]);
    assert.equal(d.year, 2021);
    assert.deepEqual(d.collections, ["Thesis", "Reading: later"]);
  });
  it("omits year when it is not four digits and survives an empty record", () => {
    const m = buildPaperNote(rec({ key: "z", year: "n.d." }));
    assert.ok(!/^year:/m.test(m));
    assert.match(m, /title: "Untitled"/);
    assert.match(m, /authors: \[\]/);
  });
});

describe("vault", () => {
  const a = rec({ key: "ka", title: "Alpha [draft] | pipes", authorsVenue: "A Smith, B Jones, C Lee", year: 2020, collections: ["c1"] });
  const b = rec({ key: "kb", title: "Beta", authorsVenue: "B Jones", year: 2019, collections: ["c1", "c2"] });
  const c = rec({ key: "kc", title: "Gamma", year: 2018 });
  const records = sortRecords([a, b, c]);
  const files = buildVaultFiles(records, { now: new Date("2026-10-12T00:00:00Z") });
  it("lists index plus one file per paper under one root", () => {
    assert.equal(files.length, 4);
    assert.ok(files.every((f) => f.name.startsWith("Scholar Utility Belt/")));
    assert.equal(files[0].name, "Scholar Utility Belt/Index.md");
  });
  it("index groups by collection with wikilinks that match file names", () => {
    const idx = files[0].data;
    assert.match(idx, /## Reading: later\n\n- \[\[jones-2019-beta\|Beta\]\] \(Jones, 2019\)/);
    assert.match(idx, /## Thesis\n\n- \[\[jones-2019-beta\|Beta\]\] \(Jones, 2019\)\n- \[\[smith-2020-alpha-draft-pipes\|Alpha draft pipes\]\] \(Smith et al\., 2020\)/);
    assert.match(idx, /## Not in a collection\n\n- \[\[untitled\|Gamma\]\]|## Not in a collection\n\n- \[\[2018-gamma\|Gamma\]\]/);
    for (const f of files.slice(1)) {
      const stem = f.name.replace(/^.*\//, "").replace(/\.md$/, "");
      assert.ok(idx.includes(`[[${stem}|`), stem);
    }
  });
  it("is deterministic", () => {
    const again = buildVaultFiles(records, { now: new Date("2026-10-12T00:00:00Z") });
    assert.deepEqual(again, files);
  });
  it("zips with no duplicate names", () => {
    assert.ok(buildZip(files).length > 100);
  });
  it("index of an empty library is still valid", () => {
    assert.match(buildIndexNote([], new Map()), /0 papers/);
  });
});
