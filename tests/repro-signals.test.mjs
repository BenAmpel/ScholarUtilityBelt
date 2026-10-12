import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { detectReproSignals, reproChips, SIGNALS_NOTE, CODE_HOST_REGEXES, DATA_HOST_REGEXES } = await import("../src/common/repro-signals.js");

const link = (href, role = "other") => ({ href, role });

describe("code links", () => {
  it("finds GitHub, GitLab, Bitbucket and Hugging Face in links", () => {
    const r = detectReproSignals({
      links: [
        link("https://github.com/acme/project"),
        link("https://gitlab.com/group/repo"),
        link("https://bitbucket.org/team/repo"),
        link("https://huggingface.co/acme/model"),
      ],
    });
    assert.deepEqual(r.code.map((c) => c.kind), ["github", "gitlab", "bitbucket", "huggingface"]);
  });
  it("finds a repository named in the snippet text", () => {
    const r = detectReproSignals({ text: "Code is available at github.com/acme/project and a demo at huggingface.co/spaces/acme/demo." });
    assert.deepEqual(r.code.map((c) => c.kind), ["github", "huggingface"]);
  });
  it("does not count a bare host or a user page as a repository", () => {
    assert.equal(detectReproSignals({ links: [link("https://github.com/"), link("https://github.com/acme")] }).code.length, 0);
  });
  it("ignores look-alike hosts", () => {
    const r = detectReproSignals({ text: "See notgithub.com/a/b and github.com.evil.example/a/b", links: [link("https://example.com/github.com/a/b")] });
    assert.equal(r.code.length, 0);
  });
  it("reports each host once", () => {
    const r = detectReproSignals({ links: [link("https://github.com/a/b"), link("https://github.com/c/d")] });
    assert.equal(r.code.length, 1);
  });
});

describe("data and materials links", () => {
  it("finds OSF, Zenodo, figshare, Dryad, Dataverse and Kaggle", () => {
    const r = detectReproSignals({
      links: [
        link("https://osf.io/abc12/"),
        link("https://zenodo.org/records/123456"),
        link("https://figshare.com/articles/dataset/x/1"),
        link("https://datadryad.org/stash/dataset/doi:10.5061/dryad.abc"),
        link("https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/ABC"),
        link("https://www.kaggle.com/datasets/acme/data"),
      ],
    });
    assert.deepEqual(r.data.map((d) => d.kind), ["osf", "zenodo", "figshare", "dryad", "dataverse", "kaggle"]);
  });
  it("ignores the result's own title link and its [PDF] column", () => {
    const r = detectReproSignals({
      links: [link("https://zenodo.org/records/1", "title"), link("https://zenodo.org/records/1/files/paper.pdf", "file")],
    });
    assert.equal(r.data.length, 0);
    assert.equal(r.code.length, 0);
  });
  it("does not count a bare host", () => {
    assert.equal(detectReproSignals({ links: [link("https://zenodo.org/")] }).data.length, 0);
  });
});

describe("preregistration", () => {
  it("finds AsPredicted and OSF Registries links", () => {
    const r = detectReproSignals({ links: [link("https://aspredicted.org/abc.pdf"), link("https://osf.io/registries/osf/xyz")] });
    assert.deepEqual(r.prereg.map((p) => p.kind), ["aspredicted", "osf-registry"]);
    assert.equal(r.data.length, 0);
  });
  it("finds NCT and PROSPERO ids in text", () => {
    const r = detectReproSignals({ text: "Registered as NCT01234567 and PROSPERO CRD42020123456." });
    assert.deepEqual(r.prereg.map((p) => p.label), ["ClinicalTrials.gov NCT01234567", "PROSPERO CRD42020123456"]);
  });
  it("finds a ClinicalTrials.gov link", () => {
    const r = detectReproSignals({ links: [link("https://clinicaltrials.gov/study/NCT07654321")] });
    assert.equal(r.prereg[0].label, "ClinicalTrials.gov NCT07654321");
  });
  it("notes a plain mention of preregistration", () => {
    assert.equal(detectReproSignals({ text: "The hypotheses and analysis plan were preregistered before data collection." }).prereg[0].kind, "mention");
    assert.equal(detectReproSignals({ text: "This registered report tests two accounts of memory." }).prereg[0].kind, "mention");
  });
  it("does not count a negated mention", () => {
    assert.equal(detectReproSignals({ text: "The study was not preregistered and results are exploratory." }).prereg.length, 0);
  });
  it("does not duplicate a mention when a registry id is present", () => {
    const r = detectReproSignals({ text: "Preregistered (NCT01234567)." });
    assert.equal(r.prereg.length, 1);
  });
  it("ignores malformed ids", () => {
    assert.equal(detectReproSignals({ text: "NCT123 and CRD42 and nct01234567" }).prereg.length, 0);
  });
});

describe("dataset DOIs", () => {
  it("finds repository-prefixed DOIs in text and links", () => {
    const r = detectReproSignals({
      text: "Data: https://doi.org/10.5281/zenodo.1234567, 10.6084/m9.figshare.123.",
      links: [link("https://doi.org/10.5061/dryad.abc123")],
    });
    assert.deepEqual(r.datasetDois.map((d) => d.label), ["Zenodo DOI", "figshare DOI", "Dryad DOI"]);
  });
  it("ignores ordinary article DOIs", () => {
    assert.equal(detectReproSignals({ text: "doi:10.1016/j.cell.2020.01.001 and 10.1145/3292500.3330701" }).datasetDois.length, 0);
  });
  it("never reports the result's own DOI", () => {
    const r = detectReproSignals({ text: "10.5281/zenodo.42", ownDoi: "https://doi.org/10.5281/ZENODO.42" });
    assert.equal(r.datasetDois.length, 0);
  });
  it("strips trailing punctuation", () => {
    assert.equal(detectReproSignals({ text: "Available (10.5281/zenodo.99)." }).datasetDois[0].doi, "10.5281/zenodo.99");
  });
});

describe("chips and wording", () => {
  it("says 'signals found' and never 'reproducible' as a claim", () => {
    const r = detectReproSignals({ links: [link("https://github.com/a/b"), link("https://zenodo.org/records/1")], text: "Preregistered. 10.5281/zenodo.5" });
    const chips = reproChips(r);
    assert.ok(chips.length >= 4);
    for (const c of chips) {
      assert.ok(c.tip.includes(SIGNALS_NOTE));
      assert.doesNotMatch(c.label, /reproducible/i);
    }
    assert.match(SIGNALS_NOTE, /does not show that the work is reproducible/);
  });
  it("returns nothing for empty or malformed input", () => {
    for (const v of [undefined, null, {}, { text: null, links: "x" }, { links: [null, {}, { href: 5 }, { href: "not a url" }] }]) {
      const r = detectReproSignals(v);
      assert.deepEqual([r.code, r.data, r.prereg, r.datasetDois], [[], [], [], []]);
    }
    assert.deepEqual(reproChips(null), []);
  });
});

describe("host regexes shared with detectArtifacts", () => {
  it("match the hosts and not look-alikes", () => {
    assert.ok(CODE_HOST_REGEXES.some((re) => re.test("see https://huggingface.co/acme/x")));
    assert.ok(!CODE_HOST_REGEXES.some((re) => re.test("nothuggingface.co")));
    assert.ok(DATA_HOST_REGEXES.some((re) => re.test("https://www.kaggle.com/datasets/x")));
    assert.ok(DATA_HOST_REGEXES.some((re) => re.test("https://datadryad.org/stash")));
  });
});
