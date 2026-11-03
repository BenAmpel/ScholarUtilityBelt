import { describe, it } from "node:test";
import assert from "node:assert/strict";

const {
  getBibAuthorField, splitBibAuthors, normalizeSurname, bibAuthorSurname,
  checkBibAuthorsAgainstCrossref, formatCrossrefAuthors, fixBibAuthors,
} = await import("../src/common/bibtex.js");

// Fixtures in the shape Scholar's "Import into BibTeX" link and api.crossref.org/works/{doi} return.
const SCHOLAR_SHORT = `@article{ampel2024large,
  title={Large language models for conducting advanced text Analytics Information Systems Research},
  author={Ampel, Benjamin and Yang, Chi-Heng and Hu, James and Chen, H},
  journal={ACM Transactions on Management Information Systems},
  year={2024},
  publisher={ACM New York, NY}
}`;
const SCHOLAR_ONE_LETTER = `@article{muller2023study,
  title={A study},
  author={M{\\"u}ller, Anna and L and Garc{\\'\\i}a, Jos{\\'e}},
  year={2023}
}`;
const CROSSREF_FULL = [
  { given: "Benjamin", family: "Ampel", sequence: "first" },
  { given: "Chi-Heng", family: "Yang", sequence: "additional" },
  { given: "James", family: "Hu", sequence: "additional" },
  { given: "Hsinchun", family: "Chen", sequence: "additional" },
  { given: "Sagar", family: "Samtani", sequence: "additional" },
];
const CROSSREF_ACCENTED = [
  { given: "Anna", family: "Müller" },
  { given: "Wei", family: "Liu" },
  { given: "José", family: "García" },
];

describe("author field parsing", () => {
  it("reads a braced author field and splits it on 'and' outside braces", () => {
    const f = getBibAuthorField(SCHOLAR_SHORT);
    assert.equal(f.value, "Ampel, Benjamin and Yang, Chi-Heng and Hu, James and Chen, H");
    assert.deepEqual(splitBibAuthors("{Barnes and Noble} and Smith, J and others"), ["{Barnes and Noble}", "Smith, J"]);
  });
  it("reads a quoted author field", () => {
    assert.equal(getBibAuthorField('@misc{k, author = "Doe, Jane and Roe, R", year=2020}').value, "Doe, Jane and Roe, R");
  });
  it("does not mistake a field ending in 'author' for the author field", () => {
    assert.equal(getBibAuthorField("@misc{k, coauthor={X}}"), null);
  });
  it("takes the surname from 'Last, First' and 'First Last'", () => {
    assert.equal(bibAuthorSurname("Chen, H"), "Chen");
    assert.equal(bibAuthorSurname("Hsinchun Chen"), "Chen");
    assert.equal(bibAuthorSurname("{World Health Organization}"), "World Health Organization");
  });
  it("normalizes accents, LaTeX accent commands and case", () => {
    assert.equal(normalizeSurname('M{\\"u}ller'), "muller");
    assert.equal(normalizeSurname("Müller"), "muller");
    assert.equal(normalizeSurname("Garc{\\'\\i}a"), "garcia");
    assert.equal(normalizeSurname("GARCÍA"), "garcia");
    assert.equal(normalizeSurname("M{\\o}ller"), normalizeSurname("Møller"));
  });
});

describe("checkBibAuthorsAgainstCrossref", () => {
  it("flags a dropped trailing author and the fix writes Crossref's full list", () => {
    const r = checkBibAuthorsAgainstCrossref(SCHOLAR_SHORT, CROSSREF_FULL);
    assert.equal(r.flagged, true);
    assert.equal(r.reason, "missing");
    assert.equal(r.missing, 1);
    assert.equal(r.bibCount, 4);
    assert.equal(r.crossrefCount, 5);
    const fixed = fixBibAuthors(SCHOLAR_SHORT, CROSSREF_FULL);
    assert.equal(getBibAuthorField(fixed).value,
      "Ampel, Benjamin and Yang, Chi-Heng and Hu, James and Chen, Hsinchun and Samtani, Sagar");
    // Everything else in the entry is unchanged.
    assert.equal(fixed.replace(/author=\{[^}]*\}/, ""), SCHOLAR_SHORT.replace(/author=\{[^}]*\}/, ""));
    assert.equal(checkBibAuthorsAgainstCrossref(fixed, CROSSREF_FULL).flagged, false);
  });
  it("flags a single-letter surname, accent and case insensitive, and fixes it", () => {
    const r = checkBibAuthorsAgainstCrossref(SCHOLAR_ONE_LETTER, CROSSREF_ACCENTED);
    assert.equal(r.flagged, true);
    assert.equal(r.reason, "truncated");
    assert.deepEqual(r.truncated, ["L"]);
    const fixed = fixBibAuthors(SCHOLAR_ONE_LETTER, CROSSREF_ACCENTED);
    assert.equal(getBibAuthorField(fixed).value, "Müller, Anna and Liu, Wei and García, José");
  });
  it("flags both problems at once", () => {
    const bib = "@article{k,\n  author={Ampel, Benjamin and Y and Hu, James},\n  year={2024}\n}";
    const r = checkBibAuthorsAgainstCrossref(bib, CROSSREF_FULL);
    assert.equal(r.reason, "missing-and-truncated");
    assert.equal(r.missing, 2);
  });
  it("shows no notice when the lists match (initials, accents, case and order of name parts differ)", () => {
    const bib = "@article{k,\n  author={Benjamin AMPEL and Yang, C-H and Hu, J and Chen, Hsinchun and Samtani, S},\n  year={2024}\n}";
    const r = checkBibAuthorsAgainstCrossref(bib, CROSSREF_FULL);
    assert.equal(r.flagged, false);
    assert.equal(r.reason, "ok");
    const accented = "@article{k, author={Muller, A and Liu, W and Garcia, J}}";
    assert.equal(checkBibAuthorsAgainstCrossref(accented, CROSSREF_ACCENTED).flagged, false);
  });
  it("does not flag when the BibTeX has more authors than Crossref", () => {
    const r = checkBibAuthorsAgainstCrossref(SCHOLAR_SHORT, CROSSREF_FULL.slice(0, 3));
    assert.equal(r.flagged, false);
  });
  it("does not flag when Crossref has no authors or the DOI is for another paper", () => {
    assert.equal(checkBibAuthorsAgainstCrossref(SCHOLAR_SHORT, []).flagged, false);
    assert.equal(checkBibAuthorsAgainstCrossref(SCHOLAR_SHORT, undefined).flagged, false);
    const other = [{ given: "A", family: "Smith" }, { given: "B", family: "Jones" }, { given: "C", family: "Brown" },
      { given: "D", family: "Taylor" }, { given: "E", family: "Wilson" }];
    const r = checkBibAuthorsAgainstCrossref(SCHOLAR_SHORT, other);
    assert.equal(r.flagged, false);
    assert.equal(r.reason, "different-authors");
  });
  it("flags an entry with no author field when Crossref has authors, and adds the field", () => {
    const bib = "@misc{k,\n  title={X},\n  year={2020}\n}";
    assert.equal(checkBibAuthorsAgainstCrossref(bib, CROSSREF_ACCENTED).flagged, true);
    const fixed = fixBibAuthors(bib, CROSSREF_ACCENTED);
    assert.equal(getBibAuthorField(fixed).value, "Müller, Anna and Liu, Wei and García, José");
    assert.match(fixed, /^@misc\{k,\n {2}author=/);
  });
});

describe("formatCrossrefAuthors", () => {
  it("writes Family, Given, braces group names and escapes LaTeX specials", () => {
    assert.equal(formatCrossrefAuthors([{ family: "Smith", given: "J" }, { name: "R&D Group" }, { family: "Plato" }]),
      "Smith, J and {R\\&D Group} and Plato");
  });
});
