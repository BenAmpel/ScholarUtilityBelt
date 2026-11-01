import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { titlesMatch, pickOpenAlexWork, summarizeFwci } from "../src/common/openalex-match.js";

const work = (title, year, authors = []) => ({
  display_name: title,
  publication_year: year,
  authorships: authors.map((n) => ({ author: { display_name: n } })),
});

describe("OpenAlex title match", () => {
  it("a work with the same year and author but a different title is not the paper", () => {
    const other = work("A systematic review of deep learning for medical image segmentation in radiology", 2021, ["Jane Doe"]);
    assert.equal(pickOpenAlexWork([other], "Deep learning for phishing detection in email: a systematic review", 2021, "Doe"), null);
  });
  it("the same title with different punctuation and case matches", () => {
    assert.equal(titlesMatch("Phishing Detection: A Survey", "phishing detection - a survey"), true);
  });
  it("a title Scholar cut short matches the full title", () => {
    assert.equal(titlesMatch("Deep learning for phishing detection in email messages", "Deep learning for phishing detection in email messages: a large-scale evaluation"), true);
  });
  it("a short title is not a prefix match for a longer one", () => {
    assert.equal(titlesMatch("Deep learning", "Deep learning for phishing detection"), false);
  });
  it("year and author choose between two works with matching titles", () => {
    const a = work("Phishing detection: a survey", 2015, ["Someone Else"]);
    const b = work("Phishing detection: a survey", 2020, ["Ben Ampel"]);
    assert.equal(pickOpenAlexWork([a, b], "Phishing detection: a survey", 2020, "Ampel"), b);
  });
});

describe("FWCI summary", () => {
  it("uses OpenAlex's fwci, not the citation percentile", () => {
    const s = summarizeFwci([{ fwci: 122.4, percentile: 99 }, { fwci: 3, percentile: 95 }, { fwci: 0.6, percentile: 50 }]);
    assert.equal(s.fwci, 42);
    assert.equal(s.fwciMedian, 3);
    assert.equal(s.fwciArticles, 3);
    assert.equal(s.top10Share, 67);
  });
  it("needs three papers with an fwci", () => {
    const s = summarizeFwci([{ fwci: 2, percentile: 90 }, { fwci: null, percentile: 80 }, { percentile: 70 }]);
    assert.equal(s.fwci, null);
    assert.equal(s.top10Share, 33);
  });
});
