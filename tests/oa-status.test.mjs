import { describe, it } from "node:test";
import assert from "node:assert/strict";

const { unpaywallBadge, OA_FOUND_TEXT, OA_NONE_TEXT } = await import("../src/common/scholar-links.js");

const NONE = "No free copy found (checked Unpaywall)";

describe("unpaywallBadge", () => {
  it("found: says Free legal copy and links to the PDF, else the landing page", () => {
    assert.equal(OA_FOUND_TEXT, "Free legal copy");
    const pdf = unpaywallBadge({ ts: 1, data: { isOa: true, oaStatus: "green", hostType: "repository", bestOaUrl: "https://repo.example.org/item/1", bestOaUrlPdf: "https://repo.example.org/item/1.pdf" } });
    assert.equal(pdf.state, "found");
    assert.equal(pdf.text, "Free legal copy");
    assert.equal(pdf.href, "https://repo.example.org/item/1.pdf");
    assert.match(pdf.title, /GREEN/);
    const page = unpaywallBadge({ ts: 1, data: { isOa: true, bestOaUrl: "https://repo.example.org/item/2", bestOaUrlPdf: "" } });
    assert.equal(page.href, "https://repo.example.org/item/2");
  });
  it("found without a usable URL: still the label, but no link", () => {
    const b = unpaywallBadge({ ts: 1, data: { isOa: true, bestOaUrl: "javascript:alert(1)", bestOaUrlPdf: "" } });
    assert.equal(b.text, "Free legal copy");
    assert.equal(b.href, "");
  });
  it("checked and none: says No free copy found (checked Unpaywall), unlinked", () => {
    assert.equal(OA_NONE_TEXT, NONE);
    const b = unpaywallBadge({ ts: 1, data: { isOa: false, oaStatus: "closed", bestOaUrl: "", bestOaUrlPdf: "" } });
    assert.equal(b.state, "none");
    assert.equal(b.text, NONE);
    assert.equal(b.href, "");
  });
  it("Unpaywall has no record of the DOI (404, stored as data null without err) counts as checked and none", () => {
    assert.equal(unpaywallBadge({ ts: 1, data: null }).text, NONE);
  });
  it("a failed lookup is never shown as no free copy", () => {
    assert.equal(unpaywallBadge({ ts: 1, data: null, err: 1 }), null);
    assert.equal(unpaywallBadge({ ts: 1, data: { isOa: false }, err: 1 }), null);
  });
  it("not checked yet or no DOI (no entry) shows no badge", () => {
    assert.equal(unpaywallBadge(null), null);
    assert.equal(unpaywallBadge(undefined), null);
    assert.equal(unpaywallBadge({ ts: 1, data: { oaStatus: "" } }), null);
    assert.equal(unpaywallBadge({ ts: 1, data: "x" }), null);
  });
});
