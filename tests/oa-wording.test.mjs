import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { OA_FOUND_TEXT } = await import("../src/common/scholar-links.js");
const content = readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");

describe("open-access wording matches the badge", () => {
  it("the filter checkbox label and title use the badge wording", () => {
    assert.ok(content.includes(`oaLabel.appendChild(document.createTextNode(" ${OA_FOUND_TEXT}"))`));
    assert.ok(content.includes(`oaCheck.title = "${OA_FOUND_TEXT} (Unpaywall)"`));
  });
  it("the PDF link labels shown in the review workspace use the badge wording", () => {
    assert.ok(content.includes(`label: "${OA_FOUND_TEXT} (PDF)"`));
    assert.ok(content.includes(`label: "${OA_FOUND_TEXT}" }`));
  });
  it("no bare OA label is left in a user-facing string", () => {
    assert.ok(!/label: "OA( PDF)?"/.test(content));
    assert.ok(!/createTextNode\(" OA"\)/.test(content));
    assert.ok(!/"PMC OA"/.test(content));
  });
});
