import { describe, it } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const sw = fs.readFileSync(new URL("../src/sw.js", import.meta.url), "utf8");
const content = fs.readFileSync(new URL("../src/content/content.js", import.meta.url), "utf8");

describe("session storage for content scripts", () => {
  it("the worker opens chrome.storage.session to content scripts on every start", () => {
    assert.match(sw, /chrome\.storage\.session\?\.setAccessLevel\?\.\(\{ accessLevel: "TRUSTED_AND_UNTRUSTED_CONTEXTS" \}\)/);
    // top level, not inside a listener, so it runs each time the worker wakes
    const idx = sw.indexOf("setAccessLevel");
    assert.ok(idx > 0 && idx < sw.indexOf("chrome.runtime.onMessage.addListener"));
  });
  it("the quality index is not offered to session storage (it exceeds the 10 MB quota)", () => {
    assert.doesNotMatch(content, /SESSION_QINDEX_KEY|serializeQualityIndex|deserializeQualityIndex/);
  });
});
