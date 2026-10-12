import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { ALLOWED_API_HOSTS, isFetchableUrl, isScholarUrl, isAllowedApiUrl, isOwnContext } = await import("../src/common/url-policy.js");

describe("isScholarUrl", () => {
  it("accepts Scholar hosts in every country domain the manifest matches", () => {
    for (const host of ["scholar.google.com", "scholar.google.co.uk", "scholar.google.com.au", "scholar.google.ca", "scholar.google.co.in", "scholar.google.com.hk"]) {
      assert.equal(isScholarUrl(`https://${host}/scholar?q=x`), true, host);
    }
  });
  it("rejects lookalikes, other Google hosts, http and junk", () => {
    for (const url of ["https://scholar.google.evil.example/", "https://scholar.google.com.evil.example/", "https://evilscholar.google.com/", "https://scholar.google.com.evil.io/x",
      "https://google.com/", "http://scholar.google.com/", "scholar.google.com", "", null, "https://scholar.google/"]) {
      assert.equal(isScholarUrl(url), false, String(url));
    }
  });
});

describe("isAllowedApiUrl", () => {
  it("accepts the documented API hosts over https only", () => {
    assert.equal(isAllowedApiUrl("https://api.openalex.org/works?filter=x"), true);
    assert.equal(isAllowedApiUrl("https://api.crossref.org/works/10.1/x"), true);
    assert.equal(isAllowedApiUrl("http://api.openalex.org/works"), false);
  });
  it("rejects other hosts, lookalikes and credentials in the URL", () => {
    for (const url of ["https://www.catalyzex.com/api/code", "https://api.openalex.org.evil.example/x", "https://evil.example/?u=api.openalex.org",
      "https://user:pw@api.openalex.org/x", "https://example.org/", "ftp://api.openalex.org/", "not a url"]) {
      assert.equal(isAllowedApiUrl(url), false, url);
    }
  });
  it("matches the host permissions in the manifest template (so a new API host cannot be forgotten in one place)", () => {
    const manifest = JSON.parse(readFileSync(new URL("../manifest.template.json", import.meta.url), "utf8"));
    const hosts = manifest.host_permissions.filter((h) => h.startsWith("https://") && !h.includes("scholar.google")).map((h) => new URL(h.replace("/*", "/")).hostname).sort();
    assert.deepEqual([...ALLOWED_API_HOSTS].sort(), hosts);
  });
});

describe("isFetchableUrl", () => {
  it("accepts http and https only", () => {
    assert.equal(isFetchableUrl("https://publisher.example/paper"), true);
    assert.equal(isFetchableUrl("http://publisher.example/paper"), true);
    for (const url of ["file:///etc/passwd", "chrome://extensions", "javascript:alert(1)", "data:text/plain,hi", "", undefined]) assert.equal(isFetchableUrl(url), false, String(url));
  });
});

describe("isOwnContext", () => {
  const id = "abcdefghijklmnopabcdefghijklmnop";
  const origin = `chrome-extension://${id}/`;
  it("accepts the extension's own pages and its content scripts on Scholar", () => {
    assert.equal(isOwnContext({ id, url: `${origin}src/library/library.html` }, id, origin), true);
    assert.equal(isOwnContext({ id, url: "https://scholar.google.com/scholar?q=x", tab: { url: "https://scholar.google.com/scholar?q=x" } }, id, origin), true);
  });
  it("rejects another extension, a missing id, and a content script on any other site", () => {
    assert.equal(isOwnContext({ id: "other", url: `${origin}x` }, id, origin), false);
    assert.equal(isOwnContext({ url: `${origin}x` }, id, origin), false);
    assert.equal(isOwnContext({ id, url: "https://evil.example/", tab: { url: "https://evil.example/" } }, id, origin), false);
    assert.equal(isOwnContext(undefined, id, origin), false);
    assert.equal(isOwnContext({ id }, id, origin), false);
  });
});
