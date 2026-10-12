import { describe, it } from "node:test";
import assert from "node:assert/strict";

// The workspace builds its DOM lazily on open(), so creating it with stub deps only checks
// that the module and its imports (including ../common/reference-import.js) load and wire up.
const { createReviewWorkspace } = await import("../src/content/review-workspace.js");

describe("createReviewWorkspace", () => {
  it("loads and exposes open()", () => {
    const noop = () => {};
    const ws = createReviewWorkspace({
      escapeHtml: String, csvEscape: String, extractDOIFromResult: noop, extractPaperFromResult: noop,
      fetchOpenAlexWorkById: noop, fetchOpenAlexWorkForPaper: noop, getBestPdfUrl: noop,
      getCachedAuthorCitationCount: noop, getCachedAuthorPaper: noop, getCitationCountFromResult: noop,
      getScholarSearchQuery: noop, getSnippetText: noop, hashString: String, normalizeDoi: String,
      normalizeTitleForMatch: String, scanResults: noop, downloadBlob: noop, getStorageMap: noop,
      setStorageMap: noop, CLS_BTN_SEC: "btn",
      getEntitlementStatus: async () => ({ purchased: false }), canUseFeature: () => false,
    });
    assert.equal(typeof ws.open, "function");
  });
});
