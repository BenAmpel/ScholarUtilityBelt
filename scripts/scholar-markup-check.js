// Scholar markup check: paste into the console of a normal Google Scholar results page (any country domain), or run it
// with a browser automation tool. It lists the parts of Scholar's page the extension reads and says which are missing.
// Why this and not a headless test: Scholar answers a headless browser with a captcha, so only a real browser session
// sees the real page. tests/scholar-markup.test.mjs keeps this list in step with the code and the offline fixtures.
(() => {
  const NEEDED = [
    { sel: "#gs_res_ccl_mid", what: "the results list (every results-page feature starts here)" },
    { sel: ".gs_r.gs_or", what: "one search result (badges, buttons and filters attach to each)", min: 5 },
    { sel: ".gs_ri", what: "the text part of a result" },
    { sel: ".gs_rt", what: "a result's title (title links, DOI lookup, reading queue)" },
    { sel: ".gs_a", what: "the author and venue line (journal badges come from the venue here)" },
    { sel: ".gs_rs", what: "the snippet (keyword highlights, method and claim cues)" },
    { sel: ".gs_fl", what: "the link row under a result (Cite, Cited by, export buttons)" },
    { sel: ".gs_or_ggsm", what: "the full-text link on the right (PDF renaming and open-access check)", optional: true },
    { sel: ".gs_ggs", what: "the full-text column on the right", optional: true },
    { sel: ".gs_or_aia", what: "author profile links in the byline (author metrics)", optional: true },
    { sel: "#gs_ab", what: "the bar above the results (extension toolbar placement)" },
  ];
  const rows = NEEDED.map((n) => {
    const count = document.querySelectorAll(n.sel).length;
    return { selector: n.sel, count, status: count >= (n.min || 1) ? "ok" : n.optional ? "absent (optional on some pages)" : "MISSING", needed_for: n.what };
  });
  const missing = rows.filter((r) => r.status === "MISSING");
  console.table(rows);
  const verdict = missing.length ? `Scholar markup changed: ${missing.map((r) => r.selector).join(", ")} not found` : "Scholar markup the extension needs is all present";
  console.log(verdict);
  return { verdict, missing: missing.map((r) => r.selector), counts: Object.fromEntries(rows.map((r) => [r.selector, r.count])) };
})();
