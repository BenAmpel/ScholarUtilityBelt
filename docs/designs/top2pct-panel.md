# Top-2% scientist panel on Google Scholar profiles

*   **Author:** Benjamin Ampel
*   **Status:** Reviewed; PR A built
*   **Date:** 2026-11-04 (review resolutions 2026-11-05)

--------------------------------------------------------------------------------

## Overview

Scholar Utility Belt (Chrome, Firefox, Edge; MV3) gets a new free-tier panel on Google Scholar profile pages. It
shows whether the profile's owner appears in the Stanford/Elsevier "Updated science-wide author databases of
standardized citation indicators", v9 (Ioannidis et al., DOI 10.17632/btchxktzyw.9, CC BY-NC 3.0, Scopus snapshot
of August 2026, citations to the end of 2025). If they do, it shows their sub-field rank in the career list and in
the 2025 single-year list. Behind a click, it shows three integrity numbers: self-citation share, citations per
citing paper, and how many of their papers Retraction Watch lists as retracted.

The data ships inside the extension as one rounded, columnar gzip file of about 4.3 MB. No network call is made
and no permission is added. The panel is free for everyone. Pro code never reads the data.

Two pieces of work, in order:

1.  **PR A, data packaging (ships first, on its own).** Gzip five existing uncompressed data files so the installed
    extension shrinks by 3.7 MB, which offsets the size PR B adds. It also adds a size budget that fails packaging
    when a store zip grows. Nothing a user sees changes.
2.  **PR B, the panel.** The build script, the data file, the matcher, the panel and its tests.

## The Problem

A student choosing an adviser, a reviewer checking an author, or a hiring committee reading a file looks at a
Scholar profile and sees citation totals and an h-index. Those numbers are not comparable across fields and say
nothing about self-citation or retractions. The Ioannidis dataset gives field-normalised context for about 346,000
researchers, but using it means downloading two 100 MB spreadsheets and searching them by hand.

The hard part is identity. Scholar profiles carry no Scopus ID, so the only way to link a profile to a dataset row
is the name and the free-text affiliation line. In the career table alone, 3,015 names belong to two or more
people, and 1,434 rows share an exact name, institution and country with another row. A wrong link puts someone
else's numbers, including a retraction count, on a named person's profile. The design treats a wrong match as the
main harm: it shows nothing when unsure, lets the user choose when several people fit, and must pass an offline
accuracy test before release.

## Goals

*   On a profile with one confident match, show the matched identity (name, institution, country) and the sub-field
    rank from each table the person appears in, as "Rank X of Y listed researchers in <sub-field>".
*   When several dataset rows fit, show a pick list with what tells them apart. Remember the user's choice on that
    device, keyed by the Scholar profile ID, and re-check it when the data version changes.
*   Behind one click, show self-citation share, citations per citing paper, and the retracted-paper count, each with
    one plain sentence of context.
*   Show the retraction count only on a confident unique match or after the user has picked, in fixed wording that
    says it does not show who was responsible. Never show a count of 0.
*   Before release, pass an offline labelled match set (100-300 cases plus saved profile pages) with a precision
    floor of 99% of shown matches correct. CI fails below the floor.
*   In the first month after release, no wrong-match complaints from profile owners. Every panel carries a "Report a
    wrong match" link so complaints have a channel.
*   Credit the dataset in the panel and in `THIRD_PARTY.md`, the store listings and the about page, with author,
    title, DOI, licence link and a note that the data was rounded and reduced.
*   Keep the feature free: it lives in its own module, and tests fail if any other code reads its data or if the
    module mentions the Pro entitlement.
*   Make next year's refresh repeatable: a checked-in, deterministic build script and a versioned file header.
*   Grow the shipped Chrome and Firefox packages by no more than the measured net of PR A and PR B, checked by a
    size budget in `package_release.sh`.

## Non-Goals

*   No Pro feature, upsell or Pro code path uses this data.
*   No badge on the user's own profile is designed specially. The same panel shows there, with no extra handling.
*   No ranking of researchers against each other, no leaderboards, no trend arrows, no colour scales.
*   No badges from this data on search results, co-author lists or citation lists.
*   No network download of the data and no telemetry.
*   No composite c-score, h-index or raw citation counts from the dataset. Scholar already shows citations.
*   No match on a Scholar name written only in a non-Latin script in v1. Those profiles show no panel.
*   No reconciliation with the extension's existing retraction badges (`retraction_bloom.json`). The panel names its
    own source and date instead.

## Open Questions

*   **Matcher thresholds**: the institution-overlap score that counts as a match, and whether a bare initial ("J.
    Smith") can ever give a single confident match. Set in the Technical Plan and tuned against the labelled set.
*   **Edge package size**: the Edge build unzips every data file. Measured in PR A: 12,559,024 bytes (zip). PR B
    records its own number; the budget in `scripts/size-budget.json` covers Edge too.
*   **AMO review of a binary data file**: Firefox reviewers must be able to rebuild it from the source zip. The
    100 MB source spreadsheets cannot go in that zip; the notes give the DOI and checksums instead. Whether that is
    enough is only known after one review.

## Critical User Journeys (CUJs)

### CUJ 1: One clear match

*   **Given**: The extension is installed (free or Pro). The user opens the Scholar profile of a researcher whose
    name and affiliation match exactly one dataset person.
*   **When**: The profile page loads.
*   **Then**: Under the author stats, a panel shows "Matched: <Surname, Given> · <institution> · <country>", "Listed
    in the top-2% list (career)" with "Rank X of Y listed researchers in <sub-field>", the same for the 2025
    single-year list if present, a "More detail" control, "Not this person?" and "Report a wrong match" links, one
    caveat sentence and the credit line with the snapshot date. Nothing differs between free and Pro.

### CUJ 2: Several people fit

*   **Given**: The profile's name and affiliation fit two or more dataset rows equally.
*   **When**: The page loads.
*   **Then**: The panel says "Several listed researchers match this profile" and offers a "Choose" control. The list
    shows each candidate's institution, country, sub-field and active years. Keyboard focus starts on "None of
    these". After a choice, the panel shows that person (CUJ 1 layout, with the retraction line allowed) and says
    "Saved on this device only". Choosing "None of these" hides the panel for that profile and is remembered.

### CUJ 3: No match

*   **Given**: No dataset row fits the name and affiliation, or the name has no Latin form, or the affiliation line
    is empty and the name is shared.
*   **When**: The page loads.
*   **Then**: The panel either does not appear, or shows one line, "No dataset match found for this profile", with
    the credit link. It never says the person is not on the list.

### CUJ 4: Detail behind the click

*   **Given**: A panel from CUJ 1, or from CUJ 2 after a pick.
*   **When**: The user activates "More detail" by mouse or keyboard.
*   **Then**: Self-citation share ("X% of citations to this person's papers are self-citations"), citations per
    citing paper ("Each citing paper cites this person Y times on average; there is no field benchmark"), and, only
    if the count is above 0 and the match is confident or picked, "N papers by this name are listed in Retraction
    Watch as retracted (dataset snapshot Aug 2026). This does not show who was responsible." with "Verify" and
    "Report a wrong match" links.

### CUJ 5: A wrong match is reported

*   **Given**: The panel shows the wrong person.
*   **When**: The user clicks "Not this person?".
*   **Then**: The panel opens the pick list (or hides itself if there is only one candidate), and the choice is
    stored as for CUJ 2. "Report a wrong match" opens a prefilled mail to the support address with the profile URL
    and the matched identity, and sends nothing on its own.

### CUJ 6: Data refresh

*   **Given**: A user has stored picks, and a new release ships dataset v10.
*   **When**: They next open a profile with a stored pick.
*   **Then**: The extension re-resolves the stored identity against v10. If it maps to exactly one person, that
    person is shown; otherwise the pick is dropped and the pick list shows again. No wrong person is shown because
    of a stale pick.

### CUJ 7: Turned off or broken

*   **Given**: The user turns the panel off in Options, or the data file fails its header check.
*   **When**: A profile opens.
*   **Then**: No panel and no error on the page. The Options data sources table shows the panel's data as
    "off" or "failed to load".

## Review resolutions (2026-11-05)

Five reviews (comprehension, implementation readiness, design critique, codebase details, zoom-out) were run on the
draft. Where this list and the PR B text below disagree, this list wins.

**Words used in this document.** *Top-2% list*: the people in the dataset, which ranks authors by a composite citation
indicator within each of 174 sub-fields. *Career table* and *single-year table*: the dataset's two lists (whole career,
and citations in 2025 only). *Sub-field*: the Science-Metrix sub-field the dataset assigns. *Confident match*, *pick
list*, *none*: the three matcher results, defined below. *Fail closed*: on any error the panel is not shown.

**Display**

1.  No match, or a non-Latin name: no panel at all. The "No dataset match found" line is dropped (CUJ 3).
2.  The retraction line appears only inside "More detail", only for a confident or user-picked match, only when the
    count is above 0, and always next to the matched name and institution.
3.  One correction route: "Not this person?" opens the pick list, or, with one candidate, hides the panel and stores
    `{none: true}` for this profile. One "Report a wrong match" link in the panel footer opens a prefilled GitHub
    issue through the existing `WRONG_MATCH_ISSUE_BASE` (`src/common/quality.js`), the same route the venue badges
    use. Nothing is sent until the user submits the issue.

**Matcher**

4.  An initial-only name match is never confident.
5.  Confident: exactly one candidate under the primary key has institution evidence, it has a strong given name, and
    no other candidate under that key has any institution evidence. A flipped-key (CJK order) hit can be confident
    only when the primary key has no hits.
6.  Pick list: two or more candidates under the key with institution evidence, or exact duplicates (same name,
    institution, country). Everything else is none.
7.  Institution evidence uses containment, not Jaccard: the share of the dataset institution's tokens found in the
    Scholar affiliation, after the same diacritic folding as names and a short alias table (MIT, ETH, UCLA, CNRS and
    similar, in `top2pct-core.js`). The threshold is tuned on a development split and checked on a held-out split.
    Scholar gives no country, so country is never evidence on its own.
8.  Names: apostrophes are removed ("O'Brien" becomes "obrien"); a hyphenated given or family name is kept as one
    token without the hyphen. Suffixes (jr, sr, ii, iii) are stripped only at the end. A particle joins the surname
    only when at least two tokens precede the last one, so "Di Wang" keeps "wang" as the surname. All of these are
    cases in the name fixture.
9.  Stored picks: `{v, name, inst, cntry, aff, t}`, where `aff` is a hash of the Scholar affiliation at the time of the
    pick. A changed affiliation re-runs the matcher. `{none: true}` entries are dropped when the dataset version
    changes, so a person added in a later version is offered again. Picks are cleared by the existing clear-data path
    and when the panel is turned off.

**Accuracy gate.** A 99% floor on 100 to 300 hand-labelled cases cannot be shown statistically, and labelling would
need live Google Scholar pages, which this project does not automate. The gate is instead structural and runs on the
whole dataset: for every name key shared by two or more people (3,015 in the career table) and every exact duplicate
(1,434), synthetic profiles built from each row's name and an affiliation variant (exact, abbreviated, empty, another
institution) must give zero wrong confident matches. A small hand-checked fixture from saved profile pages covers the
display paths. The test reports recall and the pick-list rate; it does not gate on them.

**Build and format**

10. One normalizer, in JavaScript (`top2pct-core.js`). The build is `scripts/build_top2pct.mjs` (Node). The two source
    spreadsheets are first dumped to CSV by `scripts/top2pct_xlsx_to_csv.py` (run with `python3 -I`, openpyxl, build
    time only, never in CI). No Python normalizer, no cross-language fixture, no Python step in CI. The source files'
    sha256 and the dataset DOI go in the build script header and in `top2pct.meta.json`.
11. Format: a gzip of a versioned JSON header plus columns (string tables, person list, per-table integer columns
    with fixed scaling: self-citation share in whole percent, citations per citing paper times 10, rounded half up).
    The custom varint binary is used only if the JSON-column file is over 5 MB. Rank and sub-field size come from the
    dataset, which ranks within the sub-field; the sub-field size is stored once per sub-field. The decoder checks
    every index against its table size and returns null on any mismatch; gzip's own CRC covers corruption.
12. Two modules, not four: `src/content/top2pct.js` (panel, copy strings, `runTop2pct(deps)`) and
    `src/content/top2pct-core.js` (names, decode, match; no DOM). Both get build, manifest and `CONTENT_SIDE`
    entries.
13. Load failure: one retry through `importModuleWithRetry`, then fail closed. A malformed pick entry is ignored, and a
    failed storage write is caught. A test decodes the data file from the packaged zip, not only from `src/`.

**Fence.** One test: (a) no file outside `top2pct*.js`, `scripts/build_top2pct.mjs` and `scripts/variant-data.mjs`
names `top2pct.bin`, `top2pct.json` or `top2pct.meta`; (b) the import graph of `top2pct*.js` does not reach the
entitlement or payment modules; (c) `top2pct*.js` has no whole-word `entitlement`, `isPro` or `isPaid`. A substring
ban on "pro" would match "profile". The DOM-equality test is dropped.

**Size.** The budget rule is in the PR A section: PR A at measured size plus 10%, and PR B resets each limit to its
measured size plus 5%. The success measure is the zip delta recorded in each PR, not a separate goal.

**Dropped.** `scripts/bench-top2pct.mjs` and the 150 ms / 30 MB gate (one timing on a saved profile page goes in the PR
instead); `docs/top2pct-refresh.md` (the refresh steps go in the build script header); the "no wrong-match complaints"
goal (it cannot be measured, because nothing is tracked).

**Risks**

*   **A store rejects the data file** (AMO asks for a reproducible build): the store notes give the DOI, checksums and
    build command. If that is refused, that store's package ships without the panel; a variant switch drops the file
    and the module.
*   **A person asks to be removed**: `scripts/top2pct-exclude.json` lists dataset rows the build skips; the next
    release removes them.
*   **A wrong-match report**: GitHub issues on the extension repo; the owner is the maintainer.
*   **Licence**: CC BY-NC 3.0 needs credit and a note of changes. The panel credit line says the data was rounded and
    reduced, and `THIRD_PARTY.md` carries the full credit. The panel is free and Pro never reads the data.

## Technical Plan

### Data flow

```
Two source xlsx files (scratchpad, never in git)
        |
        v
scripts/build_top2pct.py  (deterministic; columns read by name; fails on a missing column)
        |
        v
src/data/top2pct.bin.gz  +  src/data/top2pct.meta.json  (checksums, row counts, source xlsx sha256)
        |
        |  profile page only, once per page
        v
src/content/top2pct.js   (lazy ES module, loaded by content.js through importModuleWithRetry)
   readProfile(document) -> {userId, name, affiliation, emailDomain, anchor} | null
   loadTop2pct()         -> decoded index (cached per page; fails closed)
   matchProfile(...)     -> {state: "confident"|"pick"|"none", candidates}
   renderPanel(...)      -> #su-top2pct, a sibling after #su-author-stats
        |
        v
chrome.storage.local "suTop2pctPicks" (picks and "none of these", capped at 500)
```

### PR A: data packaging (as built)

The five uncompressed files gzip from 4,853,629 bytes to 1,141,191 bytes (`gzip -n -9`), a saving of 3.71 MB in the
installed Chrome and Firefox extension. The store zip barely changes, because zip already deflates each file:

| Zip | Before | After |
| :--- | ---: | ---: |
| Chrome | 12,419,693 | 12,408,566 |
| Firefox | 12,419,750 | 12,408,623 |
| Edge (files stored unzipped) | 12,558,305 | 12,559,024 |

| File | Raw | gzip -9 |
| :--- | ---: | ---: |
| scimago_2024_quartiles.json | 1,359,026 | 262,225 |
| norwegian_compact.json | 1,071,792 | 242,379 |
| era2023.txt | 951,004 | 258,626 |
| journal_impact_2024.csv | 771,687 | 202,187 |
| venue_h5_index.json | 700,120 | 175,774 |

*   **Helper** `src/common/gzip-fetch.js`: `fetchDataText(path)` and `fetchDataJson(path)`. It gunzips only when the
    URL ends in `.gz`, the same rule `content.js` and `genealogy-lineage.js` already use, so the Edge build (which
    stores the files unzipped and rewrites the path) needs no special case. No magic-byte sniffing.
*   **Literal paths**: `scripts/variant-data.mjs` rewrites only the literal string `src/data/<name>.gz`, in .js and
    .html files. Every reader now names the full path, including `BADGE_SOURCES` in `src/common/quality.js` (the
    Options data-source table). `tests/data-paths.test.mjs` fails on a path joined from a base or a template, on a
    literal path to a missing file, and on a raw copy of a gzipped file.
*   **Readers changed**: `data-loader.js` (bundled, so the helper is inlined), `library.js`, `options.js`
    (`countListEntries` strips `.gz` before its `.json` check), and `sw.js`. The service worker is a classic script
    loaded with `importScripts`, so it keeps a small inline copy (`fetchBundledJson`) instead of importing the module.
    `content.js` and `genealogy-lineage.js` keep their own copies, unchanged; `tests/variant-data.test.mjs` still
    asserts them. The dead `norwegian_register.csv` fallback (the file never ships) is removed.
*   **Tests, scripts, docs**: `tests/sw-quartiles-refresh.test.mjs` gunzips the bundled quartiles;
    `scripts/audit-venue-matches.mjs` reads `<name>.gz` when it exists; the four writer scripts
    (`convert_era_xlsx_to_txt.py`, `build_scimago_quartiles.py`, `build_norwegian_compact.js`,
    `build_venue_h5_from_orderer.js`) write `.gz` directly with a fixed mtime; `README.md` names the new files.
*   **Module wiring**: `dist/common/gzip-fetch.js` has a build entry, an ESLint module entry and a
    `web_accessible_resources` entry (the import-graph test requires it).
*   **Size budget**: `scripts/size-budget.json` holds `{chrome, firefox, edge: {maxBytes}}`; the end of
    `scripts/package_release.sh` compares each zip's byte count and exits non-zero when one is over. PR A sets each
    limit to its measured size plus 10%. Any PR that adds data sets the limit to its new measured size plus 5%.

### PR B: the panel

**File format** (`top2pct.bin.gz`, little-endian, all integers unsigned LEB128 varints unless noted):

1.  Header: magic `SUT2`, format version (1), UTF-8 JSON block with dataset version, snapshot date, table labels read
    from the source header, row counts, and a CRC32 of the body.
2.  String tables: institutions, countries, sub-fields, each newline-joined UTF-8.
3.  Person list (sorted by normalised name key): front-coded name key, display name, institution index, country
    index, first year, last year.
4.  Per table (career, single year): person index delta, sub-field index, rank in sub-field, sub-field count,
    self-citation share (0-100, 1 point), citations per citing paper (x10), retracted-paper count. Rows absent from a
    table are absent from its column block.

The JS decoder builds a `Map` from name key to person indices and keeps the columns as typed arrays; it decodes a
person's numbers only when they match. A wrong magic, unknown format version or CRC mismatch makes `loadTop2pct`
resolve to `null`, which shows no panel and marks the Options row "failed to load".

**Name normalizer** (one spec, two implementations: `scripts/top2pct_names.py` and `src/content/top2pct-names.js`,
tested against `tests/fixtures/top2pct-name-cases.json`):

1.  Lower-case, NFKD, strip combining marks; map ø→o, ł→l, đ→d, ß→ss, æ→ae, œ→oe, þ→th.
2.  Remove titles and suffixes (dr, prof, professor, phd, md, jr, sr, ii, iii, frs, and similar, kept as one list).
3.  Split on anything outside a-z; drop empty tokens. If no token remains, return null (non-Latin names give no match).
4.  Scholar names are "Given ... Surname"; particles (van, der, den, de, la, le, von, da, dos, del, di, du) before the
    last token join the surname. Dataset names are "Surname, Given".
5.  Key = `surname|first-given-initial`. The full given string is kept for scoring.

**Matcher** (`matchProfile`):

1.  Look up the key, and for two-token names also the flipped key (CJK order).
2.  Score each candidate: given-name strength (full first given name equal → strong; initial only → weak), plus
    institution overlap (tokens after stop words such as university, of, the, institute, dept; Jaccard ≥
    `INST_THRESHOLD`, start 0.5, tuned on the labelled set), plus email-domain evidence (the verified domain's
    second-level label appears in the institution tokens).
3.  Confident: exactly one candidate with a strong given name and an institution match, and no other candidate with
    the same display name, institution and country. Pick list: two or more candidates with a strong or weak name and
    any institution or country evidence, or exact duplicates. None: everything else, including empty affiliation
    with more than one candidate of that key.
4.  A stored pick for this `userId` with the current dataset version wins. A pick from an older version is
    re-resolved by (name, institution, country); one hit keeps it, otherwise it is dropped.

**Panel** (`#su-top2pct`, inserted once after `#su-author-stats`, or after `#gsc_prf_inw` if stats are absent; a
`MutationObserver` is not needed because `renderAuthorStats` only rewrites its own element's innerHTML):

*   Built with DOM methods and `textContent`, never innerHTML, since names and institutions come from data.
*   Copy lives in `src/content/top2pct-copy.js`; the string test reads that file.
*   "More detail" is a `<button aria-expanded>` controlling a region; the pick list is a fieldset of radio buttons
    with "None of these" first and focused, and a "Use this person" button. Focus returns to the panel heading after
    a choice.
*   "Report a wrong match": `mailto:` the support address with subject and body prefilled (profile URL, matched
    display name, institution, dataset version). It does nothing until the user sends from their mail client.
*   Styles in the existing content stylesheet, using the current theme tokens; no inline styles.
*   Re-runs when the `user=` parameter changes (Scholar's in-page navigation), removing the old panel first.

**Settings and storage**: `top2pctPanel: true` in `DEFAULT_SETTINGS`, an Options toggle in the profile section, a
Options data-source row. `suTop2pctPicks` entries `{v, name, inst, cntry, t}` or `{v, none: true, t}`, capped by
`capRecordCount(map, 500, e => e.t)`, removed by the existing clear-data path, and named in the privacy section.

**Fence**: `top2pct.js` exports only `runTop2pct(deps)`. content.js calls it from the profile-page path and passes
`{getSettings, storage, importModuleWithRetry}`. Tests (1) fail if any file other than `top2pct*.js`, `variant-data.mjs`,
`options.js` (name only, for the data-source row) and the build script names `top2pct.bin`; (2) fail if any
`top2pct*.js` file contains `entitlement`, `paid` or `pro`; (3) render the fixture with a stub `deps` and assert the
DOM is the same for any entitlement value.

**Credit**: one line in the panel: "Data: Ioannidis, Updated science-wide author databases of standardized citation
indicators, v9 (DOI link), CC BY-NC 3.0 (licence link). Rounded and reduced by Scholar Utility Belt." The full text
goes in `THIRD_PARTY.md` (new "Data" section), `SOURCE-NOTES.txt` and the store listing text in the repo.

## Alternatives Considered

*   **Opt-in one-time download**: rejected; needs a remote fetch and storage path, against the no-new-permission rule.
*   **Plain JSON (22.4 MB gzip) or columnar at 0.01% (5.19 MB)**: rejected for size.
*   **xz (4.51 MB)**: rejected; browsers only decompress gzip and deflate natively.
*   **Names as a 4-byte hash**: rejected; saves 0.37 MB and makes the pick list impossible.
*   **Name-only matching**: rejected; 3,015 shared names in the career table.
*   **Sharded data files by name prefix**: rejected for v1; fewer bytes per page but many files for AMO to review and
    for the Edge rewrite to track. Revisit if decode time exceeds the budget.
*   **Decoding in the service worker**: rejected; the worker is suspended and restarted, so it would decode
    repeatedly, and message passing adds a second code path to test.
*   **Unique exact match only, no pick list; career table only**: rejected by Ben (keep both tables and picks).
*   **Retraction count for any name match**: rejected; shown only on a confident or picked match.
*   **Remote kill switch**: not available; the Options toggle plus a store release is the fallback.

## Non-Functional Requirements

*   **Performance**: on a 2020 laptop, fetch, decompress and index under 150 ms at the median and under 30 MB of
    extra heap, measured by a benchmark script in `scripts/bench-top2pct.mjs` (Node with the same decoder). Nothing
    loads on search or citation pages.
*   **Privacy**: no network call; the only stored data is the pick map on the device.
*   **Accessibility**: WCAG 2.1 AA in light and dark; full keyboard use; visible focus; the existing a11y, focus and
    theme e2e checks cover the panel and pick list on the saved fixture.
*   **Failure**: any error in the module is caught inside `runTop2pct` and logged once at debug level; the page and
    other features are unaffected.
*   **Size**: Chrome and Firefox zips grow by no more than the measured PR B delta minus the PR A saving; the budget
    file records both.

## Detailed Implementation Plan

### PR A (branch `data-gzip`)

Built as described in "PR A: data packaging (as built)" above.

### PR B (branch `top2pct-panel`)

1.  `scripts/build_top2pct.py`, `scripts/top2pct_names.py` (new): build and normalizer; writes `.bin.gz` and
    `.meta.json`.
2.  `src/data/top2pct.bin.gz`, `src/data/top2pct.meta.json` (new, built).
3.  `src/content/top2pct.js`, `top2pct-names.js`, `top2pct-decode.js`, `top2pct-copy.js` (new).
4.  `src/content/content.js`: one call on profile pages; `build.js` and the manifest list the new modules;
    `tests/web-accessible.test.mjs` `CONTENT_SIDE`.
5.  `src/common/storage.js`: setting default and pick key; clear-data path.
6.  `src/options/options.html/js`: toggle and data-source row.
7.  Content stylesheet: panel and pick-list styles.
8.  `THIRD_PARTY.md`, `SOURCE-NOTES.txt` notes in `scripts/publish_stores.mjs`, privacy text, store listing text.
9.  `docs/top2pct-refresh.md` (new): yearly refresh checklist.
10. Tests: `top2pct-names.test.mjs` (shared case table, also run by `python3 -m unittest`), `top2pct-roundtrip.test.mjs`
    (200 fixture rows, corrupt-buffer case), `top2pct-match.test.mjs` (labelled set, 99% precision floor, plus recall
    reported), `top2pct-fence.test.mjs`, `top2pct-copy.test.mjs` (no emoji, no "best", no "not on the list"), e2e on
    the saved profile fixture for a11y, focus and theme.

## Verification Plan

*   `npm run build`, `npm test`, `npm run test:e2e` green on both PRs; `python3 -m unittest` for the build script.
*   PR A: Chrome and Edge variant builds unpacked into a temp folder; every badge data source loads on the saved
    fixtures (the Options data-source table shows "loaded" for all rows).
*   PR B: the labelled set reaches at least 99% precision on shown matches; the report gives precision, recall and
    the pick-list rate. Benchmark under budget. Panel checked on the saved profile fixture only, never on live
    Google Scholar.
*   Packaged zip sizes recorded in each PR description.

*Drafted with EGM*
