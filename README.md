# Scholar Utility Belt

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18645552.svg)](https://doi.org/10.5281/zenodo.18645552)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20me%20a%20coffee-support-FFDD00?logo=buymeacoffee&logoColor=000000&labelColor=FFDD00)](https://buymeacoffee.com/bampel)

Scholar Utility Belt is a Manifest V3 browser extension for Google Scholar that adds local-first research workflow support directly inside Scholar result pages and author-profile pages. The project is designed around one constraint: enrich the literature review experience without turning Scholar into a heavy network client or sending a user’s reading behavior to a remote service.

The extension layers paper triage, venue quality signals, lightweight bibliometric indicators, author-page tooling, and a local library on top of the existing Scholar interface. Most features are computed from the current DOM and packaged datasets, with external lookups used selectively and transparently.

## Why this exists

Google Scholar is unusually good at broad discovery, but it is intentionally sparse. Researchers still have to do a lot of manual work:

- deciding which results deserve immediate attention
- distinguishing strong venues from weak or ambiguous venues
- spotting promising recent papers with low raw citation counts
- comparing author profiles beyond the default Scholar metrics
- saving, tagging, and revisiting papers without leaving the search flow

Scholar Utility Belt addresses those gaps in-place. Instead of replacing Scholar, it augments the page the user is already using.

## Core capabilities

### Search-result augmentation

- Quality badges from local journal and conference lists, including FT50, UTD24, ABDC, VHB, CORE/ICORE, CCF, SCImago quartiles, ABS/AJG, ERA, Norwegian register, and venue h5 where available.
- Result-level action grid for common tasks such as save/remove, copy citation snippets, open PDFs, abstract toggles, and lineage exploration.
- Local-first “Emerging” scoring that compares a paper against venue/year cohorts, with bounded fallback to external sources when local evidence is sparse.
- Citation-velocity and skimmability cues intended to lower scan cost on dense result pages.
- Optional result filtering, grouping, hiding, and sort overlays that run client-side.

### Author-profile augmentation

- Additional author summary metrics beyond the default Scholar profile view.
- Local filters for venue quality, author position, coauthors, citation bands, and topic terms.
- Compare-authors overlay for side-by-side profile analysis.
- Citation-map and idea-lineage views built from bounded external enrichment.

### Library and workflow tooling

- Local saved-paper library with tags, notes, search, sorting, export/import, and collection-style organization.
- Review-workspace flow for sifting, sorting, and report generation.
- Popup and options pages for configuring badge sources, external enrichments, UI density, and quality datasets.

## Design principles

### Local-first

The extension prefers computation from the page already loaded in the browser and packaged data stored with the extension. Many features work without any additional network requests.

### Bounded enrichment

When external data is used, requests are deliberately constrained. The code uses caching, timeouts, and fallback behavior so enrichment does not dominate page rendering or make Scholar feel fragile.

### Scholar-preserving UX

The goal is to support Scholar workflows, not replace them. Controls are embedded into Scholar’s existing result structure and author tables rather than redirecting the user into a separate web app.

## Network and privacy model

Scholar Utility Belt does not require a backend service operated by the project. Data is stored in Chrome extension storage on the user’s machine.

Local-only or packaged-data features include:

- saved library state
- user settings
- venue-quality lists shipped with the extension
- cached page-visit and reading-load metadata
- many DOM-derived result annotations

Optional external enrichment can use public APIs such as:

- [OpenAlex](https://openalex.org/)
- [Crossref](https://www.crossref.org/)
- [Unpaywall](https://unpaywall.org/)
- [OpenCitations](https://opencitations.net/)
- [Semantic Scholar](https://www.semanticscholar.org/product/api)
- [DBLP](https://dblp.org/)
- selected metadata services such as PubMed, Europe PMC, and ROR

The extension is built to minimize those calls and to make their effects legible in the UI.

## Packaged data sources

The repository includes local snapshots under [`src/data/`](src/data):

- `ft50.txt`
- `utd24.txt`
- `abdc2022.csv`
- `vhb2024.csv`
- `abs2024.csv`
- `fnege2025.csv`
- `core_icore2026.csv`
- `core_portal_ranks.csv`
- `scimago_2024_quartiles.json`
- `journal_impact_2024.csv`
- `quality_sources.json`

The new packaged `fnege2025.csv` is derived from the latest Harzing Journal Quality List title PDF (72nd edition, 27 March 2026). It reflects the JQL-covered journal set rather than the full FNEGE master list.

Quality-list refresh helpers live in [`scripts/update_quality_lists.py`](scripts/update_quality_lists.py) and related builder scripts.

## Installation

### End users

Load the extension as an unpacked MV3 extension:

1. Open `chrome://extensions`
2. Enable `Developer mode`
3. Click `Load unpacked`
4. Select this repository root

The extension injects content scripts on Google Scholar domains configured in [`manifest.json`](manifest.json).

### Development

This repository is intentionally lightweight. The main runtime is plain JavaScript with no bundling requirement for extension development.

Useful entry points:

- content script: [`src/content/content.js`](src/content/content.js)
- service worker: [`src/sw.js`](src/sw.js)
- storage helpers: [`src/common/storage.js`](src/common/storage.js)
- venue-quality logic: [`src/common/quality.js`](src/common/quality.js)
- options UI: [`src/options/options.js`](src/options/options.js)
- library UI: [`src/library/library.js`](src/library/library.js)

## Repository layout

```text
src/
  common/      shared helpers for storage and venue-quality compilation
  content/     Scholar page augmentation logic and styles
  data/        packaged rank lists, quartiles, impact snapshots, and metadata
  library/     local saved-paper library UI
  options/     settings page for feature toggles and data management
  popup/       browser action popup
  sw.js        MV3 background service worker

scripts/
  update_quality_lists.py      refresh packaged venue-quality datasets
  build_* / parse_* helpers    data preparation and maintenance scripts
  playwright_*                 browser automation and smoke-test helpers
```

## Quality control

The project relies on a mix of parser-level checks, local data-build scripts, and browser smoke tests. The repository already includes Playwright-based verification helpers under [`scripts/`](scripts) and [`output/playwright/`](output/playwright).

Representative validation tasks include:

- syntax/build checks for core JavaScript modules
- content-script smoke tests on Scholar result pages and author pages
- release-candidate browser checks for first-load rendering and major workflows
- data-refresh scripts with row-count sanity checks for packaged rank sources

## Citation

Use GitHub’s citation panel or the metadata in [`CITATION.cff`](CITATION.cff).

Current archived software DOI:

- [10.5281/zenodo.18645552](https://doi.org/10.5281/zenodo.18645552)

## JOSS materials

This repository includes Journal of Open Source Software submission materials in the standard JOSS manuscript format (`paper.md` plus `paper.bib`):

- [`paper.md`](paper.md)
- [`paper.bib`](paper.bib)

The current manuscript attributes the software to Benjamin M. Ampel, Georgia State University, and includes a JOSS-style AI usage disclosure.

## Limitations

- The extension is tightly coupled to Scholar’s DOM, so upstream Scholar markup changes can break parts of the UI.
- Some enrichments depend on third-party public APIs and may degrade under rate limits or transient failures.
- Packaged rank lists are snapshots; they need periodic refresh to remain current.
- Some curated data sources, such as the Harzing JQL-derived FNEGE file, intentionally reflect a filtered subset rather than a full official master list.

## License

This project is distributed under the ISC license. See [`LICENSE`](LICENSE).
