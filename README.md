<p align="center">
  <img src="icons/icon128.png" alt="Scholar Utility Belt logo" width="96" height="96">
</p>

<h1 align="center">Scholar Utility Belt</h1>

<p align="center">
  Local-first productivity tooling for Google Scholar.
</p>

<p align="center">
  <a href="https://chromewebstore.google.com/detail/scholar-utility-belt/omcogfcgldfmihfogbffflbocdbjockn"><img alt="Chrome Web Store" src="https://img.shields.io/badge/Chrome%20Web%20Store-Install-4285F4?logo=googlechrome&logoColor=white"></a>
  <a href="https://github.com/BenAmpel/ScholarUtilityBelt/stargazers"><img alt="GitHub stars" src="https://img.shields.io/github/stars/BenAmpel/ScholarUtilityBelt?style=flat"></a>
  <img alt="Version" src="https://img.shields.io/badge/version-0.3.2-2f855a">
  <a href="https://doi.org/10.5281/zenodo.18645552"><img alt="DOI" src="https://zenodo.org/badge/DOI/10.5281/zenodo.18645552.svg"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-ISC-black"></a>
  <a href="https://buymeacoffee.com/bampel"><img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20me%20a%20coffee-support-FFDD00?logo=buymeacoffee&logoColor=000000&labelColor=FFDD00"></a>
</p>

Scholar Utility Belt is a Manifest V3 browser extension for Google Scholar that adds local-first research workflow support directly inside Scholar result pages and author-profile pages. The project is designed around one constraint: enrich the literature review experience without turning Scholar into a heavy network client or sending a user’s reading behavior to a remote service.

The extension layers paper triage, venue quality signals, lightweight bibliometric indicators, author-page tooling, and a local library on top of the existing Scholar interface. Most features are computed from the current DOM and packaged datasets, with external lookups used selectively and transparently.

<p align="center">
  <img src="docs/images/webstore-geoffrey-hinton.png" alt="Scholar Utility Belt author-profile augmentation on Geoffrey Hinton's Google Scholar page" width="900">
</p>

## At a glance

- Works directly on Google Scholar result pages and author profiles
- Keeps saved papers, notes, tags, and settings in browser storage
- Uses packaged venue-quality datasets for most ranking signals
- Adds bounded optional enrichment from public scholarly APIs
- Avoids a project-run backend or telemetry pipeline

## Key features

- [x] Search-result action grid for save/remove, PDF, abstract, and citation utilities
- [x] Venue-quality badges from FT50, UTD24, ABDC, VHB, ABS/AJG, CORE/ICORE, CCF, SCImago, ERA, Norwegian register, and related sources
- [x] Local-first `Emerging` and citation-velocity signals to reduce scan time on dense result pages
- [x] Author-profile filters, summary metrics, and compare-author overlays
- [x] Local saved-paper library with notes, tags, collections, export/import, and review-workspace tooling
- [x] Configurable options page for feature toggles, quality sources, and enrichment behavior

## Links

- Chrome Web Store: [Scholar Utility Belt](https://chromewebstore.google.com/detail/scholar-utility-belt/omcogfcgldfmihfogbffflbocdbjockn)
- Repository: [github.com/BenAmpel/ScholarUtilityBelt](https://github.com/BenAmpel/ScholarUtilityBelt)
- Archived release DOI: [10.5281/zenodo.18645552](https://doi.org/10.5281/zenodo.18645552)

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

Install from the Chrome Web Store or load the repository as an unpacked MV3 extension.

Chrome Web Store:

- [Install Scholar Utility Belt](https://chromewebstore.google.com/detail/scholar-utility-belt/omcogfcgldfmihfogbffflbocdbjockn)

Load unpacked:

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
