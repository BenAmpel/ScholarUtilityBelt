---
title: "Scholar Utility Belt: Local-first augmentation of Google Scholar for paper triage, venue-quality signals, and author-page analysis"
tags:
  - Google Scholar
  - browser extension
  - scientometrics
  - literature review
  - bibliometrics
  - research workflow
authors:
  - name: Benjamin M. Ampel
    orcid: 0000-0003-0603-0270
    affiliation: 1
affiliations:
  - name: Independent researcher
    index: 1
date: 8 April 2026
bibliography: paper.bib
---

# Summary

Scholar Utility Belt is a browser extension that augments Google Scholar with local-first tooling for paper triage, venue-quality assessment, author-page analysis, and lightweight library management. Rather than replacing Scholar with a separate discovery interface, the software injects workflow support directly into Scholar result pages and author-profile pages. The project adds paper-level controls, venue-quality badges, citation-velocity and cohort-based "emerging" indicators, author-profile metrics, local filtering tools, and a saved-paper library while keeping most computation in the browser and minimizing additional network traffic.

# Statement of need

Google Scholar is one of the most broadly used academic discovery tools because of its coverage, low friction, and permissive search style [@googlescholar]. However, Scholar’s interface remains intentionally minimal. Researchers still need to perform several labor-intensive tasks around the search experience itself:

- identify which papers are worth opening first
- infer venue quality from partial or ambiguous venue strings
- distinguish mature highly cited papers from unusually promising recent ones
- compare author profiles beyond Scholar’s default aggregate statistics
- save, organize, and revisit papers without constantly switching tools

Existing bibliometric tools such as Publish or Perish focus on citation retrieval and reporting [@harzing2007pop], while metadata services such as OpenAlex provide broad programmatic access to scholarly entities [@openalex]. Scholar Utility Belt addresses a different need: interactive augmentation of the page that researchers are already using. The software is designed for researchers who want richer decision support at the point of search rather than a separate analytics workflow.

The extension is also motivated by a practical systems concern. Many productivity add-ons become fragile or intrusive because they depend on a backend service, aggressive scraping, or high-volume API traffic. Scholar Utility Belt instead adopts a local-first architecture: packaged quality lists, browser storage, and DOM-derived features are used whenever possible, with public APIs such as OpenAlex, Crossref, Unpaywall, OpenCitations, and Semantic Scholar used selectively and with bounded request patterns [@crossref; @unpaywall; @opencitations; @semanticscholar].

# Functionality

Scholar Utility Belt provides three major classes of functionality.

## Result-page augmentation

On Scholar search result pages, the extension injects per-result controls and annotations. These include save/remove actions, abstract toggles, PDF shortcuts, citation-copy utilities, local filters, and compact result-level action grids. It also renders venue-quality badges using packaged and user-configurable local datasets, including FT50, UTD24, ABDC, VHB, ABS/AJG, CORE/ICORE, CCF, SCImago quartiles, ERA, Norwegian register, and venue h5-derived signals. The quality-list pipeline includes snapshots built from sources such as the Financial Times 50, UTD24, VHB JOURQUAL, and Harzing’s Journal Quality List [@ft50; @utd24; @jql].

The extension further adds lightweight bibliometric cues such as citation velocity and a local-cohort-based "Emerging" score. This score compares a paper against venue/year peers using results already present on the page and cached local cohort observations, with bounded fallback to external metadata when local evidence is too sparse.

## Author-profile augmentation

On Scholar author pages, the extension adds local filters, extra summary metrics, and a compare-authors workflow. Users can filter by venue quality, citation bands, author position, coauthors, and topical terms. The extension also computes additional profile summaries and provides side-by-side comparison overlays to help users inspect publication portfolios beyond Scholar’s default totals.

## Library and review workflow

Scholar Utility Belt includes a local library for saved papers with tags, notes, collections, searching, sorting, and export/import. The project also includes a review-workspace flow for screening and report generation. These features are intended to support active literature review work rather than citation lookup alone.

# Implementation

The software is implemented as a Manifest V3 browser extension. The main runtime components are:

- content scripts that parse and augment Scholar pages
- a background service worker for bounded network requests and extension coordination
- local storage helpers for settings, caches, and library state
- packaged data snapshots used to compile venue-quality indices
- options, popup, and library pages implemented as extension views

The architecture is deliberately local-first. Most enrichments are derived from the current page DOM and packaged resources. External APIs are used for specific features, such as open-access detection, citation graph expansion, and metadata enrichment, but these calls are cached, throttled, and treated as optional rather than foundational.

# Quality control

Quality control combines static checks, browser-based smoke tests, and data-build validation. The repository includes Playwright-based smoke-test scripts for Scholar first-load behavior, author-profile workflows, review-workspace flows, and options-page behavior. Data refresh scripts include sanity checks on parsed row counts to catch upstream source changes. The venue-quality index is cached across extension sessions, and the codebase includes explicit handling for browser-extension lifecycle issues such as context invalidation, missing storage access, and bounded retry behavior.

# Availability

Scholar Utility Belt is open source and available at [https://github.com/BenAmpel/ScholarUtilityBelt](https://github.com/BenAmpel/ScholarUtilityBelt). Archived software releases are available through Zenodo [@zenodo].

# Acknowledgements

The project builds on openly available scholarly infrastructure and curated quality-list resources, including OpenAlex, Crossref, Unpaywall, OpenCitations, SCImago, CORE, and Anne-Wil Harzing’s Journal Quality List.
