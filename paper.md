---
title: "Scholar Utility Belt: local-first augmentation of Google Scholar for literature triage and author analysis"
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
  - name: Georgia State University
    index: 1
date: 8 April 2026
bibliography: paper.bib
---

# Summary

Scholar Utility Belt is a Manifest V3 browser extension that adds literature-review support directly to Google Scholar result pages and author profiles. Instead of replacing Scholar with a separate interface, the extension augments the pages researchers already use with result-level actions, venue-quality badges, citation-velocity and cohort-based "Emerging" indicators, author-profile filters, compare-author overlays, and a local saved-paper workflow. The software is designed around a local-first principle: compute as much as possible from the current DOM, packaged datasets, and browser storage, while treating public-API enrichment as bounded and optional.

# Statement of need

Google Scholar is one of the most widely used academic discovery tools because of its broad coverage, low friction, and permissive search style [@googlescholar]. Its interface is intentionally minimal, however, which leaves researchers to do a substantial amount of manual triage around the search experience itself. Common tasks include:

- identify which papers are worth opening first
- infer venue quality from partial or ambiguous venue strings
- distinguish mature highly cited papers from unusually promising recent ones
- compare author profiles beyond Scholar’s default aggregate statistics
- save, organize, and revisit papers without leaving the search flow

Existing bibliometric tools such as Publish or Perish focus on citation retrieval and reporting [@harzing2007pop], while metadata services such as OpenAlex provide programmatic access to scholarly entities [@openalex]. Scholar Utility Belt serves a different use case: interactive, in-page decision support for search, screening, and author inspection. It is intended for researchers who want more context at the point of discovery without adopting a separate analytics application or cloud-backed reference manager.

The extension is also motivated by a practical systems concern. Browser tooling around Scholar can become fragile, intrusive, or rate-limit prone when it depends on a backend service, aggressive scraping, or high-volume API traffic. Scholar Utility Belt instead prefers packaged quality lists, browser storage, and DOM-derived signals, with public APIs such as OpenAlex, Crossref, Unpaywall, OpenCitations, and Semantic Scholar used selectively and with bounded request patterns [@crossref; @unpaywall; @opencitations; @semanticscholar]. This architecture keeps the extension usable under partial failure, makes most features available without a project-run server, and reduces the amount of network activity introduced into Scholar browsing.

# Functionality

Scholar Utility Belt provides three main classes of functionality.

## Result-page augmentation

On Scholar result pages, the extension injects per-result controls and annotations such as save/remove actions, abstract toggles, PDF shortcuts, citation-copy utilities, local filters, and a stable action grid. It also renders venue-quality badges from packaged and user-configurable local datasets, including FT50, UTD24, ABDC, VHB, ABS/AJG, CORE/ICORE, CCF, SCImago quartiles, ERA, the Norwegian register, and venue h5-derived signals. The quality-list pipeline includes local snapshots built from sources such as the Financial Times 50, UTD24, VHB JOURQUAL, and Harzing’s Journal Quality List [@ft50; @utd24; @jql].

The extension further adds lightweight bibliometric cues such as citation velocity and a local-cohort-based "Emerging" score. That score compares a paper against venue/year peers using results already present on the page together with cached local cohort observations, with bounded fallback to external metadata when local evidence is too sparse.

## Author-profile augmentation

On Scholar author pages, the extension adds local filters, extra summary metrics, and a compare-authors workflow. Users can filter by venue quality, citation bands, author position, coauthors, and topical terms. The extension also computes additional profile summaries and provides side-by-side comparison overlays that support portfolio-level inspection beyond Scholar’s default totals.

## Library and review workflow

Scholar Utility Belt includes a local library for saved papers with tags, notes, collections, search, sorting, and export/import. It also includes a review-workspace flow for screening and report generation. Together, these features support active literature review work rather than citation lookup alone.

# Implementation

The software is implemented as a Manifest V3 browser extension. Its main runtime components are:

- content scripts that parse and augment Scholar pages
- a background service worker for bounded network requests and extension coordination
- local storage helpers for settings, caches, and library state
- packaged data snapshots used to compile venue-quality indices
- options, popup, and library pages implemented as extension views

The architecture is deliberately local-first. Most enrichments are derived from the current page DOM and packaged resources. External APIs are used for targeted features such as open-access detection, citation graph expansion, and metadata enrichment, but these calls are cached, throttled, and treated as optional rather than foundational.

# Quality control

Quality control combines static checks, browser-based smoke tests, and data-build validation. The repository includes Playwright-based smoke-test scripts for Scholar first-load behavior, author-profile workflows, review-workspace flows, and options-page behavior. Data-refresh scripts include sanity checks on parsed row counts to catch upstream source changes. The codebase also includes explicit handling for browser-extension lifecycle issues such as context invalidation, storage-access failures, request timeouts, and bounded retry behavior.

# Availability

Scholar Utility Belt is open source and available at [https://github.com/BenAmpel/ScholarUtilityBelt](https://github.com/BenAmpel/ScholarUtilityBelt). Archived software releases are available through Zenodo [@zenodo].

# AI usage disclosure

Generative AI tools were used during development of the software and manuscript, including GPT-5-family coding assistance for refactoring, test scaffolding, copy-editing, and drafting support. All AI-assisted outputs were reviewed, edited, and validated by the human author, who made the design decisions, verified the implementation, and takes responsibility for the submitted materials.

# Acknowledgements

The project builds on openly available scholarly infrastructure and curated quality-list resources, including OpenAlex, Crossref, Unpaywall, OpenCitations, SCImago, CORE, and Anne-Wil Harzing’s Journal Quality List.
