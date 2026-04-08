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

# State of the field

Researchers already have access to several adjacent tools, but they solve different parts of the workflow. Publish or Perish is strong for citation retrieval, bibliometric export, and reporting rather than in-page augmentation of Google Scholar [@harzing2007pop]. OpenAlex is a broad open scholarly index and API that enables external tooling, but it is not itself a Scholar-native interface layer [@openalex]. Zotero Connector supports capture into a reference manager from the browser, but its core workflow is collection and citation management rather than interactive triage, venue assessment, and author-page augmentation on Scholar itself [@zotero]. CatalyzeX enriches papers with code and implementation links, but it focuses on reproducibility and software discovery rather than local-first literature review support across ranking signals, author analytics, and screening workflows [@catalyzex].

Scholar Utility Belt is intended to complement rather than replace those tools. The design choice to build a browser extension instead of a standalone analytics application reflects a specific scholarly contribution: support the decision-making work that happens during search itself. The project therefore emphasizes Scholar-native augmentation, persistent local state, and bounded enrichment rather than a general-purpose scholarly database or cloud-backed reference-management platform.

# Software design

The software is implemented as a Manifest V3 browser extension with content scripts for Scholar-page augmentation, a background service worker for bounded network requests, and extension pages for options, library, and popup workflows. The central design trade-off is local-first augmentation versus externally managed enrichment. Scholar Utility Belt deliberately computes most features from the current DOM, packaged datasets, and browser storage. External APIs are used only for targeted capabilities such as open-access detection, citation graph expansion, or sparse metadata lookup, and these calls are cached, throttled, timed out, and treated as optional rather than foundational.

This design matters for the research setting because it keeps the software usable even when third-party services are slow or unavailable, reduces incremental traffic introduced into Scholar browsing, and preserves a workflow in which the user remains on the source page. It also allows venue-quality signals to be derived from packaged rank lists such as FT50, UTD24, VHB JOURQUAL, and Harzing’s Journal Quality List [@ft50; @utd24; @jql], rather than requiring a continuously running backend or proprietary service.

Functionally, the extension provides three main classes of capability. On Scholar result pages it injects controls such as save/remove actions, abstract toggles, PDF shortcuts, citation-copy utilities, local filters, and a stable action grid. It also adds venue-quality badges and lightweight bibliometric cues such as citation velocity and a local-cohort-based "Emerging" score that compares a paper against venue/year peers using current-page results together with cached local cohort observations. On Scholar author pages it adds local filters, extra summary metrics, and compare-author overlays. Beyond page augmentation, it includes a local saved-paper library with tags, notes, collections, search, sorting, export/import, and a review-workspace flow for screening and report generation.

# Research impact statement

Scholar Utility Belt has been prepared as public research software rather than a private lab script. It is distributed as an installable browser extension, versioned in a public repository, archived through Zenodo, and documented with contributor-facing project metadata including a software citation file, license, and contribution guidance [@zenodo]. The repository includes Playwright-based smoke tests covering Scholar first-load behavior, author-profile workflows, review-workspace flows, and options-page behavior, along with data-refresh sanity checks for packaged venue-quality sources. These project-readiness signals support credible near-term reuse by researchers who already rely on Google Scholar as a primary discovery interface.

The research significance is practical rather than aspirational: the project supplies a reproducible, inspectable implementation of Scholar-native literature-review augmentation that others can install, study, extend, and evaluate. Because it operates at the interface between discovery, venue assessment, and lightweight bibliometric screening, it offers a concrete software contribution for researchers interested in bibliometrics, scholarly discovery tooling, and browser-based research workflows.

# AI usage disclosure

Generative AI tools were used during development of the software and manuscript, including GPT-5-family coding assistance for refactoring, test scaffolding, copy-editing, and drafting support. All AI-assisted outputs were reviewed, edited, and validated by the human author, who made the design decisions, verified the implementation, and takes responsibility for the submitted materials.

# Acknowledgements

This work received no dedicated financial support or external funding. The project builds on openly available scholarly infrastructure and curated quality-list resources, including OpenAlex, Crossref, Unpaywall, OpenCitations, SCImago, CORE, Zotero, and Anne-Wil Harzing’s Journal Quality List.
