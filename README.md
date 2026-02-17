# Scholar Utility Belt (Chrome Extension)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18645552.svg)](https://doi.org/10.5281/zenodo.18645552)

DOI: [10.5281/zenodo.18645552](https://doi.org/10.5281/zenodo.18645552)

Cite this repository: use GitHub’s **Cite this repository** button (right sidebar) or copy the BibTeX/APA below.

Please cite:

```bibtex
@software{ampel_2026_scholar_utility_belt,
  title   = {Scholar Utility Belt},
  author  = {Ampel, Benjamin M.},
  year    = {2026},
  version = {0.2.6},
  doi     = {10.5281/zenodo.18645552},
  url     = {https://doi.org/10.5281/zenodo.18645552},
  publisher = {Zenodo}
}
```

```text
Ampel, B. M. (2026). Scholar Utility Belt (Version 0.2.6) [Computer software]. Zenodo. https://doi.org/10.5281/zenodo.18645552
```

Goal: personal productivity features for Google Scholar with **minimal extra requests**.

## Current features (v0.2)
- Injects per-result buttons on Scholar pages:
  - `Save` / `Remove` (stored locally)
  - `Tag` (comma-separated)
  - `Note` (free text)
- Optional highlight for saved results
- Optional keyword highlighting (client-side)
- Optional hiding of results via regex (client-side)
- Quality index (local lists; zero network):
  - badges under results for FT50 / UTD24 membership
  - ABDC / quartile / CORE rank badges (paste your own CSV lists in Options)
- Popup:
  - saved count
  - open Library / Options
  - export/import JSON (local)
- Library page:
  - search, sort
  - open source page / PDF link
  - edit tags/notes
  - shows computed quality badges in footer when configured

## Network policy
- Default behavior: **no extra network calls** beyond the page you loaded.
- Future features that need additional Scholar endpoints (e.g., BibTeX) should be:
  - strictly user-initiated
  - cached in `chrome.storage.local`
  - throttled

## Quality lists (downloaded sources)
- This repo includes packaged snapshots under `src/data/`:
  - `ft50.txt` (FT Research Rank journal list)
  - `utd24.txt` (UT Dallas Top 100 journal list)
  - `abdc2022.csv` (ABDC 2022 JQL)
  - `core_icore2026.csv` (ICORE2026 conference ranks)
- On install, the extension seeds these into Options automatically (you can edit/clear them later).
- To refresh them from the upstream sites:
  - `python3 scripts/update_quality_lists.py`

## Quartiles (Q1/Q2/Q3/Q4) at scale
- This repo includes an SJR 2024 quartiles snapshot at `src/data/scimago_2024_quartiles.json` built from the user's
  SCImago export.
- The extension seeds this snapshot on install if you have no quartile index yet, and you can also import your own from
  the Options page. The lookup table is stored under `chrome.storage.local` and used to render `Q1`/`Q2`/`Q3`/`Q4`
  badges on Scholar results.

## Load unpacked
1. Chrome: `chrome://extensions`
2. Enable Developer mode
3. Load unpacked -> select this folder

## Notes
- This build only matches `https://scholar.google.com/*`.
  If you use `scholar.google.ca` etc, add it to `manifest.json`.
