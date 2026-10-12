# Third-party code and data

Scholar Utility Belt's own code is under the ISC licence (see `LICENSE`). This file lists what is bundled
from other projects. Licences below are as stated by each upstream project; where this file says "check",
the vendored copy carries no version or licence header and the entry rests on the upstream repository.

## Code

| Component | Where | Licence | Notes |
|---|---|---|---|
| ExtPay (ExtensionPay client) | `src/common/extpay.js`, loaded by the service worker and on extensionpay.com pages | MIT, per the upstream ExtPay repository | The copy has no version or licence header: record the version when it is next updated. It embeds Mozilla's `webextension-polyfill` (MPL-2.0 per upstream; check). |
| `@chrome-stats/app-pass-sdk` 2.0.5 | Bundled into `dist/common/apppass.sw.js` by `build.js` | MIT (from the package's own `package.json`) | Used only if the user opts in to App Pass. |

Build tools (esbuild, ESLint, Playwright) are development dependencies and are not shipped.

## Data

The venue-quality and ranking lists in `src/data/` are compiled from public sources (for example SCImago, the
Australian Business Deans Council list, ABS/CABS, CORE, the Norwegian register, ERA and VHB) and bundled in a
compact form. Each list belongs to its publisher; they are included for lookup only. The advisor-student
lineage data combines public genealogy sources (Academic Family Tree, Mathematics Genealogy Project and
others); see `CITATION.cff` and `paper.md` for the sources, and ask before redistributing any of these files.
