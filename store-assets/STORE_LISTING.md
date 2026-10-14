# Chrome Web Store Listing — Scholar Utility Belt v1.0.0

## Extension Name (45 char max)
Scholar Utility Belt — Journal Quality Badges

## Short Description (132 char max)
Journal rankings, citation analytics and author metrics on Google Scholar. Free to use, and your data stays on your computer.

## Detailed Description

Scholar Utility Belt adds journal quality badges, citation analytics, author metrics, advisor–student genealogy trees, and research trend tracking directly to Google Scholar — no extra tabs, no manual lookups.

★ JOURNAL QUALITY BADGES
See journal quality as color-coded badges, taken from established ranking lists for business, computing and other fields, plus quartile rankings, on every search result and author profile page. Instantly identify top-tier publications without leaving Scholar.

★ YOUR OWN JOURNAL LIST
Upload a CSV of journals and conferences with your own tier labels, such as a department's A, B and C list. Matching venues get a badge with your label next to the built-in ones. The file is read in your browser and stays there.

★ CITATION VELOCITY & ANALYTICS
Each paper shows citations per year with trend arrows (↑ accelerating, → stable, ↓ decaying). Compare papers not just by total citations but by research momentum.

★ AUTHOR PROFILE METRICS
On any Scholar author page, see computed h-index, m-index, L-index, g-index, hI-norm, self-citation rate, publication quality breakdown (how many papers fall in each ranking tier), and coauthor analytics — all calculated in real time.

★ ADVISOR–STUDENT GENEALOGY
Trace advisor–student genealogy across generations. See who advised whom in an interactive tree view with 900,000+ researchers drawn from public genealogy projects and institutional repository data.

★ QUERY TREND TRACKER
See how research topics are trending via OpenAlex data. Every search shows related concept velocity (e.g., "Large Language Models ↑ 847%") plus detected methodology keywords.

★ RETRACTION ALERTS
Papers flagged in the Retraction Watch database are marked with a visible warning badge. Never accidentally cite retracted research.

★ QUICK ACTIONS
One-click Save to library, BibTeX export, abstract preview, DOI lookup, and code/dataset link detection (from the result's own links and DataCite) — all inline on search results.

★ DARK MODE
Full dark theme that matches Scholar's interface, with all badges and panels adapting automatically.

★ PRO (OPTIONAL)
Everything above is free. Scholar Utility Belt Pro adds tools for literature reviews and research workflows: author compare, paper idea lineage (how a paper's ideas trace back through its citations), extended bibliometrics, a narrative CV, a review workspace with reference import, risk-of-bias notes and screening agreement, a citation-graph overlay and a Publish-or-Perish report. Newer Pro tools include exports to reference managers, note apps and document formats, collection packets, watch lists, a bibliography checker and a tenure packet. Try it free for 14 days, buy it once or subscribe, or unlock it with an App Pass if you already have one. If you installed before Pro launched, the Pro features that existed then stay free for you forever.

★ OPTIONAL AI FEATURES (PRO, OFF BY DEFAULT)
Triage cards show study design, sample and setting under a result, read from the snippet by Chrome's built-in on-device model where your Chrome offers one; a value appears only if it is in the snippet. Ask my shortlist lets you ask one question about papers you saved, using your own API key from an AI provider you choose in Options. If you use it, the titles and saved abstract or snippet text of the papers you select, plus your question, are sent to the provider you chose, only after you review the cost estimate and confirm. Each quoted sentence in the answer is checked against the saved text.

★ PRIVACY-FIRST
All ranking data is bundled locally — no API calls needed for quality badges. External lookups, such as OpenAlex and Crossref, only happen when features are enabled and never send personal data. No tracking, no analytics, and no account for any free feature. The extension itself sends nothing to its developer: it opens a getting-started page on purplelink.llc once, when you first install it, and Chrome opens a short page there if you uninstall it, and those two visits are counted like any web page, without cookies or accounts. If you choose Pro, ExtensionPay handles the trial or payment and asks for your email. If you turn on the optional bring-your-own-key feature, the paper text you select and your question go to the AI provider you picked, using your own key, and to no one else. App Pass is off by default and contacts joinapppass.com only after you turn it on.

RANKING SOURCES
The badges come from ranking lists bundled with the extension, plus Google Scholar's own venue h5-index. Journals that appear on public predatory-journal lists are flagged. The full list of sources and how each is used is in the README on GitHub.

Works on all Google Scholar country domains (google.com, google.de, google.co.uk, google.fr, etc.).

Open source: https://github.com/BenAmpel/ScholarUtilityBelt

---

## Category
Productivity

## Language
English

## Tags (5 max)
1. google scholar
2. journal ranking
3. academic research
4. citation analysis
5. h-index

---

## Files to Upload

### Screenshots (1280×800, PNG)
1. screenshot-1-1280x800.png — Quality Badges on search results
2. screenshot-2-1280x800.png — Author profile analytics
3. screenshot-3-1280x800.png — Advisor–student genealogy tree
4. screenshot-4-1280x800.png — Trend tracker panel
5. screenshot-5-1280x800.png — Dark mode + quick actions

### Promotional Images
- Small tile: promo-small-440x280.png (440×280)
- Marquee: promo-marquee-1400x560.png (1400×560)

### Icon
- Already in manifest: icons/icon128.png (128×128)

---

## Additional Fields

### Single Purpose Description
"Adds journal quality rankings, citation analytics, and research tools to Google Scholar search results and author profiles."

### Host Permission Justification
"The extension requires access to Google Scholar pages to inject quality badges, citation metrics, and research tools into search results and author profiles. The listed API hosts (OpenAlex, Crossref, Unpaywall, DataCite, OpenCitations, arXiv, Europe PMC, NCBI, NIH iCite, ROR, DBLP, SCImago, Semantic Scholar) are queried from the browser for citation data, open-access and funder signals, retraction and update status, and trend analysis; the full list with what each receives is in the README under 'Every host the extension contacts'. The optional all-sites permission is asked for only when the user turns on PDF page counts, hover summaries or Copy BibTeX from the publisher, and only fetches the page the user is pointing at. A second optional permission, for one AI provider's API host, is asked for only when the user turns on Ask my shortlist."

### Privacy Policy URL
https://purplelink.llc/privacy/#scholar-utility-belt

The page exists and covers the extension, ExtensionPay and the lookup services. Before submitting 1.0.0 re-read it against the list below: it does not yet name arXiv, DataCite or SCImago, and it has no paragraph on the optional AI provider flow (Ask my shortlist) or on the "Look up extra signals" switch. That page lives in the Purplelink site repo.

### Support URL / Homepage
https://github.com/BenAmpel/ScholarUtilityBelt

---

## Privacy tab: changes to re-check in the Web Store dashboard before submitting 1.0.0 (it covers 0.7.0 to 1.0.0)

Version 0.7.0 adds two optional flows that touch user data, and 0.7.1 adds the install and uninstall pages. Review the dashboard's Privacy practices tab (the listing's existing answers stay unless they conflict):

- **Personally identifiable information (email).** ExtensionPay collects an email address when a user starts the free trial or buys Pro. App Pass returns the signed-in user's email to the extension's service worker when they opt in; the extension does not store it (only a status and timestamp are cached locally).
- **Authentication information.** The opt-in App Pass check sends the user's joinapppass.com login cookie to joinapppass.com with each check.
- **Remote hosts contacted for these flows.** extensionpay.com (trial, purchase, entitlement check) and joinapppass.com (App Pass, opt-in only).
- **Certifications.** None of this data is sold, used for purposes unrelated to the extension's single purpose, or used for creditworthiness or lending.
- **Single purpose / permissions.** No manifest permission or host permission changed from 0.7.0 to 1.0.0. 1.0.0 narrows `web_accessible_resources` from a wildcard over `dist/common/` to the files the content script imports, so extension internals such as the payment client are no longer exposed to Scholar's pages. Two optional permissions are requested at runtime and only on a user action: all sites (publisher PDF page counts, hover summaries, Copy BibTeX) and one AI provider's API host (Ask my shortlist).
- **Remote hosts (1.0.0).** The README's host table lists every host contacted, what is sent and when. CatalyzeX was removed in 1.0.0: the extension no longer sends result titles or URLs to catalyzex.com, and the "Code" badge it drew is gone (the local Data and Code badge, which reads the result's own links, stays). The per-result lookups in the detailed view (Crossref, OpenCitations, DataCite, Unpaywall, Semantic Scholar, arXiv, Europe PMC, NCBI) have an Options switch, "Look up extra signals", on by default.
- **Promo card (0.7.2).** The settings page and the library page show a dismissible card, on macOS only, that links to purplelink.llc/moderntex/ (a plain link with utm tags; the extension makes no request and sends nothing). The only thing stored is one flag in chrome.storage.local that the card was dismissed. It never appears on Google Scholar pages. No manifest permission or host permission changed. In the Privacy practices tab nothing new is collected; no change is needed.
- **Install and uninstall pages (0.7.1).** The extension opens one getting-started page on purplelink.llc once, on a fresh install, and sets Chrome's uninstall page to a short page there. The extension sends no data and the data-collection answers do not change; the two page visits are counted by the site's first-party, cookieless analytics (docs/install-pages.md). The site's privacy page and the listing text must say the same.
- **Optional AI features (triage cards, Ask my shortlist).** Triage cards run on the device (Chrome's built-in Prompt API) and send nothing. Ask my shortlist, when the user turns it on, sends the user's question and the titles and saved abstract or snippet text of the papers they select to the AI provider they chose (Anthropic, OpenAI or Google Gemini), using an API key they paste in Options. That is user content sent to a third party at the user's request, so in the Privacy practices tab review "Website content" and "User activity" (the selected paper text and question) and the disclosure that it is used only to produce the answer the user asked for and is not sold or used for other purposes. The key is stored in chrome.storage.local only and is sent only to the chosen provider. The provider host (api.anthropic.com, api.openai.com or generativelanguage.googleapis.com) is requested at runtime from the manifest's existing optional host permissions when the user turns the feature on; the required permissions and host permissions did not change, but check that "Host permission justification" still reads accurately. The extension still has no project-run backend and no telemetry.
- **Open in Overleaf and the librarian report (1.0.0).** Overleaf: only when the user clicks "Send to Overleaf" in the LaTeX citation pack, after the full file is shown, the browser posts the chosen papers' titles, authors, years, venues, DOIs, links, tags and abstracts to www.overleaf.com in a new tab; private notes only if the user ticks a box that is off by default. It is a form navigation, not a fetch, so there is no new permission and overleaf.com is not in url-policy.js. Librarian report: the names, institutions and ORCIDs the user types are sent to api.openalex.org when they press "Find authors"; this is user-provided content (people's names) sent to a third party at the user's request, so check that the Privacy practices answers and the privacy policy cover it. Google Scholar is not used for either. Both are unverified against the live services (see the PR).
- **Privacy policy URL.** It must describe the flows above, including the AI provider flow; the README's "A note on the Pro tier" section has the wording.

---

## Release notes for 1.0.1 (paste into the dashboard's "What's new")

After you have used the extension on Google Scholar on a few different days, the popup, Options and Library pages show a small card asking whether you would rate it on the Chrome Web Store, with "Not now" and "Don't ask again". It never appears on Google Scholar pages, and the extension sends nothing: the day count stays in your browser.

## Release notes for 1.0.0 (paste into the dashboard's "What's new")

Free: your own journal list (upload a CSV of venues and tier labels); Scholar's Quick read link stays visible and has a results filter; library and popup imports now merge instead of overwriting, with a backup and undo; BibTeX export writes the right entry type with venue, tags and abstract; an Options switch for the extra per-result lookups; the CatalyzeX Code badge is removed (it sent result titles to catalyzex.com); the advisor and student dataset loads only when you ask; fixes for lost saves, a freeze in the bibliography checker and a sort loop.

Pro (14-day free trial, purchase or App Pass, for everyone): reference import (RIS, BibTeX, PubMed .nbib) with a search log, risk-of-bias grid with JBI-style checklists, screening agreement and evidence matrix, review snapshots; claim-evidence chips, reproducibility signals and reading order; Zotero, Obsidian, LaTeX (with Open in Overleaf) and Word exports; collection packets, similar papers and duplicates, watch lists; bibliography checker; tenure packet; librarian report from OpenAlex; optional AI (on-device triage card, and Ask my shortlist with your own API key).
