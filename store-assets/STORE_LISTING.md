# Chrome Web Store Listing — Scholar Utility Belt v0.7.0

## Extension Name (45 char max)
Scholar Utility Belt — Journal Quality Badges

## Short Description (132 char max)
Journal & conference rankings (ABDC, FT50, UTD24, VHB, CORE), citation analytics, author metrics & trend tracking on Google Scholar.

## Detailed Description

Scholar Utility Belt adds journal quality badges, citation analytics, author metrics, academic lineage trees, and research trend tracking directly to Google Scholar — no extra tabs, no manual lookups.

★ JOURNAL QUALITY BADGES
See ABDC, FT50, UTD24, VHB, SJR Quartile (Q1–Q4), ERA, ABS, CORE, CCF, Norwegian Register, FNEGE, and Impact Factor rankings as color-coded badges on every search result and author profile page. Instantly identify top-tier publications without leaving Scholar.

★ CITATION VELOCITY & ANALYTICS
Each paper shows citations per year with trend arrows (↑ accelerating, → stable, ↓ decaying). Compare papers not just by total citations but by research momentum.

★ AUTHOR PROFILE METRICS
On any Scholar author page, see computed h-index, m-index, L-index, g-index, hI-norm, self-citation rate, publication quality breakdown (how many Q1, FT50, UTD24 papers), and coauthor analytics — all calculated in real time.

★ ACADEMIC LINEAGE TREES
Trace advisor–student genealogy across generations. See who advised whom in an interactive tree view with 900,000+ researchers from Academic Family Tree, Mathematics Genealogy Project, and institutional repository data.

★ QUERY TREND TRACKER
See how research topics are trending via OpenAlex data. Every search shows related concept velocity (e.g., "Large Language Models ↑ 847%") plus detected methodology keywords.

★ RETRACTION ALERTS
Papers flagged in the Retraction Watch database are marked with a visible warning badge. Never accidentally cite retracted research.

★ QUICK ACTIONS
One-click Save to library, BibTeX export, abstract preview, DOI lookup, and code/dataset link detection (via CatalyzeX and DataCite) — all inline on search results.

★ DARK MODE
Full dark theme that matches Scholar's interface, with all badges and panels adapting automatically.

★ PRO (OPTIONAL)
Everything above is free. Scholar Utility Belt Pro adds author compare, citation lineage, extended bibliometrics, narrative CV, a systematic-review workspace, a citation-graph overlay, and a Publish-or-Perish report. Try it free for 14 days, buy it once or subscribe, or unlock it with an App Pass if you already have one. If you installed before Pro launched, the Pro features that existed then stay free for you forever.

★ PRIVACY-FIRST
All ranking data is bundled locally — no API calls needed for quality badges. External lookups (OpenAlex, Crossref, Unpaywall) only happen when features are enabled and never send personal data. No tracking, no analytics, and no account for any free feature. If you choose Pro, ExtensionPay handles the trial or payment and asks for your email. App Pass is off by default and contacts joinapppass.com only after you turn it on.

SUPPORTED RANKING SYSTEMS:
• ABDC (Australian Business Deans Council) — A*, A, B, C
• FT50 (Financial Times Top 50 Journals)
• UTD24 (UT Dallas Top 24 Business Journals)
• VHB-JOURQUAL (German Academic Association for Business Research) — A+, A, B, C
• SJR / Scopus Quartiles — Q1, Q2, Q3, Q4
• JCR Impact Factor Quartiles
• ERA (Excellence in Research for Australia)
• ABS (Chartered Association of Business Schools) — 4*, 4, 3, 2, 1
• CORE (Computing Research & Education) — A*, A, B, C
• CCF (China Computer Federation) — A, B, C
• Norwegian Register — Level 1, 2
• FNEGE (Fondation Nationale pour l'Enseignement de la Gestion) — 1, 2, 3, 4
• Google Scholar h5-index for venues
• Predatory journal detection (Beall's List + community lists)

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
3. screenshot-3-1280x800.png — Academic lineage tree
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
"The extension requires access to Google Scholar pages to inject quality badges, citation metrics, and research tools into search results and author profiles. API permissions (OpenAlex, Crossref, etc.) are used for real-time citation data, retraction checking, and trend analysis."

### Privacy Policy URL
(Add your privacy policy URL)

### Support URL / Homepage
https://github.com/BenAmpel/ScholarUtilityBelt

---

## Privacy tab: changes to re-check in the Web Store dashboard before submitting 0.7.0

Version 0.7.0 adds two optional flows that touch user data. Review the dashboard's Privacy practices tab (the listing's existing answers stay unless they conflict):

- **Personally identifiable information (email).** ExtensionPay collects an email address when a user starts the free trial or buys Pro. App Pass returns the signed-in user's email to the extension's service worker when they opt in; the extension does not store it (only a status and timestamp are cached locally).
- **Authentication information.** The opt-in App Pass check sends the user's joinapppass.com login cookie to joinapppass.com with each check.
- **Remote hosts contacted for these flows.** extensionpay.com (trial, purchase, entitlement check) and joinapppass.com (App Pass, opt-in only).
- **Certifications.** None of this data is sold, used for purposes unrelated to the extension's single purpose, or used for creditworthiness or lending.
- **Single purpose / permissions.** No manifest permission or host permission changed in 0.7.0.
- **Privacy policy URL.** It must describe the two flows above; the README's "A note on the Pro tier" section has the wording.
