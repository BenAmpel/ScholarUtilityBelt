# Store listing: cross-sell paragraph

Paragraph to append to the end of the Chrome Web Store detailed description, after the "Open source" line. Plain text, 490 characters (limit set for this paragraph: 500).

```text
From the same studio: Purplelink runs free web tools for academic writing. BibTeX Validator checks a .bib file for syntax errors and dead DOIs: https://purplelink.llc/tools/bib-validator/?ref=scholar-utility-belt Paper Review is a pre-submission manuscript review by four AI reviewers, from $9: https://purplelink.llc/tools/paper-review/?ref=scholar-utility-belt More tools: https://purplelink.llc/tools/?ref=scholar-utility-belt Separate sites; nothing from them is added to Scholar pages.
```

## Notes

- Updating the store listing is the owner's step. Paste the paragraph in the Chrome Web Store Developer Dashboard (Store listing, Description) and submit it with the 0.6.2 package or on its own.
- The store description is plain text, so the URLs are written out in full. They carry the same `?ref=scholar-utility-belt` query as the in-extension links (Options page, what's-new page) and the README. The query holds no personal data.
- The links appear only in the store description, the README, the Options page, and the local what's-new page. Nothing is injected into Google Scholar pages, and the extension makes no request to purplelink.llc.
- No new permission, host, or data use was added, so the listing's privacy tab and single-purpose statement need no change.
