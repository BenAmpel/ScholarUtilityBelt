/**
 * Reference lists for Word or Google Docs: APA 7, MLA 9, Chicago 17 (bibliography form) and IEEE,
 * written by hand for three kinds of source only: journal articles, conference papers and preprints.
 * Anything with no venue gets a minimal "author, year, title, link" form.
 *
 * Honest limits (also shown in the UI):
 *  - Saved papers carry no volume, issue, pages, publisher or editors, so those are never printed.
 *  - Titles are used exactly as saved. APA and IEEE expect sentence case, MLA and Chicago title case;
 *    changing case automatically would break proper nouns and acronyms.
 *  - Author names come from Scholar's author line, which usually gives initials. Initials are used as
 *    they are; a full first name is used only where the style (MLA, Chicago) prints one.
 *  - Written from memory of each style guide, not checked against a style checker. Where a rule was
 *    uncertain the simple common form was used: Chicago prints "Venue (Year)", IEEE prints no
 *    abbreviated journal titles, preprints use the repository name from the venue text.
 *
 * Output is a list of blocks; renderers turn it into plain text or HTML. Italics are the only markup.
 */

import { bibtexEntryType } from "./bibtex.js";
import { escapeHtml, isoDate, lastName, splitName } from "./export-model.js";

export const STYLES = {
  apa: { label: "APA 7", heading: "References" },
  mla: { label: "MLA 9", heading: "Works Cited" },
  chicago: { label: "Chicago 17 (bibliography)", heading: "Bibliography" },
  ieee: { label: "IEEE", heading: "References" },
};

// ---- names -------------------------------------------------------------------------------------

const INITIALS_ONLY = /^[A-Z]{1,3}\.?$/;

function givenWords(given) {
  return String(given || "").split(/\s+/).filter(Boolean);
}

/** "JA" -> ["J","A"], "Jean-Paul" -> ["J-P"], "A." -> ["A"]. */
function initialList(given) {
  const out = [];
  for (const w of givenWords(given)) {
    if (INITIALS_ONLY.test(w)) {
      for (const ch of w.replace(".", "")) out.push(ch);
    } else {
      out.push(w.split("-").map((p) => (p ? p[0].toUpperCase() : "")).filter(Boolean).join("-"));
    }
  }
  return out;
}

function initialsText(given) {
  return initialList(given).map((i) => i.split("-").map((p) => `${p}.`).join("-")).join(" ");
}

/** Given name as printed by MLA and Chicago: the full name when Scholar supplied one, else initials. */
function printedGiven(given) {
  const words = givenWords(given);
  if (!words.length) return "";
  return words.every((w) => INITIALS_ONLY.test(w)) ? initialsText(given) : words.join(" ");
}

const apaName = (a) => { const n = splitName(a); const i = initialsText(n.given); return i ? `${n.family}, ${i}` : n.family; };
const invertedName = (a) => { const n = splitName(a); const g = printedGiven(n.given); return g ? `${n.family}, ${g}` : n.family; };
const directName = (a) => { const n = splitName(a); const g = printedGiven(n.given); return g ? `${g} ${n.family}` : n.family; };
const ieeeName = (a) => { const n = splitName(a); const i = initialsText(n.given); return i ? `${i} ${n.family}` : n.family; };
const familyOf = (a) => splitName(a).family || lastName(a);

// ---- text helpers ------------------------------------------------------------------------------

const clean = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
/** Adds a period unless the text already ends in . ? or ! (also inside a closing quote or bracket). */
const stop = (s) => (/[.?!]["')\]”]?$/.test(s) ? s : `${s}.`);
const i = (text) => ({ i: text });

function server(venue) {
  const list = [[/arxiv/i, "arXiv"], [/biorxiv/i, "bioRxiv"], [/medrxiv/i, "medRxiv"], [/chemrxiv/i, "ChemRxiv"], [/ssrn/i, "SSRN"], [/research square/i, "Research Square"], [/\bosf\b/i, "OSF Preprints"], [/repec/i, "RePEc"], [/zenodo/i, "Zenodo"]];
  for (const [re, name] of list) if (re.test(venue)) return name;
  return clean(venue);
}

function arxivId(venue) {
  const m = /arxiv[:\s]*((?:[a-z-]+\/\d{7})|(?:\d{4}\.\d{4,5}))(?:v\d+)?/i.exec(venue || "");
  return m ? m[1] : "";
}

/** "article" | "conference" | "preprint" | "other" (no venue). */
export function sourceKind(r) {
  if (!clean(r.venue)) return "other";
  const t = bibtexEntryType(r.venue);
  return t === "inproceedings" ? "conference" : t === "misc" ? "preprint" : "article";
}

function link(r, { doiPrefix = "https://doi.org/" } = {}) {
  if (r.doi) return `${doiPrefix}${r.doi}`;
  if (r.url) return r.url;
  const id = arxivId(r.venue);
  return id ? `https://arxiv.org/abs/${id}` : "";
}

function authorsApa(list) {
  const a = list.map(apaName);
  if (!a.length) return "";
  if (a.length === 1) return a[0];
  if (a.length <= 20) return `${a.slice(0, -1).join(", ")}, & ${a[a.length - 1]}`;
  return `${a.slice(0, 19).join(", ")}, . . . ${a[a.length - 1]}`;
}

function authorsMla(list) {
  if (!list.length) return "";
  if (list.length === 1) return invertedName(list[0]);
  if (list.length === 2) return `${invertedName(list[0])}, and ${directName(list[1])}`;
  return `${invertedName(list[0])}, et al.`;
}

function authorsChicago(list) {
  if (!list.length) return "";
  const shown = list.length > 10 ? list.slice(0, 7) : list;
  const names = shown.map((a, k) => (k === 0 ? invertedName(a) : directName(a)));
  if (list.length > 10) return `${names.join(", ")}, et al.`;
  if (names.length === 1) return names[0];
  if (names.length === 2) return `${names[0]}, and ${names[1]}`;
  return `${names.slice(0, -1).join(", ")}, and ${names[names.length - 1]}`;
}

function authorsIeee(list) {
  const a = list.map(ieeeName);
  if (!a.length) return "";
  if (a.length > 6) return `${a[0]} et al.`;
  if (a.length === 1) return a[0];
  if (a.length === 2) return `${a[0]} and ${a[1]}`;
  return `${a.slice(0, -1).join(", ")}, and ${a[a.length - 1]}`;
}

// ---- one reference, as parts --------------------------------------------------------------------

/** Joins groups of parts with single spaces, skipping empty groups. */
function spaced(groups) {
  const out = [];
  for (const g of groups.filter((x) => x && x.length)) {
    if (out.length) out.push(" ");
    out.push(...g);
  }
  return trimParts(out);
}

function apa(r) {
  const kind = sourceKind(r);
  const title = clean(r.title) || "Untitled";
  const who = authorsApa(r.authors);
  const date = [`(${r.year || "n.d."}).`];
  let titleParts = [stop(title)];
  let source = [];
  if (kind === "preprint") {
    titleParts = [i(title), " [Preprint]."];
    source = [stop(server(r.venue))];
  } else if (kind === "conference") {
    source = ["In ", i(clean(r.venue)), "."];
  } else if (kind === "article") {
    source = [i(clean(r.venue)), "."];
  }
  const u = link(r);
  const head = who ? [[stop(who)], date, titleParts] : [titleParts, date];
  return spaced([...head, source, u ? [u] : []]);
}

function mla(r) {
  const kind = sourceKind(r);
  const title = clean(r.title) || "Untitled";
  const who = authorsMla(r.authors);
  const parts = [];
  if (who) parts.push(`${stop(who)} `);
  parts.push(/[?!]$/.test(title) ? `“${title}” ` : `“${title}.” `);
  const tail = [];
  if (clean(r.venue)) tail.push(kind === "preprint" ? server(r.venue) : clean(r.venue));
  if (r.year) tail.push(r.year);
  const u = link(r);
  if (u) tail.push(u);
  if (tail.length) {
    if (kind !== "other") {
      parts.push(i(tail[0]));
      if (tail.length > 1) parts.push(`, ${tail.slice(1).join(", ")}`);
    } else {
      parts.push(tail.join(", "));
    }
    parts.push(".");
  }
  return trimParts(parts);
}

function chicago(r) {
  const kind = sourceKind(r);
  const title = clean(r.title) || "Untitled";
  const who = authorsChicago(r.authors);
  const parts = [];
  if (who) parts.push(`${stop(who)} `);
  parts.push(/[?!]$/.test(title) ? `“${title}” ` : `“${title}.” `);
  const year = r.year || "n.d.";
  if (kind === "preprint") parts.push(`Preprint, ${server(r.venue)}, ${year}.`);
  else if (kind === "conference") parts.push("In ", i(clean(r.venue)), `, ${year}.`);
  else if (kind === "article") parts.push(i(clean(r.venue)), ` (${year}).`);
  else parts.push(`${year}.`);
  const u = link(r);
  if (u) parts.push(` ${u}.`);
  return trimParts(parts);
}

function ieee(r) {
  const kind = sourceKind(r);
  const title = clean(r.title) || "Untitled";
  const who = authorsIeee(r.authors);
  const parts = who ? [`${who}, `] : [];
  const tail = [];
  if (r.year) tail.push(r.year);
  if (kind === "preprint") {
    const id = arxivId(r.venue);
    tail.push(id ? `arXiv:${id}` : server(r.venue));
  }
  if (r.doi) tail.push(`doi: ${r.doi}`);
  const venue = kind === "conference" ? ["in ", i(clean(r.venue))] : kind === "article" ? [i(clean(r.venue))] : [];
  if (!venue.length && !tail.length) {
    parts.push(`\u201c${title}.\u201d`);
  } else {
    parts.push(`\u201c${title},\u201d `, ...venue);
    if (tail.length) parts.push(`${venue.length ? ", " : ""}${tail.join(", ")}`);
    parts.push(".");
  }
  const u = r.doi || arxivId(r.venue) ? "" : link(r, { doiPrefix: "" });
  if (u) parts.push(` [Online]. Available: ${u}`);
  return trimParts(parts);
}

function trimParts(parts) {
  const out = parts.filter((p) => p !== "" && p != null);
  if (typeof out[0] === "string") out[0] = out[0].replace(/^\s+/, "");
  const last = out.length - 1;
  if (typeof out[last] === "string") out[last] = out[last].replace(/\s+$/, "");
  return out;
}

const FORMATTERS = { apa, mla, chicago, ieee };

/** One formatted reference as parts: strings and { i: "italic text" }. */
export function formatParts(style, r) {
  const f = FORMATTERS[style];
  if (!f) throw new Error(`unknown style: ${style}`);
  return f(r);
}

export const partsToPlain = (parts) => parts.map((p) => (typeof p === "string" ? p : p.i)).join("");
export const partsToHtml = (parts) => parts.map((p) => (typeof p === "string" ? escapeHtml(p) : `<i>${escapeHtml(p.i)}</i>`)).join("");

/** Plain-text reference, for the notes export and previews. */
export function formatReference(style, r) {
  return partsToPlain(formatParts(style, r));
}

// ---- in-text citations (for the quote sheet) ----------------------------------------------------

function pageText(page, style) {
  const p = clean(page);
  if (!/^\d+(?:\s*[-–]\s*\d+)?$/.test(p)) return "";
  const range = /[-–]/.test(p);
  const norm = p.replace(/\s*[-–]\s*/, "–");
  if (style === "mla" || style === "chicago") return norm;
  return `${range ? "pp." : "p."} ${norm}`;
}

function authorLabel(style, r) {
  const f = r.authors.map(familyOf);
  if (!f.length) return `“${clean(r.title).split(" ").slice(0, 4).join(" ")}”`;
  const and = style === "apa" ? "&" : "and";
  if (f.length === 1) return f[0];
  if (f.length === 2) return `${f[0]} ${and} ${f[1]}`;
  if (style === "chicago" && f.length === 3) return `${f[0]}, ${f[1]}, and ${f[2]}`;
  return `${f[0]} et al.`;
}

/** Parenthetical citation such as "(Vaswani et al., 2017, p. 3)". `number` is the IEEE list number. */
export function inTextCitation(style, r, page = "", number = 0) {
  const pg = pageText(page, style);
  const year = r.year || "n.d.";
  if (style === "ieee") return `[${number}${pg ? `, ${pg}` : ""}]`;
  const who = authorLabel(style, r);
  if (style === "apa") return `(${who}, ${year}${pg ? `, ${pg}` : ""})`;
  if (style === "mla") return `(${who}${pg ? ` ${pg}` : ""})`;
  return `(${who} ${year}${pg ? `, ${pg}` : ""})`;
}

// ---- the document -------------------------------------------------------------------------------

/**
 * @param {object[]} records export records in output order (use sortRecords)
 * @param {{style:string, includeQuotes?:boolean, now?:Date}} opts
 * @returns {{blocks:object[], style:string, count:number}}
 */
export function buildReferenceBlocks(records, opts) {
  const style = opts.style;
  if (!STYLES[style]) throw new Error(`unknown style: ${style}`);
  const hanging = style !== "ieee";
  const blocks = [{ kind: "h", parts: [STYLES[style].heading] }];
  records.forEach((r, k) => {
    const parts = formatParts(style, r);
    blocks.push({ kind: "ref", parts: style === "ieee" ? [`[${k + 1}] `, ...parts] : parts, hanging });
  });
  if (opts.includeQuotes) {
    const quoteBlocks = [];
    records.forEach((r, k) => {
      const n = k + 1;
      if (!r.notes && !r.summary && !r.highlights.length) return;
      quoteBlocks.push({ kind: "h2", parts: [clean(r.title) || "Untitled", ` ${inTextCitation(style, r, "", n)}`] });
      if (r.notes) quoteBlocks.push({ kind: "p", parts: ["Notes: ", r.notes] });
      if (r.summary) quoteBlocks.push({ kind: "p", parts: ["Summary: ", r.summary] });
      for (const h of r.highlights) {
        const note = h.note ? ` Note: ${h.note}` : "";
        quoteBlocks.push({ kind: "quote", parts: [`“${clean(h.quote)}” ${inTextCitation(style, r, h.page, n)}${note}`] });
      }
    });
    if (quoteBlocks.length) blocks.push({ kind: "h", parts: ["Quotes and notes"] }, ...quoteBlocks);
  }
  return { blocks, style, count: records.length };
}

export function blocksToPlain(blocks) {
  return blocks.map((b) => {
    const text = partsToPlain(b.parts);
    return b.kind === "quote" ? `    ${text}` : text;
  }).join("\n\n") + "\n";
}

const HANGING = "margin-left:0.5in;text-indent:-0.5in;";

/** HTML fragment: inline styles only (it is pasted into Word or Docs, where classes would be lost). */
export function blocksToHtml(blocks) {
  return blocks.map((b) => {
    const h = partsToHtml(b.parts).replace(/\r?\n/g, "<br>");
    if (b.kind === "h") return `<h2 style="font-size:14pt;font-weight:bold;margin:12pt 0 6pt">${h}</h2>`;
    if (b.kind === "h2") return `<p style="margin:10pt 0 2pt"><b>${h}</b></p>`;
    if (b.kind === "quote") return `<p style="margin:2pt 0 2pt 0.5in">${h}</p>`;
    if (b.kind === "ref") return `<p style="margin-top:0;margin-bottom:6pt;${b.hanging ? HANGING : ""}">${h}</p>`;
    return `<p style="margin:2pt 0">${h}</p>`;
  }).join("\n");
}

/** A whole HTML document that Word opens as a .doc file. */
export function buildDocHtml(blocks, { title = "References", now = new Date() } = {}) {
  return `<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">
<head>
<meta charset="utf-8">
<title>${escapeHtml(title)}</title>
<style>body{font-family:"Times New Roman",serif;font-size:12pt;line-height:1.4}</style>
</head>
<body>
<!-- Exported ${isoDate(now)} from Scholar Utility Belt -->
${blocksToHtml(blocks)}
</body>
</html>
`;
}
