/**
 * "Open in Overleaf" for the LaTeX citation pack: builds one main.tex that carries the .bib inside
 * itself (a filecontents* block), so a single form field is enough.
 *
 * WHAT THIS RELIES ON FROM MEMORY (UNVERIFIED, cannot be called from the build environment):
 *  - Overleaf accepts an HTML form POST to https://www.overleaf.com/docs and opens a new project
 *    from it. Overleaf has no project-upload API; this is its "Open in Overleaf" mechanism.
 *  - The LaTeX source goes in a field named `snip` (a plain, unencoded string). A URL-encoded
 *    variant `encoded_snip` also exists; `snip` is used here.
 *  - Optional fields: `engine` (pdflatex, xelatex, lualatex, latex_dvipdf) and `snip_name`
 *    (the file name of the snippet, main.tex by default).
 *  - A filecontents* block before \documentclass writes refs.bib on the first compile, and
 *    Overleaf then runs BibTeX so \bibliography{refs} resolves. Current LaTeX kernels provide
 *    filecontents*; older ones need the filecontents package, which Overleaf's TeX Live has.
 * If Overleaf changes or drops any of these, the form simply opens a page that does not create a
 * project. Nothing here can send data anywhere except by the form the user submits.
 *
 * The Overleaf page is NOT in url-policy.js's ALLOWED_API_HOSTS because it is a user-initiated
 * form navigation in a new tab, not a fetch by the extension.
 */

import { buildBib } from "./latex-pack.js";
import { sanitizeBibKey } from "./bibtex.js";
import { lastName } from "./export-model.js";

export const OVERLEAF_URL = "https://www.overleaf.com/docs";
export const OVERLEAF_HOST = "www.overleaf.com";
/** Our own conservative cap on the source sent in one form (UTF-8 bytes). Overleaf's real limit is not known. */
export const MAX_SOURCE_BYTES = 400000;
const MAX_NOTE_CHARS = 1500;

const LATEX_SPECIAL = {
  "\\": "\\textbackslash{}",
  "&": "\\&",
  "%": "\\%",
  "$": "\\$",
  "#": "\\#",
  "_": "\\_",
  "{": "\\{",
  "}": "\\}",
  "~": "\\textasciitilde{}",
  "^": "\\textasciicircum{}",
};

/** Escapes text for LaTeX body use. Control characters and line breaks become spaces. */
export function escapeLatex(value) {
  let out = "";
  for (const ch of String(value == null ? "" : value)) {
    if (LATEX_SPECIAL[ch]) out += LATEX_SPECIAL[ch];
    else if (ch < " " || ch === "\u007f") out += " ";
    else out += ch;
  }
  return out.replace(/ {2,}/g, " ").trim();
}

function byteLength(s) {
  return new TextEncoder().encode(s).length;
}

function describe(r) {
  const who = r.authors.length ? (r.authors.length > 2 ? `${lastName(r.authors[0])} et al.` : r.authors.map(lastName).join(" and ")) : "Unknown";
  return `${who} (${r.year || "n.d."}). ${r.title || "Untitled"}`;
}

/**
 * @param {object[]} records export-model records with `citekey`
 * @param {{ includeNotes?: boolean, maxBytes?: number }} [opts] notes are left out unless includeNotes is true
 * @returns {{ ok: boolean, reason?: "empty"|"too-large", source: string, bytes: number, count: number, notesIncluded: number }}
 */
export function buildOverleafMain(records, opts = {}) {
  const includeNotes = opts.includeNotes === true;
  const maxBytes = opts.maxBytes || MAX_SOURCE_BYTES;
  const list = [...(records || [])].filter((r) => r && r.citekey).sort((a, b) => (a.citekey < b.citekey ? -1 : a.citekey > b.citekey ? 1 : 0));
  if (!list.length) return { ok: false, reason: "empty", source: "", bytes: 0, count: 0, notesIncluded: 0 };

  // A .bib text can never legitimately contain this; refuse to let it close the block early.
  const bib = buildBib(list).replace(/\\end\s*\{filecontents\*?\}/gi, "");
  const keys = list.map((r) => sanitizeBibKey(r.citekey));
  let notesIncluded = 0;
  const items = list.map((r) => {
    let line = `\\item ${escapeLatex(describe(r))} (\\texttt{${escapeLatex(sanitizeBibKey(r.citekey))}})`;
    if (includeNotes && r.notes) {
      notesIncluded++;
      line += `\\\\\n  \\emph{Note:} ${escapeLatex(String(r.notes).slice(0, MAX_NOTE_CHARS))}`;
    }
    return line;
  });

  const source = [
    "% Created by Scholar Utility Belt from the papers you chose in your library.",
    "\\begin{filecontents*}{refs.bib}",
    bib.replace(/\n+$/, ""),
    "\\end{filecontents*}",
    "\\documentclass{article}",
    "\\usepackage[utf8]{inputenc}",
    "\\usepackage[T1]{fontenc}",
    "\\usepackage{url}",
    "\\begin{document}",
    "\\section*{Chosen papers}",
    "\\begin{itemize}",
    ...items,
    "\\end{itemize}",
    "\\section*{Citations}",
    `\\cite{${keys.join(",")}}`,
    "\\bibliographystyle{plain}",
    "\\bibliography{refs}",
    "\\end{document}",
    "",
  ].join("\n");

  const bytes = byteLength(source);
  if (bytes > maxBytes) return { ok: false, reason: "too-large", source: "", bytes, count: list.length, notesIncluded: 0 };
  return { ok: true, source, bytes, count: list.length, notesIncluded };
}

/** The form to submit: action URL and the fields, in order. Nothing else is sent. */
export function overleafFormFields(source) {
  return {
    action: OVERLEAF_URL,
    method: "post",
    target: "_blank",
    fields: [
      ["snip", String(source)],
      ["snip_name", "main.tex"],
      ["engine", "pdflatex"],
    ],
  };
}

/** Plain-language list of what leaves the browser, for the confirmation step. */
export function overleafDisclosure(result, { includeNotes = false } = {}) {
  const parts = [
    `${result.count} ${result.count === 1 ? "paper" : "papers"}: titles, authors, years, venues, DOIs, links, tags and saved abstracts (all of it is in the text below).`,
    includeNotes
      ? `Your private notes on ${result.notesIncluded} ${result.notesIncluded === 1 ? "paper" : "papers"}.`
      : "Your private notes are not included.",
  ];
  return parts;
}

/**
 * Builds and submits the form. Call only from a user click handler.
 * @param {string} source
 * @param {Document} doc
 */
export function submitToOverleaf(source, doc = globalThis.document) {
  const spec = overleafFormFields(source);
  const form = doc.createElement("form");
  form.method = spec.method;
  form.action = spec.action;
  form.target = spec.target;
  form.acceptCharset = "UTF-8";
  for (const [name, value] of spec.fields) {
    const input = doc.createElement("input");
    input.type = "hidden";
    input.name = name;
    input.value = value;
    form.appendChild(input);
  }
  doc.body.appendChild(form);
  form.submit();
  form.remove();
}
