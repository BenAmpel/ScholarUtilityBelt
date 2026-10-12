/**
 * LaTeX citation pack: stable citekeys, a .bib, a copy-able \cite list, and a regex-based check of
 * pasted .tex text (or a list of keys) against the saved papers.
 *
 * The check is a text scan, not a LaTeX parser. It does not expand macros, \input files or
 * multi-argument commands such as \parencites(...)[...]{a}{b}; those are missed silently.
 */

import { buildBibTeXEntry } from "./bibtex.js";
import { splitName, lastName } from "./export-model.js";

const STOP_WORDS = new Set(["a", "an", "the", "of", "on", "in", "for", "and", "to", "with", "from", "at", "by", "is", "are", "do", "does", "how", "what", "why", "toward", "towards", "using", "via"]);

function asciiWord(s) {
  return String(s || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/[^A-Za-z0-9]+/g, "").toLowerCase();
}

/** Candidate key: family name + year + first meaningful title word, e.g. vaswani2017attention. */
export function baseCiteKey(r) {
  const fam = asciiWord(r.authors && r.authors.length ? splitName(r.authors[0]).family || lastName(r.authors[0]) : "") || "anon";
  const year = /^\d{4}$/.test(r.year || "") ? r.year : "nd";
  const word = String(r.title || "").split(/\s+/).map(asciiWord).find((w) => w && !STOP_WORDS.has(w)) || "";
  return `${fam}${year}${word.slice(0, 16)}`;
}

/**
 * Citekeys for papers. A paper that already has `citekey` keeps it, always: that is the stability
 * guarantee. New keys avoid every existing key (case-insensitively) by adding a, b, c...
 *
 * @param {object[]} papers saved papers or records; need `key`, optional `citekey`
 * @param {(paper:object)=>object} [toRec] maps a paper to a record (authors, year, title); identity by default
 * @param {Iterable<string>} [alsoTaken] keys held by papers outside `papers`
 * @returns {{keys: Map<string,string>, added: {key:string, citekey:string}[]}} keys: storage key -> citekey
 */
export function assignCitekeys(papers, toRec = (p) => p, alsoTaken = []) {
  const taken = new Set([...alsoTaken].map((k) => String(k).toLowerCase()));
  const keys = new Map();
  for (const p of papers) {
    if (p.citekey) {
      keys.set(p.key, p.citekey);
      taken.add(String(p.citekey).toLowerCase());
    }
  }
  const added = [];
  const todo = papers.filter((p) => !p.citekey).sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  for (const p of todo) {
    const base = baseCiteKey(toRec(p));
    let candidate = base;
    for (let i = 0; taken.has(candidate.toLowerCase()); i++) {
      candidate = base + (i < 26 ? String.fromCharCode(97 + i) : `${i}`);
    }
    taken.add(candidate.toLowerCase());
    keys.set(p.key, candidate);
    added.push({ key: p.key, citekey: candidate });
  }
  return { keys, added };
}

function bibAuthor(a) {
  const { family, given } = splitName(a);
  return given ? `${family}, ${given}` : family;
}

/** .bib text for records that carry a citekey, sorted by key. Private notes are never written. */
export function buildBib(records) {
  return [...records]
    .filter((r) => r.citekey)
    .sort((a, b) => (a.citekey < b.citekey ? -1 : a.citekey > b.citekey ? 1 : 0))
    .map((r) => buildBibTeXEntry({
      key: r.citekey, title: r.title, authors: r.authors.map(bibAuthor), year: r.year, venue: r.venue,
      doi: r.doi, url: r.url, tags: r.tags, abstract: r.abstract,
    }))
    .join("\n\n") + (records.some((r) => r.citekey) ? "\n" : "");
}

/** Text to copy: one \cite{...} line with every key, then one commented line per key. */
export function buildCiteList(records) {
  const withKey = [...records].filter((r) => r.citekey).sort((a, b) => (a.citekey < b.citekey ? -1 : 1));
  if (!withKey.length) return "";
  const lines = [`\\cite{${withKey.map((r) => r.citekey).join(",")}}`, ""];
  for (const r of withKey) {
    const who = r.authors.length ? (r.authors.length > 2 ? `${lastName(r.authors[0])} et al.` : r.authors.map(lastName).join(" and ")) : "Unknown";
    lines.push(`% \\cite{${r.citekey}}  ${who} (${r.year || "n.d."}). ${r.title || "Untitled"}`);
  }
  return lines.join("\n") + "\n";
}

/** Removes % comments (not \%) and \begin{comment}...\end{comment} blocks. */
export function stripLatexComments(text) {
  const noEnv = String(text || "").replace(/\\begin\{comment\}[\s\S]*?\\end\{comment\}/g, "");
  return noEnv
    .split(/\r?\n/)
    .map((line) => {
      for (let i = 0; i < line.length; i++) {
        if (line[i] === "\\") { i++; continue; }
        if (line[i] === "%") return line.slice(0, i);
      }
      return line;
    })
    .join("\n");
}

// \cite, \citep, \citet, \citealp, \Cite..., \parencite, \textcite, \autocite, \footcite, \smartcite, \nocite,
// with an optional star and up to two optional [..] arguments, then one {..} list.
const CITE_RE = /\\(?:[Cc]ite[A-Za-z]*|[Pp]arencite[A-Za-z]*|[Tt]extcite|[Aa]utocite[A-Za-z]*|[Ff]ootcite[A-Za-z]*|[Ss]martcite|[Ss]upercite|[Nn]ocite)\*?\s*(?:\[[^\]]*\]\s*){0,2}\{([^{}]*)\}/g;

/**
 * Keys cited in .tex text, in first-seen order. `wildcard` is true for \nocite{*}.
 * Without any backslash the input is read as a plain list of keys (comma, space or line separated;
 * lines starting with % are ignored).
 */
export function extractCiteKeys(input) {
  const raw = String(input || "");
  const keys = [];
  const seen = new Set();
  let wildcard = false;
  const add = (k) => {
    const key = k.trim();
    if (!key) return;
    if (key === "*") { wildcard = true; return; }
    if (!seen.has(key)) { seen.add(key); keys.push(key); }
  };
  if (!raw.includes("\\")) {
    stripLatexComments(raw).split(/[\s,;]+/).forEach(add);
    return { keys, wildcard };
  }
  const text = stripLatexComments(raw);
  for (const m of text.matchAll(CITE_RE)) m[1].split(",").forEach(add);
  return { keys, wildcard };
}

/**
 * @param {string} input pasted .tex text or key list
 * @param {object[]} records records with `citekey`
 * @returns {{cited:string[], missing:{key:string, suggestion:string}[], uncited:object[], wildcard:boolean}}
 */
export function checkCitations(input, records) {
  const { keys, wildcard } = extractCiteKeys(input);
  const have = new Map();
  for (const r of records) if (r.citekey) have.set(r.citekey, r);
  const lower = new Map([...have.keys()].map((k) => [k.toLowerCase(), k]));
  const missing = keys.filter((k) => !have.has(k)).map((k) => ({ key: k, suggestion: lower.get(k.toLowerCase()) || "" }));
  const cited = new Set(keys);
  const uncited = wildcard ? [] : records.filter((r) => r.citekey && !cited.has(r.citekey));
  return { cited: keys, missing, uncited, wildcard };
}
