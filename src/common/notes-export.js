/**
 * Highlights and notes document: per paper, the citation, the user's notes and every stored PDF
 * highlight as a quote. Markdown or HTML. Free feature; pure functions over export records.
 */

import { escapeHtml, isoDate } from "./export-model.js";

/** Plain "Authors (Year). Title. Venue." line used when the caller supplies no formatter. */
export function basicCitation(r) {
  const authors = r.authors.length ? r.authors.join(", ") : "Unknown author";
  const parts = [`${authors} (${r.year || "n.d."}). ${r.title || "Untitled"}.`];
  if (r.venue) parts.push(`${r.venue}.`);
  return parts.join(" ");
}

/** Backslash-escape the characters that change Markdown meaning in running text. */
export function escapeMd(s) {
  return String(s ?? "").replace(/([\\`*_[\]<>#])/g, "\\$1").replace(/\s+/g, " ").trim();
}

function quoteBlock(text) {
  return String(text).split(/\r?\n/).map((l) => `> ${l}`.trimEnd()).join("\n");
}

function pageLabel(page) {
  if (!page) return "";
  return /^[\d\s,–-]+$/.test(page) ? `p. ${page}` : page;
}

/**
 * @param {object[]} records export records
 * @param {{scope?: string, now?: Date, cite?: (r:object)=>string}} [opts]
 */
export function buildNotesMarkdown(records, opts = {}) {
  const cite = opts.cite || basicCitation;
  const out = [`# Highlights and notes: ${escapeMd(opts.scope || "All saved papers")}`, ""];
  out.push(`${records.length} ${records.length === 1 ? "paper" : "papers"}. Exported ${isoDate(opts.now || new Date())} from Scholar Utility Belt.`, "");
  records.forEach((r, i) => {
    out.push(`## ${i + 1}. ${escapeMd(r.title || "Untitled")}`, "");
    out.push(escapeMd(cite(r)), "");
    if (r.doi) out.push(`DOI: https://doi.org/${r.doi}`, "");
    let any = false;
    if (r.notes) { any = true; out.push("**Notes**", "", r.notes, ""); }
    if (r.summary) { any = true; out.push("**Summary**", "", r.summary, ""); }
    if (r.highlights.length) {
      any = true;
      out.push("**Highlights**", "");
      for (const h of r.highlights) {
        out.push(quoteBlock(h.quote));
        const tail = [pageLabel(h.page), h.note ? `Note: ${h.note}` : ""].filter(Boolean).join(". ");
        if (tail) out.push(">", `> ${tail}`);
        out.push("");
      }
    }
    if (!any) out.push("No notes or highlights saved.", "");
  });
  return out.join("\n").replace(/\n{3,}/g, "\n\n").trimEnd() + "\n";
}

function paragraphs(text) {
  return String(text).split(/\n{2,}/).map((p) => `<p>${escapeHtml(p).replace(/\n/g, "<br>")}</p>`).join("\n");
}

/** Standalone HTML file (own inline style: it is opened from disk, not inside the extension). */
export function buildNotesHtml(records, opts = {}) {
  const cite = opts.cite || basicCitation;
  const scope = opts.scope || "All saved papers";
  const parts = [];
  records.forEach((r, i) => {
    const bits = [`<section>`, `<h2>${i + 1}. ${escapeHtml(r.title || "Untitled")}</h2>`, `<p class="cite">${escapeHtml(cite(r))}</p>`];
    if (r.doi) bits.push(`<p class="doi">DOI: <a href="https://doi.org/${escapeHtml(r.doi)}">${escapeHtml(r.doi)}</a></p>`);
    let any = false;
    if (r.notes) { any = true; bits.push("<h3>Notes</h3>", paragraphs(r.notes)); }
    if (r.summary) { any = true; bits.push("<h3>Summary</h3>", paragraphs(r.summary)); }
    if (r.highlights.length) {
      any = true;
      bits.push("<h3>Highlights</h3>");
      for (const h of r.highlights) {
        const tail = [pageLabel(h.page), h.note ? `Note: ${h.note}` : ""].filter(Boolean).join(". ");
        bits.push(`<blockquote>${paragraphs(h.quote)}${tail ? `<footer>${escapeHtml(tail)}</footer>` : ""}</blockquote>`);
      }
    }
    if (!any) bits.push(`<p class="none">No notes or highlights saved.</p>`);
    bits.push("</section>");
    parts.push(bits.join("\n"));
  });
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Highlights and notes: ${escapeHtml(scope)}</title>
<style>
body{font:16px/1.5 Georgia,serif;max-width:46rem;margin:2rem auto;padding:0 1rem;color:#1a1918}
h1{font-size:1.5rem}h2{font-size:1.15rem;margin-bottom:.2rem}h3{font-size:.95rem;margin:1rem 0 .2rem}
section{border-top:1px solid #ccc;padding-top:1rem;margin-top:1.5rem}
.cite{margin:.2rem 0}.doi,.none{color:#555;font-size:.9rem}
blockquote{margin:.6rem 0;padding:.1rem 1rem;border-left:3px solid #aaa}
blockquote footer{color:#555;font-size:.85rem}
</style>
</head>
<body>
<h1>Highlights and notes: ${escapeHtml(scope)}</h1>
<p>${records.length} ${records.length === 1 ? "paper" : "papers"}. Exported ${isoDate(opts.now || new Date())} from Scholar Utility Belt.</p>
${parts.join("\n")}
</body>
</html>
`;
}
