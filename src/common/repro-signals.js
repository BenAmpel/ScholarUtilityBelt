/**
 * Reproducibility signals found in a Google Scholar result: links to code,
 * links to data or materials, a preregistration mention, and dataset DOIs.
 * Pure and local. The result is a list of "signals found", never a verdict:
 * a repository link says nothing about whether the work can be reproduced.
 *
 * Host lists and DOI prefixes below are written from memory of how these
 * services are commonly identified, not from a registry lookup. They are
 * heuristics and should be treated as such:
 *   Zenodo 10.5281, figshare 10.6084, Dryad 10.5061, Harvard Dataverse 10.7910,
 *   OSF 10.17605.
 *
 * content.js already has detectArtifacts() behind the Data & Code badge and the
 * "Code" filter. It reuses CODE_HOST_REGEXES and DATA_HOST_REGEXES from here so
 * there is one list of hosts, and this module adds the finer-grained result.
 */

export const SIGNALS_NOTE =
  "Signals found in this result's snippet and links. This does not show that the work is reproducible.";

export const CODE_HOSTS = [
  { id: "github", label: "GitHub", hosts: ["github.com"] },
  { id: "gitlab", label: "GitLab", hosts: ["gitlab.com"] },
  { id: "bitbucket", label: "Bitbucket", hosts: ["bitbucket.org"] },
  { id: "huggingface", label: "Hugging Face", hosts: ["huggingface.co"] },
];

export const DATA_HOSTS = [
  { id: "osf", label: "OSF", hosts: ["osf.io"] },
  { id: "zenodo", label: "Zenodo", hosts: ["zenodo.org"] },
  { id: "figshare", label: "figshare", hosts: ["figshare.com"] },
  { id: "dryad", label: "Dryad", hosts: ["datadryad.org", "dryad.org"] },
  { id: "dataverse", label: "Dataverse", hosts: ["dataverse.harvard.edu", "dataverse.org"] },
  { id: "kaggle", label: "Kaggle", hosts: ["kaggle.com"] },
];

const DATASET_DOI_PREFIXES = [
  { prefix: "10.5281", label: "Zenodo" },
  { prefix: "10.6084", label: "figshare" },
  { prefix: "10.5061", label: "Dryad" },
  { prefix: "10.7910", label: "Harvard Dataverse" },
  { prefix: "10.17605", label: "OSF" },
];

function escapeRe(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function hostRegexes(list) {
  return list.flatMap((h) => h.hosts.map((host) => new RegExp(`(?:^|[^a-z0-9-])${escapeRe(host)}\\b`, "i")));
}

/** Regexes for detectArtifacts() in content.js, built from the same host lists. */
export const CODE_HOST_REGEXES = hostRegexes(CODE_HOSTS);
export const DATA_HOST_REGEXES = hostRegexes(DATA_HOSTS);

function hostOf(href) {
  try {
    return new URL(href).hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    return "";
  }
}

function matchHost(host, list) {
  if (!host) return null;
  return list.find((h) => h.hosts.some((x) => host === x || host.endsWith("." + x))) || null;
}

const URL_IN_TEXT_RE = /\bhttps?:\/\/[^\s<>"')\]]+|\b(?:github\.com|gitlab\.com|bitbucket\.org|huggingface\.co|osf\.io|zenodo\.org|figshare\.com|datadryad\.org|kaggle\.com|aspredicted\.org)\/[^\s<>"')\]]+/gi;
const DOI_RE = /\b10\.\d{4,9}\/[^\s"'<>),;\]]+/gi;
const NCT_RE = /\bNCT\d{8}\b/g;
const PROSPERO_RE = /\bCRD\d{8,11}\b/g;
const PREREG_WORD_RE = /\b(?:pre-?regist\w*|registered report)\b/i;
// "not preregistered" and similar negations are a statement about the study, so they are not a signal.
const PREREG_NEGATED_RE = /\b(?:not|never|without|no)\s+(?:\w+\s+)?pre-?regist/i;

function cleanUrl(u) {
  return String(u).replace(/[.,;:!?)\]]+$/, "");
}

function normalizeDoi(d) {
  return String(d).replace(/[.,;:!?)\]]+$/, "").toLowerCase();
}

/**
 * @param {{text?: string, links?: {href: string, role?: string}[], ownDoi?: string}} input
 *   `links` with role "title" or "file" (the result's own title and its [PDF]/[HTML] column) are ignored:
 *   many papers are themselves hosted on Zenodo or OSF, and that is not a data link.
 *   `ownDoi` is the result's own DOI, which is never reported as a dataset DOI.
 */
export function detectReproSignals(rawInput) {
  const input = rawInput && typeof rawInput === "object" ? rawInput : {};
  const text = String(input.text || "");
  const ownDoi = typeof input.ownDoi === "string" && input.ownDoi ? normalizeDoi(input.ownDoi.replace(/^https?:\/\/(?:dx\.)?doi\.org\//i, "")) : "";
  const urls = [];
  const seenUrl = new Set();
  const addUrl = (u) => {
    const c = cleanUrl(u);
    if (!/^https?:\/\//i.test(c)) {
      const withScheme = "https://" + c;
      if (!seenUrl.has(withScheme)) { seenUrl.add(withScheme); urls.push(withScheme); }
      return;
    }
    if (!seenUrl.has(c)) { seenUrl.add(c); urls.push(c); }
  };
  for (const l of Array.isArray(input.links) ? input.links : []) {
    if (!l || typeof l.href !== "string") continue;
    if (l.role === "title" || l.role === "file") continue;
    addUrl(l.href);
  }
  for (const m of text.matchAll(URL_IN_TEXT_RE)) addUrl(m[0]);

  const code = [];
  const data = [];
  const prereg = [];
  const datasetDois = [];
  const seen = new Set();
  const push = (arr, key, item) => {
    if (seen.has(key)) return;
    seen.add(key);
    arr.push(item);
  };

  for (const url of urls) {
    const host = hostOf(url);
    let path = "";
    try { path = new URL(url).pathname; } catch { /* host stays empty */ }
    const c = matchHost(host, CODE_HOSTS);
    if (c) {
      // A bare host or a single path segment (a user page) is not a repository link.
      const segs = path.split("/").filter(Boolean);
      if (c.id === "huggingface" ? segs.length >= 1 : segs.length >= 2) {
        push(code, `code:${c.id}`, { kind: c.id, label: c.label, url });
      }
      continue;
    }
    if (host === "aspredicted.org" || host.endsWith(".aspredicted.org")) {
      push(prereg, "prereg:aspredicted", { kind: "aspredicted", label: "AsPredicted", url });
      continue;
    }
    if (host === "osf.io" || host.endsWith(".osf.io")) {
      if (/^\/(?:registries|prereg)\b/i.test(path)) {
        push(prereg, "prereg:osf", { kind: "osf-registry", label: "OSF Registries", url });
        continue;
      }
    }
    if (host === "clinicaltrials.gov" || host.endsWith(".clinicaltrials.gov")) {
      const id = (url.match(NCT_RE) || [])[0];
      push(prereg, `prereg:${id || "ctgov"}`, { kind: "clinicaltrials", label: id ? `ClinicalTrials.gov ${id}` : "ClinicalTrials.gov", url });
      continue;
    }
    if (host.endsWith("crd.york.ac.uk")) {
      const id = (url.match(PROSPERO_RE) || [])[0];
      push(prereg, `prereg:${id || "prospero"}`, { kind: "prospero", label: id ? `PROSPERO ${id}` : "PROSPERO", url });
      continue;
    }
    const d = matchHost(host, DATA_HOSTS);
    if (d) {
      // A paper hosted on Zenodo or OSF often links its own record; require a path so a bare host does not count.
      if (path.split("/").filter(Boolean).length >= 1) push(data, `data:${d.id}`, { kind: d.id, label: d.label, url });
    }
  }

  for (const id of text.match(NCT_RE) || []) {
    push(prereg, `prereg:${id}`, { kind: "clinicaltrials", label: `ClinicalTrials.gov ${id}`, url: `https://clinicaltrials.gov/study/${id}` });
  }
  for (const id of text.match(PROSPERO_RE) || []) {
    push(prereg, `prereg:${id}`, { kind: "prospero", label: `PROSPERO ${id}`, url: "" });
  }
  if (PREREG_WORD_RE.test(text) && !PREREG_NEGATED_RE.test(text) && !prereg.length) {
    prereg.push({ kind: "mention", label: "Preregistration mentioned", url: "" });
  }

  // Dataset DOIs from the snippet and from link targets (doi.org/10.xxxx/...).
  const doiHaystack = [text, ...urls].join(" ");
  for (const raw of doiHaystack.match(DOI_RE) || []) {
    const doi = normalizeDoi(raw);
    if (ownDoi && doi === ownDoi) continue;
    const hit = DATASET_DOI_PREFIXES.find((p) => doi.startsWith(p.prefix + "/"));
    if (hit) push(datasetDois, `doi:${doi}`, { doi, label: `${hit.label} DOI`, url: `https://doi.org/${doi}` });
  }

  return { code, data, prereg, datasetDois };
}

/** Flat chip list for display: [{id, label, tip, url}]. Empty when nothing was found. */
export function reproChips(signals) {
  const chips = [];
  if (!signals) return chips;
  for (const c of signals.code || []) chips.push({ id: `code-${c.kind}`, label: `Code: ${c.label}`, tip: `Link to ${c.label} found. ${SIGNALS_NOTE}`, url: c.url });
  for (const d of signals.data || []) chips.push({ id: `data-${d.kind}`, label: `Data: ${d.label}`, tip: `Link to ${d.label} found. ${SIGNALS_NOTE}`, url: d.url });
  for (const p of signals.prereg || []) {
    chips.push({
      id: `prereg-${p.kind}`,
      label: p.kind === "mention" ? p.label : `Prereg: ${p.label}`,
      tip: `${p.kind === "mention" ? "The snippet mentions preregistration" : `${p.label} found`}. ${SIGNALS_NOTE}`,
      url: p.url || "",
    });
  }
  for (const d of signals.datasetDois || []) chips.push({ id: "dataset-doi", label: `Dataset DOI (${d.label.replace(/ DOI$/, "")})`, tip: `A DOI with a ${d.label} prefix was found: ${d.doi}. ${SIGNALS_NOTE}`, url: d.url });
  return chips;
}
