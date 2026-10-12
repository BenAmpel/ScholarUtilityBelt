/**
 * Which URLs and callers the service worker will act for. The worker fetches pages for the
 * content script, so a bug or compromise there must not turn it into an open fetcher.
 *
 * - isScholarUrl: a real Scholar host (scholar.google.<tld>), not a lookalike such as
 *   scholar.google.evil.example, which the old startsWith check accepted. Cookies are sent only to these.
 * - isAllowedApiUrl: the public scholarly APIs the extension queries, over https. fetchExternal
 *   accepts nothing else.
 * - isOwnContext: the message came from this extension, from one of its pages or from a content
 *   script running on a Scholar page.
 *
 * Built as a global (SUUrlPolicy) for the classic service worker, like entitlement.js.
 */

/** Hosts fetchExternal may reach. Keep in step with host_permissions in manifest.template.json. */
export const ALLOWED_API_HOSTS = [
  "api.crossref.org",
  "api.openalex.org",
  "www.scimagojr.com",
  "api.unpaywall.org",
  "api.datacite.org",
  "opencitations.net",
  "export.arxiv.org",
  "www.ebi.ac.uk",
  "eutils.ncbi.nlm.nih.gov",
  "api.ror.org",
  "dblp.org",
  "sparql.dblp.org",
  "api.semanticscholar.org",
  "icite.od.nih.gov",
];

const SCHOLAR_HOST = /^scholar\.google\.(?:[a-z]{2,3}|co\.[a-z]{2}|com\.[a-z]{2,3})$/;

function parse(url) {
  try {
    return new URL(String(url));
  } catch {
    return null;
  }
}

export function isFetchableUrl(url) {
  const u = parse(url);
  return !!u && (u.protocol === "http:" || u.protocol === "https:");
}

export function isScholarUrl(url) {
  const u = parse(url);
  return !!u && u.protocol === "https:" && SCHOLAR_HOST.test(u.hostname.toLowerCase());
}

export function isAllowedApiUrl(url) {
  const u = parse(url);
  return !!u && u.protocol === "https:" && !u.username && !u.password && ALLOWED_API_HOSTS.includes(u.hostname.toLowerCase());
}

/**
 * @param {{ id?: string, url?: string, tab?: { url?: string } }} sender chrome.runtime.MessageSender
 * @param {string} extensionId chrome.runtime.id
 * @param {string} extensionOrigin chrome.runtime.getURL("") (ends with a slash)
 */
export function isOwnContext(sender, extensionId, extensionOrigin) {
  if (!sender || !extensionId || sender.id !== extensionId) return false;
  const url = String(sender.url || "");
  if (extensionOrigin && url.startsWith(extensionOrigin)) return true;
  return isScholarUrl(url) || isScholarUrl(sender.tab && sender.tab.url);
}
