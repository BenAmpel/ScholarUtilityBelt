/**
 * Author-profile-specific module: OpenAlex self-citation analysis.
 *
 * Loaded dynamically by content.js ONLY when the current page is a Scholar
 * author profile (/citations?user=…).  Not parsed or executed on search-result
 * pages, reducing cold-start cost for the common case.
 */

const SELF_CITE_CACHE_MS    = 30 * 24 * 60 * 60 * 1000; // 30 days
const SELF_CITE_SAMPLE_WORKS = 15;
const SELF_CITE_MIN_OVERLAP  = 2;
const SELF_CITE_MIN_NAME_SIM = 0.65;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeText(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[\.,\/#!$%\^&\*;:{}=\_`~?"""()\[\]]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function jaroWinkler(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
  const s1 = String(a);
  const s2 = String(b);
  const maxDist = Math.floor(Math.max(s1.length, s2.length) / 2) - 1;
  const match1 = new Array(s1.length);
  const match2 = new Array(s2.length);
  let matches = 0;
  for (let i = 0; i < s1.length; i++) {
    const start = Math.max(0, i - maxDist);
    const end   = Math.min(i + maxDist + 1, s2.length);
    for (let j = start; j < end; j++) {
      if (!match2[j] && s1[i] === s2[j]) { match1[i] = match2[j] = true; matches++; break; }
    }
  }
  if (!matches) return 0;
  let t = 0, k = 0;
  for (let i = 0; i < s1.length; i++) {
    if (match1[i]) { while (!match2[k]) k++; if (s1[i] !== s2[k]) t++; k++; }
  }
  const m    = matches;
  const jaro = (m / s1.length + m / s2.length + (m - t / 2) / m) / 3;
  let l = 0;
  while (l < 4 && s1[l] === s2[l]) l++;
  return jaro + l * 0.1 * (1 - jaro);
}

async function searchOpenAlexAuthors(name) {
  if (!name) return [];
  const url = `https://api.openalex.org/authors?search=${encodeURIComponent(name)}&per-page=10`;
  try {
    const r = await fetch(url);
    if (!r.ok) return [];
    const data = await r.json();
    return data?.results || [];
  } catch (_) { return []; }
}

async function fetchOpenAlexWorks(authorId, limit = SELF_CITE_SAMPLE_WORKS) {
  const url = `https://api.openalex.org/works?filter=author.id:${authorId}&sort=cited_by_count:desc&select=id,display_name,cited_by_count&per-page=${limit}`;
  try {
    const r = await fetch(url);
    if (!r.ok) return [];
    const data = await r.json();
    return data?.results || [];
  } catch (_) { return []; }
}

async function countOpenAlexSelfCitations(authorId, workId) {
  const url = `https://api.openalex.org/works?filter=cites:${encodeURIComponent(workId)},authorships.author.id:${authorId}&select=id&per-page=1`;
  const r = await fetch(url);
  if (!r.ok) return 0;
  const data = await r.json();
  return Number(data?.meta?.count || 0) || 0;
}

async function resolveOpenAlexAuthorId(authorName, sampleTitles) {
  const candidates = await searchOpenAlexAuthors(authorName);
  if (!candidates.length) return null;
  let bestId = null, bestScore = -1;
  for (const c of candidates) {
    const nameSim = jaroWinkler(normalizeText(authorName), normalizeText(c.display_name || ""));
    if (nameSim < SELF_CITE_MIN_NAME_SIM) continue;
    const id = String(c.id || "").replace("https://openalex.org/", "");
    if (!id) continue;
    let score = nameSim * 2;
    let overlap = 0;
    const works = await fetchOpenAlexWorks(id, 25);
    for (const title of sampleTitles) {
      for (const w of works) {
        if (jaroWinkler(normalizeText(title), normalizeText(w.display_name || "")) > 0.85) {
          overlap++; score += 1; break;
        }
      }
    }
    if (overlap >= SELF_CITE_MIN_OVERLAP && score > bestScore) { bestScore = score; bestId = id; }
  }
  return bestId;
}

/**
 * Compute the self-citation rate for an author via the OpenAlex API.
 *
 * @param {string} authorName  - Display name of the Scholar author.
 * @param {string[]} sampleTitles - A sample of the author's paper titles (used for disambiguation).
 * @returns {Promise<{status: string, rate?: number, selfCites?: number, totalCites?: number, sampleWorks?: number, source?: string, message?: string}>}
 */
export async function computeSelfCitationRate(authorName, sampleTitles) {
  const authorId = await resolveOpenAlexAuthorId(authorName, sampleTitles);
  if (!authorId) return { status: "error", message: "OpenAlex match not confident." };
  const works = await fetchOpenAlexWorks(authorId, SELF_CITE_SAMPLE_WORKS);
  if (!works.length) return { status: "error", message: "No OpenAlex works found." };
  let totalCites = 0, selfCites = 0;
  for (const work of works) {
    const cited = Number(work.cited_by_count || 0) || 0;
    if (!work.id || cited <= 0) continue;
    totalCites += cited;
    const sc = await countOpenAlexSelfCitations(authorId, work.id);
    selfCites += sc;
    await sleep(60);
  }
  if (!totalCites) return { status: "error", message: "No citation data available." };
  return {
    status: "success",
    rate: selfCites / totalCites,
    source: "OpenAlex",
    sampleWorks: works.length,
    selfCites,
    totalCites
  };
}
