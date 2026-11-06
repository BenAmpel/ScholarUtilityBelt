// Top-2% panel, the parts with no DOM: name keys, the data decoder and the matcher. The build script
// (scripts/build_top2pct.mjs) imports this same file, so the keys in the data file and the keys the browser computes
// come from one normalizer.
//
// A wrong match puts another person's numbers on a named profile, so every rule here leans towards showing nothing.

export const FORMAT_VERSION = 1;

const LETTER_MAP = { ø: "o", ł: "l", đ: "d", ð: "d", ß: "ss", æ: "ae", œ: "oe", þ: "th", ı: "i" };
// Titles are removed only at the start of a name, suffixes only at the end.
const TITLES = new Set(["dr", "prof", "professor", "mr", "mrs", "ms", "sir", "dame"]);
const SUFFIXES = new Set(["jr", "sr", "ii", "iii", "iv", "phd", "md", "frs", "facs", "mba", "msc", "bsc", "dphil", "dsc", "mph", "rn", "pe", "frcp", "fracp", "frse", "freng", "fmedsci", "faaas", "fieee", "facm", "pharmd", "ma", "ba"]);
const PARTICLES = new Set(["van", "der", "den", "de", "la", "le", "von", "da", "dos", "das", "del", "della", "di", "du", "ter", "ten", "vander", "vanden", "al", "el", "bin", "ibn"]);

/** Lower-case, fold accents and a few letters NFKD leaves alone, drop apostrophes, join hyphenated parts. */
export function foldText(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[øłđðßæœþı]/g, (c) => LETTER_MAP[c])
    .replace(/['’ʼ`´]/g, "")
    .replace(/(?<=[a-z0-9])[-‐‑–](?=[a-z0-9])/g, "");
}

/** Tokens of a-z only. A name written only in another script gives no tokens. */
export function nameTokens(s) {
  return foldText(s).split(/[^a-z]+/).filter(Boolean);
}

function trimTitlesAndSuffixes(tokens) {
  const t = tokens.slice();
  while (t.length > 1 && TITLES.has(t[0])) t.shift();
  while (t.length > 1 && SUFFIXES.has(t[t.length - 1])) t.pop();
  return t;
}

/**
 * A name split into surname (joined, no spaces) and given tokens.
 * Scholar writes "Given ... Surname"; the dataset writes "Surname, Given". A particle joins the surname only when at
 * least two tokens come before the last one, so "Di Wang" keeps "wang" as the surname.
 */
export function splitScholarName(raw) {
  // Drop a bracketed part ("Wei Wang (王伟)") and anything after the first comma ("Jane Doe, PhD").
  const base = String(raw || "").replace(/\([^)]*\)|\[[^\]]*\]/g, " ").split(/[,;]/)[0];
  const t = trimTitlesAndSuffixes(nameTokens(base));
  if (t.length < 2) return null;
  let cut = t.length - 1;
  while (cut >= 2 && PARTICLES.has(t[cut - 1])) cut--;
  return { surname: t.slice(cut).join(""), given: t.slice(0, cut) };
}

export function splitDatasetName(raw) {
  const s = String(raw || "");
  const comma = s.indexOf(",");
  if (comma < 0) return splitScholarName(s);
  const surname = nameTokens(s.slice(0, comma)).join("");
  const given = trimTitlesAndSuffixes(["x", ...nameTokens(s.slice(comma + 1))]).slice(1);
  if (!surname || !given.length) return null;
  return { surname, given };
}

export const nameKey = (n) => (n ? `${n.surname}|${n.given[0][0]}` : null);

/**
 * The keys to look up for a Scholar name. `primary` is the Scholar order. The others (surname first, as many CJK
 * names are written; a two-word surname) are used only when the primary key has no hits.
 */
export function scholarKeys(raw) {
  const n = splitScholarName(raw);
  if (!n) return null;
  const all = n.given.concat([n.surname]);
  const alt = [];
  if (all.length === 2) alt.push({ surname: all[0], given: [all[1]] });
  if (all.length >= 3 && n.given.length >= 2) alt.push({ surname: n.given[n.given.length - 1] + n.surname, given: n.given.slice(0, -1) });
  return { name: n, primary: nameKey(n), alternates: alt.map((a) => ({ name: a, key: nameKey(a) })) };
}

/**
 * Whether the given names agree well enough for a confident match: both full names (not initials) and equal, and no
 * conflicting middle initial. "John A. Smith" never matches "Smith, John B.".
 */
export function strongGiven(scholarGiven, datasetGiven) {
  const a = scholarGiven, b = datasetGiven;
  if (!a.length || !b.length || a[0].length < 2 || b[0].length < 2) return false;
  const same = a[0] === b[0] || a.join("") === b.join("");
  if (!same) return false;
  if (a.length >= 2 && b.length >= 2 && a.join("") !== b.join("") && a[1][0] !== b[1][0]) return false;
  return true;
}

// Institution words that say nothing about which institution it is.
const INST_STOP = new Set(["university", "universite", "universitat", "universidad", "universidade", "universita", "universiteit", "uniwersytet", "univ",
  "of", "the", "and", "for", "at", "in", "de", "des", "du", "di", "da", "do", "del", "la", "le", "les", "y", "e", "et", "und", "fur", "der", "die",
  "institute", "institut", "instituto", "istituto", "college", "school", "department", "dept", "faculty", "center", "centre", "hospital", "campus"]);
// Short names Scholar profiles use often, expanded to the words the dataset uses.
export const INST_ALIASES = {
  mit: "massachusetts institute of technology", eth: "eth zurich", ethz: "eth zurich", epfl: "ecole polytechnique federale de lausanne",
  ucla: "university of california los angeles", ucsd: "university of california san diego", ucsf: "university of california san francisco",
  ucb: "university of california berkeley", uc: "university of california", cnrs: "centre national de la recherche scientifique cnrs",
  caltech: "california institute of technology", cmu: "carnegie mellon university", ntu: "nanyang technological university",
  nus: "national university of singapore", kaist: "korea advanced institute of science and technology", ucl: "university college london",
  lse: "london school of economics", kth: "kth royal institute of technology", tum: "technical university of munich",
  cas: "chinese academy of sciences", inserm: "inserm", nih: "national institutes of health", upenn: "university of pennsylvania",
  penn: "university of pennsylvania", usc: "university of southern california", unc: "university of north carolina",
  ust: "hong kong university of science and technology", hkust: "hong kong university of science and technology",
  cuhk: "chinese university of hong kong", hku: "university of hong kong", sjtu: "shanghai jiao tong university",
  ustc: "university of science and technology of china", kuleuven: "ku leuven", ku: "ku", tu: "tu",
};
// Email domain labels that name no institution.
const DOMAIN_SKIP = new Set(["edu", "ac", "com", "org", "net", "gov", "mail", "email", "gmail", "yahoo", "hotmail", "outlook", "qq", "cs", "ee", "ece", "eecs", "med", "math", "phys", "chem", "bio", "student", "alumni", "staff", "uk", "us", "cn", "jp", "de", "fr", "it", "es", "au", "ca", "nl", "kr", "in", "br", "ch", "se", "il", "sg", "hk", "tw", "be", "at", "dk", "fi", "no", "nz", "ie", "pt", "pl", "cz", "gr", "tr", "ru", "mx", "ar", "cl", "za", "my", "th", "id", "vn", "ir", "sa", "ae", "eg", "pk", "ng", "co", "www"]);

export function institutionTokens(s) {
  const all = foldText(s).split(/[^a-z0-9]+/).filter(Boolean);
  const kept = all.filter((t) => !INST_STOP.has(t));
  return kept.length ? kept : all;
}

/**
 * Tokens of a Scholar affiliation line plus its verified email domain, with known short names expanded. The result
 * also carries `segments`: the token sets of each comma-separated part and of the email domain, used to check a
 * one-word institution against a whole part ("University of York" is not "State University of New York").
 */
export function affiliationTokens(affiliation, emailDomain) {
  // A short name ("MIT") is replaced in place by the words it stands for, so the parts keep their word order.
  const expand = (tokens) => tokens.flatMap((t) => (INST_ALIASES[t] ? foldText(INST_ALIASES[t]).split(/[^a-z0-9]+/).filter(Boolean) : [t]));
  const segments = foldText(affiliation).split(/[,;|/()\u2013\u2014]| - | at /).map((p) => expand(p.split(/[^a-z0-9]+/).filter(Boolean))).filter((x) => x.length);
  for (const label of String(emailDomain || "").toLowerCase().split(".")) if (label && !DOMAIN_SKIP.has(label)) segments.push(expand([label]));
  const all = new Set();
  for (const seg of segments) for (const t of seg) all.add(t);
  all.segments = segments;
  return all;
}

export const INST_THRESHOLD = 0.75;

// Words that place an institution but do not name it. An institution whose remaining words are all of this kind
// ("Western University", "National University", "State University") is contained in too many affiliations
// ("The University of Western Australia"), so it counts as evidence only when the affiliation has no other
// naming word.
const INST_GENERIC = new Set(["western", "eastern", "northern", "southern", "north", "south", "east", "west", "central", "national",
  "state", "federal", "royal", "general", "medical", "technical", "technology", "technological", "science", "sciences",
  "research", "polytechnic", "open", "free", "public", "catholic", "international", "american", "european", "city", "metropolitan",
  "health", "academy", "council", "foundation", "laboratory", "laboratories", "agricultural", "normal", "applied", "studies", "advanced"]);
// Words that may stand right next to a one-word institution name without changing which institution it is
// ("Duke University School of Medicine", "Leeds General Infirmary"). "State", "New" and "Metropolitan" are not here:
// "Pennsylvania State University" and "New York" are other places.
const INST_UNIT = new Set(["medicine", "medical", "health", "hospital", "hospitals", "infirmary", "eye", "clinic", "clinical", "general",
  "system", "teaching", "children", "childrens", "cancer", "heart", "dental", "nursing", "law", "business", "engineering", "arts"]);
const INST_HEAD = new Set(["university", "universite", "universitat", "universidad", "universidade", "universita", "universiteit", "univ", "college", "institute", "institut", "instituto"]);
const adjacentOk = (x) => x === undefined || INST_STOP.has(x) || INST_UNIT.has(x);

/**
 * Whether some part of the affiliation names a one-word institution as that institution: the words beside it are
 * stop or unit words, and "University of X" is not written "X University" (University of Washington is not
 * Washington University). An email domain label counts as naming it.
 */
function namesSingleWord(word, datasetInst, segments) {
  const d = foldText(datasetInst).split(/[^a-z0-9]+/).filter(Boolean);
  const k = d.indexOf(word);
  const ofForm = k > 0 && d[k - 1] === "of";
  const headForm = k >= 0 && INST_HEAD.has(d[k + 1]);
  // A word of the institution's own name may stand beside it too ("Chinese Academy of Sciences").
  const ok = (x) => adjacentOk(x) || d.includes(x);
  return segments.some((seg) => {
    if (seg.length === 1) return seg[0] === word;
    for (let i = 0; i < seg.length; i++) {
      if (seg[i] !== word || !ok(seg[i - 1]) || !ok(seg[i + 1])) continue;
      if (ofForm && INST_HEAD.has(seg[i + 1])) continue;
      if (headForm && seg[i - 1] === "of") continue;
      return true;
    }
    return false;
  });
}

/** Share of the dataset institution's tokens that appear in the Scholar affiliation (0 when either is empty). */
export function institutionContainment(datasetInst, affTokens) {
  if (!datasetInst || !affTokens || !affTokens.size) return 0;
  const t = institutionTokens(datasetInst);
  if (!t.length) return 0;
  if (t.every((x) => INST_GENERIC.has(x) || /^\d+$/.test(x))) {
    for (const a of affTokens) if (!INST_STOP.has(a) && !INST_GENERIC.has(a) && !t.includes(a)) return 0;
  }
  // One naming word ("York", "Washington"): some part of the affiliation must name it as that institution.
  const named = t.filter((x) => !INST_GENERIC.has(x));
  if (named.length === 1 && affTokens.segments && !namesSingleWord(named[0], datasetInst, affTokens.segments)) return 0;
  let hit = 0;
  for (const x of t) if (affTokens.has(x)) hit++;
  return hit / t.length;
}

// ---------------------------------------------------------------------------------------------------------------
// Data file: gzip of one JSON object. See scripts/build_top2pct.mjs for the writer.

function isIntArray(a, n) {
  if (!Array.isArray(a) || (n != null && a.length !== n)) return false;
  for (const x of a) if (!Number.isInteger(x)) return false;
  return true;
}

/**
 * Check and index a parsed data file. Returns null on any mismatch (wrong format, an index past its table, columns of
 * different lengths), so a bad file shows no panel rather than wrong numbers.
 */
export function decodeTop2pct(obj) {
  try {
    if (!obj || obj.format !== FORMAT_VERSION || !obj.meta || !Array.isArray(obj.tables)) return null;
    const { inst, cntry, sub } = obj;
    if (![inst, cntry, sub].every((a) => Array.isArray(a) && a.every((s) => typeof s === "string"))) return null;
    const P = obj.people;
    if (!P || !Array.isArray(P.name)) return null;
    const n = P.name.length;
    if (!isIntArray(P.inst, n) || !isIntArray(P.cntry, n) || !isIntArray(P.fy, n) || !isIntArray(P.ly, n)) return null;
    for (let i = 0; i < n; i++) {
      if (typeof P.name[i] !== "string") return null;
      if (P.inst[i] < -1 || P.inst[i] >= inst.length || P.cntry[i] < -1 || P.cntry[i] >= cntry.length) return null;
    }
    const tables = {};
    for (const t of obj.tables) {
      const T = obj.rows && obj.rows[t];
      if (!T || !isIntArray(T.p) || !isIntArray(T.subCount, sub.length)) return null;
      const m = T.p.length;
      if (!["sub", "rank", "self", "cpr", "rw"].every((c) => isIntArray(T[c], m))) return null;
      const person = new Int32Array(m);
      let acc = -1;
      for (let i = 0; i < m; i++) {
        acc += T.p[i];
        if (T.p[i] < 1 || acc >= n) return null;
        if (T.sub[i] < 0 || T.sub[i] >= sub.length) return null;
        person[i] = acc;
      }
      tables[t] = { person, sub: T.sub, rank: T.rank, self: T.self, cpr: T.cpr, rw: T.rw, subCount: T.subCount };
    }
    // People are sorted by name key. Keys are not stored (that saves about 0.8 MB); a lookup computes the keys of
    // the twenty or so rows its binary search visits, and caches them.
    return { meta: obj.meta, tables, inst, cntry, sub, people: P, size: n, keys: new Map() };
  } catch {
    return null;
  }
}

function lowerBound(arr, x) {
  let lo = 0, hi = arr.length;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (arr[mid] < x) lo = mid + 1; else hi = mid; }
  return lo;
}

/** The name key of person i ("" if the name gives none, which the build never ships). */
export function keyAt(db, i) {
  let k = db.keys.get(i);
  if (k === undefined) {
    k = nameKey(splitDatasetName(db.people.name[i])) || "";
    db.keys.set(i, k);
  }
  return k;
}

export function peopleByKey(db, key) {
  const out = [];
  if (!key) return out;
  let lo = 0, hi = db.size;
  while (lo < hi) { const mid = (lo + hi) >> 1; if (keyAt(db, mid) < key) lo = mid + 1; else hi = mid; }
  for (let i = lo; i < db.size && keyAt(db, i) === key; i++) out.push(i);
  return out;
}

/** One person's identity and numbers from each table they appear in. */
export function personRecord(db, i) {
  const P = db.people;
  const rec = {
    index: i,
    name: P.name[i],
    inst: P.inst[i] >= 0 ? db.inst[P.inst[i]] : "",
    cntry: P.cntry[i] >= 0 ? db.cntry[P.cntry[i]] : "",
    firstYear: P.fy[i],
    lastYear: P.ly[i],
    tables: {},
  };
  for (const [t, T] of Object.entries(db.tables)) {
    const r = lowerBound(T.person, i);
    if (r < T.person.length && T.person[r] === i) {
      rec.tables[t] = {
        subfield: db.sub[T.sub[r]],
        rank: T.rank[r],
        of: T.subCount[T.sub[r]],
        selfPct: T.self[r],
        citesPerCitingPaper: T.cpr[r] / 10,
        retracted: T.rw[r],
      };
    }
  }
  return rec;
}

/** The identity a pick is stored and re-found by. */
export const identityOf = (rec) => ({ name: rec.name, inst: rec.inst, cntry: rec.cntry });

/**
 * Match a Scholar profile to dataset people.
 *  confident: exactly one candidate under the key has institution evidence, and its given name is strong.
 *  pick: two or more candidates under the key have institution evidence (this covers exact duplicates), or one has
 *        evidence and a namesake's institution shares a naming word with the affiliation.
 *  none: everything else.
 * Alternate keys (surname first, two-word surname) are tried only when the primary key has no hits at all.
 */
export function matchProfile(db, profile, threshold = INST_THRESHOLD) {
  const none = { state: "none", candidates: [] };
  if (!db || !profile) return none;
  const keys = scholarKeys(profile.name);
  if (!keys) return none;
  const aff = affiliationTokens(profile.affiliation, profile.emailDomain);
  const evidencedUnder = (key) => peopleByKey(db, key).filter((i) => {
    const p = db.people.inst[i];
    return p >= 0 && institutionContainment(db.inst[p], aff) >= threshold;
  });
  // A namesake at an institution that shares a naming word with the affiliation but falls short of the threshold
  // ("Tehran University of Medical Sciences" against the affiliation "Tehran") is a weak rival: it turns a confident
  // match into a pick list, and is offered there.
  const affNaming = [...aff].filter((x) => !INST_STOP.has(x) && !INST_GENERIC.has(x));
  const weakUnder = (key, evidenced) => peopleByKey(db, key).filter((i) => {
    const p = db.people.inst[i];
    return p >= 0 && !evidenced.includes(i) && institutionTokens(db.inst[p]).some((x) => !INST_GENERIC.has(x) && affNaming.includes(x));
  });
  const decide = (name, evidenced, rivals, key) => {
    if (evidenced.length === 1 && !rivals.length) rivals = weakUnder(key, evidenced);
    if (evidenced.length + rivals.length >= 2) return { state: "pick", candidates: evidenced.concat(rivals).map((i) => personRecord(db, i)) };
    if (evidenced.length === 1) {
      const ds = splitDatasetName(db.people.name[evidenced[0]]);
      if (ds && strongGiven(name.given, ds.given)) return { state: "confident", candidates: [personRecord(db, evidenced[0])] };
    }
    return none;
  };
  const primaryHits = peopleByKey(db, keys.primary).length;
  if (primaryHits) {
    // A two-word name can be in either order ("Lei Cheng" is "Cheng, Lei" or "Lei, Cheng"). A person under the other
    // order at the same institution is a rival, so neither is confident.
    const rivals = keys.name.given.length === 1 ? keys.alternates.flatMap((a) => evidencedUnder(a.key)) : [];
    return decide(keys.name, evidencedUnder(keys.primary), rivals, keys.primary);
  }
  for (const { name, key } of keys.alternates) {
    if (!peopleByKey(db, key).length) continue;
    return decide(name, evidencedUnder(key), [], key);
  }
  return none;
}

/** Find the person a stored pick names, by exact identity. Returns null unless exactly one person has it. */
export function resolveIdentity(db, ident) {
  if (!db || !ident || typeof ident.name !== "string") return null;
  const key = nameKey(splitDatasetName(ident.name));
  const hits = peopleByKey(db, key).filter((i) => {
    const r = personRecord(db, i);
    return r.name === ident.name && r.inst === (ident.inst || "") && r.cntry === (ident.cntry || "");
  });
  return hits.length === 1 ? personRecord(db, hits[0]) : null;
}

/** A short, stable hash of an affiliation line, stored with a pick so a changed affiliation re-runs the matcher. */
export function affiliationHash(s) {
  let h = 2166136261;
  for (const c of foldText(s).replace(/\s+/g, " ").trim()) { h ^= c.codePointAt(0); h = Math.imul(h, 16777619); }
  return (h >>> 0).toString(36);
}
