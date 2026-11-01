// Finding an author in the genealogy name list (about 920,000 names) without normalizing every name first.
// Indexing the whole list by normalized name took about 3.5 s in the browser on every Look up lineage click, and a
// lookup only ever needs the handful of spellings of one author's name.
//
// Normalizing an author name (content.js normalizeAuthorName) only drops titles, credentials, suffixes and
// separators, folds diacritics and lower-cases. So for a plain ASCII name, every run of letters and digits in its
// normalized form is a substring of the lower-cased raw name. A lookup searches one lower-cased copy of all ASCII
// names for the key's longest letter run and normalizes only the names that contain it. Names with any non-ASCII
// character (about 3%) are normalized once, up front, into a small map. Results equal a full index: the same
// indices, in ascending order. Pure functions; content.js passes its normalizer in.

const NON_ASCII = /[^ -~]/;

export function buildNameSearch(names, normalize) {
  const list = Array.isArray(names) ? names : [];
  const pieces = new Array(list.length);
  const starts = new Uint32Array(list.length);
  const nonAscii = new Map();
  let offset = 0;
  for (let i = 0; i < list.length; i++) {
    const name = String(list[i] ?? "");
    // Non-ASCII names stay out of the joined text: lower-casing them can change their length, which would shift
    // every later offset.
    const piece = NON_ASCII.test(name) ? "" : name.toLowerCase();
    if (!piece && name) {
      const key = normalize(name);
      if (key) {
        const hit = nonAscii.get(key);
        if (hit) hit.push(i);
        else nonAscii.set(key, [i]);
      }
    }
    pieces[i] = piece;
    starts[i] = offset;
    offset += piece.length + 1;
  }
  return { names: list, text: pieces.join("\n"), starts, nonAscii };
}

// Index of the name whose text contains position `pos` (the last start at or before it).
function nameAt(starts, pos) {
  let lo = 0;
  let hi = starts.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (starts[mid] <= pos) lo = mid;
    else hi = mid - 1;
  }
  return lo;
}

/** Indices of every name whose normalized form is `key`, ascending (what a full Map index would hold). */
export function findNameIndices(search, key, normalize) {
  if (!search || !key) return [];
  const out = [];
  const runs = String(key).match(/[a-z0-9]+/g);
  if (runs) {
    const needle = runs.reduce((a, b) => (b.length > a.length ? b : a));
    const { text, starts, names } = search;
    let pos = text.indexOf(needle);
    while (pos >= 0) {
      const i = nameAt(starts, pos);
      if (normalize(names[i]) === key) out.push(i);
      const next = i + 1 < starts.length ? starts[i + 1] : text.length;
      pos = text.indexOf(needle, next);
    }
  }
  const other = search.nonAscii.get(key);
  if (other) {
    out.push(...other);
    out.sort((a, b) => a - b);
  }
  return out;
}
