// Reads a bundled data file as text. Large lists ship gzipped as src/data/<name>.gz and are gunzipped here with the
// browser's DecompressionStream. The Microsoft Edge package stores them uncompressed and rewrites every literal
// "src/data/<name>.gz" string to "src/data/<name>" (scripts/variant-data.mjs), so only a path that ends in .gz is
// gunzipped. Always pass the full literal path, never one built by joining strings, or that rewrite cannot find it
// (tests/data-paths.test.mjs checks this).
export async function fetchDataText(path) {
  const url = chrome.runtime.getURL(path);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  if (!url.endsWith(".gz") || !res.body || typeof DecompressionStream === "undefined") return res.text();
  return new Response(res.body.pipeThrough(new DecompressionStream("gzip"))).text();
}

export async function fetchDataJson(path) {
  return JSON.parse(await fetchDataText(path));
}
