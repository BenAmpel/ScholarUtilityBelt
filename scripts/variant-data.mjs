// Microsoft Edge Add-ons rejects a package that contains a compressed file ("The uploaded package consists of a
// compressed file"), and the genealogy dataset ships as src/data/*.gz. For Edge, make_variant.mjs stores those files
// uncompressed (the package zip compresses them anyway) and points the code at the new names; the loaders only gunzip
// a URL that ends in .gz.
import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

export function uncompressDataFiles(work) {
  const dataDir = path.join(work, "src", "data");
  if (!fs.existsSync(dataDir)) return [];
  const names = fs.readdirSync(dataDir).filter((n) => n.endsWith(".gz"));
  for (const n of names) {
    const gz = path.join(dataDir, n);
    fs.writeFileSync(gz.slice(0, -3), zlib.gunzipSync(fs.readFileSync(gz)));
    fs.rmSync(gz);
  }
  if (!names.length) return names;
  const swaps = names.map((n) => [`src/data/${n}`, `src/data/${n.slice(0, -3)}`]);
  const walk = (d) => {
    for (const e of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) { if (p !== dataDir) walk(p); continue; }
      if (!/\.(js|html)$/.test(e.name)) continue;
      const s = fs.readFileSync(p, "utf8");
      let t = s;
      for (const [a, b] of swaps) t = t.split(a).join(b);
      if (t !== s) fs.writeFileSync(p, t);
    }
  };
  walk(work);
  return names;
}
