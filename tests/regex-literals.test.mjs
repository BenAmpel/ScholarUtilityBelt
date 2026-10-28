import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const root = fileURLToPath(new URL("..", import.meta.url));
const espree = createRequire(import.meta.url)("espree");
// Regex literals that really mean a literal backslash followed by a letter.
const ALLOWED = new Set(["src/common/latex-pack.js:/\\\\begin\\{comment\\}[\\s\\S]*?\\\\end\\{comment\\}/"]);

function sources(dir, out = []) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) { if (e.name !== "vendor" && e.name !== "data") sources(p, out); }
    else if (/\.m?js$/.test(e.name) && !e.name.startsWith("extpay")) out.push(p);
  }
  return out;
}

describe("regex literals", () => {
  it("never write \\\\d, \\\\s or \\\\w where \\d, \\s or \\w was meant", () => {
    const found = [];
    for (const f of sources(join(root, "src"))) {
      const src = readFileSync(f, "utf8");
      let ast;
      try { ast = espree.parse(src, { ecmaVersion: "latest", sourceType: "module", loc: true }); }
      catch { ast = espree.parse(src, { ecmaVersion: "latest", sourceType: "script", loc: true }); }
      const visit = (n) => {
        if (!n || typeof n.type !== "string") return;
        if (n.type === "Literal" && n.regex && /\\\\[dDsSwWbBnt]/.test(n.regex.pattern)) {
          const id = `${relative(root, f)}:/${n.regex.pattern}/`;
          if (!ALLOWED.has(id)) found.push(`${relative(root, f)}:${n.loc.start.line} /${n.regex.pattern}/`);
        }
        for (const v of Object.values(n)) {
          if (Array.isArray(v)) v.forEach(visit);
          else if (v && typeof v.type === "string") visit(v);
        }
      };
      visit(ast);
    }
    assert.deepEqual(found, []);
  });
});
