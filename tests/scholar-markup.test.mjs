// Keeps scripts/scholar-markup-check.js honest: every Scholar selector it lists must be read by the extension's code,
// and every required one must exist in the offline e2e fixture, so the fixture still looks like the Scholar the code expects.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";

const read = (rel) => readFileSync(new URL(`../${rel}`, import.meta.url), "utf8");
const snippet = read("scripts/scholar-markup-check.js");
const entries = [...snippet.matchAll(/\{ sel: "([^"]+)", what: "[^"]+"(, min: \d+)?(, optional: true)? \}/g)].map((m) => ({ sel: m[1], optional: !!m[3] }));
const code = readdirSync(new URL("../src/content/", import.meta.url)).filter((f) => f.endsWith(".js")).map((f) => read(`src/content/${f}`)).join("\n");
const fixture = read("tests/e2e/fixtures/scholar-results.html");
// ".gs_r.gs_or" -> ["gs_r", "gs_or"]; "#gs_ab" -> ["gs_ab"]
const names = (sel) => sel.split(/[.#]/).filter(Boolean);

describe("Scholar markup check", () => {
  it("lists at least the core result-page selectors", () => {
    assert.ok(entries.length >= 8, `parsed ${entries.length} entries`);
    for (const core of ["#gs_res_ccl_mid", ".gs_rt", ".gs_a"]) assert.ok(entries.some((e) => e.sel === core), core);
  });
  it("every listed selector is used by the extension's content scripts", () => {
    const unused = entries.filter((e) => !names(e.sel).every((n) => code.includes(n))).map((e) => e.sel);
    assert.deepEqual(unused, [], `listed but not read by src/content: ${unused.join(", ")}`);
  });
  it("every required selector exists in the offline e2e fixture", () => {
    const absent = entries.filter((e) => !e.optional && !names(e.sel).every((n) => fixture.includes(n))).map((e) => e.sel);
    assert.deepEqual(absent, [], `required by the extension but not in tests/e2e/fixtures/scholar-results.html: ${absent.join(", ")}`);
  });
});
