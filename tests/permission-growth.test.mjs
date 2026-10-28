// Guards against the 0.6.0 regression: Chrome disables an installed extension when an update adds permission warnings,
// and users must re-enable it by hand (many never do). Compares manifest.json with the permissions of the last version
// published to the Chrome Web Store (tests/published-permissions.json). New hosts belong in optional_host_permissions,
// requested when the user first uses the feature; those do not disable anyone.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { variantManifest } from "../scripts/variant-manifest.mjs";
import { PROVIDERS } from "../src/common/ask-provider.js";

const manifest = JSON.parse(readFileSync(new URL("../manifest.json", import.meta.url), "utf8"));
const published = JSON.parse(readFileSync(new URL("./published-permissions.json", import.meta.url), "utf8"));

const added = (current, base) => [...new Set(current)].filter((x) => !base.includes(x)).sort();
const hint = (what, list) =>
  `manifest.json adds ${what} that version ${published.publishedVersion} did not have: ${list.join(", ")}. ` +
  "Chrome would disable the extension for existing users on update. Use optional_host_permissions and request them " +
  "at use time, or, if this growth is a deliberate decision, update tests/published-permissions.json in the same change.";

describe("permission growth over the published version", () => {
  it("adds no API permissions", () => {
    const list = added(manifest.permissions || [], published.permissions);
    assert.deepEqual(list, [], hint("permissions", list));
  });
  it("adds no host permissions", () => {
    const list = added(manifest.host_permissions || [], published.host_permissions);
    assert.deepEqual(list, [], hint("host permissions", list));
  });
  it("adds no content-script sites", () => {
    const matches = (manifest.content_scripts || []).flatMap((cs) => cs.matches || []);
    const list = added(matches, published.content_script_matches);
    assert.deepEqual(list, [], hint("content-script sites", list));
  });
  it("the baseline is a published version, not a later one", () => {
    const [a, b] = [manifest.version, published.publishedVersion].map((v) => v.split(".").map(Number));
    const cmp = a[0] - b[0] || a[1] - b[1] || a[2] - b[2];
    assert.ok(cmp >= 0, `baseline ${published.publishedVersion} is newer than manifest ${manifest.version}`);
  });
});

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const scriptMatches = (m) => (m.content_scripts || []).flatMap((cs) => cs.matches || []);

describe("permission growth in the store variants", () => {
  // make_variant.mjs rewrites the manifest for Edge and Firefox; neither may ask for more than Chrome does.
  for (const target of ["edge", "firefox"]) {
    it(`${target} asks for nothing the Chrome manifest does not`, () => {
      const v = variantManifest(manifest, target);
      assert.deepEqual(added(v.permissions || [], manifest.permissions || []), []);
      assert.deepEqual(added(v.host_permissions || [], manifest.host_permissions || []), []);
      assert.deepEqual(added(scriptMatches(v), scriptMatches(manifest)), []);
      assert.deepEqual(added(v.optional_permissions || [], manifest.optional_permissions || []), []);
      assert.deepEqual(added(v.optional_host_permissions || [], manifest.optional_host_permissions || []), []);
    });
  }
  it("leaves the Chrome manifest itself untouched", () => {
    const before = JSON.stringify(manifest);
    variantManifest(manifest, "firefox");
    assert.equal(JSON.stringify(manifest), before);
  });
});

describe("manifest.json is the template's output", () => {
  // The test above reads manifest.json; a permission added to manifest.template.json without regenerating would
  // slip past it and land in the next package build. Regenerate in a temporary folder and compare.
  it("has the same permission fields as a fresh build_manifest.js run", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "su-manifest-"));
    try {
      cpSync(path.join(ROOT, "manifest.template.json"), path.join(dir, "manifest.template.json"));
      cpSync(path.join(ROOT, "scripts", "build_manifest.js"), path.join(dir, "scripts", "build_manifest.js"));
      execFileSync(process.execPath, [path.join(dir, "scripts", "build_manifest.js")], { stdio: "ignore" });
      const fresh = JSON.parse(readFileSync(path.join(dir, "manifest.json"), "utf8"));
      for (const k of ["permissions", "host_permissions", "optional_permissions", "optional_host_permissions"]) {
        assert.deepEqual(fresh[k], manifest[k], `${k} differs: run npm run manifest`);
      }
      assert.deepEqual(scriptMatches(fresh), scriptMatches(manifest), "content-script matches differ: run npm run manifest");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("runtime permission requests", () => {
  // New hosts are asked for when the feature is first used, through chrome.permissions.request. Chrome only grants
  // what the manifest lists as optional, so a request outside that list fails silently for every user.
  const optionalHosts = manifest.optional_host_permissions || [];
  const optionalApis = manifest.optional_permissions || [];
  const covered = (origin) => optionalHosts.includes(origin) ||
    (optionalHosts.includes("<all_urls>") && /^(https?|wss?|ftp):\/\/[^/]+\/.*$/.test(origin));
  const files = [];
  const walk = (d) => {
    for (const e of readdirSync(d)) {
      const p = path.join(d, e);
      if (statSync(p).isDirectory()) walk(p);
      else if (p.endsWith(".js")) files.push(p);
    }
  };
  walk(path.join(ROOT, "src"));
  const calls = files.flatMap((f) => [...readFileSync(f, "utf8").matchAll(/permissions\.request\(\s*(\{[^}]*\})/g)]
    .map((m) => ({ file: path.relative(ROOT, f), arg: m[1] })));

  it("finds the request sites (Options' all-sites switch and the Ask provider)", () => {
    assert.ok(calls.length >= 2, `found ${calls.length} permissions.request calls`);
  });
  it("asks only for hosts declared optional", () => {
    for (const { file, arg } of calls) {
      for (const lit of arg.matchAll(/"([^"]+)"/g)) assert.ok(covered(lit[1]), `${file} requests ${lit[1]}, not in optional_host_permissions`);
    }
    for (const p of Object.values(PROVIDERS)) assert.ok(covered(p.origin), `Ask provider ${p.origin} is not in optional_host_permissions`);
  });
  it("asks for no API permission that is not declared optional", () => {
    for (const { file, arg } of calls) {
      const m = /permissions\s*:\s*\[([^\]]*)\]/.exec(arg);
      if (!m) continue;
      for (const lit of m[1].matchAll(/"([^"]+)"/g)) assert.ok(optionalApis.includes(lit[1]), `${file} requests the ${lit[1]} permission, not in optional_permissions`);
    }
  });
  it("no optional permission is also required (that would make it a required one)", () => {
    for (const h of optionalHosts) assert.ok(!(manifest.host_permissions || []).includes(h), h);
    for (const a of optionalApis) assert.ok(!(manifest.permissions || []).includes(a), a);
  });
});
