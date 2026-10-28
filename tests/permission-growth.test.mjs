// Guards against the 0.6.0 regression: Chrome disables an installed extension when an update adds permission warnings,
// and users must re-enable it by hand (many never do). Compares manifest.json with the permissions of the last version
// published to the Chrome Web Store (tests/published-permissions.json). New hosts belong in optional_host_permissions,
// requested when the user first uses the feature; those do not disable anyone.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

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
