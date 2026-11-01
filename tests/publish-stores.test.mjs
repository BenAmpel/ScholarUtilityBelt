// scripts/publish_stores.mjs: argument parsing, file/version/credential checks, and the requests each store client
// builds. Every fetch here is a mock that fails on any URL it does not expect; no test reaches a real store.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  parseArgs, compareVersions, checklistHashes, checkFiles, releaseFiles, loadCredentials, missingCredentials,
  secretValues, redact, amoJwt, chromeClient, edgeClient, amoClient, plan, run, sha256, STORES,
} from "../scripts/publish_stores.mjs";

const SECRETS = {
  chrome: { clientId: "cid-123.apps", clientSecret: "chrome-secret-xyz", refreshToken: "1//refresh-abc", publisherId: "pub-1" },
  edge: { clientId: "edge-client-1", apiKey: "edge-api-key-secret", productId: "prod-42" },
  firefox: { jwtIssuer: "user:1:2", jwtSecret: "amo-secret-hex" },
};
const noSleep = async () => {};

// A fetch mock: routes is a list of [method, url-or-regexp, handler(req) -> Response]; requests are recorded.
function mockFetch(routes) {
  const calls = [];
  const fn = async (url, init = {}) => {
    const method = init.method || "GET";
    calls.push({ url: String(url), method, headers: init.headers || {}, body: init.body });
    for (const [m, u, h] of routes) {
      if (m === method && (u instanceof RegExp ? u.test(url) : u === String(url))) return h({ url, ...init });
    }
    throw new Error(`unexpected request ${method} ${url}`);
  };
  fn.calls = calls;
  return fn;
}
const json = (obj, init = {}) => new Response(JSON.stringify(obj), { status: 200, headers: { "content-type": "application/json" }, ...init });

describe("parseArgs", () => {
  it("is a dry run over every store by default", () => {
    const o = parseArgs([]);
    assert.equal(o.dryRun, true);
    assert.deepEqual(o.stores, STORES);
    assert.equal(o.submit, false);
  });
  it("uploads only to the named stores, and submit is a separate flag", () => {
    const o = parseArgs(["--upload", "firefox,Chrome"]);
    assert.equal(o.dryRun, false);
    assert.deepEqual(o.stores, ["firefox", "chrome"]);
    assert.equal(o.submit, false);
    assert.equal(parseArgs(["--upload", "all", "--submit"]).submit, true);
    assert.deepEqual(parseArgs(["--upload", "all"]).stores, STORES);
  });
  it("refuses --submit without --upload, unknown stores, missing values and bad versions", () => {
    assert.throws(() => parseArgs(["--submit"]), /needs --upload/);
    assert.throws(() => parseArgs(["--upload", "safari"]), /unknown store/);
    assert.throws(() => parseArgs(["--upload"]), /needs a value/);
    assert.throws(() => parseArgs(["--upload", "--submit"]), /needs a value/);
    assert.throws(() => parseArgs(["--version", "1.x"]), /bad version/);
    assert.throws(() => parseArgs(["--publish"]), /unknown argument/);
    assert.throws(() => parseArgs(["--upload", "edge", "--only", "chrome"]), /--only/);
  });
  it("--only narrows the dry run", () => {
    assert.deepEqual(parseArgs(["--only", "edge"]).stores, ["edge"]);
  });
});

describe("compareVersions", () => {
  it("compares numerically, not as text", () => {
    assert.equal(compareVersions("1.10.0", "1.9.9"), 1);
    assert.equal(compareVersions("1.1.0", "1.1.0"), 0);
    assert.equal(compareVersions("1.1", "1.1.0"), 0);
    assert.equal(compareVersions("1.0.2", "1.1.0"), -1);
  });
});

describe("checklistHashes", () => {
  it("reads the path and sha256 columns of the upload checklist", () => {
    const md = "| Chrome Web Store | `output/ScholarUtilityBelt-1.1.0.zip` | `" + "a".repeat(64) + "` |\n" +
      "| Edge | `output/edge-store/ScholarUtilityBelt-1.1.0-edge.zip` | `" + "b".repeat(64) + "` |\n| other | text |";
    const m = checklistHashes(md);
    assert.equal(m.get("ScholarUtilityBelt-1.1.0.zip"), "a".repeat(64));
    assert.equal(m.get("ScholarUtilityBelt-1.1.0-edge.zip"), "b".repeat(64));
    assert.equal(m.size, 2);
  });
});

// A temporary repo root with manifest.json and fake packages; zip contents come from the readManifest stub.
function fixture({ version = "1.2.0", zipVersion = version, gecko = true } = {}) {
  const root = mkdtempSync(path.join(tmpdir(), "sub-publish-"));
  writeFileSync(path.join(root, "manifest.json"), JSON.stringify({ version }));
  const files = releaseFiles(version, root);
  mkdirSync(path.dirname(files.firefox.source), { recursive: true });
  for (const f of [files.chrome.package, files.edge.package, files.firefox.package, files.firefox.source]) writeFileSync(f, `zip:${path.basename(f)}`);
  writeFileSync(files.releaseNotes, "Release notes text.");
  writeFileSync(files.approvalNotes, `Source for ${version}.`);
  const readManifest = (f) => ({ version: zipVersion, ...(gecko && f.endsWith("-firefox.zip") ? { browser_specific_settings: { gecko: { id: "x@y" } } } : {}) });
  return { root, files, readManifest, cleanup: () => rmSync(root, { recursive: true, force: true }) };
}
const credFile = (root, data = SECRETS, mode = 0o600) => {
  const f = path.join(root, "creds.json");
  writeFileSync(f, JSON.stringify(data));
  chmodSync(f, mode);
  return f;
};

describe("checkFiles", () => {
  it("reports missing files, wrong manifest versions and checklist mismatches", () => {
    const fx = fixture({ zipVersion: "1.1.0" });
    try {
      const hashes = new Map([["ScholarUtilityBelt-1.2.0.zip", "0".repeat(64)]]);
      const r = checkFiles("chrome", "1.2.0", fx.files, { readManifest: fx.readManifest, hashes, root: fx.root });
      assert.ok(r.problems.some((p) => /manifest version 1\.1\.0, expected 1\.2\.0/.test(p)));
      assert.ok(r.problems.some((p) => /sha256 differs/.test(p)));
      rmSync(fx.files.edge.package);
      assert.ok(checkFiles("edge", "1.2.0", fx.files, { readManifest: fx.readManifest, root: fx.root }).problems.some((p) => /package missing/.test(p)));
    } finally { fx.cleanup(); }
  });
  it("passes a matching sha256 and checks the Firefox source zip and gecko id", () => {
    const fx = fixture({ gecko: false });
    try {
      const hashes = new Map([["ScholarUtilityBelt-1.2.0.zip", sha256(Buffer.from("zip:ScholarUtilityBelt-1.2.0.zip"))]]);
      assert.deepEqual(checkFiles("chrome", "1.2.0", fx.files, { readManifest: fx.readManifest, hashes, root: fx.root }).problems, []);
      const ff = checkFiles("firefox", "1.2.0", fx.files, { readManifest: fx.readManifest, root: fx.root });
      assert.ok(ff.problems.some((p) => /gecko\.id/.test(p)));
      assert.ok(ff.info.some((i) => i.startsWith("source:")));
    } finally { fx.cleanup(); }
  });
});

describe("credentials", () => {
  it("reads a mode-600 file and lets env vars override it", () => {
    const fx = fixture();
    try {
      const c = loadCredentials(credFile(fx.root), { SUB_EDGE_PRODUCT_ID: "from-env" });
      assert.equal(c.chrome.refreshToken, SECRETS.chrome.refreshToken);
      assert.equal(c.edge.productId, "from-env");
      assert.deepEqual(missingCredentials(c, "firefox"), []);
      assert.deepEqual(c.problems, []);
    } finally { fx.cleanup(); }
  });
  it("refuses a file other users can read, and names missing fields only", () => {
    const fx = fixture();
    try {
      const c = loadCredentials(credFile(fx.root, SECRETS, 0o644), {});
      assert.match(c.problems[0], /chmod 600/);
      assert.deepEqual(missingCredentials(c, "edge"), ["clientId", "apiKey", "productId"]);
      assert.ok(!JSON.stringify(c.problems).includes(SECRETS.edge.apiKey));
    } finally { fx.cleanup(); }
  });
  it("redacts secrets, access tokens and JWTs, but not the public ids", () => {
    const vals = secretValues(SECRETS);
    assert.ok(!vals.includes("prod-42") && !vals.includes("pub-1"));
    const s = redact(`bad key edge-api-key-secret token ya29.a0Abc-def jwt ${amoJwt("i", "s")} prod-42`, vals);
    assert.ok(!s.includes("edge-api-key-secret") && !s.includes("ya29.") && !s.includes("eyJ"));
    assert.ok(s.includes("prod-42"));
  });
});

describe("amoJwt", () => {
  it("is HS256 with iss, a unique jti and exp within five minutes", () => {
    const [h, p] = amoJwt("user:1:2", "secret", 1000).split(".").slice(0, 2).map((x) => JSON.parse(Buffer.from(x, "base64url")));
    assert.equal(h.alg, "HS256");
    assert.equal(p.iss, "user:1:2");
    assert.equal(p.iat, 1000);
    assert.ok(p.exp > p.iat && p.exp - p.iat <= 300);
    assert.notEqual(amoJwt("a", "b", 1).split(".")[1], amoJwt("a", "b", 1).split(".")[1]);
  });
});

const noSecretInUrls = (calls) => {
  for (const c of calls) for (const v of secretValues(SECRETS)) assert.ok(!c.url.includes(v), `secret in URL ${c.url}`);
};

describe("chromeClient", () => {
  it("refreshes the token in a form body, uploads, polls, then publishes with review", async () => {
    let polls = 0;
    const f = mockFetch([
      ["POST", "https://oauth2.googleapis.com/token", () => json({ access_token: "ya29.tok" })],
      ["POST", "https://chromewebstore.googleapis.com/upload/v2/publishers/pub-1/items/omcogfcgldfmihfogbffflbocdbjockn:upload",
        () => json({ uploadState: "IN_PROGRESS" })],
      ["GET", /:fetchStatus$/, () => json({ lastAsyncUploadState: ++polls < 2 ? "IN_PROGRESS" : "SUCCEEDED" })],
      ["POST", /items\/omcogfcgldfmihfogbffflbocdbjockn:publish$/, () => json({ state: "PENDING_REVIEW" })],
    ]);
    const c = chromeClient(SECRETS.chrome, { fetch: f, sleep: noSleep });
    await c.upload(Buffer.from("zip"));
    const p = await c.publish();
    assert.equal(p.state, "PENDING_REVIEW");
    const [tok, up, , , pub] = f.calls;
    assert.equal(tok.body.get("refresh_token"), SECRETS.chrome.refreshToken);
    assert.equal(tok.body.get("grant_type"), "refresh_token");
    assert.equal(up.headers.Authorization, "Bearer ya29.tok");
    assert.equal(up.headers["Content-Type"], "application/zip");
    assert.deepEqual(JSON.parse(pub.body), { publishType: "DEFAULT_PUBLISH" });
    assert.equal(f.calls.filter((x) => x.url.includes("oauth2")).length, 1, "token is fetched once");
    noSecretInUrls(f.calls);
  });
  it("fails on a FAILED upload and on an HTTP error", async () => {
    const f = mockFetch([
      ["POST", /oauth2/, () => json({ access_token: "ya29.tok" })],
      ["POST", /:upload$/, () => json({ uploadState: "FAILED" })],
    ]);
    await assert.rejects(chromeClient(SECRETS.chrome, { fetch: f, sleep: noSleep }).upload(Buffer.from("z")), /state FAILED/);
    const g = mockFetch([["POST", /oauth2/, () => new Response("invalid_grant", { status: 400 })]]);
    await assert.rejects(chromeClient(SECRETS.chrome, { fetch: g, sleep: noSleep }).upload(Buffer.from("z")), /HTTP 400 invalid_grant/);
  });
});

describe("edgeClient", () => {
  it("uploads to the draft, polls the operation, publishes with notes and polls again", async () => {
    let n = 0;
    const base = "https://api.addons.microsoftedge.microsoft.com/v1/products/prod-42";
    const f = mockFetch([
      ["POST", `${base}/submissions/draft/package`, () => new Response(null, { status: 202, headers: { location: "op-up" } })],
      ["GET", `${base}/submissions/draft/package/operations/op-up`, () => json({ status: ++n < 2 ? "InProgress" : "Succeeded" })],
      ["POST", `${base}/submissions`, () => new Response(null, { status: 202, headers: { location: "/v1/products/prod-42/submissions/operations/op-pub" } })],
      ["GET", `${base}/submissions/operations/op-pub`, () => json({ status: "Succeeded" })],
    ]);
    const c = edgeClient(SECRETS.edge, { fetch: f, sleep: noSleep });
    await c.upload(Buffer.from("zip"));
    await c.publish("Notes for testers");
    const up = f.calls[0];
    assert.equal(up.headers.Authorization, "ApiKey edge-api-key-secret");
    assert.equal(up.headers["X-ClientID"], "edge-client-1");
    assert.equal(up.headers["Content-Type"], "application/zip");
    assert.deepEqual(JSON.parse(f.calls.find((x) => x.url === `${base}/submissions`).body), { notes: "Notes for testers" });
    noSecretInUrls(f.calls);
  });
  it("reports a failed operation", async () => {
    const f = mockFetch([
      ["POST", /draft\/package$/, () => new Response(null, { status: 202, headers: { location: "op" } })],
      ["GET", /operations\/op$/, () => json({ status: "Failed", message: "bad package", errors: [{ message: "x" }] })],
    ]);
    await assert.rejects(edgeClient(SECRETS.edge, { fetch: f, sleep: noSleep }).upload(Buffer.from("z")), /Edge upload failed: bad package/);
  });
  it("gives up after the poll limit", async () => {
    const f = mockFetch([
      ["POST", /draft\/package$/, () => new Response(null, { status: 202, headers: { location: "op" } })],
      ["GET", /operations\/op$/, () => json({ status: "InProgress" })],
    ]);
    await assert.rejects(edgeClient(SECRETS.edge, { fetch: f, sleep: noSleep, tries: 3 }).upload(Buffer.from("z")), /still in progress after 3/);
  });
});

describe("amoClient", () => {
  it("uploads to the listed channel, waits for validation, creates the version with source, then sets notes", async () => {
    let n = 0;
    const api = "https://addons.mozilla.org/api/v5";
    const f = mockFetch([
      ["POST", `${api}/addons/upload/`, () => json({ uuid: "u-1" })],
      ["GET", `${api}/addons/upload/u-1/`, () => json(++n < 2 ? { processed: false } : { uuid: "u-1", processed: true, valid: true })],
      ["POST", `${api}/addons/addon/google-scholar-utility-belt/versions/`, () => json({ id: 77, version: "1.2.0", file: { status: "unreviewed" } })],
      ["PATCH", `${api}/addons/addon/google-scholar-utility-belt/versions/77/`, () => json({ id: 77 })],
    ]);
    const c = amoClient(SECRETS.firefox, { fetch: f, sleep: noSleep });
    const up = await c.upload(Buffer.from("zip"), "pkg.zip");
    const v = await c.submit(up.uuid, Buffer.from("src"), "src.zip", "What changed", "How to build");
    assert.equal(v.id, 77);
    const [upload, , , create, patch] = f.calls;
    assert.match(upload.headers.Authorization, /^JWT eyJ/);
    assert.equal(upload.body.get("channel"), "listed");
    assert.equal(upload.body.get("upload").name, "pkg.zip");
    assert.equal(create.body.get("upload"), "u-1");
    assert.equal(create.body.get("source").name, "src.zip");
    assert.deepEqual(JSON.parse(patch.body), { release_notes: { "en-US": "What changed" }, approval_notes: "How to build" });
    noSecretInUrls(f.calls);
  });
  it("stops with the validator's errors when the package is invalid", async () => {
    const f = mockFetch([
      ["POST", /addons\/upload\/$/, () => json({ uuid: "u-2" })],
      ["GET", /upload\/u-2\/$/, () => json({ processed: true, valid: false, validation: { errors: 1, messages: [{ type: "error", message: "Bad manifest" }] } })],
    ]);
    await assert.rejects(amoClient(SECRETS.firefox, { fetch: f, sleep: noSleep }).upload(Buffer.from("z"), "p.zip"), /Bad manifest/);
    assert.equal(f.calls.filter((c) => c.url.includes("/versions/")).length, 0, "no version is created");
  });
});

describe("plan and run", () => {
  it("dry run is ready when files, versions and credentials check out, and sends nothing", async () => {
    const fx = fixture();
    try {
      const opts = { ...parseArgs([]), credentials: credFile(fx.root) };
      const f = mockFetch([]);
      const lines = [];
      const code = await run(opts, { root: fx.root, env: {}, live: { chrome: "1.1.0", firefox: "1.0.2" }, readManifest: fx.readManifest, fetch: f, log: (s) => lines.push(s) });
      assert.equal(code, 0, lines.join("\n"));
      assert.equal(f.calls.length, 0);
      const out = lines.join("\n");
      for (const v of secretValues(SECRETS)) assert.ok(!out.includes(v), "a secret was printed");
      assert.match(out, /Ready\./);
    } finally { fx.cleanup(); }
  });
  it("blocks a version the store already serves and missing credentials", async () => {
    const fx = fixture();
    try {
      const r = await plan({ ...parseArgs([]), credentials: path.join(fx.root, "none.json") },
        { root: fx.root, env: {}, live: { chrome: "1.2.0", firefox: "1.0.2" }, readManifest: fx.readManifest });
      assert.equal(r.ready, false);
      assert.ok(r.stores.chrome.problems.some((p) => /not newer than the 1\.2\.0/.test(p)));
      assert.ok(r.stores.firefox.problems.some((p) => /credentials missing: jwtIssuer, jwtSecret/.test(p)));
    } finally { fx.cleanup(); }
  });
  it("an upload run that fails its checks uploads nothing", async () => {
    const fx = fixture({ zipVersion: "1.1.9" });
    try {
      const f = mockFetch([]);
      const lines = [];
      const code = await run({ ...parseArgs(["--upload", "all", "--submit"]), credentials: credFile(fx.root) },
        { root: fx.root, env: {}, live: {}, readManifest: fx.readManifest, fetch: f, log: (s) => lines.push(s) });
      assert.equal(code, 1);
      assert.equal(f.calls.length, 0);
      assert.match(lines.join("\n"), /Nothing was uploaded/);
    } finally { fx.cleanup(); }
  });
  it("--upload without --submit never calls a publish or version endpoint, and errors are redacted", async () => {
    const fx = fixture();
    try {
      const f = mockFetch([
        ["POST", /oauth2/, () => json({ access_token: "ya29.tok" })],
        ["POST", /chromewebstore.*:upload$/, () => json({ uploadState: "SUCCEEDED", crxVersion: "1.2.0" })],
        ["POST", /draft\/package$/, () => new Response("rejected key edge-api-key-secret", { status: 401 })],
        ["POST", /addons\/upload\/$/, () => json({ uuid: "u" })],
        ["GET", /addons\/upload\/u\/$/, () => json({ uuid: "u", processed: true, valid: true })],
      ]);
      const lines = [];
      const code = await run({ ...parseArgs(["--upload", "all"]), credentials: credFile(fx.root) },
        { root: fx.root, env: {}, live: {}, readManifest: fx.readManifest, fetch: f, sleep: noSleep, log: (s) => lines.push(s) });
      assert.equal(code, 1, "the Edge failure sets the exit code");
      assert.equal(f.calls.filter((c) => /:publish|\/submissions$|\/versions\//.test(c.url)).length, 0);
      const out = lines.join("\n");
      assert.match(out, /edge: FAILED .*HTTP 401 rejected key \[redacted\]/);
      assert.ok(!out.includes("edge-api-key-secret"));
      assert.match(out, /chrome: uploaded version 1\.2\.0/);
      assert.match(out, /firefox: package validated/);
    } finally { fx.cleanup(); }
  });
});
