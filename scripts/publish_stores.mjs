#!/usr/bin/env node
// Uploads a built release to the Chrome Web Store, Microsoft Edge Add-ons and Firefox Add-ons (AMO) through each
// store's API, and optionally submits it for review. Setup and the one-time credential steps: docs/store-publishing.md.
//   node scripts/publish_stores.mjs                                  dry run: checks files, versions, sha256s, credentials
//   node scripts/publish_stores.mjs --upload chrome,edge,firefox     uploads (Chrome/Edge: draft; Firefox: validation only)
//   node scripts/publish_stores.mjs --upload chrome,edge,firefox --submit   uploads and submits for review
// Credentials come only from ~/.config/scholar-belt/store-credentials.json (mode 600) or SUB_* env vars. They are sent in
// headers or form bodies, never in a URL, and never printed.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const STORES = ["chrome", "edge", "firefox"];
export const CHROME_ITEM_ID = "omcogfcgldfmihfogbffflbocdbjockn";
export const AMO_SLUG = "google-scholar-utility-belt";
export const DEFAULT_CREDENTIALS = path.join(os.homedir(), ".config", "scholar-belt", "store-credentials.json");
const CHROME_TOKEN_URL = "https://oauth2.googleapis.com/token";
const CHROME_API = "https://www.googleapis.com";
const EDGE_API = "https://api.addons.microsoftedge.microsoft.com";
const AMO_API = "https://addons.mozilla.org/api/v5";

// Each store's credential fields: [key in the JSON file, env var].
export const CREDENTIAL_FIELDS = {
  chrome: [["clientId", "SUB_CHROME_CLIENT_ID"], ["clientSecret", "SUB_CHROME_CLIENT_SECRET"],
    ["refreshToken", "SUB_CHROME_REFRESH_TOKEN"]],
  edge: [["clientId", "SUB_EDGE_CLIENT_ID"], ["apiKey", "SUB_EDGE_API_KEY"], ["productId", "SUB_EDGE_PRODUCT_ID"]],
  firefox: [["jwtIssuer", "SUB_AMO_JWT_ISSUER"], ["jwtSecret", "SUB_AMO_JWT_SECRET"]],
};
// Identifiers that are not secret.
const PUBLIC_FIELDS = new Set(["publisherId", "productId"]);

export const USAGE = `usage: node scripts/publish_stores.mjs [--upload chrome,edge,firefox] [--submit] [options]
  (no --upload)          dry run: check everything and print what would happen; nothing is sent
  --upload LIST          upload to these stores (comma-separated, or "all")
  --submit               also submit the uploaded version for review (needs --upload)
  --only LIST            limit the dry run's checks to these stores
  --version X            release version (default: manifest.json)
  --credentials FILE     credentials JSON (default ~/.config/scholar-belt/store-credentials.json)
  --release-notes FILE   Firefox release notes and Edge certification notes (default output/firefox-store/release-notes.txt)
  --approval-notes FILE  AMO notes to reviewer (default output/firefox-store/SOURCE-NOTES.txt)
  --offline              do not read the stores' public listings for their current version`;

export function parseArgs(argv) {
  const opts = { upload: [], submit: false, only: null, version: null, credentials: null, releaseNotes: null,
    approvalNotes: null, offline: false, help: false };
  const value = (v, flag) => { if (!v || v.startsWith("--")) throw new Error(`${flag} needs a value`); return v; };
  const list = (v, flag) => {
    value(v, flag);
    const items = v === "all" ? [...STORES] : v.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
    for (const s of items) if (!STORES.includes(s)) throw new Error(`unknown store "${s}" (use ${STORES.join(", ")})`);
    return [...new Set(items)];
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--upload") opts.upload = list(argv[++i], a);
    else if (a === "--submit") opts.submit = true;
    else if (a === "--only") opts.only = list(argv[++i], a);
    else if (a === "--version") opts.version = value(argv[++i], a);
    else if (a === "--credentials") opts.credentials = value(argv[++i], a);
    else if (a === "--release-notes") opts.releaseNotes = value(argv[++i], a);
    else if (a === "--approval-notes") opts.approvalNotes = value(argv[++i], a);
    else if (a === "--offline") opts.offline = true;
    else if (a === "--help" || a === "-h") opts.help = true;
    else throw new Error(`unknown argument ${a}`);
  }
  if (opts.submit && !opts.upload.length) throw new Error("--submit needs --upload: it submits what this run uploads");
  if (opts.upload.length && opts.only) throw new Error("--only is for the dry run; --upload already names the stores");
  if (opts.version && !/^\d+(\.\d+){0,3}$/.test(opts.version)) throw new Error(`bad version ${opts.version}`);
  opts.stores = opts.upload.length ? opts.upload : (opts.only || [...STORES]);
  opts.dryRun = !opts.upload.length;
  return opts;
}

export function compareVersions(a, b) {
  const pa = String(a).split(".").map(Number), pb = String(b).split(".").map(Number);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d) return d < 0 ? -1 : 1;
  }
  return 0;
}

export function releaseFiles(version, root = ROOT) {
  const out = path.join(root, "output");
  return {
    chrome: { package: path.join(out, `ScholarUtilityBelt-${version}.zip`) },
    edge: { package: path.join(out, `ScholarUtilityBelt-${version}-edge.zip`) },
    firefox: {
      package: path.join(out, `ScholarUtilityBelt-${version}-firefox.zip`),
      source: path.join(out, "firefox-store", `ScholarUtilityBelt-${version}-source.zip`),
    },
    checklist: path.join(out, `UPLOAD-${version}.md`),
    releaseNotes: path.join(out, "firefox-store", "release-notes.txt"),
    approvalNotes: path.join(out, "firefox-store", "SOURCE-NOTES.txt"),
  };
}

export const sha256 = (buf) => crypto.createHash("sha256").update(buf).digest("hex");

// The upload checklist (output/UPLOAD-<version>.md) has table rows `path` | `sha256`; map file name -> sha256.
export function checklistHashes(markdown) {
  const map = new Map();
  for (const m of markdown.matchAll(/`([^`\s]+\.zip)`\s*\|\s*`([0-9a-f]{64})`/g)) map.set(path.basename(m[1]), m[2]);
  return map;
}

const readZipManifest = (zip) => JSON.parse(execFileSync("unzip", ["-p", zip, "manifest.json"], { encoding: "utf8" }));

// File checks for one store: { problems, warnings, info }. readManifest is injectable for tests.
export function checkFiles(store, version, files, { readManifest = readZipManifest, hashes = new Map(), root = ROOT } = {}) {
  const problems = [], warnings = [], info = [];
  const check = (label, file) => {
    const rel = path.relative(root, file);
    if (!fs.existsSync(file)) { problems.push(`${label} missing: ${rel}`); return; }
    const buf = fs.readFileSync(file);
    if (!buf.length) { problems.push(`${label} is empty: ${rel}`); return; }
    const sum = sha256(buf);
    info.push(`${label}: ${rel} (${(buf.length / 1048576).toFixed(2)} MB, sha256 ${sum})`);
    const expected = hashes.get(path.basename(file));
    if (expected && expected !== sum) problems.push(`${label} sha256 differs from the upload checklist (${expected})`);
    else if (!expected && hashes.size) warnings.push(`${label} is not listed in the upload checklist`);
    let manifest;
    try { manifest = readManifest(file); } catch (e) { problems.push(`${label}: cannot read manifest.json (${e.message})`); return; }
    if (manifest.version !== version) problems.push(`${label}: manifest version ${manifest.version}, expected ${version}`);
    if (store === "firefox" && label === "package" && !manifest.browser_specific_settings?.gecko?.id)
      problems.push("package has no browser_specific_settings.gecko.id (was it made by make_variant.mjs firefox?)");
  };
  check("package", files[store].package);
  if (store === "firefox") check("source", files.firefox.source);
  return { problems, warnings, info };
}

export function loadCredentials(file, env = process.env) {
  const result = { chrome: {}, edge: {}, firefox: {}, source: [], problems: [] };
  if (fs.existsSync(file)) {
    const mode = fs.statSync(file).mode & 0o777;
    if (mode & 0o077) {
      result.problems.push(`${file} is readable by other users (mode ${mode.toString(8)}); run: chmod 600 "${file}"`);
    } else {
      let data = {};
      try { data = JSON.parse(fs.readFileSync(file, "utf8")); } catch { result.problems.push(`${file} is not valid JSON`); }
      for (const s of STORES) for (const [k] of CREDENTIAL_FIELDS[s]) if (data[s]?.[k]) result[s][k] = String(data[s][k]);
      result.source.push(file);
    }
  }
  let fromEnv = false;
  for (const s of STORES) for (const [k, e] of CREDENTIAL_FIELDS[s]) if (env[e]) { result[s][k] = env[e]; fromEnv = true; }
  if (fromEnv) result.source.push("environment");
  return result;
}

export const missingCredentials = (creds, store) => CREDENTIAL_FIELDS[store].filter(([k]) => !creds[store][k]).map(([k]) => k);

// Every secret value, so messages from a store can be scrubbed before printing.
export function secretValues(creds) {
  const vals = [];
  for (const s of STORES) for (const [k] of CREDENTIAL_FIELDS[s]) if (!PUBLIC_FIELDS.has(k) && creds[s][k]) vals.push(creds[s][k]);
  return vals;
}
export function redact(text, secrets) {
  let s = String(text);
  for (const v of secrets) if (v && v.length >= 4) s = s.split(v).join("[redacted]");
  // Google access tokens and JWTs a store may echo back.
  return s.replace(/ya29\.[\w.-]+/g, "[redacted]").replace(/eyJ[\w-]+\.[\w-]+\.[\w-]+/g, "[redacted]");
}

class StoreError extends Error {}
async function expectOk(res, what) {
  if (res.ok) return res;
  let body = "";
  try { body = (await res.text()).slice(0, 2000); } catch { /* no body */ }
  throw new StoreError(`${what}: HTTP ${res.status}${body ? ` ${body}` : ""}`);
}
async function poll(fn, { sleep, interval, tries, what }) {
  for (let i = 0; i < tries; i++) {
    const r = await fn();
    if (r.done) return r.value;
    await sleep(interval);
  }
  throw new StoreError(`${what}: still in progress after ${tries} checks; look in the store's dashboard`);
}
const defaultSleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- Chrome Web Store API v1.1 ----
// v1.1, not v2: on 2026-11-01 every v2 call for this item (with the dashboard's publisher id, "me" or "-") returned 403
// while v1.1 with the same token read the item. v1.1 needs no publisher id.
export function chromeClient(creds, { fetch = globalThis.fetch, sleep = defaultSleep, itemId = CHROME_ITEM_ID, interval = 5000, tries = 60 } = {}) {
  let token = null;
  const auth = async () => {
    if (token) return token;
    const body = new URLSearchParams({ client_id: creds.clientId, client_secret: creds.clientSecret,
      refresh_token: creds.refreshToken, grant_type: "refresh_token" });
    const res = await expectOk(await fetch(CHROME_TOKEN_URL, { method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" }, body }), "Chrome OAuth token");
    token = (await res.json()).access_token;
    if (!token) throw new StoreError("Chrome OAuth token: no access_token in the response");
    return token;
  };
  const headers = async (extra = {}) => ({ Authorization: `Bearer ${await auth()}`, "x-goog-api-version": "2", ...extra });
  const failed = (r) => `Chrome upload: ${r.uploadState} ${JSON.stringify(r.itemError || [])}`;
  return {
    async upload(buf) {
      const res = await expectOk(await fetch(`${CHROME_API}/upload/chromewebstore/v1.1/items/${itemId}`, { method: "PUT",
        headers: await headers({ "Content-Type": "application/zip" }), body: buf }), "Chrome upload");
      const r = await res.json();
      if (r.uploadState === "SUCCESS") return r;
      if (r.uploadState !== "IN_PROGRESS") throw new StoreError(failed(r));
      return poll(async () => {
        const d = await (await expectOk(await fetch(`${CHROME_API}/chromewebstore/v1.1/items/${itemId}?projection=DRAFT`,
          { headers: await headers() }), "Chrome status")).json();
        if (d.uploadState === "SUCCESS") return { done: true, value: d };
        if (d.uploadState === "FAILURE" || d.uploadState === "NOT_FOUND") throw new StoreError(failed(d));
        return { done: false };
      }, { sleep, interval, tries, what: "Chrome upload" });
    },
    async publish() {
      // Submits the draft for review; it is published once approved.
      const res = await expectOk(await fetch(`${CHROME_API}/chromewebstore/v1.1/items/${itemId}/publish`, { method: "POST",
        headers: await headers({ "Content-Length": "0" }) }), "Chrome publish");
      const r = await res.json();
      const ok = ["OK", "ITEM_PENDING_REVIEW"];
      if (!(r.status || []).some((x) => ok.includes(x))) throw new StoreError(`Chrome publish: ${JSON.stringify(r)}`);
      return r;
    },
  };
}

// ---- Microsoft Edge Add-ons API v1.1 ----
const operationId = (res) => {
  const id = (res.headers.get("location") || "").split("/").filter(Boolean).pop();
  if (!id) throw new StoreError("Edge: no operation id in the Location header");
  return id;
};
export function edgeClient(creds, { fetch = globalThis.fetch, sleep = defaultSleep, interval = 5000, tries = 120 } = {}) {
  const product = `${EDGE_API}/v1/products/${encodeURIComponent(creds.productId)}`;
  const headers = (extra = {}) => ({ Authorization: `ApiKey ${creds.apiKey}`, "X-ClientID": creds.clientId, ...extra });
  const waitFor = (url, what) => poll(async () => {
    const r = await (await expectOk(await fetch(url, { headers: headers() }), what)).json();
    if (r.status === "Succeeded") return { done: true, value: r };
    if (r.status === "Failed") throw new StoreError(`${what} failed: ${r.message || ""} ${JSON.stringify(r.errors || [])}`);
    return { done: false };
  }, { sleep, interval, tries, what });
  return {
    async upload(buf) {
      const res = await expectOk(await fetch(`${product}/submissions/draft/package`, { method: "POST",
        headers: headers({ "Content-Type": "application/zip" }), body: buf }), "Edge upload");
      return waitFor(`${product}/submissions/draft/package/operations/${operationId(res)}`, "Edge upload");
    },
    async publish(notes) {
      const res = await expectOk(await fetch(`${product}/submissions`, { method: "POST",
        headers: headers({ "Content-Type": "application/json" }), body: JSON.stringify({ notes }) }), "Edge publish");
      return waitFor(`${product}/submissions/operations/${operationId(res)}`, "Edge publish");
    },
  };
}

// ---- Firefox Add-ons (AMO) API v5 ----
const b64url = (x) => Buffer.from(typeof x === "string" ? x : JSON.stringify(x)).toString("base64url");
export function amoJwt(issuer, secret, now = Math.floor(Date.now() / 1000)) {
  // AMO rejects an exp more than five minutes after iat; jti must be unique per request.
  const head = b64url({ alg: "HS256", typ: "JWT" });
  const body = b64url({ iss: issuer, jti: crypto.randomUUID(), iat: now, exp: now + 60 });
  const sig = crypto.createHmac("sha256", secret).update(`${head}.${body}`).digest("base64url");
  return `${head}.${body}.${sig}`;
}
export function amoClient(creds, { fetch = globalThis.fetch, sleep = defaultSleep, slug = AMO_SLUG, interval = 5000, tries = 120 } = {}) {
  const headers = (extra = {}) => ({ Authorization: `JWT ${amoJwt(creds.jwtIssuer, creds.jwtSecret)}`, ...extra });
  const versions = `${AMO_API}/addons/addon/${slug}/versions/`;
  return {
    // Creates an upload and waits for AMO's validator. Reviewers see nothing until a version is created.
    async upload(buf, fileName) {
      const form = new FormData();
      form.append("upload", new Blob([buf], { type: "application/zip" }), fileName);
      form.append("channel", "listed");
      const res = await expectOk(await fetch(`${AMO_API}/addons/upload/`, { method: "POST", headers: headers(), body: form }), "AMO upload");
      const { uuid } = await res.json();
      if (!uuid) throw new StoreError("AMO upload: no uuid in the response");
      const done = await poll(async () => {
        const r = await (await expectOk(await fetch(`${AMO_API}/addons/upload/${uuid}/`, { headers: headers() }), "AMO validation")).json();
        return r.processed ? { done: true, value: r } : { done: false };
      }, { sleep, interval, tries, what: "AMO validation" });
      if (!done.valid) {
        const msgs = (done.validation?.messages || []).filter((m) => m.type === "error").slice(0, 10).map((m) => `  - ${m.message}`);
        throw new StoreError(`AMO validation failed (${done.validation?.errors ?? "?"} errors):\n${msgs.join("\n")}`);
      }
      return done;
    },
    // Creating a listed version submits it for review. The source zip goes in that request (a multipart create takes
    // only upload, source and license); the release and reviewer notes follow in a PATCH.
    async submit(uuid, sourceBuf, sourceName, releaseNotes, approvalNotes) {
      const form = new FormData();
      form.append("upload", uuid);
      form.append("source", new Blob([sourceBuf], { type: "application/zip" }), sourceName);
      const v = await (await expectOk(await fetch(versions, { method: "POST", headers: headers(), body: form }), "AMO version")).json();
      await expectOk(await fetch(`${versions}${v.id}/`, { method: "PATCH", headers: headers({ "Content-Type": "application/json" }),
        body: JSON.stringify({ release_notes: { "en-US": releaseNotes }, approval_notes: approvalNotes }) }), "AMO notes");
      return v;
    },
  };
}

// ---- Public store versions (store_status.mjs; Edge is not there) ----
async function liveVersions(offline) {
  if (offline) return {};
  const { chrome, firefox } = await import("./store_status.mjs");
  const out = {};
  for (const [s, f] of [["chrome", chrome], ["firefox", firefox]]) {
    try { out[s] = (await f()).version || null; } catch (e) { out[s] = { error: e.message }; }
  }
  return out;
}

const textOf = (f) => (fs.existsSync(f) ? fs.readFileSync(f, "utf8").trim() : "");

// Runs every check that needs no credentials; returns a per-store report and whether the selected stores are ready.
export async function plan(opts, { root = ROOT, env = process.env, live = null, readManifest } = {}) {
  const manifestVersion = JSON.parse(fs.readFileSync(path.join(root, "manifest.json"), "utf8")).version;
  const version = opts.version || manifestVersion;
  const files = releaseFiles(version, root);
  const hashes = fs.existsSync(files.checklist) ? checklistHashes(fs.readFileSync(files.checklist, "utf8")) : new Map();
  const creds = loadCredentials(opts.credentials || DEFAULT_CREDENTIALS, env);
  const versions = live || await liveVersions(opts.offline);
  const notes = { release: opts.releaseNotes || files.releaseNotes, approval: opts.approvalNotes || files.approvalNotes };
  const general = [...creds.problems];
  if (version !== manifestVersion) general.push(`manifest.json is ${manifestVersion}, not ${version}`);
  const report = { version, files, creds, notes, general, stores: {} };
  if (!hashes.size) report.general.push(`no sha256 table in ${path.relative(root, files.checklist)}; hashes are shown but not compared`);
  for (const store of opts.stores) {
    const r = checkFiles(store, version, files, { readManifest, hashes, root });
    const served = versions[store];
    if (store === "edge") r.info.push("store version: not checked (the Edge listing is not in store_status.mjs)");
    else if (served && typeof served === "object") r.warnings.push(`could not read the listing: ${served.error}`);
    else if (served) {
      r.info.push(`store serves ${served}`);
      if (compareVersions(version, served) <= 0) r.problems.push(`version ${version} is not newer than the ${served} the store serves`);
    } else if (!opts.offline) r.warnings.push("the listing showed no version");
    const missing = missingCredentials(creds, store);
    if (missing.length) r.problems.push(`credentials missing: ${missing.join(", ")} (see docs/store-publishing.md)`);
    else if (store === "chrome") r.info.push(`item ${CHROME_ITEM_ID}`);
    else if (store === "edge") r.info.push(`product id ${creds.edge.productId}`);
    const needed = store === "firefox" ? [["release notes", notes.release], ["approval notes", notes.approval]]
      : store === "edge" ? [["certification notes", notes.release]] : [];
    for (const [label, f] of needed) {
      const t = textOf(f);
      if (!t) r.problems.push(`${label} missing or empty: ${path.relative(root, f)}`);
      else r.info.push(`${label}: ${path.relative(root, f)} (${t.length} characters)`);
    }
    if (store === "firefox" && textOf(notes.approval) && !textOf(notes.approval).includes(version))
      r.warnings.push(`approval notes do not mention ${version}; check the commit and version they name`);
    report.stores[store] = r;
  }
  report.ready = !creds.problems.length && version === manifestVersion && opts.stores.every((s) => !report.stores[s].problems.length);
  return report;
}

const ACTIONS = {
  chrome: (submit) => submit ? "upload the package, then submit it for review (it publishes when approved)"
    : "upload the package as a draft (submit it in the dashboard, or rerun with --submit)",
  edge: (submit) => submit ? "upload the package to the draft submission, then publish it with the certification notes"
    : "upload the package to the draft submission (publish it in Partner Center, or rerun with --submit)",
  firefox: (submit) => submit ? "upload and validate the package, then create the listed version with the source zip and notes (this submits it for review)"
    : "upload and validate the package only (AMO keeps nothing for review until a version is created; --submit creates it)",
};

export function formatReport(report, opts) {
  const mode = opts.dryRun ? "dry run, nothing will be sent" : opts.submit ? "upload and submit" : "upload only";
  const lines = [`Scholar Utility Belt ${report.version}: ${mode}`];
  if (report.creds.source.length) lines.push(`credentials from: ${report.creds.source.join(", ")}`);
  for (const g of report.general) lines.push(`! ${g}`);
  for (const store of opts.stores) {
    const r = report.stores[store];
    lines.push("", `${store}:`);
    for (const i of r.info) lines.push(`  ${i}`);
    for (const w of r.warnings) lines.push(`  warning: ${w}`);
    for (const p of r.problems) lines.push(`  PROBLEM: ${p}`);
    lines.push(opts.dryRun ? `  with --upload ${store} --submit it would ${ACTIONS[store](true)}` : `  will ${ACTIONS[store](opts.submit)}`);
  }
  lines.push("", report.ready ? "Ready." : "Not ready: fix the problems above.");
  return lines.join("\n");
}

export async function run(opts, { log = console.log, fetch = globalThis.fetch, sleep = defaultSleep, ...deps } = {}) {
  const report = await plan(opts, deps);
  log(formatReport(report, opts));
  if (opts.dryRun) return report.ready ? 0 : 1;
  if (!report.ready) { log("Nothing was uploaded."); return 1; }
  const secrets = secretValues(report.creds);
  let failed = 0;
  for (const store of opts.stores) {
    const pkg = report.files[store].package;
    try {
      if (store === "chrome") {
        const c = chromeClient(report.creds.chrome, { fetch, sleep });
        const up = await c.upload(fs.readFileSync(pkg));
        log(`chrome: uploaded the package (${up.uploadState})`);
        if (opts.submit) {
          const p = await c.publish();
          log(`chrome: submitted, status ${(p.status || []).join(", ")}${(p.statusDetail || []).map((d) => `\n  ${d}`).join("")}`);
        }
      } else if (store === "edge") {
        const c = edgeClient(report.creds.edge, { fetch, sleep });
        await c.upload(fs.readFileSync(pkg));
        log("edge: package uploaded to the draft submission");
        if (opts.submit) { await c.publish(textOf(report.notes.release)); log("edge: submitted for certification"); }
      } else {
        const c = amoClient(report.creds.firefox, { fetch, sleep });
        const up = await c.upload(fs.readFileSync(pkg), path.basename(pkg));
        log(`firefox: package validated (upload ${up.uuid}${up.validation?.warnings ? `, ${up.validation.warnings} validator warnings` : ""})`);
        if (opts.submit) {
          const src = report.files.firefox.source;
          const v = await c.submit(up.uuid, fs.readFileSync(src), path.basename(src), textOf(report.notes.release), textOf(report.notes.approval));
          log(`firefox: version ${v.version} created and submitted for review, file status ${v.file?.status || "unknown"}`);
        }
      }
    } catch (e) {
      failed++;
      log(`${store}: FAILED ${redact(e.message, secrets)}`);
    }
  }
  return failed ? 1 : 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  let opts;
  try { opts = parseArgs(process.argv.slice(2)); } catch (e) { console.error(`${e.message}\n${USAGE}`); process.exit(2); }
  if (opts.help) { console.log(USAGE); process.exit(0); }
  process.exitCode = await run(opts);
}
