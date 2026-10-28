#!/usr/bin/env node
// Prints the public state of each store listing: version, last update, users and ratings. Reads public pages and
// APIs only (no login). Run: node scripts/store_status.mjs   (add --json for machine-readable output)
const CWS = "https://chromewebstore.google.com/detail/omcogfcgldfmihfogbffflbocdbjockn";
const AMO = "https://addons.mozilla.org/api/v5/addons/addon/google-scholar-utility-belt/";
const pick = (re, s) => (s.match(re) || [])[1] || null;
async function chrome() {
  const html = await (await fetch(CWS, { redirect: "follow" })).text();
  return {
    store: "chrome", name: pick(/<title>([^<]*?) - Chrome Web Store<\/title>/, html),
    version: pick(/Version<\/div><div[^>]*>([^<]+)</, html), updated: pick(/Updated<\/div><div>([^<]+)</, html),
    users: pick(/([\d,]+\+? users)/, html), ratings: pick(/(\d+) ratings?/, html),
    average: pick(/aria-label="Average rating ([\d.]+) out of 5/, html),
  };
}
async function firefox() {
  const d = await (await fetch(AMO)).json();
  return {
    store: "firefox", name: d.name?.["en-US"] || null, version: d.current_version?.version || null,
    updated: d.last_updated || null, users: d.average_daily_users ?? null,
    ratings: d.ratings?.count ?? null, average: d.ratings?.average ?? null, status: d.status || null,
  };
}
const out = [];
for (const f of [chrome, firefox]) { try { out.push(await f()); } catch (e) { out.push({ store: f.name, error: String(e.message || e) }); } }
if (process.argv.includes("--json")) console.log(JSON.stringify(out, null, 2));
else for (const r of out) console.log(r.error ? `${r.store}: ERROR ${r.error}` : `${r.store}: ${r.name} | v${r.version} | updated ${r.updated} | users ${r.users} | ratings ${r.ratings} (avg ${r.average})${r.status ? " | " + r.status : ""}`);
