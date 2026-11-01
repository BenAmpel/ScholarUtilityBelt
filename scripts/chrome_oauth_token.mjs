#!/usr/bin/env node
// One-time step for scripts/publish_stores.mjs: turns the OAuth client file you download from Google Cloud ("Desktop
// app" client, Download JSON) into a Chrome Web Store refresh token, and saves the client id, client secret and refresh
// token in the chrome block of ~/.config/scholar-belt/store-credentials.json (mode 600). Nothing secret is printed.
//   node scripts/chrome_oauth_token.mjs ~/Downloads/client_secret_XXXX.json
// It opens Google's consent page in your default browser; sign in with the account that owns the extension and click
// Allow. Google sends the code back to a one-off server on 127.0.0.1 (loopback flow with PKCE). Delete the downloaded
// client file afterwards.
import fs from "node:fs";
import http from "node:http";
import crypto from "node:crypto";
import { execFile } from "node:child_process";
import { DEFAULT_CREDENTIALS } from "./publish_stores.mjs";

const SCOPE = "https://www.googleapis.com/auth/chromewebstore";
const AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL = "https://oauth2.googleapis.com/token";

const clientFile = process.argv[2];
if (!clientFile || !fs.existsSync(clientFile)) {
  console.error("usage: node scripts/chrome_oauth_token.mjs <downloaded client_secret_*.json>");
  process.exit(2);
}
const client = JSON.parse(fs.readFileSync(clientFile, "utf8")).installed;
if (!client?.client_id || !client?.client_secret) {
  console.error('That file has no "installed" client. Create the OAuth client as type "Desktop app" and download its JSON.');
  process.exit(1);
}

const verifier = crypto.randomBytes(48).toString("base64url");
const challenge = crypto.createHash("sha256").update(verifier).digest("base64url");
const state = crypto.randomBytes(16).toString("hex");

const server = http.createServer();
server.listen(0, "127.0.0.1", () => {
  const redirect = `http://127.0.0.1:${server.address().port}`;
  const url = `${AUTH_URL}?${new URLSearchParams({ client_id: client.client_id, redirect_uri: redirect, response_type: "code",
    scope: SCOPE, access_type: "offline", prompt: "consent", state, code_challenge: challenge, code_challenge_method: "S256" })}`;
  console.log("Opening Google's consent page. If it does not open, paste this address into the browser:\n" + url);
  execFile("open", [url], () => {});
  server.on("request", async (req, res) => {
    const q = new URL(req.url, redirect).searchParams;
    if (!q.has("code") && !q.has("error")) { res.writeHead(404).end(); return; }
    const finish = (code, msg) => { res.writeHead(200, { "Content-Type": "text/plain" }).end(msg); console.log(msg); server.close(); process.exitCode = code; };
    if (q.get("state") !== state) return finish(1, "State mismatch; nothing saved. Run the script again.");
    if (q.get("error")) return finish(1, `Google returned "${q.get("error")}"; nothing saved.`);
    try {
      const r = await fetch(TOKEN_URL, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ client_id: client.client_id, client_secret: client.client_secret, code: q.get("code"),
          code_verifier: verifier, redirect_uri: redirect, grant_type: "authorization_code" }) });
      const t = await r.json();
      if (!r.ok || !t.refresh_token) return finish(1, `Token exchange failed (HTTP ${r.status}, ${t.error || "no refresh_token"}); nothing saved.`);
      const dir = DEFAULT_CREDENTIALS.slice(0, DEFAULT_CREDENTIALS.lastIndexOf("/"));
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      const creds = fs.existsSync(DEFAULT_CREDENTIALS) ? JSON.parse(fs.readFileSync(DEFAULT_CREDENTIALS, "utf8")) : {};
      creds.chrome = { ...creds.chrome, clientId: client.client_id, clientSecret: client.client_secret, refreshToken: t.refresh_token };
      fs.writeFileSync(DEFAULT_CREDENTIALS, JSON.stringify(creds, null, 2) + "\n", { mode: 0o600 });
      fs.chmodSync(DEFAULT_CREDENTIALS, 0o600);
      finish(0, `Saved the Chrome client and refresh token in ${DEFAULT_CREDENTIALS}. You can close this tab and delete ${clientFile}.`);
    } catch (e) {
      finish(1, `Token exchange failed (${e.message}); nothing saved.`);
    }
  });
});
