// Bring-your-own-key provider adapters for "Ask my shortlist".
//
// Each adapter turns a system message and a user message into one HTTPS request to the provider the
// user chose, and reads the text and token usage back out. The request is made by the service worker
// with the user's own API key. Nothing here knows about the DOM or chrome.*, and `fetchImpl` is
// injected so tests never touch the network.
//
// The endpoint paths, header names and JSON shapes below are written from memory of each provider's
// public documentation (Anthropic Messages API, OpenAI Chat Completions, Google Gemini
// generateContent). They have not been exercised against the live services from this code. If a
// provider changes its API, the adapter for it is the only thing to update.

import { buildAskPrompt, parseAskResponse, verifyAskRows, DEFAULT_MAX_OUTPUT_TOKENS } from "./ask-verify.js";

const MODEL_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,79}$/;

export function isValidModelName(m) {
  return typeof m === "string" && MODEL_RE.test(m);
}

export const PROVIDERS = {
  anthropic: {
    id: "anthropic",
    label: "Anthropic (Claude)",
    origin: "https://api.anthropic.com/*",
    host: "api.anthropic.com",
    buildRequest({ apiKey, model, system, user, maxOutputTokens }) {
      return {
        url: "https://api.anthropic.com/v1/messages",
        headers: {
          "content-type": "application/json",
          "x-api-key": apiKey,
          "anthropic-version": "2023-06-01",
        },
        body: { model, max_tokens: maxOutputTokens, system, messages: [{ role: "user", content: user }] },
      };
    },
    parseResponse(json) {
      const parts = Array.isArray(json && json.content) ? json.content : [];
      const text = parts.filter((p) => p && p.type === "text" && typeof p.text === "string").map((p) => p.text).join("");
      const u = (json && json.usage) || {};
      return { text, inputTokens: num(u.input_tokens), outputTokens: num(u.output_tokens) };
    },
  },
  openai: {
    id: "openai",
    label: "OpenAI",
    origin: "https://api.openai.com/*",
    host: "api.openai.com",
    buildRequest({ apiKey, model, system, user, maxOutputTokens }) {
      return {
        url: "https://api.openai.com/v1/chat/completions",
        headers: { "content-type": "application/json", authorization: "Bearer " + apiKey },
        body: {
          model,
          // Recalled: newer OpenAI models take max_completion_tokens and reject max_tokens.
          max_completion_tokens: maxOutputTokens,
          messages: [
            { role: "system", content: system },
            { role: "user", content: user },
          ],
        },
      };
    },
    parseResponse(json) {
      const c = json && Array.isArray(json.choices) ? json.choices[0] : null;
      const text = c && c.message && typeof c.message.content === "string" ? c.message.content : "";
      const u = (json && json.usage) || {};
      return { text, inputTokens: num(u.prompt_tokens), outputTokens: num(u.completion_tokens) };
    },
  },
  gemini: {
    id: "gemini",
    label: "Google Gemini",
    origin: "https://generativelanguage.googleapis.com/*",
    host: "generativelanguage.googleapis.com",
    buildRequest({ apiKey, model, system, user, maxOutputTokens }) {
      return {
        // The key goes in a header, not the query string, so it never appears in a URL.
        url: `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent`,
        headers: { "content-type": "application/json", "x-goog-api-key": apiKey },
        body: {
          systemInstruction: { parts: [{ text: system }] },
          contents: [{ role: "user", parts: [{ text: user }] }],
          generationConfig: { maxOutputTokens },
        },
      };
    },
    parseResponse(json) {
      const cand = json && Array.isArray(json.candidates) ? json.candidates[0] : null;
      const parts = cand && cand.content && Array.isArray(cand.content.parts) ? cand.content.parts : [];
      const text = parts.filter((p) => p && typeof p.text === "string").map((p) => p.text).join("");
      const u = (json && json.usageMetadata) || {};
      return { text, inputTokens: num(u.promptTokenCount), outputTokens: num(u.candidatesTokenCount) };
    },
  },
};

function num(v) {
  return Number.isFinite(v) ? v : null;
}

export function getProvider(id) {
  return Object.prototype.hasOwnProperty.call(PROVIDERS, id) ? PROVIDERS[id] : null;
}

/** Remove every occurrence of the key from a string that might be shown or logged. */
export function redactKey(text, apiKey) {
  let s = String(text == null ? "" : text);
  if (apiKey && apiKey.length >= 6) s = s.split(apiKey).join("[key removed]");
  return s;
}

function describeHttpError(status, bodyText, apiKey) {
  let detail = "";
  try {
    const j = JSON.parse(bodyText);
    const m = (j && j.error && (j.error.message || j.error.status)) || (j && j.message);
    if (typeof m === "string") detail = m;
  } catch {
    /* body was not JSON; leave detail empty */
  }
  detail = redactKey(detail, apiKey).slice(0, 200);
  let hint = "";
  if (status === 401 || status === 403) hint = " Check the API key in Options.";
  else if (status === 429) hint = " The provider is rate limiting this key or the account is out of credit.";
  return `The provider returned HTTP ${status}.${detail ? " " + detail : ""}${hint}`;
}

/**
 * Run one question over the selected papers.
 * Returns { ok: true, rows, usage } or { ok: false, error }. The error text never contains the key.
 * `papers` are the objects from selectPapersForAsk; `confirmed` must be true or nothing is sent.
 */
export async function runAsk({
  providerId, apiKey, model, question, papers, confirmed,
  maxOutputTokens = DEFAULT_MAX_OUTPUT_TOKENS, fetchImpl, timeoutMs = 60000,
}) {
  if (confirmed !== true) return { ok: false, error: "Not confirmed. Nothing was sent." };
  const provider = getProvider(providerId);
  if (!provider) return { ok: false, error: "Unknown provider." };
  if (typeof apiKey !== "string" || apiKey.trim().length < 8) return { ok: false, error: "No API key is saved. Add one in Options." };
  if (!isValidModelName(model)) return { ok: false, error: "The model name is not valid. Check it in Options." };
  if (!Array.isArray(papers) || !papers.length) return { ok: false, error: "No papers with saved text were selected." };
  if (typeof question !== "string" || !question.trim()) return { ok: false, error: "Type a question first." };
  const key = apiKey.trim();
  const { system, user } = buildAskPrompt({ question, papers });
  const req = provider.buildRequest({ apiKey: key, model, system, user, maxOutputTokens });
  const doFetch = fetchImpl || (typeof fetch === "function" ? fetch : null);
  if (!doFetch) return { ok: false, error: "fetch is not available." };
  const ctrl = typeof AbortController === "function" ? new AbortController() : null;
  const timer = ctrl ? setTimeout(() => ctrl.abort(), timeoutMs) : null;
  let res;
  try {
    res = await doFetch(req.url, {
      method: "POST",
      headers: req.headers,
      body: JSON.stringify(req.body),
      // No cookies, no referrer, and a redirect is an error: the key must only reach the chosen host.
      credentials: "omit",
      referrerPolicy: "no-referrer",
      redirect: "error",
      signal: ctrl ? ctrl.signal : undefined,
    });
  } catch (e) {
    const aborted = e && e.name === "AbortError";
    return { ok: false, error: aborted ? "The request timed out." : "Could not reach the provider. Check the connection and the host permission." };
  } finally {
    if (timer) clearTimeout(timer);
  }
  let bodyText;
  try {
    bodyText = await res.text();
  } catch {
    return { ok: false, error: "The provider's reply could not be read." };
  }
  if (!res.ok) return { ok: false, error: describeHttpError(res.status, bodyText, key) };
  let json;
  try {
    json = JSON.parse(bodyText);
  } catch {
    return { ok: false, error: "The provider's reply was not valid JSON." };
  }
  const parsed = provider.parseResponse(json);
  const raw = parseAskResponse(parsed.text);
  const rows = verifyAskRows(raw, papers);
  return {
    ok: true,
    rows,
    usage: { inputTokens: parsed.inputTokens, outputTokens: parsed.outputTokens },
    outputParsed: raw !== null,
  };
}
