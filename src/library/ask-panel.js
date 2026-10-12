// "Ask my shortlist" panel on the Library page. Pro feature, bring your own API key.
//
// Flow: pick papers (tick boxes, or a whole collection), type a question, press "Review what will be sent".
// That shows the provider, the number of papers, a token and cost estimate and the exact text. Nothing leaves
// the machine until the user presses Send. The request itself is made by the service worker, which reads the
// key from chrome.storage.local; this page never sees the key's value in a response.
import { showProPrompt } from "../common/pro-prompt.js";
import { getLibraryState, getSavedPapers } from "../common/storage.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import {
  ASK_CONFIG_KEY, NO_SUPPORT, buildAskPrompt, estimateCost, formatUsd, normalizeAskConfig, selectPapersForAsk,
  MAX_QUESTION_CHARS, DEFAULT_MAX_OUTPUT_TOKENS,
} from "../common/ask-verify.js";
import { getProvider } from "../common/ask-provider.js";

const $ = (id) => document.getElementById(id);
const picked = new Set();
let allowed = false;
let pending = null; // { keys, question } shown in the confirm box
let busy = false;

export function isPicked(key) {
  return picked.has(key);
}

export function setPicked(key, on) {
  if (on) picked.add(key);
  else picked.delete(key);
  updateCount();
}

function updateCount() {
  const n = $("askPickedCount");
  if (n) n.textContent = picked.size ? `${picked.size} ticked` : "none ticked";
}

function msg(text) {
  $("askMessage").textContent = text || "";
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

async function refreshScopes() {
  const sel = $("askScope");
  const prev = sel.value;
  const state = await getLibraryState();
  clear(sel);
  const first = document.createElement("option");
  first.value = "picked";
  first.textContent = "Papers I tick in the list";
  sel.appendChild(first);
  for (const c of Array.isArray(state?.collections) ? state.collections : []) {
    const o = document.createElement("option");
    o.value = "col:" + c.id;
    o.textContent = "Collection: " + (c.name || c.id);
    sel.appendChild(o);
  }
  sel.value = Array.from(sel.options).some((o) => o.value === prev) ? prev : "picked";
}

async function keysForScope() {
  const scope = $("askScope").value;
  if (scope === "picked") return Array.from(picked);
  const id = scope.slice(4);
  const saved = await getSavedPapers();
  return Object.values(saved).filter((p) => Array.isArray(p.collections) && p.collections.includes(id)).map((p) => p.key);
}

function row(table, cells, header = false) {
  const tr = document.createElement("tr");
  for (const c of cells) {
    const td = document.createElement(header ? "th" : "td");
    td.textContent = c.text;
    if (c.className) td.className = c.className;
    tr.appendChild(td);
  }
  table.appendChild(tr);
}

function renderResults(rows, usage, outputParsed) {
  const box = $("askResults");
  clear(box);
  const table = document.createElement("table");
  table.className = "ask-table";
  row(table, [{ text: "Paper" }, { text: "Verdict" }, { text: "Supporting sentence from the saved text" }], true);
  for (const r of rows) {
    row(table, [
      { text: r.title },
      { text: r.verdict, className: r.verdict === NO_SUPPORT ? "ask-nosupport" : "ask-verdict" },
      { text: r.quote || "" },
    ]);
  }
  box.appendChild(table);
  const note = document.createElement("p");
  note.className = "ask-note";
  note.textContent =
    "A sentence is shown only if it appears word for word in the saved text. That does not prove the verdict is right: read the sentence. " +
    "The saved text is the abstract if the paper has one, otherwise the Scholar snippet, which is short." +
    (outputParsed === false ? " The model's reply could not be read as a table, so no rows were kept." : "") +
    (usage && Number.isFinite(usage.inputTokens) ? ` The provider reported ${usage.inputTokens} input and ${usage.outputTokens ?? "?"} output tokens.` : "");
  box.appendChild(note);
}

async function reviewRequest() {
  msg("");
  $("askConfirm").hidden = true;
  pending = null;
  if (!allowed) return msg("Ask my shortlist is a Pro feature.");
  const stored = await chrome.storage.local.get(ASK_CONFIG_KEY);
  const cfg = normalizeAskConfig(stored[ASK_CONFIG_KEY]);
  const provider = getProvider(cfg.providerId);
  if (!cfg.enabled || !provider) return msg("Turn this on in Options first: choose a provider and paste your own API key.");
  const question = $("askQuestion").value.trim();
  if (!question) return msg("Type a question first.");
  const keys = await keysForScope();
  if (!keys.length) return msg("Select at least one paper: tick papers in the list, or choose a collection.");
  const saved = await getSavedPapers();
  const { papers, skipped } = selectPapersForAsk(keys.map((k) => saved[k]).filter(Boolean));
  if (!papers.length) return msg("None of the selected papers has a saved abstract or snippet to send.");
  const { system, user } = buildAskPrompt({ question, papers });
  const cost = estimateCost({ system, user, maxOutputTokens: DEFAULT_MAX_OUTPUT_TOKENS, prices: { inPerM: cfg.inPerM, outPerM: cfg.outPerM }, paperCount: papers.length });
  const lines = [
    `Provider: ${provider.label} (${provider.host})`,
    `Model: ${cfg.model}`,
    `Papers: ${papers.length}${skipped.length ? `, ${skipped.length} skipped because they have no saved abstract or snippet` : ""}`,
    `Estimated input: about ${cost.inputTokens} tokens. Expected output: about ${cost.expectedOutputTokens} tokens (cap ${cost.maxOutputTokens}).`,
    `Estimated cost: ${formatUsd(cost.usd)}; at most ${formatUsd(cost.maxUsd)} if the reply hits the cap. This uses the prices in Options and is an estimate, not a bill.`,
    "Sent: your question and the title and saved text of each paper above. Not sent: notes, tags, authors, links, anything else.",
  ];
  const summary = $("askSummary");
  clear(summary);
  for (const l of lines) {
    const p = document.createElement("p");
    p.textContent = l;
    summary.appendChild(p);
  }
  $("askPreview").textContent = `[system]\n${system}\n\n[user]\n${user}`;
  $("askSend").textContent = `Send to ${provider.label}`;
  pending = { keys: papers.map((p) => p.key), question };
  $("askConfirm").hidden = false;
}

async function send() {
  if (!pending || busy) return;
  busy = true;
  $("askSend").disabled = true;
  msg("Sending...");
  const job = pending;
  pending = null;
  try {
    const stored = await chrome.storage.local.get(ASK_CONFIG_KEY);
    const provider = getProvider(normalizeAskConfig(stored[ASK_CONFIG_KEY]).providerId);
    const has = provider ? await chrome.permissions.contains({ origins: [provider.origin] }) : false;
    if (!has) {
      msg("Access to the provider is not granted. Turn the feature on again in Options.");
      return;
    }
    const res = await chrome.runtime.sendMessage({ action: "askShortlist", keys: job.keys, question: job.question, confirmed: true });
    $("askConfirm").hidden = true;
    if (!res || !res.ok) {
      msg(res && res.error ? res.error : "No answer from the extension.");
      return;
    }
    msg("");
    renderResults(res.rows, res.usage, res.outputParsed);
  } catch {
    msg("Something went wrong. Nothing more was sent.");
  } finally {
    busy = false;
    $("askSend").disabled = false;
  }
}

export async function initAskPanel() {
  if (!$("askPanel")) return;
  $("askQuestion").maxLength = MAX_QUESTION_CHARS;
  await refreshScopes();
  updateCount();
  try {
    allowed = canUseFeature("askShortlist", await getEntitlementStatus({ needPurchased: true }));
  } catch {
    allowed = false;
  }
  $("askLocked").hidden = allowed;
  $("askForm").hidden = !allowed;
  $("askUnlock").addEventListener("click", () => showProPrompt({ featureId: "askShortlist" }));
  $("askOpenOptions").addEventListener("click", () => chrome.runtime.sendMessage({ action: "openOptionsPage" }));
  $("askReview").addEventListener("click", () => reviewRequest().catch(() => msg("Could not prepare the request.")));
  $("askCancel").addEventListener("click", () => {
    pending = null;
    $("askConfirm").hidden = true;
  });
  $("askSend").addEventListener("click", send);
  // Changing the question or the selection invalidates a review that is on screen.
  for (const id of ["askQuestion", "askScope"]) {
    $(id).addEventListener("input", () => {
      pending = null;
      $("askConfirm").hidden = true;
    });
  }
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area === "local" && (changes.libraryState || changes.savedPapers)) refreshScopes().catch(() => {});
  });
}
