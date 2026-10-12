// Library page tools: collection packets, similarity and duplicates, watch lists.
// Logic lives in ../common/*.js (pure, tested); this file only wires it to the page.

import { showProPrompt } from "../common/pro-prompt.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import { importLibraryPayload, mergeDuplicatesInStorage } from "../common/library-import.js";
import { describeImportSummary } from "../common/library-merge.js";
import { buildPacket, packetFileName, packetToImport, validatePacketText } from "../common/library-packet.js";
import { createSimilarityCache, similarTo } from "../common/similarity.js";
import { getSavedPapers, getWatchLists, setWatchLists } from "../common/storage.js";
import {
  WATCH_LIMITS,
  addWatchItem,
  markItemSeen,
  normalizeWatchState,
  removeWatchItem,
  totalUnseen
} from "../common/watchlist.js";
import { maybeRunWatchCheck } from "../common/watchlist-runner.js";

const $ = (id) => document.getElementById(id);

let ctx = null;

function clear(node) {
  node.textContent = "";
}

function add(parent, tag, text, className = "") {
  const n = document.createElement(tag);
  if (className) n.className = className;
  if (text != null) n.textContent = String(text);
  parent.appendChild(n);
  return n;
}

/** Show the shared tool panel above the list. Returns the body element. */
export function openToolView(title) {
  $("toolViewTitle").textContent = title;
  const body = $("toolViewBody");
  clear(body);
  $("toolView").hidden = false;
  return body;
}

export function closeToolView() {
  $("toolView").hidden = true;
  clear($("toolViewBody"));
  ctx?.onToolViewClosed?.();
}

/** True when the user may use a Pro feature; otherwise shows the locked card and returns false. */
export async function requirePro(featureId, label) {
  try {
    const ent = await getEntitlementStatus({ needPurchased: true });
    if (canUseFeature(featureId, ent)) return true;
  } catch {
    // fall through to the locked card
  }
  const body = openToolView(`${label} is a Pro feature`);
  add(body, "p", `${label} is part of Pro. A trial, a purchase or an App Pass unlocks it. Nothing on this page changes until then.`, "panel-note");
  const btn = add(body, "button", "Unlock Pro", "btn");
  btn.type = "button";
  btn.addEventListener("click", () => showProPrompt({ featureId }));
  return false;
}

// ---------------------------------------------------------------------------
// Collection packets
// ---------------------------------------------------------------------------

function readFromLabel() {
  try {
    return localStorage.getItem("su_packet_from") || "";
  } catch {
    return "";
  }
}

function rememberFromLabel(v) {
  try {
    localStorage.setItem("su_packet_from", v);
  } catch {
    // storage can be unavailable; the label is only a convenience
  }
}

async function openPacketExport() {
  if (!(await requirePro("collectionPackets", "Collection packets"))) return;
  const state = await ctx.loadLibraryState();
  const select = $("packetCollection");
  clear(select);
  if (!state.collections.length) {
    const body = openToolView("Collection packets");
    add(body, "p", "Create a collection and add papers to it first. A packet holds one collection.", "panel-note");
    return;
  }
  state.collections.forEach((c) => {
    const o = document.createElement("option");
    o.value = c.id;
    o.textContent = c.name || c.id;
    select.appendChild(o);
  });
  if (state.activeCollectionId) select.value = state.activeCollectionId;
  $("packetName").value = state.collections.find((c) => c.id === select.value)?.name || "";
  $("packetFrom").value = readFromLabel();
  $("packetNotes").checked = false;
  $("packetError").hidden = true;
  ctx.openModal("packetModal");
}

async function createPacket() {
  const err = $("packetError");
  err.hidden = true;
  const state = await ctx.loadLibraryState();
  const colId = $("packetCollection").value;
  const collection = state.collections.find((c) => c.id === colId);
  const saved = await getSavedPapers();
  const papers = Object.values(saved).filter((p) => (p.collections || []).includes(colId));
  const result = await buildPacket({
    papers,
    name: $("packetName").value,
    from: $("packetFrom").value,
    collectionName: collection?.name || "",
    includeNotes: $("packetNotes").checked
  });
  if (!result.ok) {
    err.textContent = result.error;
    err.hidden = false;
    return;
  }
  rememberFromLabel($("packetFrom").value.trim());
  ctx.downloadText(packetFileName(result.packet.name), JSON.stringify(result.packet, null, 2), "application/json");
  ctx.closeModal("packetModal");
  ctx.setImportStatus(
    `Packet downloaded: ${result.packet.papers.length} paper${result.packet.papers.length === 1 ? "" : "s"}, notes ${result.packet.includesNotes ? "included" : "not included"}.`
  );
}

async function importPacketFile(file) {
  const v = await validatePacketText(await file.text());
  if (!v.ok) {
    ctx.setImportStatus(`Nothing was imported. ${v.error}`);
    return;
  }
  const lines = [
    `Import "${v.packet.name}" from ${v.packet.from}?`,
    `${v.packet.papers.length} paper${v.packet.papers.length === 1 ? "" : "s"}, notes ${v.packet.includesNotes ? "included" : "not included"}.`,
    "Papers you already have are merged, not replaced, and every imported paper gets the tag " + `"from ${v.packet.from}".`
  ];
  if (v.warnings.length) lines.push("", ...v.warnings);
  if (!window.confirm(lines.join("\n"))) return;
  const { payload, options } = packetToImport(v.packet);
  const result = await importLibraryPayload(payload, options);
  if (!result.ok) {
    ctx.setImportStatus(`Nothing was imported. ${result.error}`);
    return;
  }
  ctx.setImportStatus(`Packet imported: ${describeImportSummary(result.summary)}.`, { showUndo: true });
  ctx.refresh();
}

// ---------------------------------------------------------------------------
// Similarity and possible duplicates (Pro: librarySimilarity)
// ---------------------------------------------------------------------------

const similarityCache = createSimilarityCache();
const dismissedPairs = new Set();

function progressTo(node) {
  return (p) => {
    node.textContent = `Reading the library: ${p.done} of ${p.total} papers (${p.phase}). The page stays usable while this runs.`;
  };
}

function paperMeta(p) {
  return [p.authorsVenue, p.year ? `(${p.year})` : "", p.citations ? `${p.citations} cites` : ""].filter(Boolean).join(" ");
}

function appendPaperLink(parent, p) {
  const a = add(parent, "a", p.title || "(untitled)", "tool-row-title");
  const href = p.url || p.sourcePageUrl;
  if (href && /^https?:\/\//i.test(href)) {
    a.href = href;
    a.target = "_blank";
    a.rel = "noreferrer";
  }
  return a;
}

export async function showSimilar(paper) {
  if (!(await requirePro("librarySimilarity", "Library similarity"))) return;
  const body = openToolView(`Similar to: ${paper.title || "(untitled)"}`);
  const progress = add(body, "p", "Reading the library...", "tool-progress");
  const papers = Object.values(await getSavedPapers());
  const index = await similarityCache.getIndex(papers, progressTo(progress));
  const results = similarTo(index, paper.key, { limit: 10 });
  progress.remove();
  add(
    body,
    "p",
    "Ranked by shared words in titles and abstracts or snippets, computed on this device. It does not read the papers themselves.",
    "panel-note"
  );
  if (!results.length) {
    add(body, "p", "No other saved paper shares enough words with this one.", "panel-note");
    return;
  }
  for (const r of results) {
    const row = add(body, "div", "", "tool-row");
    const main = add(row, "div", "", "tool-row-main");
    appendPaperLink(main, r.paper);
    add(main, "div", paperMeta(r.paper), "tool-row-meta");
    add(row, "span", `${Math.round(r.score * 100)}%`, "tool-row-meta");
  }
}

function pairId(pair) {
  return [pair.a, pair.b].sort().join("||");
}

function earlierFirst(a, b) {
  return String(a.savedAt || "").localeCompare(String(b.savedAt || "")) <= 0 ? [a, b] : [b, a];
}

function renderPairSide(parent, p) {
  const side = add(parent, "div", "", "tool-row-main");
  appendPaperLink(side, p);
  add(side, "div", paperMeta(p), "tool-row-meta");
  const links = [p.url, p.pdfUrl, ...(p.altUrls || [])].filter(Boolean);
  add(side, "div", `${links.length} link${links.length === 1 ? "" : "s"}${p.doi ? `, DOI ${p.doi}` : ", no DOI"}`, "tool-row-meta");
}

export async function showPossibleDuplicates() {
  if (!(await requirePro("librarySimilarity", "Possible duplicates"))) return;
  const body = openToolView("Possible duplicates");
  const progress = add(body, "p", "Reading the library...", "tool-progress");
  const papers = Object.values(await getSavedPapers());
  const byKey = new Map(papers.map((p) => [p.key, p]));
  const pairs = (await similarityCache.getPairs(papers, progressTo(progress))).filter((x) => !dismissedPairs.has(pairId(x)));
  progress.remove();
  add(
    body,
    "p",
    "Pairs with similar titles, overlapping author surnames and years within three of each other, such as a preprint and its published version. These are guesses: check each pair before merging.",
    "panel-note"
  );
  if (!pairs.length) {
    add(body, "p", "No possible duplicates found.", "panel-note");
    return;
  }
  const shown = pairs.slice(0, 50);
  if (pairs.length > shown.length) add(body, "p", `Showing the 50 closest of ${pairs.length} pairs.`, "panel-note");
  for (const pair of shown) {
    const a = byKey.get(pair.a);
    const b = byKey.get(pair.b);
    if (!a || !b) continue;
    const row = add(body, "div", "", "tool-row");
    const grid = add(row, "div", "", "tool-pair");
    renderPairSide(grid, a);
    renderPairSide(grid, b);
    const side = add(row, "div", "", "tool-row-main");
    add(side, "div", pair.reasons.join("; "), "tool-row-meta");
    const actions = add(side, "div", "", "tool-actions");
    const merge = add(actions, "button", "Merge into one", "btn");
    merge.type = "button";
    merge.title = "Combines tags, notes and highlights into the earlier-saved record and keeps both links. The other record is removed. Undo restores the library from before.";
    merge.addEventListener("click", async () => {
      const [keep, drop] = earlierFirst(a, b);
      merge.disabled = true;
      const r = await mergeDuplicatesInStorage(keep.key, drop.key);
      if (!r.ok) {
        window.alert(r.error);
        merge.disabled = false;
        return;
      }
      ctx.setImportStatus(`Merged two records of "${keep.title || "(untitled)"}". Both links are kept.`, { showUndo: true });
      await ctx.refresh();
      await showPossibleDuplicates();
    });
    const dismiss = add(actions, "button", "Not a duplicate", "btn");
    dismiss.type = "button";
    dismiss.title = "Hide this pair until the page is reloaded.";
    dismiss.addEventListener("click", () => {
      dismissedPairs.add(pairId(pair));
      row.remove();
    });
  }
}

// ---------------------------------------------------------------------------
// Watch lists (Pro: watchLists)
// ---------------------------------------------------------------------------

async function loadWatchState() {
  return normalizeWatchState(await getWatchLists());
}

async function updateWatchButton() {
  const n = totalUnseen(await loadWatchState());
  $("openWatchLists").textContent = n ? `Open watch lists (${n} new)` : "Open watch lists";
}

function fmtWhen(ms) {
  return ms ? new Date(ms).toLocaleString() : "not yet";
}

async function renderWatchView() {
  const state = await loadWatchState();
  const body = openToolView("Watch lists");
  add(
    body,
    "p",
    "Searches and author names you add here are sent to OpenAlex, at most once a day per item, and only while this page or a Scholar page is open. Nothing else is sent, and Google Scholar is not contacted.",
    "panel-note"
  );

  const form = add(body, "div", "", "tool-form");
  const kind = document.createElement("select");
  kind.setAttribute("aria-label", "What to watch");
  [["query", "Search"], ["author", "Author"]].forEach(([v, t]) => {
    const o = document.createElement("option");
    o.value = v;
    o.textContent = t;
    kind.appendChild(o);
  });
  form.appendChild(kind);
  const input = document.createElement("input");
  input.type = "text";
  input.maxLength = WATCH_LIMITS.maxValueLength;
  input.placeholder = "Search words, or an author name";
  input.setAttribute("aria-label", "Search words or author name");
  form.appendChild(input);
  const addBtn = add(form, "button", "Add", "btn");
  addBtn.type = "button";
  const msg = add(body, "p", "", "panel-note");
  msg.setAttribute("role", "alert");
  addBtn.addEventListener("click", async () => {
    const latest = await loadWatchState();
    const r = addWatchItem(latest, { type: kind.value, value: input.value });
    if (!r.ok) {
      msg.textContent = r.error;
      return;
    }
    await setWatchLists(r.state);
    msg.textContent = "Added. The first lookup runs now if none ran in the last 24 hours.";
    maybeRunWatchCheck().then(() => renderWatchView()).catch(() => {});
    await renderWatchView();
  });

  add(body, "p", `${state.items.length} of ${WATCH_LIMITS.maxItems} items. Up to ${WATCH_LIMITS.maxResultsPerItem} works are kept per item.`, "tool-progress");
  if (state.backoffUntil > Date.now()) {
    add(body, "p", `OpenAlex lookups are paused until ${fmtWhen(state.backoffUntil)} after an error.`, "panel-note");
  }

  for (const item of state.items) {
    const block = add(body, "div", "", "tool-row");
    const main = add(block, "div", "", "tool-row-main");
    add(main, "div", `${item.type === "author" ? "Author" : "Search"}: ${item.value}`, "tool-row-title");
    const bits = [`last lookup ${fmtWhen(item.lastCheckedAt)}`];
    if (item.matchedName) bits.push(`matched ${item.matchedName}`);
    add(main, "div", bits.join("; "), "tool-row-meta");
    if (item.lastError) add(main, "div", item.lastError, "tool-row-meta");
    if (!item.unseen.length) {
      add(main, "div", item.lastCheckedAt ? "Nothing new since you last looked." : "Waiting for the first lookup.", "tool-row-meta");
    }
    for (const w of item.unseen) {
      const row = add(main, "div", "", "tool-row");
      const m = add(row, "div", "", "tool-row-main");
      appendPaperLink(m, w);
      const authors = w.authors?.length ? `${w.authors.join(", ")}${w.authorCount > w.authors.length ? " et al." : ""}` : "";
      add(m, "div", [authors, w.venue, w.date || w.year].filter(Boolean).join(" | "), "tool-row-meta");
      const badges = await ctx.getQualityBadges(w.venue);
      if (badges.length) add(m, "div", `Quality: ${badges.join(", ")}`, "tool-row-meta");
    }
    const actions = add(block, "div", "", "tool-actions");
    if (item.unseen.length) {
      const seen = add(actions, "button", "Mark all seen", "btn");
      seen.type = "button";
      seen.addEventListener("click", async () => {
        await setWatchLists(markItemSeen(await loadWatchState(), item.id));
        await renderWatchView();
      });
    }
    const rm = add(actions, "button", "Remove", "btn danger");
    rm.type = "button";
    rm.addEventListener("click", async () => {
      await setWatchLists(removeWatchItem(await loadWatchState(), item.id));
      await renderWatchView();
    });
  }
  updateWatchButton();
}

async function openWatchLists() {
  if (!(await requirePro("watchLists", "Watch lists"))) return;
  await renderWatchView();
  maybeRunWatchCheck().then((r) => r.ran && renderWatchView()).catch(() => {});
}

/** Called once when the library page opens: runs a due check quietly and shows the unread count. */
async function backgroundWatchCheck() {
  const state = await loadWatchState();
  if (!state.items.length) return;
  await updateWatchButton();
  try {
    const r = await maybeRunWatchCheck();
    if (r.ran) await updateWatchButton();
  } catch {
    // a failed lookup must never break the library page
  }
}

export function initLibraryTools(context) {
  ctx = context;
  $("toolViewClose").addEventListener("click", closeToolView);
  $("openWatchLists").addEventListener("click", openWatchLists);
  chrome.storage.onChanged.addListener((changes, area) => {
    if (area !== "local" || !changes.watchLists) return;
    updateWatchButton();
  });
  setTimeout(backgroundWatchCheck, 1500);

  document.querySelectorAll("[data-tool-modal-close]").forEach((b) => {
    b.addEventListener("click", () => ctx.closeModal(b.getAttribute("data-tool-modal-close")));
  });

  $("findSimilarDuplicates").addEventListener("click", showPossibleDuplicates);
  $("exportPacket").addEventListener("click", openPacketExport);
  $("packetCollection").addEventListener("change", async () => {
    const state = await ctx.loadLibraryState();
    $("packetName").value = state.collections.find((c) => c.id === $("packetCollection").value)?.name || "";
  });
  $("packetCreate").addEventListener("click", createPacket);
  $("importPacket").addEventListener("click", async () => {
    if (await requirePro("collectionPackets", "Collection packets")) $("importPacketFile").click();
  });
  $("importPacketFile").addEventListener("change", async (e) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    try {
      await importPacketFile(file);
    } catch (err) {
      ctx.setImportStatus(`Nothing was imported. ${err && err.message ? err.message : "The file could not be read."}`);
    }
  });
}
