// Library page tools: collection packets, similarity and duplicates, watch lists.
// Logic lives in ../common/*.js (pure, tested); this file only wires it to the page.

import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import { importLibraryPayload } from "../common/library-import.js";
import { describeImportSummary } from "../common/library-merge.js";
import { buildPacket, packetFileName, packetToImport, validatePacketText } from "../common/library-packet.js";
import { getSavedPapers } from "../common/storage.js";

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
  btn.addEventListener("click", () => chrome.runtime.sendMessage({ action: "openUpsellModal" }));
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

export function initLibraryTools(context) {
  ctx = context;
  $("toolViewClose").addEventListener("click", closeToolView);

  document.querySelectorAll("[data-tool-modal-close]").forEach((b) => {
    b.addEventListener("click", () => ctx.closeModal(b.getAttribute("data-tool-modal-close")));
  });

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
