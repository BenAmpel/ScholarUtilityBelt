// Tenure packet page (Pro). Reads the snapshot the Scholar profile content
// script saved, builds the document, and offers .docx and print/PDF output.
import { showProPrompt } from "../common/pro-prompt.js";
import { canUseFeature, getEntitlementStatus } from "../common/entitlement.js";
import { buildDocx } from "../common/docx-writer.js";
import { buildTenurePacket, packetFileName, packetToHtml } from "../common/tenure-packet.js";

const DRAFT_KEY = "suTenurePacketDraft";
const el = (id) => document.getElementById(id);

function downloadBytes(name, mime, bytes) {
  const url = URL.createObjectURL(new Blob([bytes], { type: mime }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function init() {
  const allowed = await getEntitlementStatus({ needPurchased: true })
    .then((e) => canUseFeature("tenurePacket", e))
    .catch(() => false);
  if (!allowed) {
    el("lockedCard").hidden = false;
    el("unlockPro").addEventListener("click", () => showProPrompt({ featureId: "tenurePacket" }));
    return;
  }
  const draft = await chrome.storage.local.get({ [DRAFT_KEY]: null })
    .then((got) => got[DRAFT_KEY])
    .catch(() => null);
  if (!draft || !draft.snapshot) {
    el("emptyCard").hidden = false;
    return;
  }
  const generatedAt = draft.createdAt ? new Date(draft.createdAt) : new Date();
  const packet = buildTenurePacket(draft.snapshot, Number.isNaN(generatedAt.getTime()) ? new Date() : generatedAt);
  el("docRoot").innerHTML = packetToHtml(packet);
  document.title = packet.title;
  el("actions").hidden = false;
  el("printPdf").addEventListener("click", () => window.print());
  el("downloadDocx").addEventListener("click", () => {
    const bytes = buildDocx(packet.blocks, { title: packet.title, author: "Scholar Utility Belt", created: generatedAt });
    downloadBytes(`${packetFileName(packet, generatedAt)}.docx`, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", bytes);
    el("status").textContent = "Downloaded.";
  });
}

init();
