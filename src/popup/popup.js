import { getSavedPapers } from "../common/storage.js";

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

async function refresh() {
  const saved = await getSavedPapers();
  document.getElementById("savedCount").textContent = String(Object.keys(saved).length);
}

function setHint(msg) {
  document.getElementById("hint").textContent = msg;
}

document.getElementById("openLibrary").addEventListener("click", () => {
  const url = chrome.runtime.getURL("src/library/library.html");
  chrome.tabs.create({ url });
});

document.getElementById("openOptions").addEventListener("click", () => {
  chrome.runtime.openOptionsPage();
});

document.getElementById("exportJson").addEventListener("click", async () => {
  const saved = await getSavedPapers();
  const payload = {
    exportedAt: new Date().toISOString(),
    savedPapers: saved
  };
  downloadText(`scholar-utility-belt-${Date.now()}.json`, JSON.stringify(payload, null, 2));
  setHint("Exported JSON.");
});

document.getElementById("importJson").addEventListener("change", async (e) => {
  const file = e.target.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const data = JSON.parse(text);
    if (!data || typeof data !== "object" || !data.savedPapers || typeof data.savedPapers !== "object") {
      throw new Error("Invalid export format (expected { savedPapers: {...} }).");
    }
    const existing = await chrome.storage.local.get({ savedPapers: {} });
    const merged = { ...(existing.savedPapers || {}), ...(data.savedPapers || {}) };
    await chrome.storage.local.set({ savedPapers: merged });
    setHint("Imported JSON (merged).\n");
    await refresh();
  } catch (err) {
    setHint(`Import failed: ${err?.message || String(err)}`);
  } finally {
    e.target.value = "";
  }
});

refresh();
