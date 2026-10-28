import { getSavedPapers, getSettings, setSettings } from "../common/storage.js";
import { importLibraryPayload, parseLibraryFileText } from "../common/library-import.js";
import { describeImportSummary } from "../common/library-merge.js";
import { initRatingPrompt } from "../common/rating-prompt.js";

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function refresh() {
  const saved = await getSavedPapers();
  const count = Object.keys(saved).length;
  document.getElementById("savedCount").textContent = String(count);
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
    // The same merge the Library page uses: nothing is overwritten, tags, notes and
    // collections are combined, and one backup is kept so the Library page can undo it.
    const parsed = parseLibraryFileText(await file.text());
    if (!parsed.ok) throw new Error(parsed.error);
    const result = await importLibraryPayload(parsed.data, { reason: "popup import" });
    if (!result.ok) throw new Error(result.error);
    setHint(`Imported: ${describeImportSummary(result.summary)}. Existing papers were merged, not overwritten. Undo is on the Library page.`);
    await refresh();
  } catch (err) {
    setHint(`Import failed: ${err?.message || String(err)}`);
  } finally {
    e.target.value = "";
  }
});

// Quick-settings toggles
const TOGGLE_IDS = [
  "showQualityBadges",
  "showEmergingScore",
  "showRetractionWatch",
  "showHoverSummary",
  "showReadingGuide"
];

async function initToggles() {
  const settings = await getSettings();
  for (const id of TOGGLE_IDS) {
    const el = document.getElementById(id);
    if (!el) continue;
    el.checked = !!settings[id];
    el.addEventListener("change", () => {
      setSettings({ [id]: el.checked });
    });
  }
}

refresh();
initToggles();

initRatingPrompt();
