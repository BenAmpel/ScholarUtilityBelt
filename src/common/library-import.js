// Storage-facing wrapper around the pure merge in library-merge.js:
// back up, merge, write, and undo. One backup is kept (the state before the most recent import or merge).

import {
  clearLibraryBackup,
  getLibraryBackup,
  getLibraryState,
  getSavedPapers,
  replaceLibrary,
  saveLibraryBackup,
  withStorageLock
} from "./storage.js";
import { mergeDuplicatePair, mergeLibraryImport } from "./library-merge.js";

export const MAX_IMPORT_BYTES = 50 * 1024 * 1024;

/** Parse the text of a library export file. Returns { ok, data } or { ok:false, error }. */
export function parseLibraryFileText(text) {
  if (typeof text !== "string" || !text.trim()) return { ok: false, error: "The file is empty." };
  if (text.length > MAX_IMPORT_BYTES) return { ok: false, error: "The file is larger than 50 MB." };
  try {
    return { ok: true, data: JSON.parse(text) };
  } catch {
    return { ok: false, error: "The file is not valid JSON." };
  }
}

/**
 * Merge a payload into the stored library. The backup is written before anything else changes,
 * and nothing is written when the payload cannot be read.
 */
export function importLibraryPayload(payload, opts = {}) {
  // Read, merge and write as one step, so a save made meanwhile in another page of the
  // extension (the popup, options or library) is not lost.
  return withStorageLock("savedPapers", async () => {
    const [papers, state] = await Promise.all([getSavedPapers(), getLibraryState()]);
    const result = mergeLibraryImport(papers, state, payload, opts);
    if (!result.ok) return result;
    await saveLibraryBackup(opts.reason || "import", result.summary);
    await replaceLibrary(result.papers, result.state);
    return result;
  });
}

/** Merge one duplicate into another, with a backup first. */
export function mergeDuplicatesInStorage(keepKey, dropKey) {
  return withStorageLock("savedPapers", async () => {
    const papers = await getSavedPapers();
    const result = mergeDuplicatePair(papers, keepKey, dropKey);
    if (!result.ok) return result;
    await saveLibraryBackup("duplicate merge", null);
    await replaceLibrary(result.papers, null);
    return result;
  });
}

/** Restore the library to the backup. Anything changed after the import is lost. */
export async function undoLastImport() {
  const backup = await getLibraryBackup();
  if (!backup) return { ok: false, error: "There is no backup to restore." };
  await replaceLibrary(backup.savedPapers, backup.libraryState);
  await clearLibraryBackup();
  return { ok: true, backup };
}
