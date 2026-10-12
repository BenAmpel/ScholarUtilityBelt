/**
 * The dialog shown before anything sends a user to the payment page. A locked feature's
 * button opens this first: it says the feature is Pro, how it is unlocked, and where
 * "Unlock" leads. "Unlock Pro" opens the payment page, "Start free trial" opens the
 * ExtensionPay trial page, "Not now" closes it.
 *
 * Plain DOM, text set with textContent only. Styling is class-based (src/common/pro-prompt.css;
 * the same rules are in content.css for Scholar pages, where a stylesheet link would depend on
 * the page's CSP). Extension pages get the stylesheet linked automatically.
 */

export const TRIAL_LABEL = "Start 14-day free trial";

export const PRO_FEATURE_LABELS = {
  compareAuthors: "Author compare",
  citationLineage: "Idea lineage",
  extendedBibliometrics: "The extended bibliometrics",
  narrativeCv: "The narrative CV",
  reviewWorkspace: "The systematic-review workspace",
  citationGraph: "The citation graph",
  popReport: "The Publish-or-Perish report",
  referenceImport: "Reference import",
  riskOfBias: "The risk-of-bias grid",
  screeningAgreement: "The screening agreement report",
  evidenceMatrix: "The evidence matrix",
  reviewSnapshots: "Review snapshots",
  claimEvidenceChips: "Claim-evidence chips",
  reproSignals: "Reproducibility signals",
  readingOrder: "Reading order",
  triageCard: "The triage card",
  askShortlist: "Ask my shortlist",
  bibliographyCheck: "The bibliography checker",
  tenurePacket: "The tenure packet",
  collectionPackets: "Collection packets",
  librarySimilarity: "The similar-papers and duplicate finder",
  watchLists: "Watch lists",
  zoteroExport: "Zotero export",
  obsidianExport: "Obsidian export",
  latexPack: "The LaTeX citation pack",
  wordRefList: "The Word and Docs reference list",
  librarianReport: "The librarian batch report",
};

/** The words in the dialog, as data so they can be tested. */
export function proPromptCopy({ featureId = "", feature = "", trialUsed = false } = {}) {
  const label = String(feature || PRO_FEATURE_LABELS[featureId] || "This feature").trim();
  return {
    title: "This is a Pro feature",
    body: `${label} is part of Scholar Utility Belt Pro. It is included with the extension and unlocked with a free trial, a purchase or an App Pass.`,
    note: trialUsed ? "You have already used your free trial." : "Unlock Pro opens the payment page in a new tab.",
    showTrial: !trialUsed,
  };
}

let nextId = 0;

function sendBg(message) {
  return new Promise((resolve) => {
    try {
      chrome.runtime.sendMessage(message, (response) => {
        void chrome.runtime.lastError;
        resolve(response || {});
      });
    } catch {
      resolve({});
    }
  });
}

function ensureStylesheet(doc) {
  try {
    if (doc.location && doc.location.protocol === "chrome-extension:" && !doc.getElementById("su-pro-prompt-css")) {
      const link = doc.createElement("link");
      link.id = "su-pro-prompt-css";
      link.rel = "stylesheet";
      link.href = new URL("./pro-prompt.css", import.meta.url).href;
      doc.head.appendChild(link);
    }
  } catch {
    // Without the stylesheet the dialog is unstyled but still works.
  }
}

/**
 * @param {{ featureId?: string, feature?: string, trialUsed?: boolean }} [opts]
 * @returns {{ close: () => void }} handle; null when there is no document to draw in
 */
export function showProPrompt(opts = {}, doc = globalThis.document) {
  if (!doc || !doc.body) return null;
  ensureStylesheet(doc);
  doc.querySelectorAll(".su-pro-prompt-backdrop").forEach((el) => el.remove());

  let copy = proPromptCopy(opts);
  const id = `su-pro-prompt-title-${++nextId}`;
  const previousFocus = doc.activeElement;

  const backdrop = doc.createElement("div");
  backdrop.className = "su-pro-prompt-backdrop";
  const dialog = doc.createElement("div");
  dialog.className = "su-pro-prompt";
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", id);

  const title = doc.createElement("h2");
  title.id = id;
  title.className = "su-pro-prompt-title";
  const body = doc.createElement("p");
  body.className = "su-pro-prompt-body";
  const note = doc.createElement("p");
  note.className = "su-pro-prompt-note";
  note.setAttribute("role", "status");
  const actions = doc.createElement("div");
  actions.className = "su-pro-prompt-actions";

  const unlock = doc.createElement("button");
  unlock.type = "button";
  unlock.className = "su-pro-prompt-btn su-pro-prompt-primary";
  unlock.textContent = "Unlock Pro";
  const trial = doc.createElement("button");
  trial.type = "button";
  trial.className = "su-pro-prompt-btn";
  trial.textContent = TRIAL_LABEL;
  const later = doc.createElement("button");
  later.type = "button";
  later.className = "su-pro-prompt-btn";
  later.textContent = "Not now";

  const render = () => {
    title.textContent = copy.title;
    body.textContent = copy.body;
    note.textContent = copy.note;
    trial.hidden = !copy.showTrial;
  };
  render();

  const onKey = (e) => {
    if (e.key === "Escape") { e.stopPropagation(); close(); }
  };
  function close() {
    doc.removeEventListener("keydown", onKey, true);
    backdrop.remove();
    try { if (previousFocus && previousFocus.focus) previousFocus.focus(); } catch { /* element may be gone */ }
  }

  unlock.addEventListener("click", async () => {
    await sendBg({ action: "openUpsellModal" });
    close();
  });
  trial.addEventListener("click", async () => {
    trial.disabled = true;
    const res = await sendBg({ action: "startTrial" });
    if (res.ok || res.reason === "already_paid") { close(); return; }
    trial.disabled = false;
    if (res.reason === "already_used") {
      copy = proPromptCopy({ ...opts, trialUsed: true });
      render();
    } else {
      note.textContent = "Couldn't open the trial page. Check your connection and try again.";
    }
  });
  later.addEventListener("click", close);
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) close(); });

  actions.append(unlock, trial, later);
  dialog.append(title, body, note, actions);
  backdrop.appendChild(dialog);
  doc.body.appendChild(backdrop);
  doc.addEventListener("keydown", onKey, true);
  try { unlock.focus(); } catch { /* not focusable in this context */ }
  return { close };
}
