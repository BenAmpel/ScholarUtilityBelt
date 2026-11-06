// The free top-2% panel on Google Scholar profile pages. It shows where a researcher stands in the Ioannidis/Elsevier
// "Updated science-wide author databases of standardized citation indicators" (CC BY-NC 3.0) when the profile's
// name and affiliation match one listed person. Free for everyone; nothing here reads or checks a paid plan.
//
// Built with DOM methods and textContent only: names and institutions come from data. Any error hides the panel.
import { fetchDataJson } from "../common/gzip-fetch.js";
import { WRONG_MATCH_ISSUE_BASE } from "../common/quality.js";
import {
  decodeTop2pct, matchProfile, resolveIdentity, identityOf, affiliationHash, scholarKeys,
} from "./top2pct-core.js";

export const PICKS_KEY = "suTop2pctPicks";
export const PICKS_MAX = 500;
const PANEL_ID = "su-top2pct";

/** Every string the panel shows. tests/top2pct-copy.test.mjs reads these. */
export const COPY = {
  heading: "Top-2% scientists list",
  matched: "Matched: ",
  savedHere: "You chose this person. Saved on this device only.",
  tableLabel: { career: "Career", singleyr: (year) => `${year} alone` },
  rank: (rank, of, sub) => `rank ${rank} of ${of} researchers in ${sub} (self-citations excluded)`,
  caveat: "A citation ranking from one dataset, not a measure of research quality.",
  moreDetail: "More detail",
  selfShare: (pct, label) => `${label}: ${pct}% of citations to this person's papers are self-citations.`,
  perCiting: (x, label) => `${label}: each citing paper cites this person ${x} times on average. There is no field benchmark.`,
  retracted: (n, who) => `${n} ${n === 1 ? "paper" : "papers"} by ${who} ${n === 1 ? "is" : "are"} listed in Retraction Watch as retracted. This does not show who was responsible.`,
  verify: "Verify in the Retraction Watch database",
  notThisPerson: "Not this person?",
  report: "Report a wrong match",
  several: "Several listed researchers match this profile.",
  choose: "Choose",
  pickLegend: "Which listed researcher is this profile?",
  noneOfThese: "None of these",
  usePerson: "Use this person",
  active: (fy, ly) => `active ${fy} to ${ly}`,
  credit: (version) => `Data: Ioannidis, Updated science-wide author databases of standardized citation indicators, version ${version}, Elsevier Data Repository, CC BY-NC 3.0. Rounded and reduced to the columns shown.`,
};
const RETRACTION_DB_URL = "https://retractiondatabase.org/";

const fmt = (n) => Number(n).toLocaleString("en-US");
const identityLine = (r) => [r.name, r.inst, r.cntry ? r.cntry.toUpperCase() : ""].filter(Boolean).join(" · ");

/** A stored pick is used only if every field has the right type; anything else is ignored. */
export function validPick(e) {
  if (!e || typeof e !== "object" || typeof e.v !== "string" || typeof e.t !== "number") return false;
  if (e.none === true) return true;
  return ["name", "inst", "cntry", "aff"].every((k) => typeof e[k] === "string") && e.name !== "";
}

function chromeStorage() {
  return {
    async get() {
      const got = await chrome.storage.local.get(PICKS_KEY);
      const map = got && got[PICKS_KEY];
      return map && typeof map === "object" && !Array.isArray(map) ? map : {};
    },
    async set(map) { await chrome.storage.local.set({ [PICKS_KEY]: map }); },
  };
}

async function writePick(storage, userId, entry) {
  try {
    const map = { ...(await storage.get()) };
    if (entry) map[userId] = entry; else delete map[userId];
    // Keep the newest PICKS_MAX entries.
    const keys = Object.keys(map);
    if (keys.length > PICKS_MAX) {
      keys.sort((a, b) => (Number(map[b] && map[b].t) || 0) - (Number(map[a] && map[a].t) || 0));
      for (const k of keys.slice(PICKS_MAX)) delete map[k];
    }
    await storage.set(map);
  } catch { /* a failed write only means the choice is asked again next time */ }
}

async function loadDb(load) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const db = decodeTop2pct(await load());
      if (db) return db;
      return null; // a file that loads but does not decode will not decode on a retry either
    } catch { /* retry once */ }
  }
  return null;
}

function el(doc, tag, cls, text) {
  const n = doc.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
}

function reportUrl(rec, ctx) {
  const title = `Top-2% panel: wrong match for ${rec.name}`.slice(0, 200);
  const body = [
    "The top-2% panel matched a Google Scholar profile to the wrong listed researcher.",
    "",
    `Scholar profile id: ${ctx.userId}`,
    `Matched: ${identityLine(rec)}`,
    `Dataset version: ${ctx.version}`,
    `Extension version: ${ctx.extVersion || "unknown"}`,
  ].join("\n");
  return `${WRONG_MATCH_ISSUE_BASE}?title=${encodeURIComponent(title)}&body=${encodeURIComponent(body)}`;
}

/**
 * Show the panel for one profile page.
 * deps: { doc, userId, name, affiliation, emailDomain, anchor (insert after it), load, storage, extVersion, now }
 * Returns the state shown ("confident", "picked", "pick", or "none").
 */
export async function runTop2pct(deps) {
  const doc = deps.doc || document;
  const old = doc.getElementById(PANEL_ID);
  if (old) old.remove();
  if (!deps.userId || !deps.name || !scholarKeys(deps.name)) return "none";
  const db = await loadDb(deps.load || (() => fetchDataJson("src/data/top2pct.json.gz")));
  if (!db) return "none";
  const storage = deps.storage || chromeStorage();
  const now = deps.now || Date.now;
  const version = String(db.meta.version);
  const affHash = affiliationHash(deps.affiliation || "");
  const profile = { name: deps.name, affiliation: deps.affiliation || "", emailDomain: deps.emailDomain || "" };
  const ctx = { doc, userId: deps.userId, version, year: Number(db.meta.year) || 2025, extVersion: deps.extVersion };

  const result = matchProfile(db, profile);
  const savePick = async (rec) => {
    if (!rec) {
      await writePick(storage, deps.userId, { v: version, none: true, t: now() });
      const p = doc.getElementById(PANEL_ID);
      if (p) p.remove();
      return;
    }
    await writePick(storage, deps.userId, { v: version, ...identityOf(rec), aff: affHash, t: now() });
    const panel = renderMatched(ctx, rec, { picked: true, onNotThis: () => notThis() });
    mount(ctx, deps.anchor, panel);
    const h = panel.querySelector(".su-top2pct-heading");
    if (h) h.focus();
  };
  function notThis() {
    const others = result.candidates.length >= 2 ? result.candidates : [];
    if (!others.length) return savePick(null);
    const panel = renderPickList(ctx, others, savePick, true);
    mount(ctx, deps.anchor, panel);
    return undefined;
  }

  let stored;
  try { stored = (await storage.get())[deps.userId]; } catch { stored = null; }
  if (validPick(stored)) {
    if (stored.none) {
      if (stored.v === version) return "none";
      await writePick(storage, deps.userId, null); // a later dataset may list this person; ask again
    } else if (stored.aff === affHash) {
      const rec = resolveIdentity(db, stored);
      if (rec) {
        if (stored.v !== version) await writePick(storage, deps.userId, { ...stored, v: version, t: now() });
        mount(ctx, deps.anchor, renderMatched(ctx, rec, { picked: true, onNotThis: () => notThis() }));
        return "picked";
      }
      await writePick(storage, deps.userId, null);
    } else {
      await writePick(storage, deps.userId, null); // the affiliation changed: match again
    }
  }

  if (result.state === "confident") {
    mount(ctx, deps.anchor, renderMatched(ctx, result.candidates[0], { picked: false, onNotThis: () => notThis() }));
    return "confident";
  }
  if (result.state === "pick") {
    mount(ctx, deps.anchor, renderPickList(ctx, result.candidates, savePick, false));
    return "pick";
  }
  return "none";
}

function mount(ctx, anchor, panel) {
  const old = ctx.doc.getElementById(PANEL_ID);
  if (old) { old.replaceWith(panel); return; }
  if (anchor && anchor.isConnected) anchor.insertAdjacentElement("afterend", panel);
}

function shell(ctx) {
  const { doc } = ctx;
  const panel = el(doc, "section", "su-top2pct");
  panel.id = PANEL_ID;
  panel.setAttribute("aria-labelledby", "su-top2pct-heading");
  const h = el(doc, "h3", "su-top2pct-heading", COPY.heading);
  h.id = "su-top2pct-heading";
  h.tabIndex = -1;
  panel.appendChild(h);
  return panel;
}

function footer(ctx, rec, onNotThis) {
  const { doc } = ctx;
  const f = el(doc, "div", "su-top2pct-footer");
  if (onNotThis) {
    const b = el(doc, "button", "su-top2pct-link", COPY.notThisPerson);
    b.type = "button";
    b.addEventListener("click", () => onNotThis());
    f.appendChild(b);
  }
  if (rec) {
    const a = el(doc, "a", "su-top2pct-link", COPY.report);
    a.href = reportUrl(rec, ctx);
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    f.appendChild(a);
  }
  const credit = el(doc, "p", "su-top2pct-credit", COPY.credit(ctx.version) + " ");
  const doi = el(doc, "a", "su-top2pct-link", "doi:10.17632/btchxktzyw." + ctx.version);
  doi.href = `https://doi.org/10.17632/btchxktzyw.${ctx.version}`;
  doi.target = "_blank";
  doi.rel = "noopener noreferrer";
  credit.appendChild(doi);
  f.appendChild(credit);
  return f;
}

function tableLabel(t, year) {
  const l = COPY.tableLabel[t];
  return typeof l === "function" ? l(year) : l || t;
}

export function renderMatched(ctx, rec, { picked, onNotThis }) {
  const { doc } = ctx;
  const panel = shell(ctx);
  const year = ctx.year || 2025;
  panel.appendChild(el(doc, "p", "su-top2pct-who", COPY.matched + identityLine(rec)));
  if (picked) panel.appendChild(el(doc, "p", "su-top2pct-note", COPY.savedHere));
  const list = el(doc, "ul", "su-top2pct-ranks");
  for (const [t, r] of Object.entries(rec.tables)) {
    list.appendChild(el(doc, "li", null, `${tableLabel(t, year)}: ${COPY.rank(fmt(r.rank), fmt(r.of), r.subfield)}`));
  }
  panel.appendChild(list);
  panel.appendChild(el(doc, "p", "su-top2pct-note", COPY.caveat));

  const more = el(doc, "button", "su-top2pct-more", COPY.moreDetail);
  more.type = "button";
  more.setAttribute("aria-expanded", "false");
  more.setAttribute("aria-controls", "su-top2pct-detail");
  const detail = el(doc, "div", "su-top2pct-detail");
  detail.id = "su-top2pct-detail";
  detail.hidden = true;
  for (const [t, r] of Object.entries(rec.tables)) {
    const label = tableLabel(t, year);
    detail.appendChild(el(doc, "p", null, COPY.selfShare(r.selfPct, label)));
    detail.appendChild(el(doc, "p", null, COPY.perCiting(r.citesPerCitingPaper.toFixed(1), label)));
  }
  // Retracted papers: only for a match the matcher was sure of or the user chose, only above zero, and always next to
  // the matched name and institution, so the count is never read as belonging to someone else.
  const retracted = Math.max(0, ...Object.values(rec.tables).map((r) => r.retracted || 0));
  if (retracted > 0) {
    const p = el(doc, "p", "su-top2pct-retracted", COPY.retracted(retracted, `${rec.name} (${rec.inst || "institution not listed"})`) + " ");
    const v = el(doc, "a", "su-top2pct-link", COPY.verify);
    v.href = RETRACTION_DB_URL;
    v.target = "_blank";
    v.rel = "noopener noreferrer";
    p.appendChild(v);
    detail.appendChild(p);
  }
  more.addEventListener("click", () => {
    const open = more.getAttribute("aria-expanded") !== "true";
    more.setAttribute("aria-expanded", open ? "true" : "false");
    detail.hidden = !open;
  });
  panel.appendChild(more);
  panel.appendChild(detail);
  panel.appendChild(footer(ctx, rec, onNotThis));
  return panel;
}

export function renderPickList(ctx, candidates, onChoose, openNow) {
  const { doc } = ctx;
  const panel = shell(ctx);
  panel.appendChild(el(doc, "p", "su-top2pct-who", COPY.several));
  const form = el(doc, "form", "su-top2pct-pick");
  form.hidden = !openNow;
  const fs = el(doc, "fieldset");
  fs.appendChild(el(doc, "legend", null, COPY.pickLegend));
  const radio = (value, text, i) => {
    const id = `su-top2pct-opt-${i}`;
    const row = el(doc, "div", "su-top2pct-opt");
    const input = el(doc, "input");
    input.type = "radio";
    input.name = "su-top2pct-choice";
    input.id = id;
    input.value = value;
    const label = el(doc, "label", null, text);
    label.htmlFor = id;
    row.appendChild(input);
    row.appendChild(label);
    fs.appendChild(row);
    return input;
  };
  const noneInput = radio("none", COPY.noneOfThese, 0);
  noneInput.checked = true;
  candidates.forEach((c, i) => {
    const sub = Object.values(c.tables)[0];
    const bits = [identityLine(c), sub ? sub.subfield : "", c.firstYear && c.lastYear ? COPY.active(c.firstYear, c.lastYear) : ""];
    radio(String(i), bits.filter(Boolean).join(" · "), i + 1);
  });
  form.appendChild(fs);
  const use = el(doc, "button", "su-top2pct-more", COPY.usePerson);
  use.type = "submit";
  form.appendChild(use);
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const sel = form.querySelector("input[name='su-top2pct-choice']:checked");
    const v = sel ? sel.value : "none";
    onChoose(v === "none" ? null : candidates[Number(v)] || null);
  });
  if (!openNow) {
    const choose = el(doc, "button", "su-top2pct-more", COPY.choose);
    choose.type = "button";
    choose.setAttribute("aria-expanded", "false");
    choose.addEventListener("click", () => {
      form.hidden = false;
      choose.setAttribute("aria-expanded", "true");
      noneInput.focus();
    });
    panel.appendChild(choose);
  }
  panel.appendChild(form);
  panel.appendChild(footer(ctx, null, null));
  if (openNow) queueMicrotask(() => noneInput.focus());
  return panel;
}
