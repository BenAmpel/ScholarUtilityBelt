import {
  csvToTags,
  getQualityJcrIndex,
  getQualityQuartilesIndex,
  getSavedPapers,
  getSettings,
  removePaper,
  uniqTags,
  upsertPaper
} from "../common/storage.js";
import {
  compileQualityIndex,
  extractVenueFromAuthorsVenue,
  qualityBadgesForVenue
} from "../common/quality.js";

function el(id) {
  return document.getElementById(id);
}

function fmtDate(iso) {
  if (!iso) return "";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return String(iso);
  }
}

function normalize(s) {
  return String(s || "").toLowerCase();
}

function matchesQuery(p, q) {
  if (!q) return true;
  const hay = [p.title, p.authorsVenue, (p.tags || []).join(" "), p.notes].map(normalize).join("\n");
  return hay.includes(q);
}

function compareBy(sortKey, a, b) {
  if (sortKey === "year") return (b.year || 0) - (a.year || 0);
  if (sortKey === "title") return String(a.title || "").localeCompare(String(b.title || ""));
  // default savedAt
  return String(b.savedAt || "").localeCompare(String(a.savedAt || ""));
}

let qualityState = null;
async function ensureQualityState() {
  if (qualityState) return qualityState;
  const [s, quartiles, jcr] = await Promise.all([
    getSettings(),
    getQualityQuartilesIndex(),
    getQualityJcrIndex()
  ]);
  qualityState = {
    show: !!s.showQualityBadges,
    qIndex: compileQualityIndex(s, {
      quartilesIndex: quartiles.index || {},
      jcrIndex: jcr.index || {}
    }),
    quartilesMeta: quartiles.meta || null
  };
  return qualityState;
}

async function render() {
  const q = normalize(el("q").value);
  const sortKey = el("sort").value;

  const qs = await ensureQualityState();
  const saved = await getSavedPapers();
  const items = Object.values(saved)
    .filter((p) => matchesQuery(p, q))
    .sort((a, b) => compareBy(sortKey, a, b));

  const list = el("list");
  list.textContent = "";

  const tpl = document.getElementById("itemTpl");

  for (const p of items) {
    const node = tpl.content.firstElementChild.cloneNode(true);

    const titleA = node.querySelector(".title");
    titleA.textContent = p.title || "(untitled)";
    titleA.href = p.url || p.sourcePageUrl || "#";

    node.querySelector(".meta").textContent = [p.authorsVenue, p.year ? `(${p.year})` : ""]
      .filter(Boolean)
      .join(" ");

    node.querySelector(".snippet").textContent = p.snippet || "";

    const tags = (p.tags || []).join(", ");
    const note = (p.notes || "").trim();
    const venue = p.venue || extractVenueFromAuthorsVenue(p.authorsVenue);
    const badges = qs.show ? qualityBadgesForVenue(venue, qs.qIndex).map((b) => b.text) : [];
    const qualityStr = badges.length ? ` | Quality: ${badges.join(", ")}` : "";

    node.querySelector(".footer").textContent = `Saved: ${fmtDate(p.savedAt)}${
      tags ? ` | Tags: ${tags}` : ""
    }${note ? " | Notes: yes" : ""}${qualityStr}`;

    node.querySelectorAll("[data-act]").forEach((b) => {
      b.addEventListener("click", async () => {
        const act = b.getAttribute("data-act");
        if (act === "openScholar") {
          if (p.sourcePageUrl) window.open(p.sourcePageUrl, "_blank", "noreferrer");
          return;
        }
        if (act === "openPdf") {
          if (p.pdfUrl) window.open(p.pdfUrl, "_blank", "noreferrer");
          return;
        }
        if (act === "remove") {
          await removePaper(p.key);
          qualityState = null;
          await render();
          return;
        }
        if (act === "tags") {
          const next = window.prompt("Comma-separated tags:", (p.tags || []).join(", "));
          if (next === null) return;
          await upsertPaper({ ...p, tags: uniqTags(csvToTags(next)) });
          qualityState = null;
          await render();
          return;
        }
        if (act === "notes") {
          const next = window.prompt("Notes:", p.notes || "");
          if (next === null) return;
          await upsertPaper({ ...p, notes: String(next) });
          qualityState = null;
          await render();
          return;
        }
      });
    });

    list.appendChild(node);
  }
}

el("openOptions").addEventListener("click", () => {
  chrome.runtime.openOptionsPage();
});

el("q").addEventListener("input", () => render());
el("sort").addEventListener("change", () => render());

chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") return;
  if (changes.savedPapers || changes.settings) {
    qualityState = null;
    render();
  }
});

render();
