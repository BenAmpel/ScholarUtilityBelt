// Service worker kept intentionally minimal.
// We avoid extra network requests to Scholar by default.

function sanitizeFilenameSegment(s, maxLen) {
  let t = s.replace(/[/\\:*?"<>|]/g, " ").replace(/\s+/g, " ").trim();
  if (maxLen && t.length > maxLen) t = t.slice(0, maxLen).trim();
  return t || "Unknown";
}

// Smart-rename PDF: when a PDF download matches a URL we have metadata for (from Scholar result), suggest [Author] - [Year] - [Title].pdf
if (typeof chrome.downloads !== "undefined" && chrome.downloads.onDeterminingFilename) {
  chrome.downloads.onDeterminingFilename.addListener(function (downloadItem, suggest) {
    (function run() {
      const isPdf =
        (downloadItem.mimeType && downloadItem.mimeType.toLowerCase() === "application/pdf") ||
        (downloadItem.filename && /\.pdf$/i.test(downloadItem.filename));
      if (!isPdf || !downloadItem.url) {
        suggest({});
        return;
      }
      chrome.storage.session.get("pdfUrlToMetadata").then(function (st) {
        const map = st.pdfUrlToMetadata && typeof st.pdfUrlToMetadata === "object" ? st.pdfUrlToMetadata : {};
        const meta = map[downloadItem.url] || map[downloadItem.finalUrl];
        if (!meta || (meta.author == null && meta.title == null)) {
          suggest({});
          return;
        }
        const author = sanitizeFilenameSegment(String(meta.author || "Unknown").trim(), 60);
        const year = meta.year ? String(meta.year).replace(/\D/g, "").slice(0, 4) : "";
        const title = sanitizeFilenameSegment(String(meta.title || "Paper").trim(), 80);
        const parts = [author, year, title].filter(Boolean);
        suggest({ filename: (parts.join(" - ") || "download") + ".pdf" });
      }).catch(function () { suggest({}); });
    })();
    return true;
  });
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg?.action === "openTabs" && Array.isArray(msg.urls)) {
    (async () => {
      let opened = 0;
      for (const url of msg.urls) {
        if (typeof url !== "string" || !/^https?:/i.test(url)) continue;
        try {
          await new Promise((resolve) => {
            chrome.tabs.create({ url, active: false }, () => resolve());
          });
          opened += 1;
        } catch (_) {}
      }
      sendResponse({ ok: true, opened });
    })();
    return true;
  }
  if (msg?.action === "fetchBib" && typeof msg.url === "string") {
    fetch(msg.url, { credentials: "omit" })
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((body) => sendResponse({ ok: true, body }))
      .catch((err) => sendResponse({ ok: false, error: String(err?.message || err) }));
    return true;
  }
  if (msg?.action === "fetchSnippet" && typeof msg.url === "string") {
    const timeout = 8000;
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeout);
    fetch(msg.url, { credentials: "omit", signal: ctrl.signal })
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((html) => {
        clearTimeout(t);
        sendResponse({ ok: true, html });
      })
      .catch((err) => {
        clearTimeout(t);
        sendResponse({ ok: false, error: String(err?.message || err) });
      });
    return true;
  }
  if (msg?.action === "fetchPdfPageCount" && typeof msg.url === "string" && typeof msg.paperKey === "string") {
    const tailBytes = 8192;
    fetch(msg.url, {
      credentials: "omit",
      headers: { Range: `bytes=-${tailBytes}` }
    })
      .then((r) => {
        if (!r.ok && r.status !== 206) throw new Error(`HTTP ${r.status}`);
        return r.arrayBuffer();
      })
      .then((buf) => {
        const bytes = new Uint8Array(buf);
        const decoder = new TextDecoder("latin1");
        const tail = decoder.decode(bytes);
        const matches = tail.match(/\/Count\s+(\d+)/g);
        if (!matches || matches.length === 0) {
          sendResponse({ ok: false });
          return;
        }
        const counts = matches.map((m) => parseInt(m.replace(/\D/g, ""), 10));
        const pages = Math.max(...counts);
        if (pages < 1 || pages > 50000) {
          sendResponse({ ok: false });
          return;
        }
        sendResponse({ ok: true, pages });
      })
      .catch(() => sendResponse({ ok: false }));
    return true;
  }
});

chrome.runtime.onInstalled.addListener(async () => {
  // Initialize storage keys if missing.
  const { savedPapers, settings } = await chrome.storage.local.get({ savedPapers: {}, settings: {} });

  // Seed built-in quality lists once, if the user hasn't configured any yet.
  const s = settings || {};
  const hasAnyQuality =
    !!String(s.qualityFt50List || "").trim() ||
    !!String(s.qualityUtd24List || "").trim() ||
    !!String(s.qualityAbdcRanks || "").trim() ||
    !!String(s.qualityQuartiles || "").trim() ||
    !!String(s.qualityCoreRanks || "").trim() ||
    !!String(s.qualityCcfRanks || "").trim();
  
  if (!hasAnyQuality) {
    const fetchExtText = async (path) => {
      const url = chrome.runtime.getURL(path);
      const r = await fetch(url);
      if (!r.ok) throw new Error(`Failed to load ${path}: ${r.status}`);
      return await r.text();
    };

    try {
      const updatedSettings = { ...s };
      
      if (!hasAnyQuality) {
        const [ft50, utd24, abdc, core, corePortal, ccf] = await Promise.all([
          fetchExtText("src/data/ft50.txt"),
          fetchExtText("src/data/utd24.txt"),
          fetchExtText("src/data/abdc2022.csv"),
          fetchExtText("src/data/core_icore2026.csv"),
          fetchExtText("src/data/core_portal_ranks.csv"),
          fetchExtText("src/data/ccf_ranks.csv")
        ]);
        
        updatedSettings.showQualityBadges = true;
        updatedSettings.qualityFt50List = ft50.trim() + "\n";
        updatedSettings.qualityUtd24List = utd24.trim() + "\n";
        updatedSettings.qualityAbdcRanks = abdc.trim() + "\n";
        updatedSettings.qualityCoreRanks = [core.trim(), (corePortal || "").trim()].filter(Boolean).join("\n") + "\n";
        updatedSettings.qualityCcfRanks = (ccf && ccf.trim()) ? ccf.trim() + "\n" : "";
      }
      

      await chrome.storage.local.set({
        savedPapers,
        settings: updatedSettings
      });
    } catch {
      // If seeding fails, continue with empty settings.
      await chrome.storage.local.set({ savedPapers, settings: s });
    }
  } else {
    await chrome.storage.local.set({ savedPapers, settings: s });
  }

  // Seed CORE ranks from built-in CSVs if the user hasn't set any yet.
  if (!String(s.qualityCoreRanks || "").trim()) {
    try {
      const [core, corePortal] = await Promise.all([
        fetch(chrome.runtime.getURL("src/data/core_icore2026.csv")).then((r) => (r.ok ? r.text() : "")),
        fetch(chrome.runtime.getURL("src/data/core_portal_ranks.csv")).then((r) => (r.ok ? r.text() : ""))
      ]);
      const merged = [core?.trim(), corePortal?.trim()].filter(Boolean).join("\n");
      if (merged.length > 10) {
        const updated = await chrome.storage.local.get({ settings: {} });
        const st = updated.settings || {};
        st.qualityCoreRanks = merged + "\n";
        await chrome.storage.local.set({ settings: st });
      }
    } catch {
      // Ignore; user can paste CSV in Options.
    }
  }

  // Seed CCF ranks from built-in CSV if the user hasn't set any yet.
  if (!String(s.qualityCcfRanks || "").trim()) {
    try {
      const url = chrome.runtime.getURL("src/data/ccf_ranks.csv");
      const r = await fetch(url);
      if (r.ok) {
        const ccf = await r.text();
        if (ccf && ccf.trim().length > 10) {
          const updated = await chrome.storage.local.get({ settings: {} });
          const st = updated.settings || {};
          st.qualityCcfRanks = ccf.trim() + "\n";
          await chrome.storage.local.set({ settings: st });
        }
      }
    } catch {
      // Ignore; user can paste CSV in Options.
    }
  }

  // Seed SJR quartiles snapshot if the user hasn't imported one yet.
  const { qualityQuartilesIndex } = await chrome.storage.local.get({ qualityQuartilesIndex: {} });
  if (!qualityQuartilesIndex || Object.keys(qualityQuartilesIndex).length === 0) {
    try {
      const url = chrome.runtime.getURL("src/data/scimago_2024_quartiles.json");
      const r = await fetch(url);
      if (r.ok) {
        const data = await r.json();
        const index = data?.index || {};
        const meta = data?.meta || null;
        if (index && typeof index === "object" && Object.keys(index).length > 1000) {
          await chrome.storage.local.set({ qualityQuartilesIndex: index, qualityQuartilesMeta: meta });
        }
      }
    } catch {
      // Ignore; user can import manually from Options.
    }
  }
});
