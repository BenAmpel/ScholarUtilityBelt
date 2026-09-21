/**
 * Academic-lineage tree overlay — lazy-loaded from content.js only when the
 * user opens the "Lineage" view (dynamic import, see openLineageOverlay /
 * closeLineageOverlay in content.js). The eager "match found" summary card
 * shown in the author-page right panel uses a much smaller subset of the
 * genealogy feature (name matching against the names dataset only) that
 * stays inline in content.js, since it runs on every author-page visit;
 * everything here — the edges dataset, tree traversal, and the overlay
 * itself — is only needed by the minority of users who actually open it.
 *
 * All state/helpers shared with content.js's eager genealogy code are
 * passed in via `deps` rather than imported, since content.js is a classic
 * script, not a module.
 */
export function createLineageOverlay(deps) {
  const {
    escapeHtml,
    extractAuthorName,
    generateAuthorNameVariations,
    getGenealogyMatchState,
    ensureGenealogyNamesLoaded,
    resolveGenealogyMatch,
    getGenealogyDatasetState,
    GENEALOGY_SOURCES,
    CLS_BTN_SEC,
  } = deps;

  const GENEALOGY_MAX_UP = 2;
  const GENEALOGY_MAX_DOWN = 2;
  const GENEALOGY_CLICK_DOWN = 10;
  const GENEALOGY_CLICK_UP = 10;
  const GENEALOGY_MAX_PER_LEVEL = 10;
  const GENEALOGY_DESC_LIMIT = 10000;

  function getLineageViewState() {
    if (!window.suLineageView) {
      window.suLineageView = { rootIndex: null, maxUp: GENEALOGY_MAX_UP, maxDown: GENEALOGY_MAX_DOWN, stack: [], datasetKey: "merged" };
    }
    return window.suLineageView;
  }

  async function fetchGzipArrayBuffer(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Failed to load ${url}`);
    if (!res.body || typeof DecompressionStream === "undefined") {
      return await res.arrayBuffer();
    }
    const stream = res.body.pipeThrough(new DecompressionStream("gzip"));
    return await new Response(stream).arrayBuffer();
  }

  async function ensureGenealogyEdgesLoaded(key) {
    const source = GENEALOGY_SOURCES[key];
    if (!source) throw new Error("Unknown genealogy source.");
    const data = getGenealogyDatasetState(key);
    if (data.edgesLoaded) return data;
    const url = chrome.runtime.getURL(source.edgesUrl);
    const buf = await fetchGzipArrayBuffer(url);
    const arr = new Uint32Array(buf);
    const forward = new Map();
    const reverse = new Map();
    for (let i = 0; i < arr.length; i += 2) {
      const advisor = arr[i];
      const student = arr[i + 1];
      if (!Number.isFinite(advisor) || !Number.isFinite(student)) continue;
      if (!forward.has(advisor)) forward.set(advisor, []);
      forward.get(advisor).push(student);
      if (!reverse.has(student)) reverse.set(student, []);
      reverse.get(student).push(advisor);
    }
    data.forward = forward;
    data.reverse = reverse;
    data.orientation = "student->advisor";
    data.edgesLoaded = true;
    return data;
  }

  function getGenealogyMaps(data) {
    if (!data) return { advisorsMap: new Map(), studentsMap: new Map() };
    if (data.orientation === "student->advisor") {
      return { advisorsMap: data.forward || new Map(), studentsMap: data.reverse || new Map() };
    }
    return { advisorsMap: data.reverse || new Map(), studentsMap: data.forward || new Map() };
  }

  function countReachable(root, map, limit = GENEALOGY_DESC_LIMIT) {
    const visited = new Set();
    const queue = [root];
    let qi = 0;
    let count = 0;
    while (qi < queue.length) {
      const node = queue[qi++];
      const neighbors = map?.get(node) || [];
      for (const n of neighbors) {
        if (visited.has(n) || n === root) continue;
        visited.add(n);
        count += 1;
        if (count >= limit) return { count, truncated: true };
        queue.push(n);
      }
    }
    return { count, truncated: false };
  }

  function getLineageRelLabel(depth) {
    if (depth === 0) return "Focus";
    if (depth === -1) return "Advisor";
    if (depth === -2) return "Advisor’s advisor";
    if (depth < -2) { const n = Math.abs(depth); return `${n}${n === 3 ? "rd" : "th"} gen. advisor`; }
    if (depth === 1) return "Student";
    if (depth === 2) return "Grandstudent";
    if (depth > 2) return `${depth}${depth === 3 ? "rd" : "th"} gen. student`;
    return "";
  }

  function getLineageNodeType(depth) {
    if (depth === 0) return "focal";
    if (Math.abs(depth) === 1) return "direct";
    if (Math.abs(depth) === 2) return "ancestor";
    return "distant";
  }

  function buildHierarchicalTree(rootIdx, data, advisorsMap, studentsMap, maxUp, maxDown) {
    const names = data.names || [];
    const visited = new Set();
    function buildDown(nodeIdx, depth, remaining) {
      if (visited.has(nodeIdx) || remaining <= 0) return [];
      visited.add(nodeIdx);
      const kids = studentsMap.get(nodeIdx) || [];
      return kids.slice(0, GENEALOGY_MAX_PER_LEVEL).map(k => {
        const childChildren = buildDown(k, depth + 1, remaining - 1);
        return { index: k, name: names[k] || "Unknown", depth: depth + 1, children: childChildren };
      });
    }
    function buildUp(nodeIdx, depth, remaining) {
      if (remaining <= 0) return null;
      const parents = advisorsMap.get(nodeIdx) || [];
      if (parents.length === 0) return null;
      const parent = parents[0];
      if (visited.has(parent)) return null;
      visited.add(parent);
      const grandparent = buildUp(parent, depth - 1, remaining - 1);
      const node = { index: parent, name: names[parent] || "Unknown", depth: depth - 1, children: [] };
      if (grandparent) {
        grandparent.children = [node];
        return grandparent;
      }
      return node;
    }
    const focalChildren = buildDown(rootIdx, 0, maxDown);
    const focal = { index: rootIdx, name: names[rootIdx] || "Unknown", depth: 0, children: focalChildren };
    const ancestor = buildUp(rootIdx, 0, maxUp);
    if (ancestor) {
      let bottom = ancestor;
      while (bottom.children && bottom.children.length > 0) bottom = bottom.children[0];
      bottom.children = [focal];
      return ancestor;
    }
    return focal;
  }

  function countSubtree(branch) {
    if (!branch.children || branch.children.length === 0) return 0;
    let n = branch.children.length;
    for (const c of branch.children) n += countSubtree(c);
    return n;
  }

  function renderTimelineNode(branch, search, autoExpand) {
    const type = getLineageNodeType(branch.depth);
    const rel = getLineageRelLabel(branch.depth);
    const name = escapeHtml(branch.name);
    const dimmed = search && !branch.name.toLowerCase().includes(search) ? ' style="opacity:0.35"' : "";
    const isFocal = branch.depth === 0;
    const focalClass = isFocal ? " sl-timeline-focal" : "";
    const hasKids = branch.children && branch.children.length > 0;
    const subtreeCount = hasKids ? countSubtree(branch) : 0;
    const expanded = autoExpand || Math.abs(branch.depth) <= 1;
    const toggleHtml = hasKids
      ? ` <button class="sl-collapse-toggle" data-sl-toggle="1">${expanded ? "−" : `+${subtreeCount}`}</button>`
      : "";
    const nodeHtml = `<div class="sl-node sl-node-${type}" data-lineage-index="${branch.index}" data-lineage-name="${name}"${dimmed}>
      <div class="sl-node-rel">${escapeHtml(rel)}${toggleHtml}</div>
      <div class="sl-node-name">${name}</div>
      <div class="sl-node-meta">${escapeHtml(branch.meta || "")}</div>
    </div>`;
    if (!hasKids) {
      return `<div class="sl-timeline-node${focalClass}">${nodeHtml}</div>`;
    }
    const displayLimit = 5;
    const visibleKids = expanded ? branch.children : [];
    const shown = visibleKids.slice(0, displayLimit);
    const overflow = visibleKids.length > displayLimit ? visibleKids.length - displayLimit : 0;
    let childrenHtml = shown.map(c => renderTimelineNode(c, search, false)).join("");
    if (overflow > 0) {
      const hiddenHtml = visibleKids.slice(displayLimit).map(c => renderTimelineNode(c, search, false)).join("");
      childrenHtml += `<div class="sl-overflow-group" style="display:none">${hiddenHtml}</div>`;
      childrenHtml += `<div class="sl-overflow-btn" data-sl-overflow="1">+${overflow} more</div>`;
    }
    const collapsedStyle = expanded ? "" : ' style="display:none"';
    return `<div class="sl-timeline-node${focalClass}">${nodeHtml}<div class="sl-timeline-children"${collapsedStyle}>${childrenHtml}</div></div>`;
  }

  function renderGenealogyTree(data, root, advisorsMap, studentsMap, maxUp, maxDown, search) {
    const hier = buildHierarchicalTree(root, data, advisorsMap, studentsMap, maxUp, maxDown);
    return `<div class="sl-timeline-tree">${renderTimelineNode(hier, search, true)}</div>`;
  }

  function ensureLineageOverlay() {
    let overlay = document.getElementById("su-lineage-overlay");
    if (overlay) return overlay;
    overlay = document.createElement("div");
    overlay.id = "su-lineage-overlay";
    overlay.className = "su-lineage-overlay";
    overlay.innerHTML = `
      <div class="su-lineage-backdrop" data-lineage-close="1"></div>
      <div class="su-lineage-panel">
        <div class="su-lineage-header">
          <div>
            <div class="su-lineage-title">Academic Lineage</div>
            <div class="su-lineage-subtitle">Advisor–student genealogy from public datasets</div>
          </div>
          <div class="su-lineage-actions">
            <button type="button" class="${CLS_BTN_SEC}" data-lineage-back="1">Back</button>
            <button type="button" class="${CLS_BTN_SEC}" data-lineage-close="1">Close ✕</button>
          </div>
        </div>
        <div class="su-lineage-meta">
          <div id="su-lineage-status" class="su-lineage-status"></div>
        </div>
        <div style="padding: 0 20px 0 20px; flex-shrink: 0;">
          <input id="su-lineage-search" class="su-lineage-search" type="text" placeholder="Search tree…" />
        </div>
        <div id="su-lineage-stats" class="su-lineage-stats"></div>
        <div id="su-lineage-tree" class="su-lineage-tree"></div>
        <div class="su-lineage-footer">
          <span class="su-lineage-footer-text">Tree view · Click any node to re-root</span>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", (e) => {
      if (e.target.closest?.("[data-lineage-close]")) {
        overlay.classList.remove("su-visible");
        return;
      }
      const backBtn = e.target.closest?.("[data-lineage-back]");
      if (backBtn) {
        const view = getLineageViewState();
        if (view.stack.length) {
          view.rootIndex = view.stack.pop();
          view.maxDown = GENEALOGY_MAX_DOWN;
          view.maxUp = GENEALOGY_MAX_UP;
          renderLineageOverlay();
        }
        return;
      }
      const toggle = e.target.closest?.("[data-sl-toggle]");
      if (toggle) {
        e.stopPropagation();
        const timelineNode = toggle.closest(".sl-timeline-node");
        const children = timelineNode?.querySelector(":scope > .sl-timeline-children");
        if (children) {
          const hidden = children.style.display === "none";
          children.style.display = hidden ? "" : "none";
          const count = toggle.closest(".sl-timeline-node")?.querySelectorAll(".sl-timeline-node").length || 0;
          toggle.textContent = hidden ? "−" : `+${count}`;
        }
        return;
      }
      const overflowBtn = e.target.closest?.("[data-sl-overflow]");
      if (overflowBtn) {
        const group = overflowBtn.previousElementSibling;
        if (group?.classList.contains("sl-overflow-group")) {
          group.style.display = "";
          overflowBtn.remove();
        }
        return;
      }
      const node = e.target.closest?.(".sl-node");
      if (!node) return;
      const rawIndex = node.getAttribute("data-lineage-index");
      const index = rawIndex != null ? Number(rawIndex) : null;
      if (!Number.isFinite(index)) return;
      const view = getLineageViewState();
      if (view.rootIndex != null && view.rootIndex !== index) {
        view.stack.push(view.rootIndex);
      }
      view.rootIndex = index;
      view.maxUp = GENEALOGY_CLICK_UP;
      view.maxDown = GENEALOGY_CLICK_DOWN;
      renderLineageOverlay();
    });
    let searchTimer = null;
    overlay.addEventListener("input", (e) => {
      if (!e.target?.closest?.("#su-lineage-search")) return;
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => renderLineageOverlay(), 200);
    });
    return overlay;
  }

  async function renderLineageOverlay() {
    const overlay = ensureLineageOverlay();
    const statusEl = overlay.querySelector("#su-lineage-status");
    const statsEl = overlay.querySelector("#su-lineage-stats");
    const treeEl = overlay.querySelector("#su-lineage-tree");
    const match = getGenealogyMatchState();
    const view = getLineageViewState();
    const authorName = (window.suState?.authorVariations?.[0] || extractAuthorName() || "").trim();
    const authorVariations = window.suState?.authorVariations || (authorName ? generateAuthorNameVariations(authorName) : []);
    const activeKey = "merged";
    view.datasetKey = activeKey;
    if (!authorName) {
      treeEl.innerHTML = `<div class="su-lineage-empty">No author detected.</div>`;
      statsEl.innerHTML = "";
      statusEl.textContent = "";
      return;
    }
    statusEl.textContent = "Loading lineage data...";
    try {
      const dataNames = await ensureGenealogyNamesLoaded(activeKey);
      const resolved = resolveGenealogyMatch(authorName, authorVariations, dataNames);
      if (resolved == null) {
        treeEl.innerHTML = `<div class="su-lineage-empty">No genealogy match found in this dataset.</div>`;
        statsEl.innerHTML = "";
        statusEl.textContent = "";
        return;
      }
      const data = await ensureGenealogyEdgesLoaded(activeKey);
      const root = Number.isFinite(view.rootIndex) ? view.rootIndex : resolved;
      view.rootIndex = root;
      const { advisorsMap, studentsMap } = getGenealogyMaps(data);
      const parents = advisorsMap.get(root) || [];
      const kids = studentsMap.get(root) || [];
      const ancestors = countReachable(root, advisorsMap);
      const descendants = countReachable(root, studentsMap);
      const maxUp = view.maxUp || GENEALOGY_MAX_UP;
      const maxDown = view.maxDown || GENEALOGY_MAX_DOWN;
      const searchInput = overlay.querySelector("#su-lineage-search");
      const searchTerm = (searchInput?.value || "").trim().toLowerCase();
      const treeHtml = renderGenealogyTree(data, root, advisorsMap, studentsMap, maxUp, maxDown, searchTerm);
      const truncAnc = ancestors.truncated ? `${ancestors.count}+` : ancestors.count;
      const truncDesc = descendants.truncated ? `${descendants.count}+` : descendants.count;
      statusEl.textContent = "";
      statsEl.innerHTML = `
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Match</div>
          <div class="su-lineage-stat-value">${escapeHtml(data.names?.[root] || match.matchName || "Unknown")}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Advisors</div>
          <div class="su-lineage-stat-value">${parents.length}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Students</div>
          <div class="su-lineage-stat-value">${kids.length}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Ancestors</div>
          <div class="su-lineage-stat-value">${truncAnc}</div>
        </div>
        <div class="su-lineage-stat">
          <div class="su-lineage-stat-label">Descendants</div>
          <div class="su-lineage-stat-value">${truncDesc}</div>
        </div>
      `;
      treeEl.innerHTML = treeHtml;
    } catch (err) {
      statusEl.textContent = "";
      statsEl.innerHTML = "";
      treeEl.innerHTML = `<div class="su-lineage-empty">Unable to load lineage data.</div>`;
    }
  }

  async function open() {
    const match = getGenealogyMatchState();
    if (!match?.matchIndex || match.status !== "ready") return;
    const view = getLineageViewState();
    if (!view.datasetKey) view.datasetKey = match.datasetKey || "merged";
    const overlay = ensureLineageOverlay();
    overlay.classList.add("su-visible");
    await renderLineageOverlay();
  }

  function close() {
    const overlay = document.getElementById("su-lineage-overlay");
    if (overlay) overlay.classList.remove("su-visible");
  }

  return { open, close };
}
