import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

// Scratch chrome.storage.local, so the real storage functions the options page uses run against it.
const store = {};
globalThis.chrome = {
  storage: {
    local: {
      get(defaults) {
        const out = {};
        for (const [k, d] of Object.entries(defaults || {})) out[k] = k in store ? JSON.parse(JSON.stringify(store[k])) : d;
        return Promise.resolve(out);
      },
      set(items) { for (const [k, v] of Object.entries(items)) store[k] = JSON.parse(JSON.stringify(v)); return Promise.resolve(); },
      remove(keys) { for (const k of [].concat(keys)) delete store[k]; return Promise.resolve(); },
    },
  },
};

const { getCustomVenueList, setCustomVenueList } = await import("../src/common/storage.js");
const {
  addVenueToStoredList, appendVenueToList, buildAddToListControl, compileCustomVenueIndex,
  customListLabels, isAddableVenue, shouldOfferAddToList, DEFAULT_LIST_NAME,
} = await import("../src/common/custom-venues.js");
const { customBadgeForVenue, qualityBadgesForVenue, compileQualityIndex } = await import("../src/common/quality.js");

const storageFns = { getList: getCustomVenueList, setList: setCustomVenueList };

// Same shape content.js builds in refreshState from the stored list.
function customState(list) {
  return list ? { name: list.name, map: compileCustomVenueIndex(list.entries), labels: customListLabels(list) } : null;
}

// Minimal element stand-in: no DOM in the node test environment.
class El {
  constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.listeners = {}; this.hidden = false; this.className = ""; this.textContent = ""; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  appendChild(c) { this.children.push(c); return c; }
  addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
  fire(type, extra = {}) { for (const fn of this.listeners[type] || []) fn({ preventDefault() {}, stopPropagation() {}, ...extra }); }
  focus() {}
  find(cls) {
    if (String(this.className).split(" ").includes(cls)) return this;
    for (const c of this.children) { const hit = c.find?.(cls); if (hit) return hit; }
    return null;
  }
}
const doc = { createElement: (t) => new El(t) };
const flush = () => new Promise((r) => setTimeout(r, 0));

describe("Add to list: storage", () => {
  beforeEach(() => { for (const k of Object.keys(store)) delete store[k]; });

  it("adds the venue to the existing list through the options page's storage path, and the badge then renders", async () => {
    await setCustomVenueList({ name: "Business School list", uploadedAt: "2026-01-01T00:00:00.000Z", entries: [["MIS Quarterly", "A"]] });
    const venue = "Journal of Small Widget Studies";
    assert.equal(customBadgeForVenue(venue, customState(await getCustomVenueList())), null);

    const r = await addVenueToStoredList(storageFns, venue, "B");
    assert.equal(r.added, true);
    const saved = await getCustomVenueList();
    assert.equal(saved.name, "Business School list");
    assert.equal(saved.uploadedAt, "2026-01-01T00:00:00.000Z");
    assert.deepEqual(saved.entries, [["MIS Quarterly", "A"], [venue, "B"]]);

    // This result and any other result with the same venue (different case and spacing) now get the badge.
    const state = customState(saved);
    for (const shown of [venue, "journal of  small widget studies"]) {
      const badge = customBadgeForVenue(shown, state);
      assert.equal(badge?.kind, "custom");
      assert.equal(badge.text, "B");
      assert.equal(badge.metadata.listName, "Business School list");
    }
  });

  it("starts a list called My list when there is none", async () => {
    const r = await addVenueToStoredList(storageFns, "Journal of Small Widget Studies", "Tier 1");
    assert.equal(r.added, true);
    const saved = await getCustomVenueList();
    assert.equal(saved.name, DEFAULT_LIST_NAME);
    assert.ok(!Number.isNaN(Date.parse(saved.uploadedAt)));
    assert.deepEqual(saved.entries, [["Journal of Small Widget Studies", "Tier 1"]]);
  });

  it("does not save a repeat venue or an empty label", async () => {
    await setCustomVenueList({ name: "L", uploadedAt: "2026-01-01T00:00:00.000Z", entries: [["Nature", "A"]] });
    assert.equal((await addVenueToStoredList(storageFns, "NATURE", "B")).added, false);
    const empty = await addVenueToStoredList(storageFns, "Science", "  ");
    assert.equal(empty.added, false);
    assert.match(empty.error, /tier/i);
    assert.deepEqual((await getCustomVenueList()).entries, [["Nature", "A"]]);
  });

  it("never changes the list it was given", () => {
    const list = { name: "L", uploadedAt: "x", entries: [["Nature", "A"]] };
    const r = appendVenueToList(list, "Science", "A");
    assert.equal(list.entries.length, 1);
    assert.equal(r.list.entries.length, 2);
  });
});

describe("Add to list: which venues get the chip", () => {
  it("accepts a normal venue and rejects junk", () => {
    assert.equal(isAddableVenue("Journal of Small Widget Studies"), true);
    for (const junk of ["", "   ", "2019", "arXiv preprint arXiv:2101.00001", "SSRN", "books.google.com",
      "Journal of Small Widget…", "Journal of Small Widget ...", "x"]) {
      assert.equal(isAddableVenue(junk), false, junk);
    }
  });
});

describe("Add to list: chip", () => {
  beforeEach(() => { for (const k of Object.keys(store)) delete store[k]; });

  it("is a real button with an accessible name naming the list, and offers the list's tiers", () => {
    const w = buildAddToListControl(doc, { venue: "Journal of Small Widget Studies", listName: "Dept list", labels: ["A", "B"], onAdd() {} });
    const chip = w.find("su-add-to-list-btn");
    assert.equal(chip.tag, "button");
    assert.equal(chip.type, "button");
    assert.match(chip.attrs["aria-label"], /Journal of Small Widget Studies.*Dept list/);
    const form = w.find("su-add-to-list-form");
    assert.equal(form.hidden, true);
    chip.fire("click");
    assert.equal(form.hidden, false);
    assert.equal(chip.attrs["aria-expanded"], "true");
    assert.equal(w.find("su-add-to-list-label").textContent, 'Tier in "Dept list"');
    assert.equal(w.find("su-add-to-list-input").value, "A");
    const dl = form.children.find((c) => c.tag === "datalist");
    assert.deepEqual(dl.children.map((o) => o.value), ["A", "B"]);
  });

  it("says a new My list will be made when there is no list", () => {
    const w = buildAddToListControl(doc, { venue: "Journal of Small Widget Studies", listName: "", labels: [], onAdd() {} });
    assert.equal(w.find("su-add-to-list-label").textContent, 'Tier (new list "My list")');
  });

  it("saves the typed tier on Add, and the badge renders for the venue", async () => {
    const venue = "Journal of Small Widget Studies";
    const w = buildAddToListControl(doc, { venue, listName: "", labels: [],
      onAdd: (label) => addVenueToStoredList(storageFns, venue, label) });
    w.find("su-add-to-list-btn").fire("click");
    w.find("su-add-to-list-input").value = "A*";
    w.find("su-add-to-list-save").fire("click");
    await flush();
    assert.match(w.find("su-add-to-list-status").textContent, /Added/);
    assert.equal(customBadgeForVenue(venue, customState(await getCustomVenueList()))?.text, "A*");
  });

  it("is not built for a venue that cannot be added", () => {
    assert.equal(buildAddToListControl(doc, { venue: "2019", onAdd() {} }), null);
  });
});

describe("Add to list: not on matched results", () => {
  const qIndex = compileQualityIndex({ qualityAbdcRanks: "Journal of Finance,A*\n" });
  const settings = { showQualityBadges: true };
  // Same steps as renderQuality in content.js: built-in badges, then the user's own list in front.
  const rawFor = (venue, custom) => {
    const raw = [...(qualityBadgesForVenue(venue, qIndex) || [])];
    const c = customBadgeForVenue(venue, custom);
    return c ? [c, ...raw] : raw;
  };
  const offer = (venue, custom, extra = {}) =>
    shouldOfferAddToList({ venue, rawBadges: rawFor(venue, custom), settings, isAuthorProfile: false, ...extra });

  it("is not offered when a built-in list or the user's own list already matched", () => {
    assert.ok(rawFor("The Journal of Finance", null).length, "fixture matches ABDC");
    assert.equal(offer("The Journal of Finance", null), false);
    const custom = customState({ name: "L", entries: [["Journal of Small Widget Studies", "B"]] });
    assert.equal(offer("Journal of Small Widget Studies", custom), false);
    assert.equal(offer("Journal of Other Widget Studies", custom), true);
  });

  it("is not offered on author profiles, with badges off, or with Your own list badges hidden", () => {
    const v = "Journal of Other Widget Studies";
    assert.equal(offer(v, null, { isAuthorProfile: true }), false);
    assert.equal(offer(v, null, { settings: { showQualityBadges: false } }), false);
    assert.equal(offer(v, null, { settings: { showQualityBadges: true, qualityBadgeKinds: { custom: false } } }), false);
  });
});
