import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { PROMO_URL, DISMISS_KEY, isMacPlatform, shouldShowPromo, initPromoCard } from "../src/common/promo-card.js";

function fakeDoc({ hasCard = true } = {}) {
  const listeners = {};
  const card = { hidden: true };
  const button = { addEventListener: (type, fn) => { listeners[type] = fn; } };
  const link = { href: "" };
  const els = hasCard ? { promoCard: card, promoDismiss: button, promoLink: link } : {};
  return { doc: { getElementById: (id) => els[id] || null }, card, link, click: () => listeners.click && listeners.click() };
}
const fakeStorage = (initial = {}, { failGet = false, failSet = false } = {}) => {
  const data = { ...initial };
  return {
    data,
    get: async (k) => { if (failGet) throw new Error("get"); return { [k]: data[k] }; },
    set: async (o) => { if (failSet) throw new Error("set"); Object.assign(data, o); },
  };
};

describe("promo link", () => {
  it("points at the ModernTex page on purplelink.llc over https, tagged for the sub channel, with no referral code", () => {
    const u = new URL(PROMO_URL);
    assert.equal(u.protocol, "https:");
    assert.equal(u.hostname, "purplelink.llc");
    assert.equal(u.pathname, "/moderntex/");
    assert.equal(u.searchParams.get("utm_source"), "sub");
    assert.equal(u.searchParams.get("ref"), null);
  });
});

describe("who sees it", () => {
  it("detects macOS from userAgentData first, then the platform string", () => {
    assert.equal(isMacPlatform({ userAgentData: { platform: "macOS" } }), true);
    assert.equal(isMacPlatform({ userAgentData: { platform: "Windows" }, platform: "MacIntel" }), false);
    assert.equal(isMacPlatform({ platform: "MacIntel" }), true);
    assert.equal(isMacPlatform({ platform: "Win32" }), false);
    assert.equal(isMacPlatform({ platform: "Linux x86_64" }), false);
    assert.equal(isMacPlatform(undefined), false);
  });
  it("shows only on a Mac and only until dismissed", () => {
    assert.equal(shouldShowPromo({ isMac: true, dismissed: false }), true);
    assert.equal(shouldShowPromo({ isMac: true, dismissed: true }), false);
    assert.equal(shouldShowPromo({ isMac: false, dismissed: false }), false);
    assert.equal(shouldShowPromo({ isMac: false, dismissed: true }), false);
  });
});

describe("initPromoCard", () => {
  const mac = { platform: "MacIntel" };
  it("reveals the card on a Mac when it has not been dismissed, and sets the link", async () => {
    const d = fakeDoc(); const s = fakeStorage();
    assert.equal(await initPromoCard({ doc: d.doc, storage: s, nav: mac }), true);
    assert.equal(d.card.hidden, false);
    assert.equal(d.link.href, PROMO_URL);
  });
  it("dismissing hides it and remembers, and it stays hidden next time", async () => {
    const d = fakeDoc(); const s = fakeStorage();
    await initPromoCard({ doc: d.doc, storage: s, nav: mac });
    d.click(); await new Promise((r) => setTimeout(r, 0));
    assert.equal(d.card.hidden, true);
    assert.equal(s.data[DISMISS_KEY], true);
    const again = fakeDoc();
    assert.equal(await initPromoCard({ doc: again.doc, storage: s, nav: mac }), false);
    assert.equal(again.card.hidden, true);
  });
  it("stays hidden off a Mac", async () => {
    const d = fakeDoc();
    assert.equal(await initPromoCard({ doc: d.doc, storage: fakeStorage(), nav: { platform: "Win32" } }), false);
    assert.equal(d.card.hidden, true);
  });
  it("fails closed: a storage read error leaves it hidden; a write error still hides it for the visit", async () => {
    const d1 = fakeDoc();
    assert.equal(await initPromoCard({ doc: d1.doc, storage: fakeStorage({}, { failGet: true }), nav: mac }), false);
    assert.equal(d1.card.hidden, true);
    const d2 = fakeDoc();
    await initPromoCard({ doc: d2.doc, storage: fakeStorage({}, { failSet: true }), nav: mac });
    d2.click(); await new Promise((r) => setTimeout(r, 0));
    assert.equal(d2.card.hidden, true);
  });
  it("does nothing on a page without the card", async () => {
    assert.equal(await initPromoCard({ doc: fakeDoc({ hasCard: false }).doc, storage: fakeStorage(), nav: mac }), false);
  });
});
