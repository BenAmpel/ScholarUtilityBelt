import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  REVIEW_URL, STATE_KEY, MIN_ACTIVE_DAYS, MIN_DAYS_SINCE_FIRST, SNOOZE_DAYS,
  localDay, nextActiveState, recordActiveDay, shouldShowRatingPrompt, initRatingPrompt,
} from "../src/common/rating-prompt.js";

const DAY = 24 * 60 * 60 * 1000;
const T0 = new Date(2026, 9, 1, 10, 0, 0).getTime();

const fakeStorage = (initial = {}, { failGet = false, failSet = false } = {}) => {
  const data = { ...initial };
  return {
    data,
    get: async (k) => { if (failGet) throw new Error("get"); return { [k]: data[k] }; },
    set: async (o) => { if (failSet) throw new Error("set"); Object.assign(data, o); },
  };
};
function fakeDoc({ complete = true } = {}) {
  const handlers = {};
  const el = (id) => ({ id, hidden: true, href: "", addEventListener: (t, fn) => { handlers[id] = fn; } });
  const els = { ratingCard: el("ratingCard"), ratingLink: el("ratingLink"), ratingLater: el("ratingLater"), ratingNever: el("ratingNever") };
  if (!complete) delete els.ratingNever;
  return { doc: { getElementById: (id) => els[id] || null }, els, click: (id) => handlers[id] && handlers[id]() };
}
const ready = () => ({ firstSeen: T0, lastDay: "2026-10-09", activeDays: MIN_ACTIVE_DAYS });

describe("review link", () => {
  it("opens the extension's review page on the Chrome Web Store over https", () => {
    const u = new URL(REVIEW_URL);
    assert.equal(u.protocol, "https:");
    assert.equal(u.hostname, "chromewebstore.google.com");
    assert.match(u.pathname, /\/omcogfcgldfmihfogbffflbocdbjockn\/reviews$/);
  });
});

describe("counting days of use", () => {
  it("counts a day once, however many pages are opened", () => {
    const first = nextActiveState(undefined, T0);
    assert.deepEqual(first, { firstSeen: T0, lastDay: localDay(T0), activeDays: 1 });
    assert.equal(nextActiveState(first, T0 + 60 * 60 * 1000), null);
    const second = nextActiveState(first, T0 + DAY);
    assert.equal(second.activeDays, 2);
    assert.equal(second.firstSeen, T0);
  });
  it("keeps the choices already made on the card", () => {
    assert.equal(nextActiveState({ done: true, activeDays: 3, firstSeen: T0, lastDay: "x" }, T0).done, true);
  });
  it("records through storage and never throws", async () => {
    const s = fakeStorage();
    assert.equal(await recordActiveDay({ storage: s, now: T0 }), true);
    assert.equal(await recordActiveDay({ storage: s, now: T0 + 1000 }), false);
    assert.equal(s.data[STATE_KEY].activeDays, 1);
    assert.equal(await recordActiveDay({ storage: fakeStorage({}, { failGet: true }), now: T0 }), false);
    assert.equal(await recordActiveDay({ storage: fakeStorage({}, { failSet: true }), now: T0 }), false);
  });
});

describe("when the card shows", () => {
  const after = T0 + MIN_DAYS_SINCE_FIRST * DAY;
  it("needs enough days of use and enough time since the first", () => {
    assert.equal(shouldShowRatingPrompt(ready(), after), true);
    assert.equal(shouldShowRatingPrompt({ ...ready(), activeDays: MIN_ACTIVE_DAYS - 1 }, after), false);
    assert.equal(shouldShowRatingPrompt(ready(), after - 1), false);
    assert.equal(shouldShowRatingPrompt(undefined, after), false);
  });
  it("stays away after it was answered, and while snoozed", () => {
    assert.equal(shouldShowRatingPrompt({ ...ready(), done: true }, after), false);
    assert.equal(shouldShowRatingPrompt({ ...ready(), snoozedUntil: after + 1 }, after), false);
    assert.equal(shouldShowRatingPrompt({ ...ready(), snoozedUntil: after }, after), true);
  });
});

describe("initRatingPrompt", () => {
  const now = T0 + MIN_DAYS_SINCE_FIRST * DAY;
  it("shows the card with the review link once the user qualifies", async () => {
    const d = fakeDoc();
    assert.equal(await initRatingPrompt({ doc: d.doc, storage: fakeStorage({ [STATE_KEY]: ready() }), now }), true);
    assert.equal(d.els.ratingCard.hidden, false);
    assert.equal(d.els.ratingLink.href, REVIEW_URL);
  });
  it("stays hidden for a new user", async () => {
    const d = fakeDoc();
    assert.equal(await initRatingPrompt({ doc: d.doc, storage: fakeStorage(), now }), false);
    assert.equal(d.els.ratingCard.hidden, true);
  });
  it("opening the review page ends it for good", async () => {
    const s = fakeStorage({ [STATE_KEY]: ready() });
    const d = fakeDoc();
    await initRatingPrompt({ doc: d.doc, storage: s, now });
    await d.click("ratingLink");
    assert.equal(d.els.ratingCard.hidden, true);
    assert.equal(s.data[STATE_KEY].done, true);
    assert.equal(await initRatingPrompt({ doc: fakeDoc().doc, storage: s, now: now + 400 * DAY }), false);
  });
  it("Not now asks again after the snooze", async () => {
    const s = fakeStorage({ [STATE_KEY]: ready() });
    const d = fakeDoc();
    await initRatingPrompt({ doc: d.doc, storage: s, now });
    await d.click("ratingLater");
    assert.equal(await initRatingPrompt({ doc: fakeDoc().doc, storage: s, now: now + DAY }), false);
    assert.equal(await initRatingPrompt({ doc: fakeDoc().doc, storage: s, now: now + SNOOZE_DAYS * DAY }), true);
  });
  it("Don't ask again ends it for good", async () => {
    const s = fakeStorage({ [STATE_KEY]: ready() });
    const d = fakeDoc();
    await initRatingPrompt({ doc: d.doc, storage: s, now });
    await d.click("ratingNever");
    assert.equal(s.data[STATE_KEY].done, true);
    assert.equal(s.data[STATE_KEY].rated, undefined);
  });
  it("keeps the card hidden when storage fails or the markup is incomplete", async () => {
    assert.equal(await initRatingPrompt({ doc: fakeDoc().doc, storage: fakeStorage({}, { failGet: true }), now }), false);
    assert.equal(await initRatingPrompt({ doc: fakeDoc({ complete: false }).doc, storage: fakeStorage({ [STATE_KEY]: ready() }), now }), false);
  });
  it("still hides the card when saving the choice fails", async () => {
    const d = fakeDoc();
    await initRatingPrompt({ doc: d.doc, storage: fakeStorage({ [STATE_KEY]: ready() }, { failSet: true }), now });
    await d.click("ratingNever");
    assert.equal(d.els.ratingCard.hidden, true);
  });
});
