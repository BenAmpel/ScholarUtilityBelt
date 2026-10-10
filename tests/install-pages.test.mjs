import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  START_PAGE,
  UNINSTALL_PAGE,
  startPageUrl,
  uninstallPageUrl,
  onInstalledPlan,
} from "../src/common/install-pages.js";

describe("page URLs", () => {
  it("carry the version when it looks like one", () => {
    assert.equal(startPageUrl("0.8.0"), `${START_PAGE}?v=0.8.0`);
    assert.equal(uninstallPageUrl("1.12.3.4"), `${UNINSTALL_PAGE}?v=1.12.3.4`);
  });

  it("leave the version off when it does not look like one", () => {
    for (const bad of [undefined, null, "", "latest", "0.8.0&x=1", "0.8.0/../..", "1.2.3.4.5", "<b>1.0</b>"]) {
      assert.equal(startPageUrl(bad), START_PAGE);
      assert.equal(uninstallPageUrl(bad), UNINSTALL_PAGE);
    }
  });

  it("point at purplelink.llc over https and nowhere else", () => {
    for (const u of [START_PAGE, UNINSTALL_PAGE]) {
      const url = new URL(u);
      assert.equal(url.protocol, "https:");
      assert.equal(url.hostname, "purplelink.llc");
      assert.ok(url.pathname.startsWith("/scholar-utility-belt/"));
    }
  });
});

describe("onInstalledPlan", () => {
  it("opens the start page on a fresh install", () => {
    const plan = onInstalledPlan("install", "0.8.0", false);
    assert.equal(plan.startUrl, `${START_PAGE}?v=0.8.0`);
    assert.equal(plan.uninstallUrl, `${UNINSTALL_PAGE}?v=0.8.0`);
  });

  it("never opens the start page twice", () => {
    assert.equal(onInstalledPlan("install", "0.8.0", true).startUrl, null);
  });

  it("never opens anything for an update of any kind", () => {
    for (const reason of ["update", "chrome_update", "shared_module_update"]) {
      assert.equal(onInstalledPlan(reason, "0.8.0", false).startUrl, null, reason);
    }
  });

  it("always sets the uninstall URL, so it follows the version", () => {
    for (const reason of ["install", "update", "chrome_update"]) {
      assert.equal(onInstalledPlan(reason, "0.9.1", true).uninstallUrl, `${UNINSTALL_PAGE}?v=0.9.1`);
    }
  });
});
