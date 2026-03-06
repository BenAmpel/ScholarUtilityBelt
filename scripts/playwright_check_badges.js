const path = require("path");
const fs = require("fs");
const { chromium } = require("playwright");

async function main() {
  const extensionPath = process.argv[2]
    ? path.resolve(process.argv[2])
    : path.resolve(__dirname, ".." );
  const url = process.argv[3] || "https://scholar.google.com/scholar?q=information+systems+research";
  const maxAttempts = Number(process.argv[4] || 3);

  const outputDir = path.resolve(__dirname, "..", "output", "playwright");
  fs.mkdirSync(outputDir, { recursive: true });

  const userDataDir = path.join(outputDir, "pw-user-data");

  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: false,
    args: [
      `--disable-extensions-except=${extensionPath}`,
      `--load-extension=${extensionPath}`
    ]
  });

  const page = await context.newPage();
  let finalCounts = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    await page.goto(url, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(4000);

    const diag = await page.evaluate(() => {
      const qualityRows = document.querySelectorAll(".su-quality");
      const badges = document.querySelectorAll(".su-quality .su-badge");
      const anySu = document.querySelector("[data-su-theme],[data-su-badge-palette],.su-quality,.su-badge");
      const earlyTheme = document.getElementById("su-early-theme");
      const hasConsent = !!document.querySelector("form[action*='consent'], #gsr form[action*='consent']");
      const hasCaptcha = /captcha|unusual traffic/i.test(document.body?.innerText || "");
      const firstRow = document.querySelector(".gsc_a_tr") || document.querySelector(".gs_r");
      const grayDivs = firstRow ? Array.from(firstRow.querySelectorAll(".gs_gray")).map((n) => n.innerText) : [];
      const venueGray = grayDivs.length ? grayDivs[grayDivs.length - 1] : null;
      const qualityCalled = firstRow?.getAttribute("data-su-quality-called") ?? null;
      const qualityRaw = firstRow?.getAttribute("data-su-quality-raw") ?? null;
      const qualityFiltered = firstRow?.getAttribute("data-su-quality-filtered") ?? null;
      const qualityVenue = firstRow?.getAttribute("data-su-quality-venue") ?? null;
      const qualityError = firstRow?.getAttribute("data-su-quality-error") ?? null;
      const suState = window.suState || null;
      const settings = suState?.settings || null;
      const showQualityBadges = settings?.showQualityBadges;
      const qualityBadgeKinds = settings?.qualityBadgeKinds || null;
      const qIndex = suState?.qIndex || null;
      const qIndexSizes = qIndex ? {
        ft50: qIndex.ft50?.size || 0,
        utd24: qIndex.utd24?.size || 0,
        quartiles: qIndex.quartiles?.size || 0,
        abdc: qIndex.abdc?.size || 0,
        vhb: qIndex.vhb?.size || 0
      } : null;
      return {
        qualityRows: qualityRows.length,
        badges: badges.length,
        anySu: !!anySu,
        earlyTheme: !!earlyTheme,
        hasConsent,
        hasCaptcha,
        venueGray: venueGray || null,
        grayDivs,
        qualityCalled,
        qualityRaw,
        qualityFiltered,
        qualityVenue,
        qualityError,
        showQualityBadges,
        qualityBadgeKinds,
        qIndexSizes
      };
    });

    const screenshotPath = path.join(outputDir, `scholar_badges_attempt_${attempt}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: true });

    console.log(`Attempt ${attempt}/${maxAttempts}`);
    console.log("Quality rows:", diag.qualityRows, "Badges:", diag.badges);
    console.log("SU markers present:", diag.anySu, "Early theme:", diag.earlyTheme);
    console.log("Consent:", diag.hasConsent, "Captcha:", diag.hasCaptcha);
    console.log("Venue gray:", diag.venueGray);
    console.log("Gray divs:", diag.grayDivs);
    console.log("Quality called:", diag.qualityCalled, "Raw:", diag.qualityRaw, "Filtered:", diag.qualityFiltered, "Venue:", diag.qualityVenue, "Error:", diag.qualityError);
    console.log("showQualityBadges:", diag.showQualityBadges);
    console.log("qualityBadgeKinds:", diag.qualityBadgeKinds);
    console.log("qIndexSizes:", diag.qIndexSizes);
    console.log("Screenshot:", screenshotPath);

    finalCounts = diag;
    if (diag.badges > 0) break;
  }

  // Keep browser open briefly for visual inspection
  await page.waitForTimeout(5000);
  await context.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
