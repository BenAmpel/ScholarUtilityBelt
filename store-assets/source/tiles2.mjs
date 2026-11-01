// Store tiles, November 2026: the tile shows a real Scholar result (from the 2026-10-16 capture) with the extension's
// own badge styles from src/content/content.css, so the search tile shows what the extension adds.
// Run from the repo root: node store-assets/source/tiles2.mjs  (writes store-assets/promo-*.png)
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const fonts = "/Volumes/Extreme SSD/Purplelink LLC/site/assets/fonts";
const b64 = (p) => fs.readFileSync(p).toString("base64");
const icon = `data:image/png;base64,${b64(path.join(root, "icons/icon128.png"))}`;
const extCss = fs.readFileSync(path.join(root, "src/content/content.css"), "utf8");
const base = `@font-face{font-family:Fraunces;src:url(data:font/woff2;base64,${b64(fonts + "/fraunces-latin.woff2")}) format("woff2");font-weight:100 900}
@font-face{font-family:Jakarta;src:url(data:font/woff2;base64,${b64(fonts + "/plus-jakarta-sans-latin.woff2")}) format("woff2");font-weight:200 800}
${extCss}
*{margin:0;box-sizing:border-box}html,body{overflow:hidden}body{position:relative;font-family:Jakarta,sans-serif;
background:radial-gradient(700px 400px at 90% -20%,#4a1478 0%,transparent 60%),linear-gradient(160deg,#1b0532 0%,#12001E 60%,#160328 100%)}
.card{position:absolute;background:#fff;border-radius:12px;box-shadow:0 18px 50px rgba(0,0,0,.55);font-family:Arial,sans-serif;transform-origin:0 0}
.card .t{color:#1a0dab;font-size:17px;line-height:1.3}.card .t b{font-weight:700}.card .a{color:#006621;font-size:13px;margin-top:3px}
.card .row{display:flex;flex-wrap:wrap;gap:5px;margin-top:7px}.card .row .su-badge{display:inline-block}
.card .f{color:#1a0dab;font-size:13px;margin-top:7px}`;
const card = `<div class="t">Functional imaging of <b>working memory</b> after 24 hr of total <b>sleep deprivation</b></div>
<div class="a">MWL Chee, WC Choo - The Journal of Neuroscience, 2004</div>
<div class="row"><span class="su-badge su-quartile">Q1</span><span class="su-badge su-era">ERA 2023</span><span class="su-badge su-norwegian">Level 2</span><span class="su-badge su-h5">h5: 52</span><span class="su-badge su-jcr">IF 4.0</span></div>
<div class="f">Cited by 654 &nbsp; 30/yr <span style="color:#15803d">&#8593;</span></div>`;
const tiles = [
  { out: "promo-small-440x280.png", w: 440, h: 280, html: `<style>${base}
    .i{position:absolute;left:24px;top:22px;width:44px;height:44px}
    h1{position:absolute;left:78px;top:22px;font:600 22px/1.05 Fraunces,serif;color:#F7EBFF;letter-spacing:-.2px}
    .s{position:absolute;left:78px;top:48px;font-size:12.5px;color:#D9C5EE}
    .card{left:24px;top:96px;width:520px;padding:16px 18px;transform:scale(.75)}
    .card .su-badge{font-size:13px;padding:3px 8px}
    p.n{position:absolute;left:24px;bottom:16px;font-size:13px;font-weight:600;color:#F7EBFF}</style>
    <img class="i" src="${icon}" alt=""><h1>Scholar Utility Belt</h1><div class="s">for Google Scholar</div>
    <div class="card">${card}</div><p class="n">Journal rankings and h-index. Free, no account.</p>` },
  { out: "promo-marquee-1400x560.png", w: 1400, h: 560, html: `<style>${base}
    .i{position:absolute;left:80px;top:96px;width:88px;height:88px}
    h1{position:absolute;left:80px;top:208px;width:560px;font:600 60px/1.02 Fraunces,serif;color:#F7EBFF;letter-spacing:-.8px}
    p{position:absolute;left:80px;top:300px;width:500px;font-size:22px;line-height:1.45;color:#D9C5EE}
    .card{left:690px;top:150px;width:560px;padding:22px 24px;transform:scale(1.15)}
    .card .su-badge{font-size:13px;padding:3px 8px}</style>
    <img class="i" src="${icon}" alt=""><h1>Scholar Utility Belt</h1>
    <p>Journal rankings, impact factor and h-index beside every Google Scholar result. Free, no account.</p>
    <div class="card">${card}</div>` },
];
const browser = await chromium.launch({ channel: "chrome", headless: true });
for (const t of tiles) {
  const page = await browser.newPage({ viewport: { width: t.w, height: t.h } });
  await page.setContent(`<body data-su-badge-palette="bold">${t.html}</body>`); await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(root, "store-assets", t.out) }); console.log("wrote", t.out); await page.close();
}
await browser.close();
