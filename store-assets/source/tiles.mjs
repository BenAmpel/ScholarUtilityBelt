import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
const dir = path.resolve(process.argv[2]);
const fonts = "/Volumes/Extreme SSD/Purplelink LLC/site/assets/fonts";
const b64 = (p) => fs.readFileSync(p).toString("base64");
const icon = `data:image/png;base64,${b64(path.join(dir, "prepped/icon.png"))}`;
const shot = `data:image/png;base64,${b64(path.join(dir, "prepped/results.png"))}`;
const base = `@font-face{font-family:Fraunces;src:url(data:font/woff2;base64,${b64(fonts + "/fraunces-latin.woff2")}) format("woff2");font-weight:100 900}
@font-face{font-family:Jakarta;src:url(data:font/woff2;base64,${b64(fonts + "/plus-jakarta-sans-latin.woff2")}) format("woff2");font-weight:200 800}
*{margin:0;box-sizing:border-box}body{overflow:hidden;position:relative;font-family:Jakarta,sans-serif;
background:radial-gradient(700px 400px at 90% -20%,#4a1478 0%,transparent 60%),linear-gradient(160deg,#1b0532 0%,#12001E 60%,#160328 100%)}`;
const tiles = [
  { out: "promo-small-440x280.png", w: 440, h: 280, html: `<style>${base}
    img{position:absolute;left:32px;top:40px;width:84px;height:84px}
    h1{position:absolute;left:32px;top:142px;font:600 34px/1.05 Fraunces,serif;color:#F7EBFF;letter-spacing:-.4px}
    p{position:absolute;left:32px;top:226px;font-size:15px;color:#D9C5EE}</style>
    <img src="${icon}" alt=""><h1>Scholar<br>Utility Belt</h1><p>Journal rankings and metrics in Google Scholar</p>` },
  { out: "promo-marquee-1400x560.png", w: 1400, h: 560, html: `<style>${base}
    .i{position:absolute;left:80px;top:120px;width:96px;height:96px}
    h1{position:absolute;left:80px;top:244px;width:600px;font:600 64px/1.02 Fraunces,serif;color:#F7EBFF;letter-spacing:-.8px}
    p{position:absolute;left:80px;top:328px;width:520px;font-size:22px;line-height:1.45;color:#D9C5EE}
    .s{position:absolute;left:690px;top:70px;width:780px;border-radius:14px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.5);border:1px solid rgba(255,255,255,.14)}
    .s img{width:780px;display:block}</style>
    <img class="i" src="${icon}" alt=""><h1>Scholar Utility Belt</h1><p>Journal rankings, author metrics and a review workspace, inside Google Scholar. Local-first, no account.</p>
    <div class="s"><img src="${shot}" alt=""></div>` },
];
const browser = await chromium.launch({ channel: "chrome", headless: true });
for (const t of tiles) {
  const page = await browser.newPage({ viewport: { width: t.w, height: t.h } });
  await page.setContent(t.html); await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(dir, "final", t.out) }); console.log("wrote", t.out); await page.close();
}
await browser.close();
