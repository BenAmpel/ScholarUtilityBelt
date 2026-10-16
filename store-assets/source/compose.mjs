import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";
const dir = path.resolve(process.argv[2]);
const fonts = "/Volumes/Extreme SSD/Purplelink LLC/site/assets/fonts";
const b64 = (p) => fs.readFileSync(p).toString("base64");
const css = `@font-face{font-family:Fraunces;src:url(data:font/woff2;base64,${b64(fonts + "/fraunces-latin.woff2")}) format("woff2");font-weight:100 900}
@font-face{font-family:Jakarta;src:url(data:font/woff2;base64,${b64(fonts + "/plus-jakarta-sans-latin.woff2")}) format("woff2");font-weight:200 800}
*{margin:0;box-sizing:border-box}body{width:1280px;height:800px;overflow:hidden;position:relative;font-family:Jakarta,sans-serif;
background:radial-gradient(900px 500px at 85% -10%,#4a1478 0%,transparent 60%),linear-gradient(160deg,#1b0532 0%,#12001E 55%,#160328 100%)}
.icon{position:absolute;top:44px;right:60px;width:64px;height:64px}
h1{position:absolute;left:60px;top:44px;width:1000px;font-family:Fraunces,serif;font-weight:600;font-size:44px;line-height:1.1;letter-spacing:-.5px;color:#F7EBFF}
p{position:absolute;left:60px;top:104px;width:1040px;font-size:20px;line-height:1.4;color:#D9C5EE}
.tag{display:inline-block;margin-left:10px;padding:2px 10px;border-radius:999px;background:#C474F9;color:#12001E;font:700 15px Jakarta,sans-serif;vertical-align:6px}
.shot{position:absolute;left:60px;top:196px;width:1160px;height:604px;border-radius:14px 14px 0 0;overflow:hidden;box-shadow:0 -6px 40px rgba(0,0,0,.45);border:1px solid rgba(255,255,255,.12);border-bottom:0}
.shot img{width:1160px;display:block}`;
const specs = JSON.parse(fs.readFileSync(path.join(dir, "specs.json"), "utf8"));
const browser = await chromium.launch({ channel: "chrome", headless: true });
for (const s of specs) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1 });
  const img = `data:image/png;base64,${b64(path.join(dir, "prepped", s.img))}`;
  const icon = `data:image/png;base64,${b64(path.join(dir, "prepped/icon.png"))}`;
  await page.setContent(`<style>${css}</style><img class="icon" src="${icon}" alt=""><h1>${s.title}${s.pro ? '<span class="tag">Pro</span>' : ""}</h1><p>${s.sub}</p><div class="shot"><img src="${img}" alt=""></div>`);
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(dir, "final", s.out) });
  console.log("wrote", s.out); await page.close();
}
await browser.close();
