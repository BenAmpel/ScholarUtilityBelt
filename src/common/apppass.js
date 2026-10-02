// Entry point bundled (esbuild, IIFE, global `SUAppPass`) into dist/common/apppass.sw.js
// for the classic service worker. Only the official SDK's three functions are
// exposed; sw.js decides when (and whether) any of them run.
export { checkAppPass, activateAppPass, manageAppPass } from "@chrome-stats/app-pass-sdk";
