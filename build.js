#!/usr/bin/env node
/**
 * Build script for Scholar Utility Belt
 *
 * Minifies all content scripts and their dynamically-loaded modules into dist/.
 * The manifest.json points at dist/ so Chrome loads the minified files.
 *
 * Usage:
 *   node build.js          — one-shot build
 *   node build.js --watch  — rebuild on every save (reload extension manually after each)
 */

const path = require('path');
const { mkdirSync } = require('fs');
const { execFileSync, spawn } = require('child_process');

const ESBUILD = path.join(__dirname, 'node_modules/.bin/esbuild');
const WATCH = process.argv.includes('--watch');

// Generate manifest.json from template
try {
  require('./scripts/build_manifest.js');
} catch (e) {
  console.error('  ✗ manifest generation failed:', e.message);
  process.exit(1);
}

// Ensure output directories exist
mkdirSync(path.join(__dirname, 'dist/content'), { recursive: true });
mkdirSync(path.join(__dirname, 'dist/common'), { recursive: true });
mkdirSync(path.join(__dirname, 'dist/popup'), { recursive: true });

const TARGETS = [
  // ── Injected content scripts ─────────────────────────────────────────────────
  {
    label: 'content.js',
    args: ['src/content/content.js', '--bundle=false', '--minify', '--outfile=dist/content/content.js', '--log-level=warning'],
  },
  {
    label: 'content.css',
    args: ['src/content/content.css', '--bundle=false', '--minify', '--outfile=dist/content/content.css', '--log-level=warning'],
  },
  {
    label: 'content-early.js',
    args: ['src/content/content-early.js', '--bundle=false', '--minify', '--outfile=dist/content/content-early.js', '--log-level=warning'],
  },

  // ── Popup (bundled — storage.js import resolved at build time) ──────────────
  {
    label: 'popup/popup.js',
    args: ['src/popup/popup.js', '--bundle=true', '--format=esm', '--minify', '--outfile=dist/popup/popup.js', '--log-level=warning'],
  },

  // ── Dynamically-loaded modules (web_accessible_resources) ───────────────────
  // These are fetched at runtime via chrome.runtime.getURL(); minifying them
  // reduces the bytes Chrome must parse on startup and on lazy-load.
  {
    label: 'common/storage.js',
    args: ['src/common/storage.js', '--bundle=false', '--minify', '--outfile=dist/common/storage.js', '--log-level=warning'],
  },
  {
    label: 'common/quality.js',
    args: ['src/common/quality.js', '--bundle=false', '--minify', '--outfile=dist/common/quality.js', '--log-level=warning'],
  },
  {
    label: 'common/entitlement.js',
    args: ['src/common/entitlement.js', '--bundle=false', '--minify', '--outfile=dist/common/entitlement.js', '--log-level=warning'],
  },
  {
    // Third-party library — not minified, so the shipped bytes match what was audited.
    label: 'common/extpay.js',
    args: ['src/common/extpay.js', '--bundle=false', '--minify=false', '--outfile=dist/common/extpay.js', '--log-level=warning'],
  },
  {
    label: 'content/dom-cache.js',
    args: ['src/content/dom-cache.js', '--bundle=false', '--minify', '--outfile=dist/content/dom-cache.js', '--log-level=warning'],
  },
  {
    label: 'content/content-author.js',
    args: ['src/content/content-author.js', '--bundle=false', '--minify', '--outfile=dist/content/content-author.js', '--log-level=warning'],
  },
  {
    label: 'content/worker.js',
    args: ['src/content/worker.js', '--bundle=false', '--minify', '--outfile=dist/content/worker.js', '--log-level=warning'],
  },
  {
    label: 'content/data-loader.js',
    args: ['src/content/data-loader.js', '--bundle=true', '--format=esm', '--minify', '--outfile=dist/content/data-loader.js', '--log-level=warning'],
  },
  {
    label: 'content/trend-tracker.js',
    args: ['src/content/trend-tracker.js', '--bundle=true', '--format=esm', '--minify', '--outfile=dist/content/trend-tracker.js', '--log-level=warning'],
  },
  {
    label: 'content/trend-methods.js',
    args: ['src/content/trend-methods.js', '--bundle=false', '--minify', '--outfile=dist/content/trend-methods.js', '--log-level=warning'],
  },
];

function formatBytes(n) {
  return n >= 1024 ? (n / 1024).toFixed(1) + ' KB' : n + ' B';
}

function fileSize(p) {
  try { return require('fs').statSync(p).size; } catch { return 0; }
}

if (!WATCH) {
  // ── One-shot build ──────────────────────────────────────────────────────────
  console.log('\nBuilding Scholar Utility Belt...\n');
  let ok = true;

  for (const { label, args } of TARGETS) {
    const src = args[0];
    const outfile = args.find(a => a.startsWith('--outfile=')).split('=')[1];
    const before = fileSize(src);
    try {
      execFileSync(ESBUILD, args, { cwd: __dirname, stdio: ['ignore', 'pipe', 'inherit'] });
      const after = fileSize(outfile);
      const pct = Math.round((1 - after / before) * 100);
      console.log(`  ✓ ${label.padEnd(24)} ${formatBytes(before).padStart(8)} → ${formatBytes(after).padStart(8)}  (−${pct}%)`);
    } catch (e) {
      console.error(`  ✗ ${label} failed${e.stderr ? ': ' + e.stderr.toString().trim() : ''}`);
      ok = false;
    }
  }

  if (ok) {
    console.log('\n  ✅ dist/ ready. Reload the extension at chrome://extensions.\n');
  } else {
    process.exit(1);
  }

} else {
  // ── Watch mode ──────────────────────────────────────────────────────────────
  console.log('\nWatching src/ for changes...');
  console.log('Reload the extension at chrome://extensions after each rebuild.\n');

  for (const { label, args } of TARGETS) {
    const proc = spawn(ESBUILD, [...args, '--watch', '--sourcemap=inline'], {
      cwd: __dirname,
      stdio: 'inherit',
    });
    proc.on('error', err => console.error(`${label}: ${err.message}`));
  }
}
