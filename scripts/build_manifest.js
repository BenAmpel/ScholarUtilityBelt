#!/usr/bin/env node
/**
 * Generates manifest.json from manifest.template.json,
 * expanding $SCHOLAR_MATCHES into the full list of Scholar domains.
 */
const { readFileSync, writeFileSync } = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');

const SCHOLAR_MATCHES = [
  "https://scholar.google.com/*",
  "https://scholar.google.ca/*",
  "https://scholar.google.com.au/*",
  "https://scholar.google.co.uk/*",
  "https://scholar.google.co.in/*",
  "https://scholar.google.co.jp/*",
  "https://scholar.google.co.kr/*",
  "https://scholar.google.com.br/*",
  "https://scholar.google.com.mx/*",
  "https://scholar.google.com.tr/*",
  "https://scholar.google.com.hk/*",
  "https://scholar.google.com.sg/*",
  "https://scholar.google.com.tw/*",
  "https://scholar.google.com.ar/*",
  "https://scholar.google.com.co/*",
  "https://scholar.google.com.pe/*",
  "https://scholar.google.de/*",
  "https://scholar.google.fr/*",
  "https://scholar.google.es/*",
  "https://scholar.google.it/*",
  "https://scholar.google.nl/*",
  "https://scholar.google.be/*",
  "https://scholar.google.at/*",
  "https://scholar.google.ch/*",
  "https://scholar.google.se/*",
  "https://scholar.google.no/*",
  "https://scholar.google.fi/*",
  "https://scholar.google.dk/*",
  "https://scholar.google.pl/*",
  "https://scholar.google.cz/*",
  "https://scholar.google.hu/*",
  "https://scholar.google.ie/*",
  "https://scholar.google.il/*",
  "https://scholar.google.pt/*",
  "https://scholar.google.gr/*",
  "https://scholar.google.ro/*",
  "https://scholar.google.ru/*",
  "https://scholar.google.ae/*",
  "https://scholar.google.sa/*",
  "https://scholar.google.co.za/*"
];

const template = JSON.parse(readFileSync(path.join(ROOT, 'manifest.template.json'), 'utf8'));

function expand(obj) {
  if (Array.isArray(obj)) {
    const expanded = [];
    for (const item of obj) {
      if (item === '$SCHOLAR_MATCHES') {
        expanded.push(...SCHOLAR_MATCHES);
      } else {
        expanded.push(expand(item));
      }
    }
    return expanded;
  }
  if (obj && typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) {
      out[k] = expand(v);
    }
    return out;
  }
  return obj;
}

const manifest = expand(template);
writeFileSync(path.join(ROOT, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
console.log('  ✓ manifest.json generated from template');
