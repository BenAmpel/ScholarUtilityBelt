#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/output"
VERSION="${1:-}"

if [[ -z "${VERSION}" ]]; then
  VERSION="$(node -p "require('${ROOT_DIR}/manifest.json').version")"
fi

ZIP_PATH="${OUT_DIR}/ScholarUtilityBelt-${VERSION}.zip"

mkdir -p "${OUT_DIR}"

rm -f "${ZIP_PATH}"

pushd "${ROOT_DIR}" >/dev/null

# The manifest loads src/content/* only through dist/, so the unminified copies are left out
# (src/content/data-loader.js stays: the library page imports it). The predatory-venue lists
# are not read by any code. Together that is about a quarter of a megabyte compressed.
# dist/ is what the manifest actually loads for content scripts — rebuild it
# fresh so the zip can never ship stale bundles.
node build.js

zip -r "${ZIP_PATH}" \
  "manifest.json" \
  "icons" \
  "src" \
  "dist" \
  -x \
  ".DS_Store" \
  "*/.DS_Store" \
  ".git/*" \
  ".claude/*" \
  "node_modules/*" \
  "output/*" \
  "scripts/*" \
  "test_*.mhtml" \
  "Scientometric Measure Scholar/*" \
  "README.md" \
  "CITATION.cff" \
  ".gitignore" \
  "norwegian_register.csv" \
  "src/data/norwegian_register.csv" \
  "src/data/aft_ultraslim.edges.bin.gz" \
  "src/data/aft_ultraslim.names.json.gz" \
  "src/data/econ_genealogy.edges.bin.gz" \
  "src/data/econ_genealogy.names.json.gz" \
  "src/data/se_genealogy.edges.bin.gz" \
  "src/data/se_genealogy.names.json.gz" \
  "src/data/predatory_venues.txt" \
  "src/data/predatory_venues_bealls.txt" \
  "src/data/predatory_venues_predatoryjournals.txt" \
  "src/content/content.js" \
  "src/content/content.css" \
  "src/content/content-early.js" \
  "src/content/content-author.js" \
  "src/content/dom-cache.js" \
  "src/content/genealogy-lineage.js" \
  "src/content/review-workspace.js" \
  "src/content/signals-ui.js" \
  "src/content/trend-methods.js" \
  "src/content/trend-tracker.js" \
  "src/content/worker.js"

popd >/dev/null

echo "Wrote ${ZIP_PATH}"
