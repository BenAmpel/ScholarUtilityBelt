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

zip -r "${ZIP_PATH}" \
  "manifest.json" \
  "icons" \
  "src" \
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
  "src/data/se_genealogy.names.json.gz"

popd >/dev/null

echo "Wrote ${ZIP_PATH}"
