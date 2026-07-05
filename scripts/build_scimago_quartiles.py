#!/usr/bin/env python3
"""
Build a compact quartile index from a SCImago CSV export.

Input: SCImago Journal Rank export CSV (often semicolon-delimited) with columns:
- Title
- SJR Best Quartile (Q1-Q4 or '-')

Output: src/data/scimago_<year>_quartiles.json containing:
{ meta: {...}, index: { normalized_title: "Q1"|"Q2"|"Q3"|"Q4" } }
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def normalize_venue_name(s: str) -> str:
    s = str(s or "").lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_quartile(s: str) -> str:
    s = str(s or "").strip().upper()
    m = re.search(r"\bQ([1-4])\b", s)
    return f"Q{m.group(1)}" if m else ""


def best_quartile(a: str, b: str) -> str:
    qa, qb = normalize_quartile(a), normalize_quartile(b)
    if not qa:
        return qb
    if not qb:
        return qa
    return qa if int(qa[1]) <= int(qb[1]) else qb


def sniff_delimiter(sample: str) -> str:
    # SCImago exports commonly use ';'.
    candidates = [";", ",", "\t"]
    best = ";"
    best_count = -1
    for d in candidates:
        c = sample.count(d)
        if c > best_count:
            best_count = c
            best = d
    return best


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("input_csv", help="Path to SCImago export CSV")
    ap.add_argument("--year", type=int, required=True, help="Year for the snapshot name/meta")
    ap.add_argument(
        "--out",
        default="src/data",
        help="Output directory (default: src/data)",
    )
    args = ap.parse_args()

    in_path = Path(args.input_csv).expanduser().resolve()
    if not in_path.exists():
        raise SystemExit(f"Missing input: {in_path}")

    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"scimago_{args.year}_quartiles.json"

    text0 = in_path.read_text(encoding="utf-8", errors="ignore")
    first_line = (text0.splitlines() or [""])[0]
    delim = sniff_delimiter(first_line)

    idx: dict[str, str] = {}
    row_count = 0

    with in_path.open(newline="", encoding="utf-8", errors="ignore") as f:
        r = csv.DictReader(f, delimiter=delim)
        if not r.fieldnames:
            raise SystemExit("No headers found.")
        if "Title" not in r.fieldnames or "SJR Best Quartile" not in r.fieldnames:
            raise SystemExit(f"Missing expected columns. Have: {r.fieldnames}")

        for row in r:
            title = (row.get("Title") or "").strip()
            q = best_quartile("", row.get("SJR Best Quartile") or "")
            if not title or not q:
                continue
            k = normalize_venue_name(title)
            if not k:
                continue
            idx[k] = best_quartile(idx.get(k, ""), q)
            row_count += 1

    meta = {
        "source": "SCImago Journal Rank (CSV export)",
        "year": args.year,
        "importedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rowCount": row_count,
        "journalCount": len(idx),
        "inputFilename": in_path.name,
    }

    out = {"meta": meta, "index": idx}
    out_path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {out_path} (journals={len(idx)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

