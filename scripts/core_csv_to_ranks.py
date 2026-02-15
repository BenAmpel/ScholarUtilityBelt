#!/usr/bin/env python3
"""
Convert CORE.csv (portal export: id, full name, short name, source, rank, ...)
to Venue,Rank format for the extension (FullName|ShortName,Rank).
Reads from src/data/core.csv, writes src/data/core_portal_ranks.csv.
"""

import csv
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "src" / "data"
INPUT_CSV = DATA_DIR / "core.csv"
OUTPUT_CSV = DATA_DIR / "core_portal_ranks.csv"


def main() -> int:
    if not INPUT_CSV.exists():
        print(f"Missing {INPUT_CSV}; run after moving CORE.csv to src/data/core.csv")
        return 1
    lines = []
    seen = set()
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 5:
                continue
            full_name = (row[1] or "").strip()
            short_name = (row[2] or "").strip()
            rank = (row[4] or "").strip()
            if not full_name or not rank:
                continue
            venue = f"{full_name}|{short_name}" if short_name and short_name != full_name else full_name
            key = (venue, rank)
            if key in seen:
                continue
            seen.add(key)
            line = f'"{venue}",{rank}' if "," in venue else f"{venue},{rank}"
            lines.append(line)
    OUTPUT_CSV.write_text("Venue,Rank\n" + "\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(lines)} rows to {OUTPUT_CSV.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
