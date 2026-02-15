#!/usr/bin/env python3
"""
Convert era2023.xlsx to era2023.txt (one journal/venue name per line) for the extension.
Run from repo root: python3 scripts/convert_era_xlsx_to_txt.py

Uses only stdlib (zipfile + xml). Reads src/data/era2023.xlsx and writes src/data/era2023.txt.
"""
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
}

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "src" / "data"
XLSX = DATA / "era2023.xlsx"
OUT = DATA / "era2023.txt"


def get_shared_strings(zipf):
    try:
        with zipf.open("xl/sharedStrings.xml") as f:
            tree = ET.parse(f)
            root = tree.getroot()
            strings = []
            for si in root.findall(".//main:si", NS):
                t = si.find("main:t", NS)
                if t is not None and t.text:
                    strings.append(t.text)
                else:
                    r = si.find("main:r", NS)
                    if r is not None:
                        parts = [r.find("main:t", NS).text or "" for r in si.findall("main:r", NS)]
                        strings.append("".join(p for p in parts if p))
                    else:
                        strings.append("")
            return strings
    except KeyError:
        return []


def _col_ref_to_index(ref):
    """Convert cell ref like A1, B2 to (row_0based, col_0based)."""
    col_letters = ""
    for i, ch in enumerate(ref):
        if ch.isdigit():
            row_1based = int(ref[i:])
            break
        col_letters += ch
    else:
        return 0, 0
    col_idx = 0
    for ch in col_letters.upper():
        col_idx = col_idx * 26 + (ord(ch) - ord("A") + 1)
    return row_1based - 1, col_idx - 1


def first_sheet_rows(zipf, shared_strings):
    try:
        with zipf.open("xl/worksheets/sheet1.xml") as f:
            tree = ET.parse(f)
            root = tree.getroot()
            rows_dict = {}
            for row in root.findall(".//main:row", NS):
                row_r = row.get("r")
                row_idx = int(row_r) - 1 if row_r and row_r.isdigit() else len(rows_dict)
                for c in row.findall("main:c", NS):
                    ref = c.get("r", "")
                    if not ref:
                        continue
                    ri, ci = _col_ref_to_index(ref)
                    v = c.find("main:v", NS)
                    if v is not None and v.text is not None:
                        t = c.get("t")
                        if t == "s":
                            idx = int(v.text)
                            val = shared_strings[idx] if 0 <= idx < len(shared_strings) else ""
                        else:
                            val = v.text.strip()
                    else:
                        val = ""
                    if ri not in rows_dict:
                        rows_dict[ri] = {}
                    rows_dict[ri][ci] = val
            if not rows_dict:
                return []
            max_row = max(rows_dict.keys())
            max_col = max(max(rows_dict[r].keys()) for r in rows_dict)
            return [
                [rows_dict.get(ri, {}).get(ci, "") for ci in range(max_col + 1)]
                for ri in range(max_row + 1)
            ]
    except KeyError:
        return []


def main():
    if not XLSX.exists():
        print(f"Not found: {XLSX}")
        return 1
    with zipfile.ZipFile(XLSX, "r") as z:
        shared = get_shared_strings(z)
        rows = first_sheet_rows(z, shared)
    if not rows:
        print("No rows in sheet")
        return 1
    header = rows[0]
    col = 1
    for i, h in enumerate(header):
        if h and ("title" in str(h).lower() and "foreign" not in str(h).lower()):
            col = i
            break
    names = []
    for row in rows[1:]:
        if col < len(row) and row[col]:
            name = str(row[col]).strip()
            if name and not name.startswith("#"):
                names.append(name)
    OUT.write_text("\n".join(names), encoding="utf-8")
    print(f"Wrote {len(names)} entries to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
