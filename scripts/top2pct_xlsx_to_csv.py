#!/usr/bin/env python3
"""Dump the columns the top-2% panel needs from one Ioannidis author table (xlsx) to CSV.

Build time only; never run in CI. The source spreadsheets are about 100 MB each and stay outside the repository.
Run it with python3 -I from outside the folder that holds the spreadsheet, once per table:

    python3 -I scripts/top2pct_xlsx_to_csv.py <Table_1_Authors_career_...xlsx> <out>/career.csv
    python3 -I scripts/top2pct_xlsx_to_csv.py <Table_1_Authors_singleyr_...xlsx> <out>/singleyr.csv

Then build the data file with scripts/build_top2pct.mjs. Columns are found by their header name in the "Data"
sheet, and the script stops if one is missing, so a renamed column in a later version fails loudly.
"""
import csv
import hashlib
import sys

import openpyxl

COLUMNS = [
    "authfull", "inst_name", "cntry", "firstyr", "lastyr",
    "self%", "cprat", "np6025_rw",
    "sm-subfield-1", "rank sm-subfield-1 (ns)", "sm-subfield-1 count",
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(src, out):
    wb = openpyxl.load_workbook(src, read_only=True)
    if "Data" not in wb.sheetnames:
        sys.exit(f"{src}: no 'Data' sheet (found {wb.sheetnames})")
    rows = wb["Data"].iter_rows(values_only=True)
    header = [str(h).strip() if h is not None else "" for h in next(rows)]
    missing = [c for c in COLUMNS if c not in header]
    if missing:
        sys.exit(f"{src}: missing columns {missing}")
    idx = [header.index(c) for c in COLUMNS]
    n = 0
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for row in rows:
            if row is None or row[idx[0]] in (None, ""):
                continue
            w.writerow(["" if row[i] is None else row[i] for i in idx])
            n += 1
    print(f"{out}: {n} rows; source sha256 {sha256(src)}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
