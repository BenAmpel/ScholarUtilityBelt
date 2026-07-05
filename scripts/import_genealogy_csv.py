#!/usr/bin/env python3
"""
Import advisor-student pairs from CSV/JSON into the genealogy dataset format.

Input formats:
  CSV:  advisor,student  (one pair per line, header optional)
  JSON: [{"advisor": "Name A", "student": "Name B"}, ...]

Output: .names.json.gz + .edges.bin.gz compatible with merge_genealogy_datasets.py
"""
import argparse
import csv
import gzip
import itertools
import json
import struct
import sys
from pathlib import Path


def load_csv(path):
    pairs = []
    HEADER_ADV = {"advisor", "supervisor", "mentor", "parent"}
    HEADER_STU = {"student", "advisee", "mentee", "child"}
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        first_row = next(reader, None)
        if not first_row or len(first_row) < 2:
            return pairs
        normalized = [c.strip().lower() for c in first_row]
        has_header = any(c in HEADER_ADV | HEADER_STU for c in normalized)
        adv_col, stu_col = 0, 1
        if has_header:
            for i, h in enumerate(normalized):
                if h in HEADER_ADV:
                    adv_col = i
                elif h in HEADER_STU:
                    stu_col = i
        else:
            reader = itertools.chain([first_row], reader)
        for row in reader:
            if len(row) < 2:
                continue
            adv = row[adv_col].strip()
            stu = row[stu_col].strip()
            if adv and stu:
                pairs.append((adv, stu))
    return pairs


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    pairs = []
    for entry in data:
        adv = entry.get("advisor", "").strip()
        stu = entry.get("student", "").strip()
        if adv and stu:
            pairs.append((adv, stu))
    return pairs


def main():
    ap = argparse.ArgumentParser(description="Import advisor-student pairs from CSV/JSON")
    ap.add_argument("input", help="Input CSV or JSON file")
    ap.add_argument("--out-dir", default=".", help="Output directory")
    ap.add_argument("--label", default="manual", help="Dataset label for output files")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        print(f"Error: {inp} not found", file=sys.stderr)
        sys.exit(1)

    if inp.suffix.lower() == ".json":
        pairs = load_json(inp)
    else:
        pairs = load_csv(inp)

    if not pairs:
        print("No valid pairs found in input", file=sys.stderr)
        sys.exit(1)

    names = []
    name_index = {}

    def get_or_add(name):
        if name in name_index:
            return name_index[name]
        idx = len(names)
        name_index[name] = idx
        names.append(name)
        return idx

    edges = set()
    for adv, stu in pairs:
        ai = get_or_add(adv)
        si = get_or_add(stu)
        if ai != si:
            edges.add((si, ai))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    label = args.label

    names_path = out_dir / f"{label}.names.json.gz"
    edges_path = out_dir / f"{label}.edges.bin.gz"

    with gzip.open(names_path, "wt", encoding="utf-8") as f:
        json.dump({"n": names}, f, ensure_ascii=False)

    with gzip.open(edges_path, "wb") as f:
        for a, b in edges:
            f.write(struct.pack("<II", a, b))

    print(f"Imported {len(pairs)} pairs -> {len(names)} names, {len(edges)} edges")
    print(f"  Names: {names_path}")
    print(f"  Edges: {edges_path}")
    print(f"\nTo merge into main dataset:")
    print(f"  python scripts/merge_genealogy_datasets.py \\")
    print(f"    --datasets {label}={names_path},{edges_path} \\")
    print(f"    --out-names src/data/genealogy_merged.names.json.gz \\")
    print(f"    --out-edges src/data/genealogy_merged.edges.bin.gz")


if __name__ == "__main__":
    main()
