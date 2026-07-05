#!/usr/bin/env python3
"""
Merge multiple genealogy datasets into a single deduplicated dataset.
Outputs:
  - merged names json.gz with {"n": [names...]}
  - merged edges bin.gz (uint32 advisor, uint32 student)
"""
import argparse
import gzip
import json
import re
import struct
from pathlib import Path


SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
PREFIXES = {"dr", "prof", "professor", "mr", "mrs", "ms", "miss", "sir", "dame"}
CREDENTIALS = {
    "phd", "dphil", "md", "mba", "jd", "esq", "dds", "dmd", "dvm", "pharmd", "do", "dnp", "dpt", "od",
    "ms", "ma", "msc", "mcs", "mse", "meng", "m.eng", "mph", "mpa", "mpp", "msw",
    "bs", "ba", "bsc", "beng", "b.eng",
    "cpa", "cfa", "cissp", "cism", "cisa", "csp", "pe", "peng", "p.eng",
    "rn", "lcsw", "lmft", "lpc", "np", "pa",
    "facp", "facc", "facs", "frcpc", "frcs", "frs"
}


def normalize_token(token: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (token or "").lower())


def strip_trailing_suffixes(tokens):
    while len(tokens) > 1:
        last = normalize_token(tokens[-1].rstrip(".,"))
        if last in SUFFIXES:
            tokens.pop()
            continue
        break
    return tokens


def strip_leading_titles(tokens):
    while len(tokens) > 1:
        raw = tokens[0].strip().rstrip(".,")
        norm = normalize_token(raw)
        if norm in PREFIXES:
            tokens.pop(0)
            continue
        break
    return tokens


def strip_credentials(name: str) -> str:
    if not name:
        return ""
    name = name.replace("*", "").strip()
    parts = [p.strip() for p in re.split(r"\s*[,，‚‛﹐﹑;]+\s*", name) if p.strip()]
    if not parts:
        return ""
    primary = parts[0]
    kept = []
    for seg in parts[1:]:
        tokens = [t for t in seg.split() if t]
        norm = [normalize_token(t) for t in tokens if t]
        if len(norm) == 1 and norm[0] in SUFFIXES:
            kept.append(seg)
            continue
        if tokens and all(normalize_token(t) in CREDENTIALS or normalize_token(t) == "peng" for t in tokens):
            continue
        kept.append(seg)
    joined = f"{primary}, {', '.join(kept)}" if kept else primary
    return joined.strip()


def normalize_name(name: str) -> str:
    base = strip_credentials(name)
    tokens = [t for t in base.split() if t]
    tokens = strip_leading_titles(tokens)
    tokens = strip_trailing_suffixes(tokens)
    normalized = "".join(normalize_token(t) for t in tokens)
    return normalized


def load_names(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        obj = json.load(f)
    return obj["n"]


def iter_edges(path: Path):
    with gzip.open(path, "rb") as f:
        data = f.read()
    for off in range(0, len(data), 8):
        a, b = struct.unpack_from("<II", data, off)
        yield a, b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", required=True, help="name=names.gz,edges.gz")
    ap.add_argument("--out-names", required=True)
    ap.add_argument("--out-edges", required=True)
    args = ap.parse_args()

    merged_names = []
    name_index = {}
    edge_set = set()

    def get_or_add(name):
        key = normalize_name(name)
        if not key:
            return None
        if key in name_index:
            return name_index[key]
        idx = len(merged_names)
        name_index[key] = idx
        merged_names.append(name)
        return idx

    for spec in args.datasets:
        if "=" not in spec:
            raise SystemExit(f"Invalid dataset spec: {spec}")
        label, paths = spec.split("=", 1)
        names_path, edges_path = paths.split(",", 1)
        names_path = Path(names_path)
        edges_path = Path(edges_path)
        names = load_names(names_path)
        # build mapping old->new
        mapping = {}
        for i, name in enumerate(names):
            idx = get_or_add(name)
            if idx is None:
                continue
            mapping[i] = idx
        # add edges
        for a, b in iter_edges(edges_path):
            if a not in mapping or b not in mapping:
                continue
            na = mapping[a]
            nb = mapping[b]
            if na == nb:
                continue
            key = (na << 32) | nb
            edge_set.add(key)
        print(f"[merge] {label} names={len(names)} mapped={len(mapping)} edges={len(edge_set)}")

    # write names
    out_names = Path(args.out_names)
    out_names.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_names, "wt", encoding="utf-8") as f:
        json.dump({"n": merged_names}, f, ensure_ascii=False)
    # write edges
    out_edges = Path(args.out_edges)
    out_edges.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_edges, "wb") as f:
        for key in edge_set:
            a = (key >> 32) & 0xFFFFFFFF
            b = key & 0xFFFFFFFF
            f.write(struct.pack("<II", a, b))
    print(f"[merge] merged names={len(merged_names)} edges={len(edge_set)}")


if __name__ == "__main__":
    main()
