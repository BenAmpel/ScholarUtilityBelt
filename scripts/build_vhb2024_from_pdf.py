#!/usr/bin/env python3
"""
Build VHB JOURQUAL 2024 journal list from official VHB area-rating PDFs.
Outputs src/data/vhb2024.csv and updates src/data/quality_sources.json.
"""
from __future__ import annotations

import io
import json
import re
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "src" / "data"
OUT_CSV = DATA_DIR / "vhb2024.csv"
SOURCES_JSON = DATA_DIR / "quality_sources.json"

RATING_STRUCTURE_URL = "https://www.vhbonline.org/en/services/vhb-rating-2024/rating-structure"

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dX]$")
RANK_RE = re.compile(r"^(A\+|A|B|C|D)$")

RANK_ORDER = {"A+": 5, "A": 4, "B": 3, "C": 2, "D": 1, "E": 0}


def fetch_pdf_urls() -> list[str]:
    html = requests.get(RATING_STRUCTURE_URL, timeout=30).text
    pdfs = re.findall(r'href=["\"]([^"\"]+\.pdf)["\"]', html, flags=re.I)
    pdfs = [urljoin(RATING_STRUCTURE_URL, p) for p in pdfs]
    # Only VHB 2024 area rating PDFs.
    pdfs = [p for p in pdfs if "VHB_Rating_2024_Area_rating_" in p]
    # Exclude commentary/practice lists (non-scientific journals).
    pdfs = [p for p in pdfs if not re.search(r"_(comm|practice|prac)\.pdf$", p, re.I)]
    # De-duplicate while preserving order.
    seen = set()
    out = []
    for p in pdfs:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def normalize_name(s: str) -> str:
    t = str(s or "").lower().replace("&", " and ")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"^proceedings of the\s+", "", t)
    t = re.sub(r"^proceedings of\s+", "", t)
    t = re.sub(r"^the\s+", "", t)
    t = re.sub(r"^\d+(st|nd|rd|th)\s+", "", t)
    t = re.sub(r"^(19|20)\d{2}\s+", "", t)
    t = re.sub(r"\s+\d+\s*-\s*\d+$", "", t)
    while re.match(r"^\S.*\s+\d+\s+\d+$", t):
        t = re.sub(r"\s+\d+\s+\d+$", "", t).strip()
    t = re.sub(r"\s+\d+$", "", t).strip()
    t = re.sub(r"\s+(?:tois|isr|isj|jmis|misq|ejis|jais|jsis|dss|kais|tkdd|tocs|tods|tis)$", "", t, flags=re.I).strip()
    return t


def group_lines(words, y_tol=2):
    words_sorted = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines = []
    for w in words_sorted:
        if not lines or abs(w["top"] - lines[-1]["top"]) > y_tol:
            lines.append({"top": w["top"], "words": [w]})
        else:
            lines[-1]["words"].append(w)
    return lines


def parse_pdf(url: str) -> list[tuple[str, str]]:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    content = resp.content

    rows: list[tuple[str, str]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        title_x0 = None
        issn_x0 = None
        rating_x0 = None
        votes_x0 = None
        in_scientific = False
        title_buf: list[str] = []

        for page in pdf.pages:
            words = page.extract_words(x_tolerance=1, y_tolerance=2, keep_blank_chars=False)
            lines = group_lines(words, y_tol=2)
            for line in lines:
                texts = [w["text"] for w in line["words"]]
                line_text = " ".join(texts)

                if "Type" in texts and "publication" in texts:
                    in_scientific = "Scientific" in texts and "journals" in texts
                    title_buf = []
                    continue
                if not in_scientific:
                    continue

                if "Title" in texts and "ISSN" in texts and "Rating" in texts:
                    for w in line["words"]:
                        if w["text"] == "Title":
                            title_x0 = w["x0"]
                        elif w["text"] == "ISSN":
                            issn_x0 = w["x0"]
                        elif w["text"] == "Rating":
                            rating_x0 = w["x0"]
                        elif w["text"] == "Votes":
                            votes_x0 = w["x0"]
                    title_buf = []
                    continue

                if line_text.startswith("©") or line_text.lower().startswith("letzte redaktionelle"):
                    continue

                if title_x0 is None or rating_x0 is None:
                    continue

                issn_words = [w for w in line["words"] if ISSN_RE.match(w["text"])]
                rating_words = [
                    w
                    for w in line["words"]
                    if w["x0"] >= rating_x0 - 1
                    and (votes_x0 is None or w["x0"] < votes_x0 - 1)
                    and RANK_RE.match(w["text"])
                ]
                title_words = [
                    w
                    for w in line["words"]
                    if w["x0"] >= title_x0 - 1
                    and (issn_x0 is None or w["x0"] < issn_x0 - 1)
                    and not ISSN_RE.match(w["text"])
                ]

                if issn_words and rating_words:
                    title_parts = title_buf + [w["text"] for w in title_words]
                    title = " ".join(title_parts).strip()
                    rank = rating_words[0]["text"].strip().replace("*", "+")
                    if title and rank:
                        rows.append((title, rank))
                    title_buf = []
                else:
                    if title_words:
                        title_buf.extend([w["text"] for w in title_words])

    return rows


def load_aliases(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    alias_map: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or "," not in line:
            continue
        name, _ = line.rsplit(",", 1)
        if "|" not in name:
            continue
        parts = [p.strip() for p in name.split("|") if p.strip()]
        for p in parts:
            n = normalize_name(p)
            if not n:
                continue
            cur = alias_map.get(n)
            if not cur or cur.count("|") < name.count("|"):
                alias_map[n] = name
    return alias_map


def build():
    pdf_urls = fetch_pdf_urls()
    if not pdf_urls:
        raise SystemExit("No PDF URLs found on rating structure page.")

    entries: list[tuple[str, str]] = []
    for url in pdf_urls:
        entries.extend(parse_pdf(url))

    # Merge by normalized title, keeping best rank and a stable display name.
    merged: dict[str, dict] = {}
    for title, rank in entries:
        rank = rank.replace("*", "+")
        if rank not in RANK_ORDER:
            continue
        n = normalize_name(title)
        if not n:
            continue
        cur = merged.get(n)
        if not cur:
            merged[n] = {"name": title, "rank": rank}
            continue
        cur_rank = cur["rank"]
        if RANK_ORDER[rank] > RANK_ORDER[cur_rank]:
            merged[n] = {"name": title, "rank": rank}
        elif rank == cur_rank and len(title) > len(cur["name"]):
            merged[n]["name"] = title

    # Preserve existing aliases where possible.
    aliases = load_aliases(OUT_CSV)
    for n, alias_name in aliases.items():
        if n in merged:
            merged[n]["name"] = alias_name

    # Output sorted CSV.
    rows = sorted(merged.values(), key=lambda r: r["name"].lower())
    out_lines = [f"{r['name']},{r['rank']}" for r in rows]
    OUT_CSV.write_text("\n".join(out_lines) + "\n", encoding="utf-8")

    # Update metadata.
    meta = {}
    if SOURCES_JSON.exists():
        meta = json.loads(SOURCES_JSON.read_text(encoding="utf-8"))
    meta.setdefault("sources", {})
    meta["fetchedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta["sources"]["vhb2024"] = {
        "rows": len(rows),
        "source": "VHB JOURQUAL 2024 (official VHB area-rating PDFs)",
        "url": RATING_STRUCTURE_URL,
        "pdfs": pdf_urls,
    }
    SOURCES_JSON.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Wrote {OUT_CSV} with {len(rows)} rows.")


if __name__ == "__main__":
    build()
