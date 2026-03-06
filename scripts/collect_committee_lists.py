#!/usr/bin/env python3
"""
Collect advisor/advisee relationships from public committee/student lists.
Input: text file with one URL per line (public pages only, respect ToS/robots).
Output: JSONL edges {"advisor","student","source","url"}.
"""
import argparse
import json
import re
import urllib.request
from pathlib import Path
from html.parser import HTMLParser


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
    def handle_data(self, data):
        if data:
            self.parts.append(data)


PAIR_PATTERNS = [
    re.compile(r"(?P<student>[A-Z][A-Za-z\\-'. ]+)\\s*—\\s*(?P<advisor>[A-Z][A-Za-z\\-'. ]+)", re.UNICODE),
    re.compile(r"(?P<student>[A-Z][A-Za-z\\-'. ]+)\\s*\\((?:Advisor|Supervisor)[:\\s]+(?P<advisor>[A-Z][A-Za-z\\-'. ]+)\\)", re.IGNORECASE),
]


def fetch_text(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "ScholarUtilityBelt/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        html = resp.read().decode("utf-8", errors="ignore")
    parser = TextExtractor()
    parser.feed(html)
    return " ".join(parser.parts)


def extract_pairs(text):
    pairs = []
    for pat in PAIR_PATTERNS:
        for m in pat.finditer(text):
            student = m.group("student").strip()
            advisor = m.group("advisor").strip()
            if student and advisor:
                pairs.append((advisor, student))
    return pairs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    urls = [u.strip() for u in in_path.read_text().splitlines() if u.strip() and not u.strip().startswith("#")]
    count = 0
    with out_path.open("w", encoding="utf-8") as out:
        for url in urls:
            try:
                text = fetch_text(url)
            except Exception:
                continue
            for advisor, student in extract_pairs(text):
                out.write(json.dumps({
                    "advisor": advisor,
                    "student": student,
                    "source": "committee_list",
                    "url": url
                }, ensure_ascii=False) + "\n")
                count += 1
    print(f"[committee] pairs={count} out={out_path}")


if __name__ == "__main__":
    main()
