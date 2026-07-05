#!/usr/bin/env python3
"""
Scrape academic genealogy databases for advisor-student relationships.

Supported sources:
  - Mathematics Genealogy Project (genealogy.math.ndsu.nodak.edu)
  - Academic Family Tree (academictree.org)
  - Neurotree (neurotree.org)

Output: .names.json.gz + .edges.bin.gz per source, compatible with merge_genealogy_datasets.py

Usage:
  python scripts/scrape_genealogy_databases.py --source mgp --out-dir data/genealogy/
  python scripts/scrape_genealogy_databases.py --source academictree --out-dir data/genealogy/
  python scripts/scrape_genealogy_databases.py --source all --out-dir data/genealogy/

Rate limiting: 1 request/second by default (--delay to adjust)
"""
import argparse
import gzip
import json
import re
import struct
import sys
import time
import urllib.parse
import urllib.request
from collections import deque
from html.parser import HTMLParser
from pathlib import Path


UA = "ScholarUtilityBelt/1.0 (academic genealogy harvester; ben.ampel@gmail.com)"


def fetch(url, delay=1.0, timeout=30, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            time.sleep(delay)
            return data
        except Exception as e:
            if attempt >= retries:
                raise
            time.sleep(2 * (attempt + 1))
    return ""


class MGPParser(HTMLParser):
    """Parse a Mathematics Genealogy Project person page."""

    def __init__(self):
        super().__init__()
        self.name = ""
        self.advisors = []
        self.students = []
        self.student_ids = []
        self.advisor_ids = []
        self._in_h2 = False
        self._in_student_table = False
        self._in_advisor_p = False
        self._capture = None
        self._current_text = ""
        self._current_href = ""

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == "h2":
            self._in_h2 = True
            self._current_text = ""
        if tag == "a":
            self._current_href = d.get("href", "")
            self._current_text = ""

    def handle_data(self, data):
        self._current_text += data
        if self._in_h2:
            self.name += data

    def handle_endtag(self, tag):
        if tag == "h2" and self._in_h2:
            self._in_h2 = False
            self.name = self.name.strip()
        if tag == "a" and self._current_href:
            href = self._current_href
            text = self._current_text.strip()
            if "id=" in href and text:
                m = re.search(r"id=(\d+)", href)
                if m:
                    mgp_id = int(m.group(1))
                    if self._in_advisor_p:
                        self.advisors.append(text)
                        self.advisor_ids.append(mgp_id)
                    elif self._in_student_table:
                        self.students.append(text)
                        self.student_ids.append(mgp_id)
            self._current_href = ""


def scrape_mgp(out_dir, delay=1.0, max_pages=50000, seed_ids=None):
    """Scrape Mathematics Genealogy Project via BFS from seed IDs."""
    print("[mgp] Starting Mathematics Genealogy Project scrape")

    names = []
    name_index = {}
    edges = set()
    mgp_to_idx = {}

    def get_or_add(name, mgp_id=None):
        if mgp_id is not None and mgp_id in mgp_to_idx:
            return mgp_to_idx[mgp_id]
        if name in name_index:
            idx = name_index[name]
            if mgp_id is not None:
                mgp_to_idx[mgp_id] = idx
            return idx
        idx = len(names)
        name_index[name] = idx
        names.append(name)
        if mgp_id is not None:
            mgp_to_idx[mgp_id] = idx
        return idx

    if not seed_ids:
        seed_ids = [
            18231,   # Carl Friedrich Gauss
            7298,    # Leonhard Euler
            143630,  # Andrew Ng
            69303,   # Yoshua Bengio
            117765,  # Geoffrey Hinton
        ]

    visited = set()
    queue = deque(seed_ids)
    page_count = 0

    while queue and page_count < max_pages:
        mgp_id = queue.popleft()
        if mgp_id in visited:
            continue
        visited.add(mgp_id)

        url = f"https://www.genealogy.math.ndsu.nodak.edu/id.php?id={mgp_id}"
        try:
            html = fetch(url, delay=delay)
        except Exception as e:
            print(f"  [mgp] Failed to fetch {mgp_id}: {e}", file=sys.stderr)
            continue

        parser = MGPParser()
        try:
            parser.feed(html)
        except Exception:
            continue

        page_count += 1
        if page_count % 100 == 0:
            print(f"  [mgp] Scraped {page_count} pages, {len(names)} names, {len(edges)} edges")

        if not parser.name:
            continue

        person_idx = get_or_add(parser.name, mgp_id)

        for adv_name, adv_id in zip(parser.advisors, parser.advisor_ids):
            adv_idx = get_or_add(adv_name, adv_id)
            if adv_idx != person_idx:
                edges.add((adv_idx, person_idx))
            if adv_id not in visited:
                queue.append(adv_id)

        for stu_name, stu_id in zip(parser.students, parser.student_ids):
            stu_idx = get_or_add(stu_name, stu_id)
            if person_idx != stu_idx:
                edges.add((person_idx, stu_idx))
            if stu_id not in visited:
                queue.append(stu_id)

    write_output(out_dir, "mgp", names, edges)
    print(f"[mgp] Done: {len(names)} names, {len(edges)} edges from {page_count} pages")


class AcademicTreeParser(HTMLParser):
    """Parse an Academic Family Tree person page."""

    def __init__(self):
        super().__init__()
        self.name = ""
        self.connections = []  # (name, person_id, role)
        self._in_title = False
        self._current_href = ""
        self._current_text = ""
        self._section = ""

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == "title":
            self._in_title = True
            self._current_text = ""
        if tag == "a":
            self._current_href = d.get("href", "")
            self._current_text = ""

    def handle_data(self, data):
        self._current_text += data
        if self._in_title:
            self.name += data
        lower = data.strip().lower()
        if "advisor" in lower or "mentor" in lower:
            self._section = "advisor"
        elif "student" in lower or "trainee" in lower or "mentee" in lower:
            self._section = "student"

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False
            self.name = self.name.split("-")[0].strip() if "-" in self.name else self.name.strip()
        if tag == "a" and self._current_href and self._section:
            href = self._current_href
            text = self._current_text.strip()
            m = re.search(r"p/(\w+)", href)
            if m and text and len(text) > 2:
                pid = m.group(1)
                self.connections.append((text, pid, self._section))
            self._current_href = ""


def scrape_academictree(out_dir, delay=1.5, max_pages=20000, seed_ids=None):
    """Scrape Academic Family Tree via BFS."""
    print("[academictree] Starting Academic Family Tree scrape")

    names = []
    name_index = {}
    edges = set()
    at_to_idx = {}

    def get_or_add(name, at_id=None):
        if at_id and at_id in at_to_idx:
            return at_to_idx[at_id]
        if name in name_index:
            idx = name_index[name]
            if at_id:
                at_to_idx[at_id] = idx
            return idx
        idx = len(names)
        name_index[name] = idx
        names.append(name)
        if at_id:
            at_to_idx[at_id] = idx
        return idx

    if not seed_ids:
        seed_ids = ["geoffrey-hinton", "andrew-ng", "yann-lecun"]

    visited = set()
    queue = deque(seed_ids)
    page_count = 0

    while queue and page_count < max_pages:
        at_id = queue.popleft()
        if at_id in visited:
            continue
        visited.add(at_id)

        url = f"https://academictree.org/p/{at_id}"
        try:
            html = fetch(url, delay=delay)
        except Exception as e:
            print(f"  [academictree] Failed {at_id}: {e}", file=sys.stderr)
            continue

        parser = AcademicTreeParser()
        try:
            parser.feed(html)
        except Exception:
            continue

        page_count += 1
        if page_count % 100 == 0:
            print(f"  [academictree] {page_count} pages, {len(names)} names, {len(edges)} edges")

        if not parser.name:
            continue

        person_idx = get_or_add(parser.name, at_id)

        for conn_name, conn_id, role in parser.connections:
            conn_idx = get_or_add(conn_name, conn_id)
            if role == "advisor" and conn_idx != person_idx:
                edges.add((conn_idx, person_idx))
            elif role == "student" and conn_idx != person_idx:
                edges.add((person_idx, conn_idx))
            if conn_id not in visited:
                queue.append(conn_id)

    write_output(out_dir, "academictree", names, edges)
    print(f"[academictree] Done: {len(names)} names, {len(edges)} edges from {page_count} pages")


def write_output(out_dir, label, names, edges):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    names_path = out_dir / f"{label}.names.json.gz"
    edges_path = out_dir / f"{label}.edges.bin.gz"
    with gzip.open(names_path, "wt", encoding="utf-8") as f:
        json.dump({"n": names}, f, ensure_ascii=False)
    with gzip.open(edges_path, "wb") as f:
        for a, b in edges:
            f.write(struct.pack("<II", a, b))
    print(f"  Output: {names_path}, {edges_path}")


def main():
    ap = argparse.ArgumentParser(description="Scrape academic genealogy databases")
    ap.add_argument("--source", required=True, choices=["mgp", "academictree", "all"],
                    help="Which database to scrape")
    ap.add_argument("--out-dir", default="data/genealogy", help="Output directory")
    ap.add_argument("--delay", type=float, default=1.0, help="Seconds between requests")
    ap.add_argument("--max-pages", type=int, default=50000, help="Max pages to scrape per source")
    ap.add_argument("--seed-ids", nargs="*", help="Starting IDs (source-specific)")
    args = ap.parse_args()

    if args.source in ("mgp", "all"):
        seed = [int(s) for s in args.seed_ids] if args.seed_ids and args.source == "mgp" else None
        scrape_mgp(args.out_dir, delay=args.delay, max_pages=args.max_pages, seed_ids=seed)

    if args.source in ("academictree", "all"):
        seed = args.seed_ids if args.seed_ids and args.source == "academictree" else None
        scrape_academictree(args.out_dir, delay=args.delay, max_pages=args.max_pages, seed_ids=seed)


if __name__ == "__main__":
    main()
