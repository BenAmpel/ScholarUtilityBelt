#!/usr/bin/env python3
"""
Discover OAI-PMH base URLs from the COAR IRD directory (CC0) and
generate a harvester config with likely ETD-capable metadata prefixes.
"""
import argparse
import json
import re
import time
import urllib.request
from pathlib import Path
from html.parser import HTMLParser


IRD_BASE = "https://ird.coar-repositories.org"


class LinkCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k == "href":
                href = v
                break
        if href:
            self.links.append(href)


def fetch(url, timeout=60):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (ScholarUtilityBelt IRD harvester)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def parse_detail(html):
    base_url = None
    formats = []
    oai_online = "OAI-PMH online" in html
    # Base URL
    m = re.search(r'OAI-PMH Base URL</h5>\s*<div class="property-value">([^<]+)</div>', html)
    if m:
        base_url = m.group(1).strip()
    # Metadata formats list
    for fmt in re.findall(r"<li>\s*([^<]+)\s*</li>", html):
        f = re.sub(r"\\s+", " ", fmt).strip()
        if f in ("ETDMS 1.0", "ETDMS 1.1", "UKETD_DC", "MODS 3", "MODS"):
            formats.append(f)
    return base_url, formats, oai_online


def prefixes_for_formats(formats):
    prefixes = ["oai_dc"]
    fmt = " ".join(formats).lower()
    if "etdms" in fmt:
        prefixes.insert(0, "oai_etdms")
    if "uketd" in fmt:
        prefixes.insert(0, "uketd_dc")
    if "mods" in fmt:
        prefixes.insert(0, "mods")
    return list(dict.fromkeys(prefixes))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="output/ird_oai_sources.json")
    ap.add_argument("--max-pages", type=int, default=377)
    ap.add_argument("--max-records", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--timeout", type=int, default=20)
    args = ap.parse_args()

    system_links = []
    for page in range(1, args.max_pages + 1):
        url = f"{IRD_BASE}/browser?lang=en&page={page}"
        html = fetch(url, timeout=args.timeout)
        parser = LinkCollector()
        parser.feed(html)
        for href in parser.links:
            if href.startswith("/systems/"):
                system_links.append(IRD_BASE + href)
        if page % 25 == 0:
            print(f"[ird] scanned pages={page} systems={len(system_links)}")
        time.sleep(args.sleep)
    system_links = list(dict.fromkeys(system_links))
    if args.max_records:
        system_links = system_links[: args.max_records]

    sources = []
    for idx, link in enumerate(system_links, 1):
        try:
            html = fetch(link, timeout=args.timeout)
            base_url, formats, online = parse_detail(html)
            if not base_url:
                continue
            if not online:
                continue
            prefixes = prefixes_for_formats(formats)
            key = "ird_" + link.rsplit("/", 1)[-1]
            sources.append({
                "key": key,
                "label": f"IRD {key}",
                "base": base_url,
                "metadataPrefixes": prefixes
            })
        except Exception:
            continue
        if idx % 50 == 0:
            time.sleep(args.sleep)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"sources": sources}, indent=2))
    print(f"[ird] sources={len(sources)} out={out_path}")


if __name__ == "__main__":
    main()
