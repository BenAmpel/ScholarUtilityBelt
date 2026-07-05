#!/usr/bin/env python3
"""
Build OAI-PMH source list from ROAR rawlist.xml.
"""
import argparse
import json
import re
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


ROAR_RAWLIST_URL = "http://roar.eprints.org/rawlist.xml"
NS = {"ep": "http://eprints.org/ep2/data/2.0"}


def norm_key(text):
    text = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return text or "roar"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=ROAR_RAWLIST_URL)
    ap.add_argument("--out", default="output/roar_oai_sources.json")
    args = ap.parse_args()

    req = urllib.request.Request(args.url, headers={"User-Agent": "ScholarUtilityBelt/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()

    root = ET.fromstring(data)
    sources = []
    seen = set()
    idx = 0
    for eprint in root.findall("ep:eprint", NS):
        title = (eprint.findtext("ep:title", default="", namespaces=NS) or "").strip()
        oai_el = eprint.find("ep:oai_pmh", NS)
        if oai_el is None:
            continue
        for item in oai_el.findall("ep:item", NS):
            base = (item.text or "").strip()
            if not base or base in seen:
                continue
            seen.add(base)
            idx += 1
            key = f"roar_{idx:05d}"
            label = f"ROAR {title}".strip() if title else f"ROAR {idx}"
            sources.append({
                "key": key,
                "label": label,
                "base": base,
                "metadataPrefixes": ["oai_etdms", "mods", "uketd_dc", "oai_dc"]
            })

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"sources": sources}, indent=2))
    print(f"[roar] sources={len(sources)} out={out_path}")


if __name__ == "__main__":
    main()
