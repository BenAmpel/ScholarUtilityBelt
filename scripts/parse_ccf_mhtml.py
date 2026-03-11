#!/usr/bin/env python3
"""
Parse CCF MHTML files (CCF1.mhtml through CCF10.mhtml) and extract
journal/conference names with their tier (A, B, C).

Output: src/data/ccf_ranks.csv (Venue|Alias,Tier) for extension qualityCcfRanks.
"""

import re
import quopri
from pathlib import Path
from html import unescape

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "src" / "data"
CCF_GLOB = "CCF*.mhtml"
OUTPUT_CSV = DATA_DIR / "ccf_ranks.csv"


def get_html_from_mhtml(path: Path) -> str:
    """Extract and decode the main text/html part from an MHTML file."""
    raw = path.read_text(encoding="utf-8", errors="ignore")
    # Remove quoted-printable soft line breaks
    raw = re.sub(r'=\r?\n', '', raw)
    # Find the HTML part (after Content-Type: text/html)
    idx = raw.find("Content-Type: text/html")
    if idx == -1:
        return unescape(raw)
    # Start of payload: after first blank line
    start = raw.find("\n\n", idx)
    if start == -1:
        start = raw.find("\r\n\r\n", idx)
    if start == -1:
        return unescape(raw)
    start += 2
    # End at next boundary or end
    end = raw.find("------MultipartBoundary", start)
    if end == -1:
        end = len(raw)
    payload = raw[start:end]
    # Decode quoted-printable
    try:
        payload = quopri.decodestring(payload.encode("latin-1")).decode("utf-8", errors="replace")
    except Exception:
        payload = payload.replace("=3D", "=").replace("=E2=80=93", "-").replace("=E2=80=99", "'")
    return unescape(payload)


def strip_html(s: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_one_ccf_file(path: Path) -> list[tuple[str, str, str]]:
    """
    Parse one CCF MHTML file. Returns list of (full_name, short_name, tier).
    """
    html = get_html_from_mhtml(path)
    results = []

    # Find each "Category A", "Category B", "Category C" (allowing font tags in between)
    for tier_match in re.finditer(r"Category\s+([ABC])\s*</", html, re.IGNORECASE):
        tier = tier_match.group(1).upper()
        pos = tier_match.end()
        # Find the next <ul class="g-ul x-list3"> after this
        ul_match = re.search(r'<ul\s+[^>]*class="g-ul x-list3"[^>]*>', html[pos:pos + 2000])
        if not ul_match:
            continue
        ul_start = pos + ul_match.start()
        ul_end = html.find("</ul>", ul_start)
        if ul_end == -1:
            continue
        ul_end += 5  # include "</ul>"
        ul_block = html[ul_start:ul_end]

        # Split into <li>...</li> (content only, not the tag)
        li_contents = re.findall(r"<li>\s*(.*?)\s*</li>", ul_block, re.DOTALL)
        for li_html in li_contents:
            # Get all div contents in order (first is serial, second is sname, third is full name)
            divs = re.findall(r"<div[^>]*>(.*?)</div>", li_html, re.DOTALL)
            if len(divs) < 3:
                continue
            serial = strip_html(divs[0])
            if not serial.isdigit():
                continue  # skip header row
            short_name = strip_html(divs[1])
            full_name = strip_html(divs[2])
            # Skip header text
            if "Full name" in full_name or "Publication Name" in full_name or "Serial" in serial and not serial.isdigit():
                continue
            if not full_name or len(full_name) < 2:
                continue
            # Skip if it looks like a URL
            if full_name.startswith("http") or "dblp." in full_name:
                continue
            results.append((full_name, short_name or full_name, tier))

    return results


def parse_all_ccf_files() -> list[tuple[str, str, str]]:
    """Parse all CCF*.mhtml in ROOT."""
    all_entries = []
    for path in sorted(ROOT.glob(CCF_GLOB)):
        if path.name.startswith("CCF") and path.suffix.lower() == ".mhtml":
            try:
                entries = parse_one_ccf_file(path)
                all_entries.extend(entries)
                print(f"  {path.name}: {len(entries)} entries")
            except Exception as e:
                print(f"  {path.name}: error - {e}")
    return all_entries


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Parsing CCF MHTML files...")
    all_entries = parse_all_ccf_files()

    # Deduplicate by full name (keep last tier if same venue in multiple files)
    by_full_name = {}
    for full_name, short_name, tier in all_entries:
        full_name = full_name.strip()
        if not full_name:
            continue
        by_full_name[full_name] = (short_name.strip() or full_name, tier)

    # Build CSV lines: "Full Name|ShortName,Tier" or "Full Name,Tier"
    lines = []
    for full_name, (short_name, tier) in sorted(by_full_name.items(), key=lambda x: (x[1][1], x[0].lower())):
        venue = f"{full_name}|{short_name}" if (short_name and short_name != full_name) else full_name
        line = f'"{venue}",{tier}' if "," in venue else f"{venue},{tier}"
        lines.append(line)

    OUTPUT_CSV.write_text("Venue,Rank\n" + "\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nTotal unique venues: {len(lines)}")
    print(f"Wrote: {OUTPUT_CSV.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
