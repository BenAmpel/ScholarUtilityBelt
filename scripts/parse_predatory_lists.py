#!/usr/bin/env python3
"""
Parse and combine predatory venue lists from multiple sources:
- pred_list.mhtml (Beall's List HTML)
- pred_list2.csv (CSV with numbered entries)
- pred_list3.csv (CSV with numbered entries)

Output: src/data/predatory_venues.txt (one venue per line)
"""

import re
import csv
from pathlib import Path
from html import unescape

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "src" / "data"
PRED_LIST_MHTML = ROOT / "pred_list.mhtml"
PRED_LIST2_MHTML = ROOT / "pred_list2.mhtml"
PRED_LIST2_CSV = ROOT / "pred_list2.csv"
PRED_LIST3_CSV = ROOT / "pred_list3.csv"
OUTPUT_FILE_BEALLS = DATA_DIR / "predatory_venues_bealls.txt"
OUTPUT_FILE_PREDATORYJOURNALS = DATA_DIR / "predatory_venues_predatoryjournals.txt"
OUTPUT_FILE = DATA_DIR / "predatory_venues.txt"  # Combined for backward compatibility


def decode_quoted_printable(text):
    """Decode quoted-printable encoding."""
    import quopri
    try:
        return quopri.decodestring(text.encode('utf-8')).decode('utf-8', errors='ignore')
    except:
        return text


def parse_mhtml(mhtml_path):
    """Extract journal/publisher names from MHTML file."""
    venues = set()
    
    print(f"Parsing {mhtml_path.name}...")
    content = mhtml_path.read_text(encoding="utf-8", errors="ignore")
    
    # Handle quoted-printable line breaks (remove = at end of line)
    content = re.sub(r'=\r?\n', '', content)
    
    # Decode HTML entities
    content = unescape(content)
    
    # Decode quoted-printable sequences
    content = content.replace('=3D', '=').replace('=E2=80=99', "'").replace('=E2=80=93', '-')
    content = content.replace('=E2=80=9C', '"').replace('=E2=80=9D', '"')
    
    # Look for list items with links or text containing journal/publisher names
    # Pattern: <li><a href="...">Journal Name</a></li> or <li>Journal Name</li>
    patterns = [
        r'<li><a[^>]*>([^<]+)</a></li>',
        r'<li>([^<]+)</li>',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            # Clean up the text
            text = match.strip()
            # Remove common prefixes/suffixes
            text = re.sub(r'^\d+\.\s*', '', text)  # Remove leading numbers
            text = re.sub(r'\s*\([^)]*\)\s*$', '', text)  # Remove trailing parentheses
            text = text.strip()
            
            # Filter out non-venue entries
            if text and len(text) > 3:
                # Skip if it's clearly not a venue name
                skip_patterns = [
                    r'^https?://',
                    r'^www\.',
                    r'^Skip to',
                    r'^Beall',
                    r'^How to',
                    r'^Standalone',
                    r'^Hijacked',
                    r'^All journals',
                    r'^Use the',
                    r'^Find the',
                    r'^This is an',
                    r'^We will only',
                    r'^Original list',
                    r'^Go to update',
                ]
                if not any(re.match(p, text, re.IGNORECASE) for p in skip_patterns):
                    venues.add(text)
    
    print(f"  Found {len(venues)} venues from MHTML")
    return venues


def parse_csv(csv_path):
    """Extract venue names from CSV file (format: number,name)."""
    venues = set()
    
    print(f"Parsing {csv_path.name}...")
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    # Skip the number, take the name
                    name = row[1].strip()
                    if name and len(name) > 3:
                        venues.add(name)
    except Exception as e:
        print(f"  Error parsing CSV: {e}")
    
    print(f"  Found {len(venues)} venues from CSV")
    return venues


def normalize_venue_name(name):
    """Normalize venue name for deduplication."""
    # Remove common suffixes/prefixes
    name = re.sub(r'\s*\([^)]*\)\s*$', '', name)  # Remove trailing parentheses
    name = re.sub(r'^\d+\.\s*', '', name)  # Remove leading numbers
    name = name.strip()
    return name


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    bealls_venues = set()
    predatoryjournals_venues = set()
    
    # Parse MHTML files (Beall's List)
    if PRED_LIST_MHTML.exists():
        mhtml_venues = parse_mhtml(PRED_LIST_MHTML)
        bealls_venues.update(mhtml_venues)
    else:
        print(f"Warning: {PRED_LIST_MHTML.name} not found")
    
    if PRED_LIST2_MHTML.exists():
        mhtml2_venues = parse_mhtml(PRED_LIST2_MHTML)
        bealls_venues.update(mhtml2_venues)
    else:
        print(f"Warning: {PRED_LIST2_MHTML.name} not found")
    
    # Parse CSV files (PredatoryJournals.org)
    if PRED_LIST2_CSV.exists():
        csv2_venues = parse_csv(PRED_LIST2_CSV)
        predatoryjournals_venues.update(csv2_venues)
    else:
        print(f"Warning: {PRED_LIST2_CSV.name} not found")
    
    if PRED_LIST3_CSV.exists():
        csv3_venues = parse_csv(PRED_LIST3_CSV)
        predatoryjournals_venues.update(csv3_venues)
    else:
        print(f"Warning: {PRED_LIST3_CSV.name} not found")
    
    # Normalize and deduplicate for each source
    def normalize_and_dedup(venues):
        normalized_map = {}
        for venue in venues:
            normalized = normalize_venue_name(venue).lower()
            if normalized and len(normalized) > 3:
                # Keep the longest/most complete version
                if normalized not in normalized_map or len(venue) > len(normalized_map[normalized]):
                    normalized_map[normalized] = venue
        return sorted(set(normalized_map.values()), key=str.lower)
    
    bealls_final = normalize_and_dedup(bealls_venues)
    predatoryjournals_final = normalize_and_dedup(predatoryjournals_venues)
    
    # Write separate output files
    bealls_text = "\n".join(bealls_final) + "\n"
    predatoryjournals_text = "\n".join(predatoryjournals_final) + "\n"
    OUTPUT_FILE_BEALLS.write_text(bealls_text, encoding="utf-8")
    OUTPUT_FILE_PREDATORYJOURNALS.write_text(predatoryjournals_text, encoding="utf-8")
    
    # Also write combined file for backward compatibility
    all_venues = sorted(set(bealls_final + predatoryjournals_final), key=str.lower)
    combined_text = "\n".join(all_venues) + "\n"
    OUTPUT_FILE.write_text(combined_text, encoding="utf-8")
    
    print(f"\nBeall's List: {len(bealls_final)} unique venues")
    print(f"PredatoryJournals.org: {len(predatoryjournals_final)} unique venues")
    print(f"Combined: {len(all_venues)} unique venues")
    print(f"Wrote: {OUTPUT_FILE_BEALLS.relative_to(ROOT)}")
    print(f"Wrote: {OUTPUT_FILE_PREDATORYJOURNALS.relative_to(ROOT)}")
    print(f"Wrote: {OUTPUT_FILE.relative_to(ROOT)}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
