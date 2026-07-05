#!/usr/bin/env python3
"""
Scrape H5 index data from Google Scholar Metrics pages.

This script scrapes H5 index and H5 median values for journals/conferences
from Google Scholar Metrics category pages.

Usage:
    python3 scripts/scrape_scholar_h5.py [--category CATEGORY] [--output OUTPUT_FILE]

Examples:
    # Scrape Computer Science journals
    python3 scripts/scrape_scholar_h5.py --category eng_computerscience
    
    # Scrape all categories (takes a long time)
    python3 scripts/scrape_scholar_h5.py --all-categories
    
    # Scrape specific category and save to custom file
    python3 scripts/scrape_scholar_h5.py --category eng_computerscience --output h5_compsci.json

Design constraints:
- Respects rate limiting (delays between requests)
- Handles Google Scholar's anti-bot measures
- Outputs JSON format compatible with extension's quality index system
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "src" / "data"

# Google Scholar Metrics base URL
SCHOLAR_METRICS_BASE = "https://scholar.google.com/citations"
SCHOLAR_METRICS_VIEW = "view_op=list_venues"

# Common categories (you can add more)
CATEGORIES = {
    "eng_computerscience": "Computer Science",
    "eng_electricalengineering": "Electrical Engineering",
    "eng_chemicalengineering": "Chemical Engineering",
    "eng_mechanicalengineering": "Mechanical Engineering",
    "eng_civilengineering": "Civil Engineering",
    "bus": "Business",
    "med": "Medicine",
    "bio": "Biology",
    "phy": "Physics",
    "chem": "Chemistry",
    "mat": "Mathematics",
    "soc": "Social Sciences",
    "hum": "Humanities",
}

# Rate limiting: delay between requests (seconds)
REQUEST_DELAY = 2.0


def die(msg: str) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(2)


def normalize_venue_name(name: str) -> str:
    """Normalize venue name for consistent matching."""
    return (
        name.lower()
        .replace("&", " and ")
        .replace("/", " ")
        .replace("-", " ")
        .replace("'", "")
        .replace('"', "")
        .replace(".", "")
        .replace(",", "")
        .replace(":", "")
        .replace(";", "")
        .replace("(", "")
        .replace(")", "")
        .replace("[", "")
        .replace("]", "")
        .replace("{", "")
        .replace("}", "")
    )


def parse_h5_from_text(text: str) -> Optional[dict]:
    """
    Parse H5 index and H5 median from text like "h5-index: 123, h5-median: 456"
    or "H5-index: 123 | H5-median: 456"
    """
    h5_index = None
    h5_median = None

    # Try various patterns
    patterns = [
        r"h5[-\s]?index[:\s]+(\d+)",
        r"h5[-\s]?median[:\s]+(\d+)",
        r"h5[:\s]+(\d+)",
    ]

    # Look for h5-index
    for pattern in [
        r"h5[-\s]?index[:\s]+(\d+)",
        r"h5[:\s]+(\d+)\s*[,\|]",
    ]:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            h5_index = int(match.group(1))
            break

    # Look for h5-median
    for pattern in [
        r"h5[-\s]?median[:\s]+(\d+)",
        r"h5[-\s]?median[:\s]+(\d+)",
    ]:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            h5_median = int(match.group(1))
            break

    if h5_index is None and h5_median is None:
        return None

    return {"h5_index": h5_index, "h5_median": h5_median}


def scrape_category(category: str, category_name: str) -> dict:
    """
    Scrape H5 index data from a Google Scholar Metrics category page.
    
    Returns a dict mapping normalized venue names to H5 data.
    """
    url = f"{SCHOLAR_METRICS_BASE}?{SCHOLAR_METRICS_VIEW}&hl=en&vq={category}"
    print(f"[{category_name}] Fetching {url}...", flush=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Referer": "https://scholar.google.com/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}", file=sys.stderr)
        return {}

    soup = BeautifulSoup(response.text, "html.parser")
    venues = {}

    # Google Scholar Metrics pages have tables with venue information
    # Try multiple selectors to find the table rows
    rows = soup.select("tr.gs_md_wa, tr.gs_ml_wa, table.gs_md_wa_table tr, table tr")
    
    for row in rows:
        try:
            # Skip header rows
            if row.select("th"):
                continue

            cells = row.select("td")
            if len(cells) < 2:
                continue

            # First cell usually contains the venue name
            name_cell = cells[0]
            venue_name = name_cell.get_text(strip=True)
            
            # Remove any links or extra formatting
            if not venue_name:
                # Try finding a link in the cell
                link = name_cell.select_one("a")
                if link:
                    venue_name = link.get_text(strip=True)

            if not venue_name or len(venue_name) < 3:
                continue

            # H5 index and median are usually in subsequent cells
            h5_index = None
            h5_median = None

            # Look for numeric values in cells (h5-index and h5-median)
            for i, cell in enumerate(cells[1:], start=1):
                cell_text = cell.get_text(strip=True)
                # Try to extract numbers
                numbers = re.findall(r'\d+', cell_text)
                if numbers:
                    num = int(numbers[0])
                    # Usually h5-index comes before h5-median
                    if h5_index is None:
                        h5_index = num
                    elif h5_median is None:
                        h5_median = num
                        break

            # If we didn't find in cells, try parsing from row text
            if h5_index is None:
                row_text = row.get_text()
                h5_data = parse_h5_from_text(row_text)
                if h5_data:
                    h5_index = h5_data.get("h5_index")
                    h5_median = h5_data.get("h5_median")

            if h5_index:
                normalized = normalize_venue_name(venue_name)
                venues[normalized] = {
                    "name": venue_name,
                    "h5_index": h5_index,
                    "h5_median": h5_median,
                }
                median_str = f", H5-median={h5_median}" if h5_median else ""
                print(f"  Found: {venue_name} (H5-index={h5_index}{median_str})")

        except Exception as e:
            print(f"  Error parsing row: {e}", file=sys.stderr)
            continue

    print(f"  Scraped {len(venues)} venues from {category_name}")
    return venues


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape H5 index data from Google Scholar Metrics"
    )
    parser.add_argument(
        "--category",
        choices=list(CATEGORIES.keys()),
        help="Category to scrape (e.g., eng_computerscience)",
    )
    parser.add_argument(
        "--all-categories",
        action="store_true",
        help="Scrape all available categories (takes a long time)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (default: src/data/scholar_h5_index.json)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help=f"Delay between requests in seconds (default: {REQUEST_DELAY})",
    )

    args = parser.parse_args()

    if not args.category and not args.all_categories:
        parser.print_help()
        return 1

    output_path = args.output or (DATA_DIR / "scholar_h5_index.json")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_venues = {}

    categories_to_scrape = (
        list(CATEGORIES.items()) if args.all_categories else [(args.category, CATEGORIES[args.category])]
    )

    for category, category_name in categories_to_scrape:
        venues = scrape_category(category, category_name)
        all_venues.update(venues)

        # Rate limiting
        if category != categories_to_scrape[-1][0]:
            print(f"  Waiting {args.delay}s before next request...", flush=True)
            time.sleep(args.delay)

    # Save results
    output_data = {
        "meta": {
            "source": "Google Scholar Metrics",
            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "categories": [cat for cat, _ in categories_to_scrape],
            "venue_count": len(all_venues),
        },
        "index": all_venues,
    }

    output_path.write_text(json.dumps(output_data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nSaved {len(all_venues)} venues to {output_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
