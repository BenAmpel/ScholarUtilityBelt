#!/usr/bin/env python3
"""
Sync journal metrics from Clarivate's Web of Science Journals API (wos-journals/v1).

This script is designed to:
- fetch all journals for a given JCR year via the paginated /journals endpoint
- (optionally) fetch per-journal reports for a given year to get the "full suite" of metrics
- write a compact local index keyed by normalized journal title for use by the Chrome extension

Auth:
- Provide your API key via env var: CLARIVATE_API_KEY (header X-ApiKey).

Notes:
- Respect Clarivate's license/terms and your rate limits/quota.
- This script does not interact with Google Scholar.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import requests


BASE_URL = "https://api.clarivate.com/apis/wos-journals/v1"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_venue_name(s: str) -> str:
    s = str(s or "").lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def qnorm(x: Any) -> str:
    s = str(x or "").strip().upper()
    m = re.search(r"\bQ([1-4])\b", s)
    return f"Q{m.group(1)}" if m else ""


def qbest(a: Any, b: Any) -> str:
    qa, qb = qnorm(a), qnorm(b)
    if not qa:
        return qb
    if not qb:
        return qa
    return qa if int(qa[1]) <= int(qb[1]) else qb


def fnum(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def make_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "X-ApiKey": api_key,
            "Accept": "application/json",
            "User-Agent": "Scholar-Utility-Belt/clarivate-sync",
        }
    )
    return s


def req_json(sess: requests.Session, url: str, *, params: Optional[dict] = None, timeout_s: int = 60) -> dict:
    r = sess.get(url, params=params, timeout=timeout_s)
    if r.status_code == 429:
        raise RuntimeError("HTTP 429 rate limited")
    r.raise_for_status()
    return r.json()


def fetch_all_journals(sess: requests.Session, *, jcr_year: int, limit: int, edition: Optional[str]) -> list[dict]:
    out: list[dict] = []
    page = 1
    total = None

    while True:
        params: dict[str, Any] = {"jcrYear": jcr_year, "page": page, "limit": limit}
        if edition:
            params["edition"] = edition

        data = req_json(sess, f"{BASE_URL}/journals", params=params)
        meta = data.get("metadata") or {}
        hits = data.get("hits") or []

        if total is None:
            total = meta.get("total")

        out.extend(hits)
        if not hits:
            break

        # Stop when we've reached the total or the last page.
        if isinstance(total, int) and len(out) >= total:
            break
        if len(hits) < limit:
            break

        page += 1

    return out


def extract_best_quartiles_from_ranks(ranks: dict) -> dict:
    # ranks can include arrays for: jif, jci, articleInfluence, etc.
    # Each item has a 'quartile' plus a category/edition.
    out = {}

    def consume(arr, key):
        best = ""
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                best = qbest(best, it.get("quartile"))
        if best:
            out[key] = best

    consume(ranks.get("jif"), "jifQ")
    consume(ranks.get("jci"), "jciQ")
    consume(ranks.get("articleInfluence"), "aisQ")
    consume(ranks.get("immediacyIndex"), "immediacyQ")
    consume(ranks.get("eigenFactorScore"), "eigenQ")
    consume(ranks.get("esiCitations"), "esiQ")

    return out


def compact_from_report(report: dict) -> dict:
    metrics = report.get("metrics") or {}
    impact = metrics.get("impactMetrics") or {}
    influence = metrics.get("influenceMetrics") or {}
    source = metrics.get("sourceMetrics") or {}
    ranks = report.get("ranks") or {}

    compact: dict[str, Any] = {}

    # Key metrics.
    compact["totalCites"] = fnum(impact.get("totalCites"))
    compact["jif"] = fnum(impact.get("jif"))
    compact["jifWithoutSelfCitations"] = fnum(impact.get("jifWithoutSelfCitations"))
    compact["jif5Years"] = fnum(impact.get("jif5Years"))
    compact["immediacyIndex"] = fnum(impact.get("immediacyIndex"))
    compact["jci"] = fnum(impact.get("jci"))

    compact["eigenFactor"] = fnum(influence.get("eigenFactor"))
    compact["articleInfluence"] = fnum(influence.get("articleInfluence"))

    compact["jifPercentile"] = fnum(source.get("jifPercentile"))
    half = source.get("halfLife") or {}
    if isinstance(half, dict):
        compact["citedHalfLife"] = fnum(half.get("cited"))
        compact["citingHalfLife"] = fnum(half.get("citing"))

    # Best quartiles derived from rank arrays.
    compact.update(extract_best_quartiles_from_ranks(ranks))

    # Remove nulls to shrink.
    compact = {k: v for k, v in compact.items() if v is not None and v != ""}
    return compact


def fetch_report_one(
    sess: requests.Session,
    *,
    jid: str,
    year: int,
    sleep_s: float,
    retries: int,
) -> Tuple[str, Optional[dict], Optional[str]]:
    url = f"{BASE_URL}/journals/{jid}/reports/year/{year}"
    last_err = None
    for attempt in range(retries + 1):
        try:
            data = req_json(sess, url, timeout_s=60)
            if sleep_s:
                time.sleep(sleep_s)
            return jid, data, None
        except Exception as e:
            last_err = str(e)
            # Basic backoff.
            time.sleep(min(10.0, 0.8 * (attempt + 1)))
    return jid, None, last_err


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="JCR year to fetch (e.g. 2024)")
    ap.add_argument("--edition", type=str, default="", help="Optional: SCIE|SSCI|AHCI|ESCI")
    ap.add_argument("--limit", type=int, default=100, help="Page size for /journals (default 100)")
    ap.add_argument(
        "--with-reports",
        action="store_true",
        help="Fetch per-journal /reports/year/{year} for full metrics (many API calls).",
    )
    ap.add_argument("--workers", type=int, default=3, help="Concurrent report fetches (default 3)")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between report calls per worker")
    ap.add_argument("--retries", type=int, default=2, help="Retries per journal report call (default 2)")
    ap.add_argument(
        "--out",
        default="output/clarivate",
        help="Output directory (default output/clarivate)",
    )
    args = ap.parse_args()

    api_key = os.environ.get("CLARIVATE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing env var CLARIVATE_API_KEY")

    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    sess = make_session(api_key)

    # Record last updated if available.
    last_updated = None
    try:
        lu = req_json(sess, f"{BASE_URL}/last-updated", timeout_s=30)
        last_updated = lu
    except Exception:
        pass

    edition = args.edition.strip().upper() or None
    if edition and edition not in {"SCIE", "SSCI", "AHCI", "ESCI"}:
        raise SystemExit("Invalid --edition (expected SCIE|SSCI|AHCI|ESCI)")

    journals = fetch_all_journals(sess, jcr_year=args.year, limit=args.limit, edition=edition)
    list_path = out_dir / f"journals_{args.year}{('_'+edition) if edition else ''}.json"
    list_path.write_text(json.dumps(journals, indent=2), encoding="utf-8")

    index: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    if args.with_reports:
        ids = [j.get("id") for j in journals if isinstance(j, dict) and j.get("id")]
        total = len(ids)
        print(f"Fetching {total} journal reports for year={args.year} ...")

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futs = [
                ex.submit(
                    fetch_report_one,
                    sess,
                    jid=str(jid),
                    year=args.year,
                    sleep_s=float(args.sleep),
                    retries=int(args.retries),
                )
                for jid in ids
            ]
            done = 0
            for fut in as_completed(futs):
                jid, report, err = fut.result()
                done += 1
                if done % 250 == 0:
                    print(f"  {done}/{total}")
                if err or not report:
                    errors[jid] = err or "unknown"
                    continue
                name = ((report.get("journal") or {}).get("name") or "").strip()
                if not name:
                    continue
                key = normalize_venue_name(name)
                if not key:
                    continue
                index[key] = compact_from_report(report)

        # Write errors separately to allow resume/manual debugging.
        if errors:
            (out_dir / f"errors_{args.year}.json").write_text(
                json.dumps(errors, indent=2, sort_keys=True), encoding="utf-8"
            )

    meta = {
        "source": "Clarivate Web of Science Journals API (wos-journals/v1)",
        "year": args.year,
        "edition": edition,
        "fetchedAt": now_iso(),
        "lastUpdated": last_updated,
        "journalCount": len(journals),
        "indexedCount": len(index),
        "withReports": bool(args.with_reports),
    }

    compact_path = out_dir / f"compact_index_{args.year}{('_'+edition) if edition else ''}.json"
    compact_path.write_text(json.dumps({"meta": meta, "index": index}, indent=2, sort_keys=True), encoding="utf-8")

    print("Wrote:")
    print(" -", list_path)
    print(" -", compact_path)
    if errors:
        print(" -", out_dir / f"errors_{args.year}.json")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

