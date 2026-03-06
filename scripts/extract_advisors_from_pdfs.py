#!/usr/bin/env python3
"""
Extract advisor/advisee pairs from OA thesis PDFs.
Input: JSONL with fields {"title","author","pdf_url"} or CSV with pdf_url column.
Output: JSONL edges {"advisor","student","source","pdf_url","title"}.
"""
import argparse
import csv
import json
import re
import tempfile
import urllib.request
from pathlib import Path

import pdfplumber


ADVISOR_PATTERNS = [
    re.compile(r"advisor[:\\s]+(.+)", re.IGNORECASE),
    re.compile(r"supervisor[:\\s]+(.+)", re.IGNORECASE),
    re.compile(r"thesis advisor[:\\s]+(.+)", re.IGNORECASE),
    re.compile(r"dissertation advisor[:\\s]+(.+)", re.IGNORECASE),
    re.compile(r"committee chair[:\\s]+(.+)", re.IGNORECASE),
]


def read_inputs(path):
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)
    elif path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    else:
        raise SystemExit("Unsupported input format (use .jsonl or .csv)")


def extract_advisors_from_text(text):
    advisors = set()
    for pat in ADVISOR_PATTERNS:
        for m in pat.finditer(text):
            val = m.group(1).strip()
            # trim trailing punctuation/lines
            val = re.split(r"[\\r\\n]|\\s{2,}", val)[0].strip(" :;,.")
            if val:
                advisors.add(val)
    return list(advisors)


def fetch_pdf(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ScholarUtilityBelt/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--max-pages", type=int, default=2)
    ap.add_argument("--max-records", type=int, default=None)
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with out_path.open("w", encoding="utf-8") as out:
        for row in read_inputs(in_path):
            if args.max_records and count >= args.max_records:
                break
            pdf_url = (row.get("pdf_url") or row.get("pdf") or "").strip()
            author = (row.get("author") or row.get("student") or "").strip()
            title = (row.get("title") or "").strip()
            if not pdf_url or not author:
                continue
            try:
                data = fetch_pdf(pdf_url)
            except Exception:
                continue
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                tmp.write(data)
                tmp.flush()
                try:
                    with pdfplumber.open(tmp.name) as pdf:
                        text = ""
                        for i in range(min(args.max_pages, len(pdf.pages))):
                            text += (pdf.pages[i].extract_text() or "") + "\n"
                except Exception:
                    continue
            advisors = extract_advisors_from_text(text)
            for adv in advisors:
                out.write(json.dumps({
                    "advisor": adv,
                    "student": author,
                    "title": title,
                    "pdf_url": pdf_url,
                    "source": "oa_pdf"
                }, ensure_ascii=False) + "\n")
            count += 1
    print(f"[pdf] processed={count} out={out_path}")


if __name__ == "__main__":
    main()
