#!/usr/bin/env python3
"""
Collect OA PDF URLs (and minimal metadata) from OAI-PMH sources.
Writes JSONL rows: {title, author, pdf_url, source, identifier}
"""
import argparse
import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


def fetch_xml(url, timeout=60):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ScholarUtilityBelt/1.0 (OAI PDF collector)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return ET.fromstring(data)


def build_url(base, params):
    return base + "?" + urllib.parse.urlencode(params)


def iter_records(base, metadata_prefix, set_specs=None, from_date=None, until_date=None, sleep_s=1.0, max_records=None, timeout=60):
    total = 0
    set_specs = set_specs or [None]
    for set_spec in set_specs:
        token = None
        while True:
            params = {"verb": "ListRecords"}
            if token:
                params = {"verb": "ListRecords", "resumptionToken": token}
            else:
                params["metadataPrefix"] = metadata_prefix
                if set_spec:
                    params["set"] = set_spec
                if from_date:
                    params["from"] = from_date
                if until_date:
                    params["until"] = until_date
            url = build_url(base, params)
            root = fetch_xml(url, timeout=timeout)
            err = root.find(".//{http://www.openarchives.org/OAI/2.0/}error")
            if err is not None:
                code = err.attrib.get("code", "")
                if code in ("cannotDisseminateFormat", "noRecordsMatch"):
                    return
                raise RuntimeError(f"OAI error {code}: {err.text}")
            for rec in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
                yield rec
                total += 1
                if max_records and total >= max_records:
                    return
            token_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
            token = (token_el.text or "").strip() if token_el is not None else ""
            if not token:
                break


def get_texts(el, tag):
    return [t.text.strip() for t in el.findall(f".//{tag}") if t.text and t.text.strip()]


def extract_oai_dc(meta_el):
    ns = "{http://purl.org/dc/elements/1.1/}"
    title = get_texts(meta_el, ns + "title")
    creator = get_texts(meta_el, ns + "creator")
    identifier = get_texts(meta_el, ns + "identifier")
    fmt = get_texts(meta_el, ns + "format")
    return title, creator, identifier, fmt


def find_pdf_urls(identifiers, formats):
    urls = []
    for ident in identifiers or []:
        if ident.lower().endswith(".pdf"):
            urls.append(ident)
    for ident in identifiers or []:
        if "pdf" in ident.lower() and ident.lower().startswith("http"):
            urls.append(ident)
    for fmt in formats or []:
        if "pdf" in fmt.lower():
            pass
    # de-dup
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--metadata-prefix", default="oai_dc")
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--until-date", default=None)
    ap.add_argument("--max-records", type=int, default=500)
    ap.add_argument("--max-sources", type=int, default=None)
    ap.add_argument("--timeout", type=int, default=30)
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    sources = [s for s in cfg.get("sources", []) if s.get("enabled") is not False]
    if args.max_sources:
        sources = sources[: args.max_sources]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8") as out:
        for src in sources:
            base = src["base"]
            label = src.get("key") or src.get("label") or "source"
            try:
                for rec in iter_records(
                    base,
                    args.metadata_prefix,
                    from_date=args.from_date,
                    until_date=args.until_date,
                    max_records=args.max_records,
                    timeout=args.timeout,
                ):
                    meta = rec.find("{http://www.openarchives.org/OAI/2.0/}metadata")
                    if meta is None:
                        continue
                    title, creator, identifier, fmt = extract_oai_dc(meta)
                    pdfs = find_pdf_urls(identifier, fmt)
                    if not pdfs:
                        continue
                    for pdf_url in pdfs:
                        out.write(json.dumps({
                            "title": title[0] if title else "",
                            "author": creator[0] if creator else "",
                            "pdf_url": pdf_url,
                            "identifier": identifier[0] if identifier else "",
                            "source": label
                        }, ensure_ascii=False) + "\n")
                        count += 1
            except Exception:
                continue
    print(f"[pdf-collector] rows={count} out={out_path}")


if __name__ == "__main__":
    main()
