#!/usr/bin/env python3
"""
Validate OAI-PMH base URLs and keep only sources that respond and expose
ETD-capable metadata formats (oai_etdms / mods / uketd_dc).
"""
import argparse
import concurrent.futures as futures
import json
import random
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


RICH_PREFIXES = ["oai_etdms", "mods", "uketd_dc"]
FALLBACK_PREFIXES = ["oai_dc", "qdc"]


def build_url(base, params):
    return base + "?" + urllib.parse.urlencode(params)


def fetch_xml(url, timeout=15):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ScholarUtilityBelt/1.0 (OAI validator)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return ET.fromstring(data)


def list_metadata_formats(base, timeout=15):
    root = fetch_xml(build_url(base, {"verb": "ListMetadataFormats"}), timeout=timeout)
    formats = []
    for fmt in root.findall(".//{http://www.openarchives.org/OAI/2.0/}metadataFormat"):
        prefix = fmt.findtext("{http://www.openarchives.org/OAI/2.0/}metadataPrefix")
        if prefix:
            formats.append(prefix.strip())
    return formats


def identify(base, timeout=15):
    root = fetch_xml(build_url(base, {"verb": "Identify"}), timeout=timeout)
    repo = root.findtext(".//{http://www.openarchives.org/OAI/2.0/}repositoryName")
    return repo or ""


def list_identifiers(base, prefix, timeout=15):
    root = fetch_xml(build_url(base, {"verb": "ListIdentifiers", "metadataPrefix": prefix}), timeout=timeout)
    err = root.find(".//{http://www.openarchives.org/OAI/2.0/}error")
    if err is not None:
        code = err.attrib.get("code", "")
        if code == "noRecordsMatch":
            return True
        return False
    return True


def normalize_base(base):
    if not base:
        return ""
    base = base.strip()
    base = base.split("#", 1)[0]
    base = base.split("?", 1)[0]
    if not re.match(r"^https?://", base, re.I):
        base = "https://" + base
    return base.rstrip("/")


def candidate_bases(base):
    base = normalize_base(base)
    if not base:
        return []
    candidates = []

    def add(u):
        if u and u not in candidates:
            candidates.append(u)

    add(base)
    lower = base.lower()
    parsed = urllib.parse.urlparse(base)
    root = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else base

    if "oai" in lower:
        if re.search(r"/oai(?:[-_]?pmh)?/?$", lower):
            add(base + "/request")
        if re.search(r"/oai2/?$", lower):
            add(base + "/request")
        if re.search(r"/oai/request$", lower):
            add(base.rsplit("/request", 1)[0])
    else:
        for suffix in ["/oai/request", "/oai", "/oai-pmh", "/oai2", "/cgi/oai2", "/oai2d", "/do/oai"]:
            add(root + suffix)
        if base != root:
            for suffix in ["/oai/request", "/oai", "/oai2", "/cgi/oai2", "/oai2d", "/do/oai"]:
                add(base + suffix)
    return candidates


def validate_source(src, timeout=15, require_rich=True):
    base = src.get("base")
    if not base:
        return None, "missing base"
    last_err = "unknown error"
    for cand in candidate_bases(base):
        try:
            identify(cand, timeout=timeout)
        except Exception as e:
            last_err = f"identify failed: {e}"
            continue
        try:
            formats = list_metadata_formats(cand, timeout=timeout)
        except Exception as e:
            last_err = f"ListMetadataFormats failed: {e}"
            continue
        formats_lower = [f.lower() for f in formats]
        rich = [p for p in RICH_PREFIXES if p in formats_lower]
        fallback = [p for p in FALLBACK_PREFIXES if p in formats_lower]
        if require_rich and not rich:
            last_err = "no rich prefixes"
            continue
        test_prefix = (rich or fallback)[0] if (rich or fallback) else None
        if not test_prefix:
            last_err = "no usable prefixes"
            continue
        try:
            ok = list_identifiers(cand, test_prefix, timeout=timeout)
            if not ok:
                last_err = f"ListIdentifiers failed for {test_prefix}"
                continue
        except Exception as e:
            last_err = f"ListIdentifiers error: {e}"
            continue
        keep_prefixes = rich + fallback if not require_rich else rich
        cleaned = dict(src)
        cleaned["base"] = cand
        if normalize_base(base) != cand:
            cleaned["originalBase"] = base
        cleaned["metadataPrefixes"] = keep_prefixes
        cleaned.pop("setRegex", None)
        return cleaned, None
    return None, last_err


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--timeout", type=int, default=15)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--allow-fallback", action="store_true")
    ap.add_argument("--errors-output", default=None)
    ap.add_argument("--max-sources", type=int, default=None)
    ap.add_argument("--shuffle", action="store_true")
    args = ap.parse_args()

    src = json.loads(Path(args.input).read_text())
    sources = src.get("sources", [])
    if args.shuffle:
        random.shuffle(sources)
    if args.max_sources:
        sources = sources[: args.max_sources]
    kept = []
    errors = []

    def worker(s):
        cleaned, err = validate_source(s, timeout=args.timeout, require_rich=not args.allow_fallback)
        if cleaned:
            return cleaned, None
        return None, {"base": s.get("base"), "reason": err}

    with futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for cleaned, err in ex.map(worker, sources):
            if cleaned:
                kept.append(cleaned)
            else:
                errors.append(err)

    out = {"sources": kept}
    Path(args.output).write_text(json.dumps(out, indent=2))
    print(f"[validate] in={len(sources)} kept={len(kept)} dropped={len(errors)}")
    print(f"[validate] out={args.output}")
    if args.errors_output:
        Path(args.errors_output).write_text(json.dumps(errors, indent=2))
        print(f"[validate] errors={args.errors_output}")


if __name__ == "__main__":
    main()
