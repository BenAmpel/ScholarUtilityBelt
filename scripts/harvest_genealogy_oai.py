#!/usr/bin/env python3
"""
Harvest mentor/mentee edges from OAI-PMH sources (direct inference only).
Only creates edges when advisor/supervisor roles are explicitly present in metadata.
"""
import argparse
import concurrent.futures as futures
import gzip
import json
import re
import struct
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


ROLE_OK = {"advisor", "supervisor", "thesis advisor", "thesis_advisor", "dissertation advisor"}


def localname(tag):
    return tag.split("}", 1)[-1].lower()


def norm_space(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_name(name):
    name = norm_space(name).lower()
    name = re.sub(r"\b(jr|sr|ii|iii|iv)\b\.?", "", name).strip()
    return name


def sanitize_xml(text):
    # Strip invalid XML 1.0 control characters.
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x84\x86-\x9F]", "", text)


def fetch_xml(url, timeout=60, retries=2):
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "ScholarUtilityBelt/1.0 (OAI-PMH harvester)"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            try:
                text = data.decode("utf-8", errors="replace")
            except Exception:
                text = data.decode("latin-1", errors="replace")
            text = sanitize_xml(text)
            return ET.fromstring(text)
        except Exception as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(1.5 * (attempt + 1))
    raise last_err


def build_url(base, params):
    return base + "?" + urllib.parse.urlencode(params)


def list_sets(base, sleep_s=0.5, limit=None, timeout=60):
    sets = []
    token = None
    while True:
        params = {"verb": "ListSets"}
        if token:
            params = {"verb": "ListSets", "resumptionToken": token}
        url = build_url(base, params)
        root = fetch_xml(url, timeout=timeout)
        err = root.find(".//{http://www.openarchives.org/OAI/2.0/}error")
        if err is not None:
            code = err.attrib.get("code", "")
            if code in ("noSetHierarchy", "badArgument"):
                return sets
            raise RuntimeError(f"OAI error {code}: {err.text}")
        for s in root.findall(".//{http://www.openarchives.org/OAI/2.0/}set"):
            spec = s.findtext("{http://www.openarchives.org/OAI/2.0/}setSpec")
            name = s.findtext("{http://www.openarchives.org/OAI/2.0/}setName")
            if spec:
                sets.append({"spec": spec, "name": name or ""})
            if limit and len(sets) >= limit:
                return sets
        token_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        token = norm_space(token_el.text) if token_el is not None else ""
        if not token:
            break
        time.sleep(sleep_s)
    return sets


def iter_records(base, metadata_prefixes, set_specs=None, from_date=None, until_date=None, sleep_s=1.0, max_records=None, timeout=60):
    total = 0
    set_specs = set_specs or [None]
    for prefix in metadata_prefixes:
        got_any_prefix = False
        for set_spec in set_specs:
            token = None
            got_any = False
            while True:
                params = {"verb": "ListRecords"}
                if token:
                    params = {"verb": "ListRecords", "resumptionToken": token}
                else:
                    params["metadataPrefix"] = prefix
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
                    if code == "cannotDisseminateFormat":
                        break
                    if code == "noRecordsMatch":
                        break
                    raise RuntimeError(f"OAI error {code}: {err.text}")
                for rec in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
                    got_any = True
                    got_any_prefix = True
                    yield rec, prefix
                    total += 1
                    if max_records and total >= max_records:
                        return
                token_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
                token = norm_space(token_el.text) if token_el is not None else ""
                if not token:
                    break
                time.sleep(sleep_s)
            if max_records and total >= max_records:
                return
        if got_any_prefix:
            return


def extract_from_datacite(meta_el):
    creators = []
    advisors = []
    # Find the embedded datacite resource payload
    for creator in meta_el.findall(".//{*}creator"):
        name = creator.findtext(".//{*}creatorName") or creator.findtext(".//{*}name")
        if name:
            creators.append(norm_space(name))
    for contrib in meta_el.findall(".//{*}contributor"):
        ctype = (contrib.attrib.get("contributorType") or "").strip().lower()
        name = contrib.findtext(".//{*}contributorName") or contrib.findtext(".//{*}name")
        if not name:
            continue
        if ctype in ROLE_OK:
            advisors.append(norm_space(name))
    return creators, advisors


def extract_from_oai_dc(meta_el):
    creators = []
    advisors = []
    for el in meta_el.iter():
        lname = localname(el.tag)
        text = norm_space(el.text)
        role = norm_space(el.attrib.get("role") or el.attrib.get("contributorType") or "")
        role_low = role.lower()
        if not text:
            continue
        if lname == "creator":
            creators.append(text)
        if "advisor" in lname or "supervisor" in lname:
            advisors.append(text)
        if lname == "contributor":
            if role_low in ROLE_OK or role_low.startswith("advisor") or role_low.startswith("supervisor"):
                advisors.append(text)
            else:
                low = text.lower()
                if low.startswith("advisor") or low.startswith("supervisor"):
                    advisors.append(text.split(":", 1)[-1].strip() or text)
    return creators, advisors


def extract_from_etdms(meta_el):
    creators = []
    advisors = []
    for el in meta_el.iter():
        lname = localname(el.tag)
        text = norm_space(el.text)
        role = norm_space(el.attrib.get("role") or el.attrib.get("contributorType") or "")
        role_low = role.lower()
        if not text:
            continue
        if lname in ("creator", "author"):
            creators.append(text)
        if "advisor" in lname or "supervisor" in lname:
            advisors.append(text)
        if lname in ("contributor", "advisor", "supervisor"):
            if role_low in ROLE_OK or role_low.startswith("advisor") or role_low.startswith("supervisor"):
                advisors.append(text)
            else:
                low = text.lower()
                if low.startswith("advisor") or low.startswith("supervisor"):
                    advisors.append(text.split(":", 1)[-1].strip() or text)
    return creators, advisors


def extract_from_mods(meta_el):
    creators = []
    advisors = []
    for name_el in meta_el.findall(".//{*}name"):
        role_terms = [norm_space(rt.text).lower() for rt in name_el.findall(".//{*}roleTerm") if rt.text]
        role_is_advisor = any("advisor" in rt or "supervisor" in rt for rt in role_terms)
        parts = [norm_space(p.text) for p in name_el.findall(".//{*}namePart") if p.text]
        full = norm_space(" ".join(parts)) if parts else ""
        if not full:
            full = norm_space(name_el.findtext(".//{*}displayForm") or "")
        if not full:
            continue
        if role_is_advisor:
            advisors.append(full)
        else:
            creators.append(full)
    return creators, advisors


def parse_record(rec, prefix):
    header = rec.find("{http://www.openarchives.org/OAI/2.0/}header")
    ident = header.findtext("{http://www.openarchives.org/OAI/2.0/}identifier") if header is not None else ""
    meta = rec.find("{http://www.openarchives.org/OAI/2.0/}metadata")
    if meta is None:
        return None, False
    if prefix == "oai_datacite":
        creators, advisors = extract_from_datacite(meta)
    elif prefix == "oai_etdms":
        creators, advisors = extract_from_etdms(meta)
    elif prefix == "mods":
        creators, advisors = extract_from_mods(meta)
    else:
        creators, advisors = extract_from_oai_dc(meta)
    creators = [c for c in creators if c]
    advisors = [a for a in advisors if a]
    has_advisor = bool(advisors)
    if not creators or not advisors:
        return None, has_advisor
    return {
        "identifier": ident,
        "creators": creators,
        "advisors": advisors,
    }, has_advisor


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="scripts/genealogy_sources.json")
    ap.add_argument("--out", default="output/oai_genealogy")
    ap.add_argument("--max-records", type=int, default=500)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--until-date", default=None)
    ap.add_argument("--set-regex", default=None)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--max-no-advisor", type=int, default=200)
    ap.add_argument("--workers", type=int, default=1)
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "oai_genealogy.harvest.log"
    log_file = open(log_path, "w", encoding="utf-8")
    log_lock = threading.Lock()

    def log(msg):
        with log_lock:
            print(msg, flush=True)
            log_file.write(msg + "\n")
            log_file.flush()

    canonical = []
    key_to_id = {}
    edges = []
    provenance_count = 0
    data_lock = threading.Lock()
    prov_lock = threading.Lock()
    prov_path = out_dir / "oai_genealogy.provenance.jsonl"
    prov_file = open(prov_path, "w", encoding="utf-8")

    def get_id_unsafe(name):
        key = normalize_name(name)
        if key not in key_to_id:
            key_to_id[key] = len(canonical)
            canonical.append(name)
        return key_to_id[key]

    def add_edge_unsafe(advisor, student):
        aid = get_id_unsafe(advisor)
        sid = get_id_unsafe(student)
        if aid == sid:
            return False
        edges.append((aid, sid))
        return True

    def harvest_source(src):
        nonlocal provenance_count
        if src.get("enabled") is False:
            return
        base = src["base"]
        prefixes = src.get("metadataPrefixes") or [src.get("metadataPrefix", "oai_dc")]
        # If richer formats exist, skip oai_dc/qdc to reduce noise.
        rich_prefixes = [p for p in prefixes if p in ("oai_etdms", "mods", "uketd_dc")]
        if rich_prefixes:
            prefixes = [p for p in prefixes if p in rich_prefixes or p not in ("oai_dc", "qdc")]
        prefixes = list(dict.fromkeys(prefixes))
        set_spec = src.get("set")
        set_specs = None
        if set_spec:
            set_specs = [set_spec]
        else:
            regex = src.get("setRegex") or args.set_regex
            if regex:
                try:
                    all_sets = list_sets(base, sleep_s=args.sleep, limit=2000, timeout=args.timeout)
                    pat = re.compile(regex, re.IGNORECASE)
                    set_specs = [s["spec"] for s in all_sets if pat.search(s["spec"]) or pat.search(s["name"] or "")]
                    if not set_specs:
                        log(f"[oai] {src.get('key')} no set match for regex; skipping source")
                        return
                except Exception:
                    log(f"[oai] {src.get('key')} set listing failed; skipping source")
                    return
        log(f"[oai] {src.get('label','source')} {base} ({','.join(prefixes)})")
        try:
            source_count = 0
            record_count = 0
            advisor_hit_count = 0
            used_prefixes = set()
            early_stop = False
            pending_edges = []
            pending_prov = []

            def flush_edges():
                nonlocal pending_edges, source_count
                if not pending_edges:
                    return
                with data_lock:
                    for advisor, student in pending_edges:
                        if add_edge_unsafe(advisor, student):
                            source_count += 1
                            if source_count == 1:
                                log(f"[oai] {src.get('key')} first edge (prefix={used_prefix})")
                            if source_count % 100 == 0:
                                log(f"[oai] {src.get('key')} edges={source_count}")
                pending_edges = []

            def flush_prov():
                nonlocal pending_prov, provenance_count
                if not pending_prov:
                    return
                with prov_lock:
                    for prov in pending_prov:
                        prov_file.write(json.dumps(prov, ensure_ascii=False) + "\n")
                    provenance_count += len(pending_prov)
                    if provenance_count % 100 == 0:
                        prov_file.flush()
                pending_prov = []

            for rec, used_prefix in iter_records(
                base,
                prefixes,
                set_specs=set_specs,
                from_date=args.from_date,
                until_date=args.until_date,
                sleep_s=args.sleep,
                max_records=args.max_records,
                timeout=args.timeout,
            ):
                record_count += 1
                used_prefixes.add(used_prefix)
                parsed, has_advisor = parse_record(rec, used_prefix)
                if has_advisor:
                    advisor_hit_count += 1
                if not parsed:
                    if args.max_no_advisor and record_count >= args.max_no_advisor and advisor_hit_count == 0:
                        log(f"[oai] {src.get('key')} no advisor fields in first {record_count} records; skipping rest")
                        early_stop = True
                        break
                    continue
                for student in parsed["creators"]:
                    for advisor in parsed["advisors"]:
                        pending_edges.append((advisor, student))
                        pending_prov.append({
                            "source": src.get("key"),
                            "identifier": parsed["identifier"],
                            "advisor": advisor,
                            "student": student,
                            "metadataPrefix": used_prefix,
                        })
                if len(pending_edges) >= 200:
                    flush_edges()
                if len(pending_prov) >= 200:
                    flush_prov()
                if args.max_no_advisor and record_count >= args.max_no_advisor and advisor_hit_count == 0:
                    log(f"[oai] {src.get('key')} no advisor fields in first {record_count} records; skipping rest")
                    early_stop = True
                    break
            flush_edges()
            flush_prov()
            prefix_list = ",".join(sorted(used_prefixes)) if used_prefixes else ",".join(prefixes)
            log(f"[oai] {src.get('key')} done edges={source_count} records={record_count} advisors={advisor_hit_count} prefixes={prefix_list}{' early_stop' if early_stop else ''}")
        except Exception as e:
            err_msg = f"[oai] source failed: {src.get('key')} - {e}"
            print(err_msg, file=sys.stderr, flush=True)
            log(err_msg)

    sources = [s for s in cfg.get("sources", []) if s.get("enabled") is not False]
    if args.workers <= 1:
        for src in sources:
            harvest_source(src)
    else:
        with futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(ex.map(harvest_source, sources))

    # write names
    names_json = {"n": canonical}
    (out_dir / "oai_genealogy.names.json").write_text(json.dumps(names_json, ensure_ascii=False))
    with gzip.open(out_dir / "oai_genealogy.names.json.gz", "wt", encoding="utf-8") as f:
        json.dump(names_json, f, ensure_ascii=False)
    # write edges
    edge_path = out_dir / "oai_genealogy.edges.bin"
    with open(edge_path, "wb") as f:
        for a, b in edges:
            f.write(struct.pack("<II", a, b))
    with gzip.open(out_dir / "oai_genealogy.edges.bin.gz", "wb") as f:
        f.write(edge_path.read_bytes())
    # provenance
    prov_file.flush()
    prov_file.close()

    log(f"[oai] names={len(canonical)} edges={len(edges)} provenance={provenance_count}")
    log(f"[oai] out={out_dir}")
    log_file.close()


if __name__ == "__main__":
    main()
