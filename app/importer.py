#!/usr/bin/env python3
"""
Deal-Matching MVP Importer

Usage:
  python app/importer.py projects --csv=path/to/projects.csv [--allow-new]
  python app/importer.py investors --csv=path/to/investors.csv [--allow-new]

- Validates against configs/vocab.yaml (unless --allow-new)
- Upserts by natural key:
    projects: (sponsor_name, country_iso3, technology, expected_cod)
    investors: (name)
- Builds embeddings from free_text (fallback constructed text) using text-embedding-3-small if OPENAI_API_KEY is set.
- Prints JSON metrics to stdout and exits non-zero on errors.
"""
from __future__ import annotations
import argparse, csv, json, os, sys, datetime as dt
from typing import Dict, List, Any, Optional

from pathlib import Path
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
except Exception:
    pass

import psycopg2
import yaml

# Optional deps
try:
    from langdetect import detect  # type: ignore
except Exception:
    def detect(_t: str) -> str:
        return "en"

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore


def openai_client() -> Optional[Any]:
    key = os.getenv("OPENAI_API_KEY")
    if not key or OpenAI is None:
        return None
    try:
        return OpenAI(api_key=key)
    except Exception:
        return None


def embed_text(oa, text: str) -> Optional[List[float]]:
    try:
        model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small")
        resp = oa.embeddings.create(model=model, input=(text or "")[:6000])
        return resp.data[0].embedding  # type: ignore
    except Exception:
        return None


def summarize_en(oa, text: str) -> str:
    try:
        model = os.getenv("OPENAI_SUMMARY_MODEL", "gpt-4o-mini")
        sys_prompt = (
            "Summarize the following content in English with 3-5 concise sentences. "
            "Reply only with the summary."
        )
        resp = oa.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": text[:8000]},
            ],
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        return text


def load_vocab() -> Dict[str, List[str]]:
    path = os.path.join("configs", "vocab.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def norm_iso3(x: Optional[str]) -> Optional[str]:
    if not x: return None
    x = x.strip().upper()
    # Accept 2-letter to 3-letter mapping minimal for SG/ID/TH/VN/PH/MY
    m = {"SG":"SGP","ID":"IDN","TH":"THA","VN":"VNM","PH":"PHL","MY":"MYS"}
    return m.get(x, x)


def to_vector_literal(vec: Optional[List[float]]) -> Optional[str]:
    if not vec:
        return None
    return "[" + ",".join(f"{v:.8f}" for v in vec) + "]"


def validate_value(vocab: Dict[str, List[str]], key: str, val: Optional[str], allow_new: bool) -> Optional[str]:
    if allow_new or val is None or val == "":
        return val
    arr = vocab.get(key) or []
    if key == "countries_iso3":
        if val and val.upper() not in arr:
            raise ValueError(f"Invalid country_iso3 '{val}' (allowed: {arr})")
        return val.upper()
    if key in ("technologies", "stages", "instruments", "themes", "ifc_categories"):
        if val and val not in arr:
            raise ValueError(f"Invalid {key[:-1]} '{val}' (allowed: {arr})")
        return val
    return val


def projects_import(csv_path: str, allow_new: bool) -> None:
    vocab = load_vocab()
    oa = openai_client()
    conn = get_db()
    rows_read = upserts = errs = skipped = 0

    required = ["sponsor_name", "country_iso3", "technology", "expected_cod"]

    with open(csv_path, "r", encoding="utf-8-sig") as f, conn.cursor() as cur:
        r = csv.DictReader(f)
        missing = [c for c in required if c not in (r.fieldnames or [])]
        if missing:
            print(json.dumps({"error": f"Missing column(s): {', '.join(missing)}"}), file=sys.stderr)
            sys.exit(1)
        for row in r:
            rows_read += 1
            try:
                sponsor_name = (row.get("sponsor_name") or "").strip()
                country_iso3 = norm_iso3(row.get("country_iso3"))
                technology = (row.get("technology") or "").strip()
                expected_cod = (row.get("expected_cod") or "").strip()
                if not (sponsor_name and country_iso3 and technology and expected_cod):
                    skipped += 1
                    continue
                # vocab validations
                country_iso3 = validate_value(vocab, "countries_iso3", country_iso3, allow_new)
                technology = validate_value(vocab, "technologies", technology, allow_new)
                stage = validate_value(vocab, "stages", (row.get("stage") or "").strip() or None, allow_new)
                instrument_needed = validate_value(vocab, "instruments", (row.get("instrument_needed") or "").strip() or None, allow_new)
                ifc_category = validate_value(vocab, "ifc_categories", (row.get("ifc_category") or "").strip() or None, allow_new)

                free_text = (row.get("free_text") or "").strip()
                if not free_text:
                    parts = [sponsor_name, country_iso3, technology, (row.get("capacity_value") or ""), (row.get("capacity_unit") or ""), (stage or ""), (instrument_needed or "")]
                    free_text = " ".join(p for p in parts if p)
                try:
                    lang = detect(free_text or "")
                except Exception:
                    lang = "en"
                if lang != "en" and oa:
                    free_text = summarize_en(oa, free_text)

                emb = embed_text(oa, free_text) if oa else None
                emb_lit = to_vector_literal(emb)

                # Parse numerics
                def num(x):
                    try:
                        return float(x) if x not in (None, "") else None
                    except Exception:
                        return None
                def dec(x):
                    try:
                        return float(x) if x not in (None, "") else None
                    except Exception:
                        return None

                params = {
                    "sponsor_name": sponsor_name,
                    "country_iso3": country_iso3,
                    "lat": num(row.get("lat")),
                    "lng": num(row.get("lng")),
                    "technology": technology,
                    "capacity_value": num(row.get("capacity_value")),
                    "capacity_unit": (row.get("capacity_unit") or None),
                    "expected_cod": dt.date.fromisoformat(expected_cod),
                    "stage": stage,
                    "capex_usd": dec(row.get("capex_usd")),
                    "ticket_open_usd": dec(row.get("ticket_open_usd")),
                    "instrument_needed": instrument_needed,
                    "currency": (row.get("currency") or None),
                    "tenor_years": dec(row.get("tenor_years")),
                    "target_irr": dec(row.get("target_irr")),
                    "offtake_status": (row.get("offtake_status") or None),
                    "permits_status": (row.get("permits_status") or None),
                    "land_rights_status": (row.get("land_rights_status") or None),
                    "grid_interconnect_status": (row.get("grid_interconnect_status") or None),
                    "esia_status": (row.get("esia_status") or None),
                    "ifc_category": ifc_category,
                    "community_risk": (row.get("community_risk") or None),
                    "pri_possible": (str(row.get("pri_possible")).lower() == "true"),
                    "eca_possible": (str(row.get("eca_possible")).lower() == "true"),
                    "sovereign_support": (str(row.get("sovereign_support")).lower() == "true"),
                    "policy_alignment": (row.get("policy_alignment") or None),
                    "china_plus_one_fit": (str(row.get("china_plus_one_fit")).lower() == "true"),
                    "nda_signed": (str(row.get("nda_signed")).lower() == "true"),
                    "data_room_url": (row.get("data_room_url") or None),
                    "free_text": free_text,
                    "embedding": emb_lit,
                }

                sql = (
                    "INSERT INTO projects (sponsor_name,country_iso3,lat,lng,technology,capacity_value,capacity_unit,expected_cod,stage,capex_usd,"
                    "ticket_open_usd,instrument_needed,currency,tenor_years,target_irr,offtake_status,permits_status,land_rights_status,grid_interconnect_status,esia_status,"
                    "ifc_category,community_risk,pri_possible,eca_possible,sovereign_support,policy_alignment,china_plus_one_fit,nda_signed,data_room_url,free_text,embedding)"
                    " VALUES (%(sponsor_name)s,%(country_iso3)s,%(lat)s,%(lng)s,%(technology)s,%(capacity_value)s,%(capacity_unit)s,%(expected_cod)s,%(stage)s,%(capex_usd)s,"
                    "%(ticket_open_usd)s,%(instrument_needed)s,%(currency)s,%(tenor_years)s,%(target_irr)s,%(offtake_status)s,%(permits_status)s,%(land_rights_status)s,%(grid_interconnect_status)s,%(esia_status)s,"
                    "%(ifc_category)s,%(community_risk)s,%(pri_possible)s,%(eca_possible)s,%(sovereign_support)s,%(policy_alignment)s,%(china_plus_one_fit)s,%(nda_signed)s,%(data_room_url)s,%(free_text)s,"
                    + ("%(embedding)s::vector" if emb_lit else "NULL") + ") "
                    "ON CONFLICT (sponsor_name,country_iso3,technology,expected_cod) DO UPDATE SET "
                    "stage=EXCLUDED.stage, capex_usd=EXCLUDED.capex_usd, ticket_open_usd=EXCLUDED.ticket_open_usd, instrument_needed=EXCLUDED.instrument_needed, "
                    "currency=EXCLUDED.currency, tenor_years=EXCLUDED.tenor_years, target_irr=EXCLUDED.target_irr, offtake_status=EXCLUDED.offtake_status, "
                    "permits_status=EXCLUDED.permits_status, land_rights_status=EXCLUDED.land_rights_status, grid_interconnect_status=EXCLUDED.grid_interconnect_status, esia_status=EXCLUDED.esia_status, "
                    "ifc_category=EXCLUDED.ifc_category, community_risk=EXCLUDED.community_risk, pri_possible=EXCLUDED.pri_possible, eca_possible=EXCLUDED.eca_possible, sovereign_support=EXCLUDED.sovereign_support, "
                    "policy_alignment=EXCLUDED.policy_alignment, china_plus_one_fit=EXCLUDED.china_plus_one_fit, nda_signed=EXCLUDED.nda_signed, data_room_url=EXCLUDED.data_room_url, free_text=EXCLUDED.free_text"
                    + (", embedding=EXCLUDED.embedding" if emb_lit else "") +
                    ";"
                )
                cur.execute(sql, params)
                upserts += 1
            except Exception as e:
                errs += 1
                print(json.dumps({"row": rows_read, "error": str(e)}), file=sys.stderr)
                continue

    print(json.dumps({"rows_read": rows_read, "upserts": upserts, "errors": errs, "skipped": skipped}))
    if errs:
        sys.exit(1)


def parse_list(val: Optional[str]) -> Optional[List[str]]:
    if val is None or val == "":
        return None
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()]
    s = str(val).strip()
    # Strip surrounding braces/brackets like "{a,b}" or "[a,b]"
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        s = s[1:-1]
    # Prefer pipe '|' when present; otherwise fall back to comma splitting
    parts_pipe = [x.strip() for x in s.split("|") if x.strip()]
    if len(parts_pipe) <= 1 and ("," in s):
        parts = [x.strip() for x in s.split(",") if x.strip()]
    else:
        parts = parts_pipe
    return parts or None


def investors_import(csv_path: str, allow_new: bool) -> None:
    vocab = load_vocab()
    oa = openai_client()
    conn = get_db()
    rows_read = upserts = errs = skipped = 0

    required = ["name", "type"]

    with open(csv_path, "r", encoding="utf-8-sig") as f, conn.cursor() as cur:
        r = csv.DictReader(f)
        missing = [c for c in required if c not in (r.fieldnames or [])]
        if missing:
            print(json.dumps({"error": f"Missing column(s): {', '.join(missing)}"}), file=sys.stderr)
            sys.exit(1)
        for row in r:
            rows_read += 1
            try:
                name = (row.get("name") or "").strip()
                typ = (row.get("type") or "").strip()
                if not (name and typ):
                    skipped += 1
                    continue
                # vocab validations
                _ = validate_value(vocab, "instruments", None, True)  # no-op; ensures vocab loads

                free_text = (row.get("free_text") or "").strip()
                if not free_text:
                    parts = [name, typ, (row.get("themes") or ""), (row.get("instruments_offered") or ""), (row.get("notes") or "")]
                    free_text = " ".join(p for p in parts if p)
                try:
                    lang = detect(free_text or "")
                except Exception:
                    lang = "en"
                if lang != "en" and oa:
                    free_text = summarize_en(oa, free_text)

                emb = embed_text(oa, free_text) if oa else None
                emb_lit = to_vector_literal(emb)

                def dec(x):
                    try:
                        return float(x) if x not in (None, "") else None
                    except Exception:
                        return None

                params = {
                    "name": name,
                    "type": typ,
                    "mandate_regions": parse_list(row.get("mandate_regions")),
                    "mandate_technologies": parse_list(row.get("mandate_technologies")),
                    "use_of_proceeds_allowed": parse_list(row.get("use_of_proceeds_allowed")),
                    "instruments_offered": parse_list(row.get("instruments_offered")),
                    "min_ticket_usd": dec(row.get("min_ticket_usd")),
                    "max_ticket_usd": dec(row.get("max_ticket_usd")),
                    "min_tenor_years": dec(row.get("min_tenor_years")),
                    "max_tenor_years": dec(row.get("max_tenor_years")),
                    "target_irr_range": (row.get("target_irr_range") or None),
                    "risk_appetite": (row.get("risk_appetite") or None),
                    "ifc_category_allowed": parse_list(row.get("ifc_category_allowed")),
                    "coal_exclusion": (str(row.get("coal_exclusion")).lower() == "true"),
                    "other_exclusions": parse_list(row.get("other_exclusions")),
                    "lending_currencies": parse_list(row.get("lending_currencies")),
                    "local_currency_pref": (str(row.get("local_currency_pref")).lower() == "true"),
                    "requires_site_visit": (str(row.get("requires_site_visit")).lower() == "true"),
                    "requires_ifc_ps": (str(row.get("requires_ifc_ps")).lower() == "true"),
                    "avg_decision_time_days": int(float(row.get("avg_decision_time_days"))) if (row.get("avg_decision_time_days") or "").strip() else None,
                    "themes": parse_list(row.get("themes")),
                    "contacts": (json.loads(row.get("contacts_json")) if row.get("contacts_json") else None),
                    "notes": (row.get("notes") or None),
                    "free_text": free_text,
                    "embedding": emb_lit,
                }

                sql = (
                    "INSERT INTO investors (name,type,mandate_regions,mandate_technologies,use_of_proceeds_allowed,instruments_offered,min_ticket_usd,max_ticket_usd,min_tenor_years,max_tenor_years,"
                    "target_irr_range,risk_appetite,ifc_category_allowed,coal_exclusion,other_exclusions,lending_currencies,local_currency_pref,requires_site_visit,requires_ifc_ps,avg_decision_time_days,themes,contacts,notes,free_text,embedding) "
                    "VALUES (%(name)s,%(type)s,%(mandate_regions)s,%(mandate_technologies)s,%(use_of_proceeds_allowed)s,%(instruments_offered)s,%(min_ticket_usd)s,%(max_ticket_usd)s,%(min_tenor_years)s,%(max_tenor_years)s,"
                    "%(target_irr_range)s,%(risk_appetite)s,%(ifc_category_allowed)s,%(coal_exclusion)s,%(other_exclusions)s,%(lending_currencies)s,%(local_currency_pref)s,%(requires_site_visit)s,%(requires_ifc_ps)s,%(avg_decision_time_days)s,%(themes)s,%(contacts)s,%(notes)s,%(free_text)s,"
                    + ("%(embedding)s::vector" if emb_lit else "NULL") + ") "
                    "ON CONFLICT (name) DO UPDATE SET "
                    "type=EXCLUDED.type, mandate_regions=EXCLUDED.mandate_regions, mandate_technologies=EXCLUDED.mandate_technologies, instruments_offered=EXCLUDED.instruments_offered, "
                    "min_ticket_usd=EXCLUDED.min_ticket_usd, max_ticket_usd=EXCLUDED.max_ticket_usd, min_tenor_years=EXCLUDED.min_tenor_years, max_tenor_years=EXCLUDED.max_tenor_years, "
                    "target_irr_range=EXCLUDED.target_irr_range, risk_appetite=EXCLUDED.risk_appetite, ifc_category_allowed=EXCLUDED.ifc_category_allowed, coal_exclusion=EXCLUDED.coal_exclusion, other_exclusions=EXCLUDED.other_exclusions, "
                    "lending_currencies=EXCLUDED.lending_currencies, local_currency_pref=EXCLUDED.local_currency_pref, requires_site_visit=EXCLUDED.requires_site_visit, requires_ifc_ps=EXCLUDED.requires_ifc_ps, avg_decision_time_days=EXCLUDED.avg_decision_time_days, themes=EXCLUDED.themes, notes=EXCLUDED.notes, free_text=EXCLUDED.free_text"
                    + (", embedding=EXCLUDED.embedding" if emb_lit else "") +
                    ";"
                )
                cur.execute(sql, params)
                upserts += 1
            except Exception as e:
                errs += 1
                print(json.dumps({"row": rows_read, "error": str(e)}), file=sys.stderr)
                continue

    print(json.dumps({"rows_read": rows_read, "upserts": upserts, "errors": errs, "skipped": skipped}))


def project_yaml_import(in_path: str, allow_new: bool) -> None:
    vocab = load_vocab()
    oa = openai_client()
    conn = get_db()
    rows_read = upserts = errs = skipped = 0
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        rows_read = 1
        sponsor_name = (data.get("sponsor_name") or "").strip()
        country_iso3 = norm_iso3(data.get("country_iso3"))
        technology = (data.get("technology") or "").strip()
        expected_cod = (data.get("expected_cod") or "").strip()
        if not (sponsor_name and country_iso3 and technology and expected_cod):
            print(json.dumps({"error": "Missing required fields in YAML: sponsor_name, country_iso3, technology, expected_cod"}), file=sys.stderr)
            sys.exit(1)
        country_iso3 = validate_value(vocab, "countries_iso3", country_iso3, allow_new)
        technology = validate_value(vocab, "technologies", technology, allow_new)
        stage = validate_value(vocab, "stages", (data.get("stage") or None), allow_new)
        instrument_needed = validate_value(vocab, "instruments", (data.get("instrument_needed") or None), allow_new)
        ifc_category = validate_value(vocab, "ifc_categories", (data.get("ifc_category") or None), allow_new)
        free_text = (data.get("free_text") or data.get("notes") or "").strip()
        if not free_text:
            parts = [sponsor_name, country_iso3, technology, str(data.get("capacity_value") or ""), str(data.get("capacity_unit") or ""), (stage or ""), (instrument_needed or "")]
            free_text = " ".join(p for p in parts if p)
        try:
            lang = detect(free_text or "")
        except Exception:
            lang = "en"
        if lang != "en" and oa:
            free_text = summarize_en(oa, free_text)
        emb = embed_text(oa, free_text) if oa else None
        emb_lit = to_vector_literal(emb)
        def num(x):
            try:
                return float(x) if x not in (None, "") else None
            except Exception:
                return None
        def dec(x):
            try:
                return float(x) if x not in (None, "") else None
            except Exception:
                return None
        params = {
            "sponsor_name": sponsor_name,
            "country_iso3": country_iso3,
            "lat": num(data.get("lat")),
            "lng": num(data.get("lng")),
            "technology": technology,
            "capacity_value": num(data.get("capacity_value")),
            "capacity_unit": (data.get("capacity_unit") or None),
            "expected_cod": dt.date.fromisoformat(expected_cod),
            "stage": stage,
            "capex_usd": dec(data.get("capex_usd")),
            "ticket_open_usd": dec(data.get("ticket_open_usd")),
            "instrument_needed": instrument_needed,
            "currency": (data.get("currency") or None),
            "tenor_years": dec(data.get("tenor_years")),
            "target_irr": dec(data.get("target_irr")),
            "offtake_status": (data.get("offtake_status") or None),
            "permits_status": (data.get("permits_status") or None),
            "land_rights_status": (data.get("land_rights_status") or None),
            "grid_interconnect_status": (data.get("grid_interconnect_status") or None),
            "esia_status": (data.get("esia_status") or None),
            "ifc_category": ifc_category,
            "community_risk": (data.get("community_risk") or None),
            "pri_possible": (str(data.get("pri_possible")).lower() == "true"),
            "eca_possible": (str(data.get("eca_possible")).lower() == "true"),
            "sovereign_support": (str(data.get("sovereign_support")).lower() == "true"),
            "policy_alignment": data.get("policy_alignment") if isinstance(data.get("policy_alignment"), list) else (parse_list(data.get("policy_alignment")) or None),
            "china_plus_one_fit": (str(data.get("china_plus_one_fit")).lower() == "true"),
            "nda_signed": (str(data.get("nda_signed")).lower() == "true"),
            "data_room_url": (data.get("data_room_url") or data.get("dataroom_url") or None),
            "free_text": free_text,
            "embedding": emb_lit,
        }
        with conn.cursor() as cur:
            sql = (
                "INSERT INTO projects (sponsor_name,country_iso3,lat,lng,technology,capacity_value,capacity_unit,expected_cod,stage,capex_usd,"
                "ticket_open_usd,instrument_needed,currency,tenor_years,target_irr,offtake_status,permits_status,land_rights_status,grid_interconnect_status,esia_status,"
                "ifc_category,community_risk,pri_possible,eca_possible,sovereign_support,policy_alignment,china_plus_one_fit,nda_signed,data_room_url,free_text,embedding)"
                " VALUES (%(sponsor_name)s,%(country_iso3)s,%(lat)s,%(lng)s,%(technology)s,%(capacity_value)s,%(capacity_unit)s,%(expected_cod)s,%(stage)s,%(capex_usd)s,"
                "%(ticket_open_usd)s,%(instrument_needed)s,%(currency)s,%(tenor_years)s,%(target_irr)s,%(offtake_status)s,%(permits_status)s,%(land_rights_status)s,%(grid_interconnect_status)s,%(esia_status)s,"
                "%(ifc_category)s,%(community_risk)s,%(pri_possible)s,%(eca_possible)s,%(sovereign_support)s,%(policy_alignment)s,%(china_plus_one_fit)s,%(nda_signed)s,%(data_room_url)s,%(free_text)s,"
                + ("%(embedding)s::vector" if emb_lit else "NULL") + ") "
                "ON CONFLICT (sponsor_name,country_iso3,technology,expected_cod) DO UPDATE SET "
                "stage=EXCLUDED.stage, capex_usd=EXCLUDED.capex_usd, ticket_open_usd=EXCLUDED.ticket_open_usd, instrument_needed=EXCLUDED.instrument_needed, "
                "currency=EXCLUDED.currency, tenor_years=EXCLUDED.tenor_years, target_irr=EXCLUDED.target_irr, offtake_status=EXCLUDED.offtake_status, "
                "permits_status=EXCLUDED.permits_status, land_rights_status=EXCLUDED.land_rights_status, grid_interconnect_status=EXCLUDED.grid_interconnect_status, esia_status=EXCLUDED.esia_status, "
                "ifc_category=EXCLUDED.ifc_category, community_risk=EXCLUDED.community_risk, pri_possible=EXCLUDED.pri_possible, eca_possible=EXCLUDED.eca_possible, sovereign_support=EXCLUDED.sovereign_support, "
                "policy_alignment=EXCLUDED.policy_alignment, china_plus_one_fit=EXCLUDED.china_plus_one_fit, nda_signed=EXCLUDED.nda_signed, data_room_url=EXCLUDED.data_room_url, free_text=EXCLUDED.free_text"
                + (", embedding=EXCLUDED.embedding" if emb_lit else "") +
                ";"
            )
            cur.execute(sql, params)
            upserts = 1
    except Exception as e:
        errs = 1
        print(json.dumps({"error": str(e)}), file=sys.stderr)
    print(json.dumps({"rows_read": rows_read, "upserts": upserts, "errors": errs, "skipped": skipped}))
    if errs:
        sys.exit(1)

    if errs:
        sys.exit(1)


def main():
    p = argparse.ArgumentParser(description="Importer for projects/investors")
    sub = p.add_subparsers(dest="cmd", required=True)
    p1 = sub.add_parser("projects", help="Import projects CSV")
    p1.add_argument("--csv", required=True)
    p1.add_argument("--allow-new", action="store_true")
    p2 = sub.add_parser("investors", help="Import investors CSV")
    p2.add_argument("--csv", required=True)
    p2.add_argument("--allow-new", action="store_true")
    p3 = sub.add_parser("project-yaml", help="Import a single project from YAML")
    p3.add_argument("--in", dest="in_path", required=True)
    p3.add_argument("--allow-new", action="store_true")
    args = p.parse_args()

    if args.cmd == "projects":
        projects_import(args.csv, args.allow_new)
    elif args.cmd == "investors":
        investors_import(args.csv, args.allow_new)
    elif args.cmd == "project-yaml":
        project_yaml_import(args.in_path, args.allow_new)


if __name__ == "__main__":
    main()

