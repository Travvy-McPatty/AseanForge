#!/usr/bin/env python3
"""
Deal-Matching MVP Matcher

Usage:
  python app/matcher.py rank --project-id=<uuid> --top=10
  python app/matcher.py batch --min-score=60 --top=5

- Applies hard filters then soft scoring (0-100)
- Upserts into matches table (idempotent by project_id, investor_id)
- Prints a Markdown summary of top matches
"""
from __future__ import annotations
import argparse, json, math, os, sys, datetime as dt
from typing import List, Dict, Any, Optional, Tuple

from pathlib import Path
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
except Exception:
    pass

import psycopg2


import yaml

_WEIGHTS_CACHE = None
_VOCAB_CACHE = None


def load_weights() -> dict:
    global _WEIGHTS_CACHE
    if _WEIGHTS_CACHE is not None:
        return _WEIGHTS_CACHE
    defaults = {"mandate": 0.25, "readiness": 0.20, "commercials": 0.20, "risk": 0.15, "impact": 0.10, "strategic": 0.10}
    try:
        with open("configs/weights.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            # Coerce and fill defaults
            out = {k: float(data.get(k, defaults[k])) for k in defaults.keys()}
            _WEIGHTS_CACHE = out
            return out
    except Exception:
        _WEIGHTS_CACHE = defaults
        return defaults


def load_vocab() -> dict:
    global _VOCAB_CACHE
    if _VOCAB_CACHE is not None:
        return _VOCAB_CACHE
    try:
        with open("configs/vocab.yaml", "r", encoding="utf-8") as f:
            _VOCAB_CACHE = yaml.safe_load(f) or {}
            return _VOCAB_CACHE
    except Exception:
        _VOCAB_CACHE = {}
        return {}

def get_db():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        print("ERROR: NEON_DATABASE_URL not set", file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(url)
    conn.autocommit = True
    return conn


def cosine(a: Optional[List[float]], b: Optional[List[float]]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x*y for x,y in zip(a,b))
    na = math.sqrt(sum(x*x for x in a))
    nb = math.sqrt(sum(y*y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot/(na*nb)


def parse_vector(val) -> Optional[List[float]]:
    # Accept psycopg2 returning string like "[0.1,0.2,...]" or list
    if val is None:
        return None
    if isinstance(val, list):
        return [float(x) for x in val]
    s = str(val).strip()
    if s.startswith("[") and s.endswith("]"):
        try:
            return [float(x) for x in s[1:-1].split(',') if x.strip()]
        except Exception:
            return None
    return None


# ---------------- Scoring ----------------

def readiness_score(p: Dict[str, Any]) -> float:
    # permits_status, land_rights_status, grid_interconnect_status, esia_status
    def level(x: Optional[str]) -> int:
        x = (x or "").lower()
        if x in ("final","executed","secured","granted","complete"): return 3
        if x in ("draft","loi","applied","in_progress"): return 2
        if x in ("scoping","none","pending","unknown"): return 1
        return 0
    vals = [level(p.get("permits_status")), level(p.get("land_rights_status")), level(p.get("grid_interconnect_status")), level(p.get("esia_status"))]
    return (sum(vals)/ (3*4)) * 20.0  # max 20


def commercials_score(p: Dict[str, Any], inv: Dict[str, Any]) -> float:
    score = 0.0
    # ticket within range tightness
    t = p.get("ticket_open_usd")
    if t is not None:
        lo = inv.get("min_ticket_usd")
        hi = inv.get("max_ticket_usd")
        if lo is not None and hi is not None:
            mid = (float(lo)+float(hi))/2.0
            span = max(1.0, float(hi)-float(lo))
            score += max(0.0, 10.0 * (1.0 - min(1.0, abs(float(t)-mid)/span)))
        else:
            score += 6.0
    # tenor
    ten = p.get("tenor_years")
    if ten is not None:
        lo = inv.get("min_tenor_years")
        hi = inv.get("max_tenor_years")
        if lo is not None and hi is not None and float(lo) <= float(ten) <= float(hi):
            score += 5.0
    # irr hint
    if (p.get("target_irr") and inv.get("target_irr_range")):
        try:
            low, high = [float(x) for x in str(inv.get("target_irr_range")).replace('%','').split('-')]
            if low <= float(p.get("target_irr")) <= high:
                score += 3.0
        except Exception:
            pass
    # currency heuristic
    if p.get("currency") and inv.get("lending_currencies") and p["currency"] in (inv.get("lending_currencies") or []):
        score += 2.0
    return min(20.0, score)


def risk_score(p: Dict[str, Any]) -> float:
    score = 0.0
    if p.get("pri_possible"): score += 5.0
    if p.get("eca_possible"): score += 5.0
    if p.get("sovereign_support"): score += 5.0
    return min(15.0, score)


def impact_score(p: Dict[str, Any], inv: Dict[str, Any]) -> float:
    score = 0.0
    # IFC category tolerance
    cat = (p.get("ifc_category") or "").upper()
    allowed = [str(x).upper() for x in (inv.get("ifc_category_allowed") or [])]
    if cat and (not allowed or cat in allowed):
        score += 6.0
    # community risk
    cr = (p.get("community_risk") or "low").lower()
    score += {"low": 4.0, "med": 2.0, "high": 0.0}.get(cr, 2.0)
    return min(10.0, score)


def strategic_score(p: Dict[str, Any], inv: Dict[str, Any]) -> float:
    score = 0.0
    p_themes = set([x.lower() for x in (p.get("policy_alignment") or [])])
    i_themes = set([x.lower() for x in (inv.get("themes") or [])])
    if p.get("china_plus_one_fit"): score += 3.0
    if p_themes and i_themes:
        score += min(7.0, 1.5 * len(p_themes.intersection(i_themes)))
    return min(10.0, score)


def mandate_score(p: Dict[str, Any], inv: Dict[str, Any]) -> float:
    score = 0.0
    # geography
    if not inv.get("mandate_regions") or (p.get("country_iso3") in (inv.get("mandate_regions") or [])):
        score += 10.0
    # technology
    if not inv.get("mandate_technologies") or (p.get("technology") in (inv.get("mandate_technologies") or [])):
        score += 10.0
    # embedding sim (optional)
    sim = cosine(parse_vector(p.get("embedding")), parse_vector(inv.get("embedding")))
    score += max(0.0, min(5.0, 5.0 * sim))
    return min(25.0, score)


def hard_filters_pass(p: Dict[str, Any], inv: Dict[str, Any]) -> Tuple[bool, List[str]]:
    reasons: List[str] = []
    # 1 Geography
    regions = inv.get("mandate_regions") or []
    iso3 = (p.get("country_iso3") or "").upper()
    regions_u = [str(r).upper() for r in regions]
    asean_countries = [str(x).upper() for x in (load_vocab().get("countries_iso3") or [])]
    geo_ok = True
    if regions_u:
        geo_ok = (iso3 in regions_u) or ("ASEAN" in regions_u and iso3 in asean_countries)
    if not geo_ok:
        reasons.append("geography")
    # 2 Tech
    techs = inv.get("mandate_technologies") or []
    if techs and p.get("technology") not in techs:
        reasons.append("technology")
    # 3 Ticket range
    t = p.get("ticket_open_usd")
    lo, hi = inv.get("min_ticket_usd"), inv.get("max_ticket_usd")
    if t is not None and ((lo is not None and float(t) < float(lo)) or (hi is not None and float(t) > float(hi))):
        reasons.append("ticket")
    # 4 Instrument
    instr = inv.get("instruments_offered") or []
    if p.get("instrument_needed") and instr and p.get("instrument_needed") not in instr:
        reasons.append("instrument")
    # 5 Exclusions
    if inv.get("coal_exclusion") and (p.get("technology") or "").lower() == "coal":
        reasons.append("coal_exclusion")
    allowed = [str(x).upper() for x in (inv.get("ifc_category_allowed") or [])]
    if p.get("ifc_category") and allowed and str(p.get("ifc_category")).upper() not in allowed:
        reasons.append("ifc_category")
    # 6 Stage gates
    if inv.get("requires_ifc_ps"):
        st = (p.get("esia_status") or "").lower()
        if st not in ("draft","final"):
            reasons.append("ifc_ps_stage")
    return (len(reasons) == 0), reasons


def fetch_project(conn, pid: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM projects WHERE project_id=%s", (pid,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def fetch_open_projects(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM projects WHERE COALESCE(ticket_open_usd,0) > 0 AND COALESCE(stage,'') <> 'financial-close'")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]


def fetch_investors(conn) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM investors")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]


def build_explain(p: Dict[str, Any], inv: Dict[str, Any], passed_filters: bool, failed: List[str], total: float) -> Tuple[List[str], List[str], str]:
    drivers: List[str] = []
    blockers: List[str] = []
    if not inv.get("mandate_regions") or p.get("country_iso3") in (inv.get("mandate_regions") or []):
        drivers.append(f"Geography match: {p.get('country_iso3')} in mandate")
    else:
        blockers.append("Geography outside mandate")
    if not inv.get("mandate_technologies") or p.get("technology") in (inv.get("mandate_technologies") or []):
        drivers.append(f"Technology fit: {p.get('technology')}")
    else:
        blockers.append("Technology outside mandate")
    if p.get("permits_status") in ("draft","final","granted","secured"):
        drivers.append("Readiness: permits progressing")
    if p.get("land_rights_status") in ("draft","final","granted","secured"):
        drivers.append("Readiness: land rights progressing")
    if p.get("grid_interconnect_status") in ("draft","final","granted","secured"):
        drivers.append("Readiness: grid interconnect progressing")
    if p.get("esia_status") not in ("draft","final"):
        blockers.append("ESIA not yet draft/final")
    if p.get("pri_possible"): drivers.append("PRI possible")
    if p.get("eca_possible"): drivers.append("ECA possible")
    if p.get("sovereign_support"): drivers.append("Sovereign support")
    if inv.get("requires_ifc_ps") and p.get("esia_status") not in ("draft","final"):
        blockers.append("IFC PS required: advance ESIA")
    # next action heuristic
    next_action = ""
    if "ifc_ps_stage" in failed or p.get("esia_status") not in ("draft","final"):
        next_action = "Finalize ESIA"
    elif (p.get("grid_interconnect_status") or "").lower() not in ("draft","final","granted","secured"):
        next_action = "Obtain grid interconnect letter"
    elif not drivers:
        next_action = "Share teaser and request mandate fit feedback"
    return (drivers[:5], blockers[:2], next_action)


def score_one(p: Dict[str, Any], inv: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ok, failed = hard_filters_pass(p, inv)
    if not ok:
        return None
    w = load_weights()
    m = mandate_score(p, inv)        # max 25
    rd = readiness_score(p)          # max 20
    cm = commercials_score(p, inv)   # max 20
    rk = risk_score(p)               # max 15
    im = impact_score(p, inv)        # max 10
    st = strategic_score(p, inv)     # max 10
    total = 100.0 * (
        w.get("mandate", 0.25)   * (m / 25.0) +
        w.get("readiness", 0.20) * (rd / 20.0) +
        w.get("commercials", 0.20) * (cm / 20.0) +
        w.get("risk", 0.15)      * (rk / 15.0) +
        w.get("impact", 0.10)    * (im / 10.0) +
        w.get("strategic", 0.10) * (st / 10.0)
    )
    total = max(0.0, min(100.0, total))
    drivers, blockers, next_action = build_explain(p, inv, True, [], total)
    return {
        "investor": inv,
        "score": total,
        "drivers": drivers,
        "blockers": blockers,
        "next_action": next_action,
    }


def upsert_match(conn, project_id: str, investor_id: str, score: float, drivers: List[str], blockers: List[str], next_action: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO matches (project_id, investor_id, score_numeric, drivers, blockers, next_action)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (project_id, investor_id) DO UPDATE SET
              score_numeric = EXCLUDED.score_numeric,
              drivers = EXCLUDED.drivers,
              blockers = EXCLUDED.blockers,
              next_action = EXCLUDED.next_action,
              created_at = NOW()
            """,
            (project_id, investor_id, float(score), drivers, blockers, next_action)
        )


def print_markdown(project: Dict[str, Any], matches: List[Dict[str, Any]], top: int) -> None:
    topn = matches[:top]
    print("## Top Matches")
    for m in topn:
        inv = m["investor"]
        print(f"- Investor: {inv.get('name')} ({inv.get('type')}) — Score: {m['score']:.1f}")
        print("  - Drivers:")
        for d in m["drivers"]:
            print(f"    - {d}")
        if m["blockers"]:
            print("  - Blockers:")
            for b in m["blockers"]:
                print(f"    - {b}")
        if m["next_action"]:
            print(f"  - Next Action: {m['next_action']}")
    print()


def cmd_rank(conn, project_id: str, top: int) -> int:
    p = fetch_project(conn, project_id)
    if not p:
        print(f"ERROR: project_id not found: {project_id}", file=sys.stderr)
        return 1
    investors = fetch_investors(conn)
    scored: List[Dict[str, Any]] = []
    for inv in investors:
        res = score_one(p, inv)
        if res:
            scored.append(res)
    scored.sort(key=lambda x: x["score"], reverse=True)
    for m in scored[:top]:
        upsert_match(conn, p["project_id"], m["investor"]["investor_id"], m["score"], m["drivers"], m["blockers"], m["next_action"])
    print_markdown(p, scored, top)
    print(json.dumps({"project_id": project_id, "considered": len(investors), "matched": len(scored[:top])}))
    return 0


def cmd_batch(conn, min_score: float, top: int) -> int:
    projects = fetch_open_projects(conn)
    investors = fetch_investors(conn)
    total_matches = 0
    for p in projects:
        scored: List[Dict[str, Any]] = []
        for inv in investors:
            res = score_one(p, inv)
            if res and res["score"] >= float(min_score):
                scored.append(res)
        scored.sort(key=lambda x: x["score"], reverse=True)
        keep = scored[:top]
        for m in keep:
            upsert_match(conn, p["project_id"], m["investor"]["investor_id"], m["score"], m["drivers"], m["blockers"], m["next_action"])
        total_matches += len(keep)
        if keep:
            print(f"### Project: {p.get('sponsor_name')} — {p.get('country_iso3')} — {p.get('technology')} — {p.get('capacity_value')} {p.get('capacity_unit')}")
            print_markdown(p, keep, top)
    print(json.dumps({"projects": len(projects), "total_matches": total_matches, "min_score": min_score, "top": top}))
    return 0


def main():
    ap = argparse.ArgumentParser(description="Matcher CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_rank = sub.add_parser("rank", help="Rank investors for a given project")
    p_rank.add_argument("--project-id", required=True)
    p_rank.add_argument("--top", type=int, default=10)

    p_batch = sub.add_parser("batch", help="Batch match for all open projects")
    p_batch.add_argument("--min-score", type=float, default=60)
    p_batch.add_argument("--top", type=int, default=5)

    args = ap.parse_args()
    conn = get_db()
    if args.cmd == "rank":
        rc = cmd_rank(conn, args.project_id, args.top)
        sys.exit(rc)
    elif args.cmd == "batch":
        rc = cmd_batch(conn, args.min_score, args.top)
        sys.exit(rc)


if __name__ == "__main__":
    main()

