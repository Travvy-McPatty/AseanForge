#!/usr/bin/env python3
import os, glob, json, sys, time
from urllib.parse import urlparse

"""
Generate a candidate YAML from the most recent Deep Research sources JSON.
- Input: data/output/deep_research_sources_<ts>.json
- Output: config/sources_candidates_<ts>.yaml

Does NOT modify config/sources.yaml.
"""

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT_DIR = os.path.join(REPO_ROOT, "data", "output")
CONFIG_DIR = os.path.join(REPO_ROOT, "config")

def latest_dr_json() -> str:
    files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "deep_research_sources_*.json")))
    if not files:
        raise SystemExit("No deep_research_sources_*.json found in data/output/. Run a Deep Research report first.")
    # pick latest by timestamp in filename if present; otherwise by mtime
    def key(p):
        base = os.path.basename(p)
        try:
            ts = int(base.split("_")[-1].split(".")[0])
        except Exception:
            ts = int(os.path.getmtime(p))
        return ts
    files.sort(key=key)
    return files[-1]

def domain(url: str) -> str:
    try:
        return urlparse(url or "").netloc or ""
    except Exception:
        return ""

def to_yaml(sources: list[dict]) -> str:
    lines = []
    lines.append("# Deep Research candidate sources (manual review recommended)")
    lines.append(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime())}")
    lines.append("# Notes:")
    lines.append("# - Review and copy selected entries into config/sources.yaml")
    lines.append("# - Suggested defaults are provided as comments (limit, max_depth)")
    lines.append("")
    lines.append("candidates:")
    for s in sources:
        name = s.get("title") or s.get("domain") or "(untitled)"
        url = s.get("url", "")
        dom = s.get("domain") or domain(url)
        lines.append(f"  - name: \"{name.replace('"','\\\"')}\"")
        lines.append(f"    url: {url}")
        lines.append(f"    category: candidate")
        lines.append(f"    # suggested: limit: 8")
        lines.append(f"    # suggested: max_depth: 2")
        lines.append(f"    # domain: {dom}")
        lines.append(f"    # accessed_at: {s.get('accessed_at','')}")
    return "\n".join(lines) + "\n"

def main():
    src_json = latest_dr_json()
    with open(src_json, "r", encoding="utf-8") as f:
        data = json.load(f) or []
    # Deduplicate by URL (keep first occurence) and then by domain keep up to 2 entries
    by_url = {}
    for s in data:
        url = s.get("url")
        if not url or url in by_url:
            continue
        by_url[url] = s
    by_domain = {}
    filtered = []
    for url, s in by_url.items():
        dom = domain(url)
        cnt = by_domain.get(dom, 0)
        if cnt >= 2:
            continue
        by_domain[dom] = cnt + 1
        s.setdefault("domain", dom)
        filtered.append(s)

    # Emit YAML
    ts = src_json.split("_")[-1].split(".")[0]
    os.makedirs(CONFIG_DIR, exist_ok=True)
    out_path = os.path.join(CONFIG_DIR, f"sources_candidates_{ts}.yaml")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(to_yaml(filtered))
    print(f"Wrote candidate YAML: {out_path}")

if __name__ == "__main__":
    main()

