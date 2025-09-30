#!/usr/bin/env python3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import yaml

OUT_PATH = os.path.join("data","output","validation","latest","vendor_flags.txt")
CFG_PATH = os.path.join("configs","vendor_overrides.yaml")

def main():
    load_dotenv("app/.env")  # no prints
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    bnm = {"unlock": False}
    kom = {"unlock": False}
    if os.path.exists(CFG_PATH):
        try:
            with open(CFG_PATH, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                bnm = dict(cfg.get("bnm") or bnm)
                kom = dict(cfg.get("kominfo") or kom)
        except Exception:
            pass
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(f"[{now}] Vendor override check\n")
        f.write(f"BNM unlock: {str(bool(bnm.get('unlock'))).lower()}\n")
        f.write(f"KOMINFO unlock: {str(bool(kom.get('unlock'))).lower()}\n")
        if bnm.get("notes"):
            f.write(f"BNM notes: {bnm.get('notes')}\n")
        if kom.get("notes"):
            f.write(f"KOMINFO notes: {kom.get('notes')}\n")
    # Also print a one-liner note if any flag active (no secrets)
    if bool(bnm.get("unlock")):
        print("[VENDOR_OVERRIDE] BNM unlock flag set to true; will attempt escalated ingestion pass")
    if bool(kom.get("unlock")):
        print("[VENDOR_OVERRIDE] KOMINFO unlock flag set to true; will attempt escalated ingestion pass")

if __name__ == "__main__":
    main()

