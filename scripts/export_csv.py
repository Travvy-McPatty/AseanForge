#!/usr/bin/env python3
import os, sys, csv
import psycopg2
from contextlib import closing

OUT_EVENTS = "deliverables/events.csv"
OUT_DOCS = "deliverables/documents.csv"
VALIDATION_DIR = "data/output/validation/latest"

EVENTS_SQL = """
SELECT
  event_id, pub_date, country, authority, policy_area, action_type,
  title, url, source_tier, content_type, lang, is_ocr, ocr_quality,
  source_confidence, summary_en
FROM events
ORDER BY pub_date DESC, event_id DESC;
"""

DOCS_SQL = """
SELECT
  doc_id, event_id, source_url, rendered,
  LENGTH(clean_text) AS char_count
FROM documents
ORDER BY doc_id ASC;
"""


def export_csv(cur, sql: str, out_path: str, headers: list[str]):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cur.execute(sql)
    rows = cur.fetchall()
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(list(r))
    return len(rows)


def main():
    os.makedirs(VALIDATION_DIR, exist_ok=True)
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        print("NEON_DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    with closing(psycopg2.connect(url)) as conn, conn, conn.cursor() as cur:
        n_events = export_csv(cur, EVENTS_SQL, OUT_EVENTS, [
            "event_id","pub_date","country","authority","policy_area","action_type",
            "title","url","source_tier","content_type","lang","is_ocr","ocr_quality",
            "source_confidence","summary_en"
        ])
        n_docs = export_csv(cur, DOCS_SQL, OUT_DOCS, [
            "doc_id","event_id","source_url","rendered","char_count"
        ])

    # Write counts summary for validation
    counts_path = os.path.join(VALIDATION_DIR, "csv_export_counts.txt")
    with open(counts_path, "w", encoding="utf-8") as f:
        f.write(f"events.csv_rows\t{n_events}\n")
        f.write(f"documents.csv_rows\t{n_docs}\n")
    print(f"Export complete: {OUT_EVENTS} ({n_events} rows), {OUT_DOCS} ({n_docs} rows)")


if __name__ == "__main__":
    main()

