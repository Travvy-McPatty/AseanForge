import os, sys
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

OUT_DIR = "data/output/validation/latest"
SINCE = os.environ.get("AF_SINCE", "2025-06-01")

FILES = {
    "auth_counts": os.path.join(OUT_DIR, "db_auth_counts.txt"),
    "doc_lengths": os.path.join(OUT_DIR, "db_doc_lengths.txt"),
    "recent": os.path.join(OUT_DIR, "db_recent.txt"),
    "totals": os.path.join(OUT_DIR, "db_totals.txt"),
}

SQL = {
    "auth_counts": """
        SELECT authority, COUNT(*)
        FROM events
        GROUP BY 1
        ORDER BY 1;
    """,
    "doc_lengths": """
        SELECT source_url, length(clean_text) AS len
        FROM documents
        ORDER BY len DESC NULLS LAST
        LIMIT 10;
    """,
    "recent": """
        SELECT event_id, pub_date, authority, policy_area, action_type, left(title,120) AS title
        FROM events
        ORDER BY pub_date DESC
        LIMIT 10;
    """,
    "totals": """
        SELECT count(*) AS events_cnt FROM events;
        SELECT count(*) AS documents_cnt FROM documents;
    """,
}


def write_rows(path: str, rows):
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            # header
            f.write("\t".join(rows[0].keys()) + "\n")
            for r in rows:
                f.write("\t".join(str(r[k]) for k in rows[0].keys()) + "\n")
        else:
            for r in rows:
                f.write(str(r) + "\n")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        print("NEON_DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    conn = psycopg2.connect(url)
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            # auth_counts
            cur.execute(SQL["auth_counts"])
            rows = cur.fetchall()
            write_rows(FILES["auth_counts"], rows)
            # doc_lengths
            cur.execute(SQL["doc_lengths"])
            rows = cur.fetchall()
            write_rows(FILES["doc_lengths"], rows)
            # recent
            cur.execute(SQL["recent"])
            rows = cur.fetchall()
            write_rows(FILES["recent"], rows)
        # totals (multiple statements)
        with conn, conn.cursor() as cur2:
            with open(FILES["totals"], "w", encoding="utf-8") as f:
                cur2.execute("SELECT count(*) FROM events;")
                f.write(f"events_cnt\t{cur2.fetchone()[0]}\n")
                cur2.execute("SELECT count(*) FROM documents;")
                f.write(f"documents_cnt\t{cur2.fetchone()[0]}\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

