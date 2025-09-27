## ASEANForge Policy Tape — Runbook (MVP: MAS + IMDA)

Timezone: Asia/Jakarta

### Prerequisites
- Python 3.10+
- psql client (Neon)
- Neon Postgres database URL with `?sslmode=require`
- OpenAI API key
- Optional: Firecrawl API key

### 1) Setup Python environment
```
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install openai psycopg2-binary pyyaml firecrawl-py python-dotenv pdfminer.six langdetect
```

### 2) Configure environment
```
cp app/.env.example app/.env
```
Fill in app/.env values:
- OPENAI_API_KEY
- FIRECRAWL_API_KEY (optional)
- NEON_DATABASE_URL (postgresql://USER:PASSWORD@HOST/DB?sslmode=require)
- OCR_ENABLED=false
- TIMEZONE=Asia/Jakarta
- OPENAI_SUMMARY_MODEL=gpt-4o-mini
- OPENAI_EMBED_MODEL=text-embedding-3-small
- MAX_CONCURRENCY=2
- DELAY_MS=1200

### 3) Apply database schema
```
psql "$NEON_DATABASE_URL" -f infra/neon/schema.sql
```

### 4) Dry-run ingestion (no DB writes)
```
python app/ingest.py dry-run --since=2025-09-20
```
Expected: JSON logs with metrics, discovered links, summary/classification logs.

### 5) Run ingestion (writes to Neon)
```
python app/ingest.py run --since=2025-09-20
```
- Sources: MAS + IMDA only
- Fetch Firecrawl/HTTP, extract PDF text via pdfminer.six (OCR disabled)
- Summarize with gpt-4o-mini (3 sentences if English; 5 sentences for non-English)
- Classify using rules.yaml (MAS/IMDA) keywords only
- Embeddings via text-embedding-3-small (1536)
- Upsert into events/documents with event_hash idempotency

Validate rows:
```
psql "$NEON_DATABASE_URL" -c 'SELECT COUNT(*) FROM events;'
psql "$NEON_DATABASE_URL" -c 'SELECT COUNT(*) FROM documents;'
```

### 6) Generate report (last 24h, top 3)
```
python app/report_stub.py
```
Markdown format matches acceptance format.

### Acceptance Tests
1) Schema applies cleanly:
```
psql "$NEON_DATABASE_URL" -f infra/neon/schema.sql
```
2) Ingestion fetches ≥1 item from MAS or IMDA:
```
python app/ingest.py run --since=2025-09-20
```
3) Report emits 1–3 items:
```
python app/report_stub.py
```
4) Idempotency: re-run the ingestion with the same since-date; expect 0 new rows.



### Tier-1 Expansion Playbook (≥9/15 with idempotency)

Prereqs
- Ensure app/.env contains OPENAI_API_KEY, NEON_DATABASE_URL, optional FIRECRAWL_API_KEY
- Respect security patterns (no secrets printed)

1) Health validation (HEAD) and seed selection
```
.venv/bin/python scripts/health.py > data/output/validation/latest/health.csv 2> data/output/validation/latest/health.err
```
- Use the first candidate per authority that responds 200; update both configs:
  - configs/firecrawl_seed.json (authoritative)
  - config/sources.yaml (mirror)

2) Ingestion pipeline
```
env $(grep -Ev '^#|^$' app/.env | xargs) .venv/bin/python app/ingest.py dry-run --since=2025-06-01 > data/output/validation/latest/dry_run.log 2>&1

env $(grep -Ev '^#|^$' app/.env | xargs) .venv/bin/python app/ingest.py run --since=2025-06-01 > data/output/validation/latest/run.log 2>&1
```

3) Database validation (write all outputs)
```
env "$(grep '^NEON_DATABASE_URL=' app/.env)" bash -lc 'psql "$NEON_DATABASE_URL" -c "SELECT authority, COUNT(*) FROM events GROUP BY 1 ORDER BY 1;"' > data/output/validation/latest/db_auth_counts.txt

env "$(grep '^NEON_DATABASE_URL=' app/.env)" bash -lc 'psql "$NEON_DATABASE_URL" -c "SELECT source_url, length(clean_text) len FROM documents ORDER BY len DESC NULLS LAST LIMIT 10;"' > data/output/validation/latest/db_doc_lengths.txt

env "$(grep '^NEON_DATABASE_URL=' app/.env)" bash -lc 'psql "$NEON_DATABASE_URL" -c "SELECT event_id, pub_date, authority, policy_area, action_type, left(title,120) title FROM events ORDER BY pub_date DESC LIMIT 10;"' > data/output/validation/latest/db_recent.txt

env "$(grep '^NEON_DATABASE_URL=' app/.env)" bash -lc 'psql "$NEON_DATABASE_URL" -c "SELECT count(*) events_cnt FROM events; SELECT count(*) documents_cnt FROM documents;"' > data/output/validation/latest/counts_final.txt
```

4) Idempotency validation
```
env $(grep -Ev '^#|^$' app/.env | xargs) .venv/bin/python app/ingest.py run --since=2025-06-01 > data/output/validation/latest/rerun.log 2>&1
```
- Expect items_new=0; DB counts unchanged

5) Documentation & PR
- Update docs/SPEC.md seeds/limits/redirect policy
- Append this playbook to docs/runbook.md
- Write data/output/validation/latest/final_report.md
- Commit seeds + docs; push branch fix/ingest-hardening; create/update PR with report body

Acceptance
- ≥9/15 authorities with items_new>0
- Rerun idempotent (items_new=0; counts unchanged)
- Final artifacts present (health.csv, DB proofs, final_report.md)
