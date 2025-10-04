#!/usr/bin/env bash
set -euo pipefail

# Daily KOMDIGI refresh: Phase A (ingest) + Phase B (embeddings backfill)
# Exit codes:
#  0 = success
#  1 = FAIL gate (insufficient counts/coverage)
#  2 = 3x429 circuit breaker (propagated by underlying tools if implemented)
#  3 = DB/ingestion failure

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Activate venv
if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

# Load env (quoted values in app/.env are supported)
if [[ -f app/.env ]]; then
  # shellcheck disable=SC2046
  export $(grep -E -v '^#|^$' app/.env | xargs)
fi

LOG_DIR="data/output/validation/latest"
mkdir -p "$LOG_DIR"

echo "[Daily KOMDIGI] Budgets: Firecrawl ≤200 URLs, OpenAI Batch ≤$5"

# Phase A — KOMDIGI ingestion
set +e
.venv/bin/python app/ingest.py run --since 2024-01-01 --authorities KOMDIGI --limit-per-source 120
rc_ingest=$?
set -e
if [[ $rc_ingest -ne 0 ]]; then
  echo "Ingestion failed with code $rc_ingest" | tee -a "$LOG_DIR/daily_komdigi_status.txt"
  exit 3
fi

# Metrics from DB
DB_URL="$(grep '^NEON_DATABASE_URL=' app/.env | cut -d= -f2-)"
if [[ -z "${DB_URL:-}" ]]; then
  echo "NEON_DATABASE_URL not set" | tee -a "$LOG_DIR/daily_komdigi_status.txt"
  exit 3
fi

events=$(psql "$DB_URL" -X -t -A -c "SELECT COUNT(*) FROM events WHERE authority='KOMDIGI';" || echo 0)
docs400=$(psql "$DB_URL" -X -t -A -c "SELECT COUNT(*) FROM documents d JOIN events e ON e.event_id=d.event_id WHERE e.authority='KOMDIGI' AND length(coalesce(d.clean_text,''))>=400;" || echo 0)

# Phase B — Embeddings backfill (documents-first, 365d, KOMDIGI)
emb_out=$(python - <<'PY'
from scripts.run_flagship_v1_1 import phase_a_embeddings_docs
res = phase_a_embeddings_docs(budget_cap_usd=5.0)
print("EMB_COVERAGE_PCT", res.get("coverage_pct", 0.0))
print("EMB_COST", res.get("projected_cost", 0.0))
PY
)
emb_pct=$(awk '/EMB_COVERAGE_PCT/{print $2}' <<<"$emb_out")
emb_cost=$(awk '/EMB_COST/{print $2}' <<<"$emb_out")

# PASS/FAIL gates
status="PASS"
exit_code=0
if [[ ${events:-0} -lt 25 || ${docs400:-0} -lt 25 ]]; then
  status="FAIL"; exit_code=1
fi

# Write status file
{
  echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "Events(KOMDIGI): $events"
  echo "Docs≥400(KOMDIGI): $docs400"
  echo "Embeddings Coverage(365d overall): ${emb_pct:-0}%"
  echo "Batch Cost: $${emb_cost:-0} / $5.00"
  echo "Status: $status"
} | tee "$LOG_DIR/daily_komdigi_status.txt"

exit $exit_code

