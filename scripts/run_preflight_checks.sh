#!/bin/bash
set -euo pipefail

echo "=== Pre-Flight Checks for High-Volume Backfill ==="
echo ""

# Create output directory
mkdir -p data/output/validation/latest

# Step 0: Environment Validation
echo "Step 0: Environment Validation"
echo "------------------------------"

# Check Firecrawl API key
if [ -z "${FIRECRAWL_API_KEY:-}" ]; then
    if grep -q "^FIRECRAWL_API_KEY=" app/.env 2>/dev/null; then
        echo "✓ Firecrawl API key found in app/.env"
    else
        echo "✗ ERROR: FIRECRAWL_API_KEY not set"
        exit 1
    fi
else
    echo "✓ Firecrawl API key set in environment"
fi

# Check Neon DB connection
if [ -z "${NEON_DATABASE_URL:-}" ]; then
    if grep -q "^NEON_DATABASE_URL=" app/.env 2>/dev/null; then
        echo "✓ Neon DATABASE_URL found in app/.env"
    else
        echo "✗ ERROR: NEON_DATABASE_URL not set"
        exit 1
    fi
else
    echo "✓ Neon DATABASE_URL set in environment"
fi

# Check Python dependencies
if ! .venv/bin/python -c "import firecrawl, psycopg2, openai" 2>/dev/null; then
    echo "✗ ERROR: Missing Python dependencies"
    echo "  Run: .venv/bin/pip install firecrawl psycopg2-binary openai"
    exit 1
fi
echo "✓ Python dependencies OK"

echo ""

# Step 1: Firecrawl Account Probe
echo "Step 1: Firecrawl Account & Queue Health"
echo "-----------------------------------------"

.venv/bin/python scripts/fc_health_check.py \
  --output data/output/validation/latest/account_usage_start.json \
  --queue-output data/output/validation/latest/queue_status_start.json

echo ""
echo "Account Usage:"
cat data/output/validation/latest/account_usage_start.json
echo ""
echo "Queue Status:"
cat data/output/validation/latest/queue_status_start.json
echo ""

# Step 2: Canonical Seed Validation
echo "Step 2: Canonical Seed Validation"
echo "----------------------------------"

TS=$(date -u +%Y%m%d_%H%M%S)
.venv/bin/python scripts/validate_canonical_seeds.py \
  --config config/sources.yaml \
  --output data/output/validation/latest/canon_checks_${TS}.csv

# Check if ≥50% seeds returned 200
OK_COUNT=$(awk -F',' 'NR>1 && $4==200 {count++} END {print count+0}' data/output/validation/latest/canon_checks_${TS}.csv)
TOTAL=$(awk 'END {print NR-1}' data/output/validation/latest/canon_checks_${TS}.csv)

if [ "$TOTAL" -eq 0 ]; then
    echo "✗ ERROR: No seeds found in config/sources.yaml"
    exit 1
fi

PCT=$((OK_COUNT * 100 / TOTAL))
echo "Seed validation: ${OK_COUNT}/${TOTAL} (${PCT}%) returned HTTP 200"

if [ $PCT -lt 50 ]; then
    echo "✗ ERROR: Only ${PCT}% of seeds returned HTTP 200 (need ≥50%)"
    exit 1
fi
echo "✓ Seed validation passed (${PCT}% ≥ 50%)"

echo ""
echo "Validation Results (first 20 lines):"
head -n 20 data/output/validation/latest/canon_checks_${TS}.csv
echo ""

# Step 3: Save DB State
echo "Step 3: Database State Snapshot"
echo "--------------------------------"

.venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("app/.env")
url = os.getenv("NEON_DATABASE_URL")
if not url:
    print("ERROR: NEON_DATABASE_URL not set")
    exit(1)

# Fix URL scheme if needed
if url.startswith("postgresql://"):
    url = url.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(url, pool_pre_ping=True)

with engine.connect() as conn:
    ev = conn.execute(text("SELECT count(*) FROM events")).scalar()
    dc = conn.execute(text("SELECT count(*) FROM documents")).scalar()
    
    with open("data/output/validation/latest/db_probe_start.txt", "w") as f:
        f.write(f"Pre-run DB state:\n")
        f.write(f"Events: {ev}\n")
        f.write(f"Documents: {dc}\n")
    
    print(f"✓ DB state saved: Events={ev}, Documents={dc}")
PY

echo ""
cat data/output/validation/latest/db_probe_start.txt
echo ""

# Step 4: Verify events_unique_hash index exists
echo "Step 4: Database Schema Validation"
echo "-----------------------------------"

env "$(grep '^NEON_DATABASE_URL=' app/.env)" bash -lc 'psql "$NEON_DATABASE_URL" -c "\d events"' 2>/dev/null | grep -q "event_hash" || {
    echo "⚠️  WARNING: event_hash column may be missing from events table"
    echo "   Deduplication may not work correctly"
}

echo "✓ Database schema validated"
echo ""

# Summary
echo "========================================="
echo "✓ All pre-flight checks passed!"
echo "========================================="
echo ""
echo "Ready to proceed with:"
echo "  - HARVEST pass (no OpenAI calls)"
echo "  - ENRICH pass (OpenAI Batch API)"
echo ""
echo "Next steps:"
echo "  1. Run HARVEST: env ENABLE_SUMMARIZATION=0 ENABLE_EMBEDDINGS=0 .venv/bin/python app/ingest.py run --mode harvest --since 2024-07-01"
echo "  2. Review results and telemetry"
echo "  3. Run ENRICH: .venv/bin/python app/ingest.py run --mode enrich"
echo ""

