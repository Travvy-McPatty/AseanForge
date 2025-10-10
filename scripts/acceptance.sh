#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Deal-Matching MVP Acceptance Test ==="

# Step A: Load environment
echo "[Step A] Loading environment from app/.env..."
if [[ ! -f app/.env ]]; then
  echo "  ✗ app/.env not found" >&2
  exit 2
fi
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" =~ ^# ]] && continue
  key="${line%%=*}"
  val="${line#*=}"
  export "${key}=${val}"
done < app/.env
echo "  NEON_DATABASE_URL: ${NEON_DATABASE_URL:+set}"
echo "  OPENAI_API_KEY: ${OPENAI_API_KEY:+set}"

# Step B: Check database connectivity (with fallback for older libpq)
echo "[Step B] Checking database connectivity..."
set +e
psql "$NEON_DATABASE_URL" -Atc "SELECT current_database() || '|' || current_user;"
rc=$?
if [[ $rc -eq 0 ]]; then
  echo "  ✓ Connected successfully"
else
  echo "  ⚠ Initial connection failed; retrying without channel_binding=require (libpq <15 compatibility)"
  NEON_NO_CB="${NEON_DATABASE_URL//&channel_binding=require/}"
  NEON_NO_CB="${NEON_NO_CB//?channel_binding=require/}"
  psql "$NEON_NO_CB" -Atc "SELECT current_database() || '|' || current_user;"
  rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "  ✓ Connected with fallback URL (consider upgrading psql/libpq to 16+)"
    export NEON_DATABASE_URL="$NEON_NO_CB"
  else
    echo "  ✗ Database connection failed even with fallback"
    exit 1
  fi
fi
set -e

# Step C: Apply schema (idempotent)
echo "[Step C] Applying database schema..."
psql "$NEON_DATABASE_URL" -f infra/neon/schema.sql

# Step D: Ensure sample data files exist
echo "[Step D] Checking sample data files..."
mkdir -p data/samples
if [ ! -f data/samples/projects.csv ] || [ $(wc -l < data/samples/projects.csv) -lt 2 ]; then
  echo "  ⚠ data/samples/projects.csv missing or empty; using template"
  cp -f data/templates/projects_template.csv data/samples/projects.csv
fi
if [ ! -f data/samples/investors.csv ] || [ $(wc -l < data/samples/investors.csv) -lt 2 ]; then
  echo "  ⚠ data/samples/investors.csv missing or empty; using template"
  cp -f data/templates/investors_template.csv data/samples/investors.csv
fi

# Step E: Import data (idempotent UPSERTs)
echo "[Step E] Importing projects and investors..."
.venv/bin/python app/importer.py projects --csv=data/samples/projects.csv
.venv/bin/python app/importer.py investors --csv=data/samples/investors.csv

# Step F: Run batch matching
echo "[Step F] Running batch matcher..."
.venv/bin/python app/matcher.py batch --min-score=1 --top=5

# Step G: Generate report
echo "[Step G] Generating match report..."
.venv/bin/python app/report_stub.py matches --since-days=7 --top=3

echo ""
echo "=== Acceptance Test Complete ==="

