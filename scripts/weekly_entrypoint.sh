#!/usr/bin/env bash
set -euo pipefail

WEEK_DIR="data/output/weekly/$(date +%Y%m%d)"
mkdir -p "$WEEK_DIR"

echo "=== Weekly Refresh: $(date) ==="

# Step 1: Harvest
bash -lc 'set -a; source app/.env || true; set +a; .venv/bin/python scripts/coverage_expansion_step2_canonical.py' \
  | tee "$WEEK_DIR/harvest.log"

# Step 2: Enrich
bash -lc 'set -a; source app/.env || true; set +a; .venv/bin/python scripts/coverage_expansion_step3_micro_enrich.py' \
  | tee "$WEEK_DIR/enrich.log"

# Step 3: QA
bash -lc 'set -a; source app/.env || true; set +a; .venv/bin/python scripts/coverage_expansion_step4_qa_kpis.py' \
  | tee "$WEEK_DIR/qa.log"

# Step 4: Snapshot
bash -lc 'set -a; source app/.env || true; set +a; .venv/bin/python scripts/coverage_expansion_step5_sales_pack.py' \
  | tee "$WEEK_DIR/snapshot.log"

echo "=== Weekly refresh complete. Check $WEEK_DIR for logs. ==="

