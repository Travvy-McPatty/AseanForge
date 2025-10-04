#!/usr/bin/env bash
set -euo pipefail

# Weekly Tier-1 + KOMDIGI refresh (Phases A–D via flagship pipeline)
# Authorities coverage: KOMDIGI, MAS, SC, PDPC, MIC, BI, OJK, IMDA, SBV, BSP, BOT, ASEAN
# Exit code mirrors pipeline result (0 on success; non-zero if pipeline exits due to blockers)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Activate venv
if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

# Load env
if [[ -f app/.env ]]; then
  # shellcheck disable=SC2046
  export $(grep -E -v '^#|^$' app/.env | xargs)
fi

LOG_DIR="data/output/validation/latest"
mkdir -p "$LOG_DIR"

echo "[Weekly Tier-1] Budgets: Firecrawl \u2264 200 URLs, OpenAI Batch \u2264 $5"

# Run the flagship pipeline (includes embeddings, RAG eval, and flagship regeneration)
set +e
.venv/bin/python scripts/run_flagship_v1_1.py
rc=$?
set -e

# Collect RAG metric if available
hit="N/A"; avg="N/A"; p95="N/A"
if [[ -f "$LOG_DIR/rag_eval_results.json" ]]; then
  hit=$(python - <<'PY'
import json,sys
p='data/output/validation/latest/rag_eval_results.json'
try:
    with open(p) as f:
        j=json.load(f)
    print(j.get('hit_rate_at_5', 'N/A'))
except Exception:
    print('N/A')
PY
  )
fi

{
  echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "Weekly pipeline exit: $rc"
  echo "RAG Hit@5: $hit"
} | tee "$LOG_DIR/weekly_tier1_status.txt"

exit $rc

