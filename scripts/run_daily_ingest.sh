#!/usr/bin/env bash
# Daily AseanForge ingestion runner
# Usage: bash scripts/run_daily_ingest.sh
# Optional env vars:
#   INGEST_LIMIT_PER_SOURCE (default 8)
#   LOG_DIR (default logs)

set -euo pipefail

cd "$(dirname "$0")/.."

: "${INGEST_LIMIT_PER_SOURCE:=8}"
: "${LOG_DIR:=logs}"
mkdir -p "$LOG_DIR"

ts="$(date +%Y-%m-%d_%H-%M-%S)"
log="$LOG_DIR/ingest_${ts}.log"

# Prefer venv python if present
PYTHON_BIN="./venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

echo "[$(date -u +%FT%TZ)] Starting daily ingest ..." | tee -a "$log"
{
  echo "[$(date -u +%FT%TZ)] Running ingestion with limit-per-source=$INGEST_LIMIT_PER_SOURCE"
  "$PYTHON_BIN" scripts/ingest_sources.py --limit-per-source "$INGEST_LIMIT_PER_SOURCE"
} >> "$log" 2>&1 || {
  echo "[$(date -u +%FT%TZ)] Ingestion failed (see $log)" | tee -a "$log"
  exit 1
}

echo "[$(date -u +%FT%TZ)] Ingestion completed successfully" | tee -a "$log"

