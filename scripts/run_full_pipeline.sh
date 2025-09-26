#!/usr/bin/env bash
set -euo pipefail

# Simple, reproducible end-to-end pipeline runner for AseanForge
# Runs: DB init -> Ingest (with embeddings) -> Report -> PDF
# Usage: bash scripts/run_full_pipeline.sh
# Optional env vars:
#   TOPIC        (default: "Vietnam manufacturing FDI trends 2024")
#   TIMEFRAME    (default: "2024")
#   K            (default: 10)
#   MODE         (default: publish)
#   MODEL        (default: o4-mini)
#   BACKEND      (optional: auto|langchain|responses)
#   CONFIG       (default: config/sources.yaml)
#   LIMIT_PER    (default: 10)
#   REALISTIC    (optional; if set non-empty and MODE=draft, passes --realistic)
#   FORCE_DEEP_RESEARCH (optional; if set non-empty and MODE=publish, passes --force-deep-research)

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TOPIC=${TOPIC:-"Vietnam manufacturing FDI trends 2024"}
TIMEFRAME=${TIMEFRAME:-"2024"}
K=${K:-10}
MODE=${MODE:-publish}
MODEL=${MODEL:-}
BACKEND=${BACKEND:-}
CONFIG=${CONFIG:-config/sources.yaml}
LIMIT_PER=${LIMIT_PER:-10}
REALISTIC=${REALISTIC:-}
FORCE_DEEP_RESEARCH=${FORCE_DEEP_RESEARCH:-}

# Resolve Python interpreter preference: venv > python > python3
if [[ -x "./venv/bin/python" ]]; then
  PY="./venv/bin/python"
elif command -v python >/dev/null 2>&1; then
  PY="python"
elif command -v python3 >/dev/null 2>&1; then
  PY="python3"
else
  echo "[error] No Python interpreter found. Please install Python 3.10+ or create a venv." >&2
  exit 1
fi

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
START_TS_FILE=$(date -u +"%Y%m%dT%H%M%SZ")
LOG_DIR="data/output/logs"
BUNDLE_DIR="data/output/bundles"
SLUG="$(echo "$TOPIC" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '_' | sed -e 's/^_//' -e 's/_$//')"
mkdir -p "$LOG_DIR" "$BUNDLE_DIR"


log() { echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"; }

on_error() {
  local ec=$?
  echo "[error] Pipeline failed with exit code $ec" >&2
  exit "$ec"
}
trap on_error ERR

log "Pipeline start at $START_TS"

# 1) Init DB
log "Step 1/4: Initialize database schema"
$PY scripts/init_db.py

# 2) Ingest sources with embeddings (Option A is baked into ingest_sources.py)
log "Step 2/4: Ingesting sources (config=$CONFIG, limit-per-source=$LIMIT_PER)"
$PY scripts/ingest_sources.py --config "$CONFIG" --limit-per-source "$LIMIT_PER"

# 3) Generate report
log "Step 3/4: Generating report (topic=\"$TOPIC\", timeframe=$TIMEFRAME, k=$K, mode=$MODE, model=$MODEL, backend=${BACKEND:-auto})"
REPORT_CMD_OUT=$(mktemp)
set +e
$PY scripts/generate_report.py \
  --topic "$TOPIC" \
  --timeframe "$TIMEFRAME" \
  --k "$K" \
  --mode "$MODE" \
  ${MODEL:+--model "$MODEL"} \
  ${BACKEND:+--backend "$BACKEND"} \
  ${REALISTIC:+--realistic} \
  ${FORCE_DEEP_RESEARCH:+--force-deep-research} | tee "$REPORT_CMD_OUT"
EC=$?
set -e

# Persist run log derived from report generation output
RUN_LOG="$LOG_DIR/pipeline_${START_TS_FILE}_${SLUG:-run}.log"
cp "$REPORT_CMD_OUT" "$RUN_LOG" || true

if [[ $EC -ne 0 ]]; then
  echo "[error] Report generation failed" >&2
  cat "$REPORT_CMD_OUT" >&2 || true
  exit $EC
fi

# Extract produced Markdown path from stdout
REPORT_MD=$(grep -oE 'data/output/[^ ]+\.md' "$REPORT_CMD_OUT" | tail -n1 || true)
if [[ -z "$REPORT_MD" || ! -f "$REPORT_MD" ]]; then
  echo "[error] Unable to locate generated report .md path in output" >&2
  exit 1
fi

# 4) Build PDF from Markdown
REPORT_PDF="${REPORT_MD%.md}.pdf"
log "Step 4/4: Building PDF -> $REPORT_PDF"
$PY scripts/build_pdf.py --input "$REPORT_MD" --output "$REPORT_PDF" --mode auto


# 5) Create client-ready ZIP bundle with artifacts
BASE_NAME="$(basename "$REPORT_MD")"
RPTS_TS="${BASE_NAME#report_}"; RPTS_TS="${RPTS_TS%.md}"
BUNDLE_ZIP="$BUNDLE_DIR/${RPTS_TS}_bundle.zip"
# Build README.txt with run metadata and usage
USAGE_JSONL="data/output/logs/usage_${RPTS_TS}.jsonl"
README_TXT="data/output/README_${RPTS_TS}.txt"
$PY - "$TOPIC" "$TIMEFRAME" "$MODE" "${MODEL:-}" "$K" "$USAGE_JSONL" "$README_TXT" << 'PYEOF'
import sys, json, os, time
TOPIC, TIMEFRAME, MODE, MODEL, K, USAGE, OUT = sys.argv[1:8]
input_tokens = output_tokens = 0
cost = 0.0
if os.path.exists(USAGE):
    try:
        with open(USAGE, 'r', encoding='utf-8') as f:
            last = None
            for line in f:
                line=line.strip()
                if line:
                    last = line
            if last:
                rec = json.loads(last)
                input_tokens = int(rec.get('input_tokens', 0))
                output_tokens = int(rec.get('output_tokens', 0))
                cost = float(rec.get('total_cost_usd', 0.0))
    except Exception:
        pass
with open(OUT, 'w', encoding='utf-8') as f:
    f.write(f"Topic: {TOPIC}\n")
    f.write(f"Timeframe: {TIMEFRAME}\n")
    f.write(f"Mode: {MODE}\n")
    f.write(f"Model: {MODEL or '(auto/strict)'}\n")
    f.write(f"K: {K}\n")
    f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Input tokens: {input_tokens}\n")
    f.write(f"Output tokens: {output_tokens}\n")
    f.write(f"Estimated cost (USD): {cost:.6f}\n")
    f.write("Contact: support@aseanforge.com\n")
PYEOF

FILES=("$REPORT_MD" "$REPORT_PDF" "$README_TXT")
if [[ -f "data/output/ingestion_summary.json" ]]; then FILES+=("data/output/ingestion_summary.json"); fi
if [[ -n "${RUN_LOG:-}" && -f "$RUN_LOG" ]]; then FILES+=("$RUN_LOG"); fi
zip -j "$BUNDLE_ZIP" "${FILES[@]}" >/dev/null
log "Bundle:   $BUNDLE_ZIP"

END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
log "Pipeline complete at $END_TS"

# Final summary
log "Artifacts:"
log "- Markdown: $REPORT_MD"
log "- PDF:      $REPORT_PDF"
log "- Bundle:   $BUNDLE_ZIP"


# Show 2-3 sentence Executive Summary snippet (first lines under Executive Summary)
if command -v awk >/dev/null 2>&1; then
  log "Executive Summary (snippet):"
  awk 'BEGIN{p=0; c=0} /Executive Summary/{p=1; next} p && c<5 {print; c++} c>=5{exit}' "$REPORT_MD" | sed -e 's/^/  /'
fi

