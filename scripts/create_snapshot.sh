#!/bin/bash
# Create Snapshot ZIP
#
# Bundles all deliverables, reports, and telemetry into a timestamped ZIP file.
#
# Usage:
#   bash scripts/create_snapshot.sh

set -e

TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")
SNAPSHOT_NAME="backfill_snapshot_${TIMESTAMP}.zip"
OUTPUT_DIR="data/output/validation/latest"
DELIVERABLES_DIR="${OUTPUT_DIR}/deliverables"
SNAPSHOT_PATH="${DELIVERABLES_DIR}/${SNAPSHOT_NAME}"

echo "=== Creating Snapshot ZIP ==="
echo "Timestamp: ${TIMESTAMP}"
echo "Output: ${SNAPSHOT_PATH}"
echo ""

# Create deliverables directory if it doesn't exist
mkdir -p "${DELIVERABLES_DIR}"

# Generate reports if they don't exist
if [ ! -f "${OUTPUT_DIR}/final_report.md" ]; then
    echo "Generating final report..."
    .venv/bin/python scripts/generate_final_report.py
fi

if [ ! -d "${DELIVERABLES_DIR}" ] || [ -z "$(ls -A ${DELIVERABLES_DIR}/*.csv 2>/dev/null)" ]; then
    echo "Generating CSV deliverables..."
    .venv/bin/python scripts/generate_deliverables.py
fi

# Create temporary staging directory
STAGING_DIR=$(mktemp -d)
trap "rm -rf ${STAGING_DIR}" EXIT

echo "Staging files..."

# Copy reports
cp -f "${OUTPUT_DIR}/final_report.md" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ final_report.md not found"
cp -f "${OUTPUT_DIR}/enrichment_report.md" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ enrichment_report.md not found"

# Copy CSV deliverables
cp -f "${DELIVERABLES_DIR}"/*.csv "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ No CSV deliverables found"

# Copy telemetry
cp -f "${OUTPUT_DIR}/provider_events.csv" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ provider_events.csv not found"
cp -f "${OUTPUT_DIR}/fc_errors.csv" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ fc_errors.csv not found"
cp -f "${OUTPUT_DIR}/robots_blocked.csv" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ robots_blocked.csv not found"

# Copy database snapshots
cp -f "${OUTPUT_DIR}/db_totals_backfill.txt" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ db_totals_backfill.txt not found"
cp -f "${OUTPUT_DIR}/db_auth_counts_backfill.txt" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ db_auth_counts_backfill.txt not found"

# Copy migration log
cp -f "${OUTPUT_DIR}/migration_enrichment_columns.log" "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ migration_enrichment_columns.log not found"

# Copy batch metadata
cp -f data/batch/*.batch.json "${STAGING_DIR}/" 2>/dev/null || echo "  ⚠ No batch metadata found"

# Create ZIP
echo ""
echo "Creating ZIP archive..."

# Ensure output directory exists
mkdir -p "${DELIVERABLES_DIR}"

# Create ZIP from staging directory
(cd "${STAGING_DIR}" && zip -q -r - .) > "${SNAPSHOT_PATH}"

# Get file count and size
FILE_COUNT=$(ls -1 "${STAGING_DIR}" | wc -l | tr -d ' ')
ZIP_SIZE=$(du -h "${SNAPSHOT_PATH}" | cut -f1)

echo ""
echo "✓ Snapshot created successfully"
echo "  Files: ${FILE_COUNT}"
echo "  Size: ${ZIP_SIZE}"
echo "  Path: ${SNAPSHOT_PATH}"
echo ""
echo "Contents:"
unzip -l "${SNAPSHOT_PATH}"

echo ""
echo "=== Snapshot Complete ==="

