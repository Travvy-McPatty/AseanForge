#!/usr/bin/env bash
set -euo pipefail
# Load app/.env safely (handles special chars like '&' in URLs)
if [[ ! -f app/.env ]]; then
  echo "ERROR: app/.env not found" >&2
  exit 2
fi
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ -z "$line" || "$line" =~ ^# ]] && continue
  key="${line%%=*}"
  val="${line#*=}"
  export "${key}=${val}"
done < app/.env
echo "NEON_DATABASE_URL set: ${NEON_DATABASE_URL:+yes}"
echo "OPENAI_API_KEY set: ${OPENAI_API_KEY:+yes}"

