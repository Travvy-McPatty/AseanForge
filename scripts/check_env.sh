#!/usr/bin/env bash
set -euo pipefail

mask() {
  local s="$1"; local n=${#s}
  if (( n <= 8 )); then echo "****"; else echo "${s:0:8}****"; fi
}

# Load from app/.env if present (safely; support special chars like '&')
if [[ -f "app/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . app/.env
  set +a
fi

# Check NEON_DATABASE_URL
if [[ -z "${NEON_DATABASE_URL:-}" ]]; then
  echo "ERROR: NEON_DATABASE_URL not set. Ensure app/.env contains NEON_DATABASE_URL." >&2
  exit 2
fi
# Normalize quotes around URL for connectivity tests
if [[ -n "${NEON_DATABASE_URL:-}" ]]; then
  if [[ ${NEON_DATABASE_URL} == "'"*"'" ]]; then
    NEON_DATABASE_URL=${NEON_DATABASE_URL:1:${#NEON_DATABASE_URL}-2}
  elif [[ ${NEON_DATABASE_URL} == '"'*'"' ]]; then
    NEON_DATABASE_URL=${NEON_DATABASE_URL:1:${#NEON_DATABASE_URL}-2}
  fi
fi

if [[ "$NEON_DATABASE_URL" != postgresql://* ]]; then
  echo "ERROR: NEON_DATABASE_URL must start with 'postgresql://'. Current: '$NEON_DATABASE_URL'" >&2
  echo "Hint: Do not include quotes and do not use '+psycopg' suffix." >&2
  exit 2
fi

# Print masked
echo "NEON_DATABASE_URL: ok (postgresql://...)"

# psql connectivity
if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql not found in PATH" >&2
  exit 2
fi

set +e
psql "$NEON_DATABASE_URL" -c '\\conninfo' >/dev/null 2>&1
rc=$?
if [[ $rc -ne 0 ]]; then
  # Fallback: strip channel_binding=require which may be unsupported in some local libpq builds
  ALT_URL="$NEON_DATABASE_URL"
  ALT_URL="${ALT_URL//&channel_binding=require/}"
  ALT_URL="${ALT_URL//?channel_binding=require/}"
  if [[ "$ALT_URL" != "$NEON_DATABASE_URL" ]]; then
    psql "$ALT_URL" -c '\\conninfo' >/dev/null 2>&1
    rc=$?
    if [[ $rc -eq 0 ]]; then
      set -e
      echo "psql connectivity: ok (fallback without channel_binding)"
    else
      set -e
      echo "ERROR: Failed to connect to Neon via psql. Check sslmode and credentials." >&2
      echo "Hint: URL should look like postgresql://user:pass@host/db?sslmode=require" >&2
      exit 3
    fi
  else
    set -e
    echo "ERROR: Failed to connect to Neon via psql. Check sslmode and credentials." >&2
    echo "Hint: URL should look like postgresql://user:pass@host/db?sslmode=require" >&2
    exit 3
  fi
else
  set -e
  echo "psql connectivity: ok"
fi

# OPENAI key
if [[ -z "${OPENAI_API_KEY:-}" ]]; then
  echo "OPENAI_API_KEY: not set (importer will skip embeddings)"
else
  echo "OPENAI_API_KEY: $(mask "$OPENAI_API_KEY")"
fi

echo "Environment check complete."

