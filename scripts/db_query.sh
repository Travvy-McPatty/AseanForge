#!/usr/bin/env bash
set -euo pipefail
# Usage: scripts/db_query.sh "SQL"
SQL="$1"
# Load only NEON DB URL into env for this subshell
exec env "$(grep '^NEON_DATABASE_URL=' app/.env)" sh -lc 'psql "$NEON_DATABASE_URL" -c "$SQL"'

