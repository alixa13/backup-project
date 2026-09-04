#!/usr/bin/env bash
# Apply every ClickHouse DDL migration in lexical order.
#
# Idempotent: all statements are CREATE ... IF NOT EXISTS, so re-running is safe.
# Reads the CLICKHOUSE_* variables documented in .env.example.
set -euo pipefail

# Connection settings for the target ClickHouse server. Every variable is
# overridable from the environment; the defaults below match a local dev
# ClickHouse started with no auth and the default HTTP port.
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-netsec_ml}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

# Resolve the repository root from this script's own location, so the script
# works from any working directory.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DDL_DIR="${REPO_ROOT}/infrastructure/clickhouse/ddl"
BASE_URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"

# POST one SQL statement. $2 is the database, or empty for server-level DDL.
post() {
  local sql="$1" db="${2:-}" url="${BASE_URL}/?"
  if [ -n "$db" ]; then
    url="${url}database=${db}&"
  fi
  curl --fail --silent --show-error \
       --user "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
       --data-binary "$sql" "$url"
}

# Apply one .sql file. The ClickHouse HTTP interface accepts a single statement
# per request, so line comments are stripped first (a ';' inside a comment would
# otherwise split a statement) and the remainder is executed statement by
# statement.
apply_file() {
  local file="$1" statement
  while IFS= read -r -d ';' statement; do
    if [ -n "${statement//[[:space:]]/}" ]; then
      post "$statement" "${CLICKHOUSE_DATABASE}"
    fi
  done < <(sed 's/--.*//' "$file")
}

# The DDL files assume the target database already exists; create it up front
# so the first-ever run against a fresh server has somewhere to apply tables.
echo "Creating database ${CLICKHOUSE_DATABASE} if absent"
post "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}" ""

# nullglob: if no .sql files are present yet, the glob expands to nothing (not
# the literal pattern), so the loop body simply doesn't run instead of erroring.
shopt -s nullglob
for ddl in "${DDL_DIR}"/*.sql; do
  echo "Applying $(basename "$ddl")"
  apply_file "$ddl"
done

echo "DDL applied to ${CLICKHOUSE_DATABASE}"
