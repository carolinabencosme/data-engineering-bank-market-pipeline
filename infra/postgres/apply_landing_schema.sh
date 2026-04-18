#!/bin/sh
# Applied by postgres-schema-init service. Keep POSIX sh (no bashisms).
set -eu

echo "Waiting for PostgreSQL from schema-init container..."
i=0
until pg_isready -h postgres -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 60 ]; then
    echo "PostgreSQL is not reachable from postgres-schema-init" >&2
    exit 1
  fi
  sleep 2
done

echo "Checking landing DDL file..."
test -f /workspace/sql/landing/V001__create_landing_tables.sql

echo "Applying landing schema..."
export PGPASSWORD="${POSTGRES_PASSWORD}"
psql -v ON_ERROR_STOP=1 \
  -h postgres \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -f /workspace/sql/landing/V001__create_landing_tables.sql

echo "Verifying landing tables..."
created_count="$(
  psql -t -A \
    -h postgres \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'landing' AND tablename IN ('bank_basic_info','bank_fundamentals','holders','ratings','stock_daily_price');" |
    tr -d '[:space:]'
)"

if [ "$created_count" != "5" ]; then
  echo "Landing schema verification failed. Expected 5 tables, got ${created_count}" >&2
  exit 1
fi

echo "Landing schema initialized successfully."
