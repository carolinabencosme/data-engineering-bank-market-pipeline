#!/bin/sh
# Applied by clickhouse-schema-init service. Keep POSIX sh (no bashisms).
set -eu

i=0
until clickhouse-client --host clickhouse --query "SELECT 1" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "ClickHouse is not reachable from schema init container" >&2
    exit 1
  fi
  sleep 2
done

clickhouse-client --host clickhouse --multiquery < /workspace/sql/olap/V001__create_staging_curated_tables.sql
# V002 INSERTs into monthly-partitioned cur_*; large staging backfills exceed default 100 partitions.
clickhouse-client --host clickhouse --max_partitions_per_insert_block=10000 --multiquery < /workspace/sql/olap/V002__curated_dedup_policies.sql
