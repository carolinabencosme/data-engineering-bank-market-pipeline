# dbt-postgres + dbt-clickhouse for dual-target project (Postgres analytics + ClickHouse OLAP marts).
ARG DBT_BASE_IMAGE=ghcr.io/dbt-labs/dbt-postgres:1.8.latest
FROM ${DBT_BASE_IMAGE}

USER root
RUN pip install --no-cache-dir "dbt-clickhouse>=1.8.0,<1.9.0"
