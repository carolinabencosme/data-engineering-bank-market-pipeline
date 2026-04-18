ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.11.2
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow
RUN pip install --no-cache-dir dbt-postgres dbt-clickhouse clickhouse-connect "yfinance>=0.2.40,<0.3"
