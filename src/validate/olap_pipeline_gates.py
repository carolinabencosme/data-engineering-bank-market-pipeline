"""
Operational quality gates between PostgreSQL landing and ClickHouse OLAP.

Used by Airflow to fail fast when curated OLAP or the monthly mart are empty
while upstream data exists (avoids false success).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import clickhouse_connect
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)


def _ch_session_settings() -> dict[str, int]:
    raw = os.getenv("CLICKHOUSE_MAX_PARTITIONS_PER_INSERT_BLOCK", "10000").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 10000
    return {"max_partitions_per_insert_block": max(n, 101)}


def _clickhouse_client() -> Any:
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123")))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        settings=_ch_session_settings(),
    )


def _scalar_ch(client: Any, sql: str) -> int:
    return int(client.query(sql).result_rows[0][0])


def validate_landing_stock_implies_cur_daily(*, landing_conn_id: str) -> dict[str, int]:
    """
    Fail if landing has stock_daily rows but ClickHouse cur_stock_daily_price is empty.
    """
    started = time.perf_counter()
    hook = PostgresHook(postgres_conn_id=landing_conn_id)
    landing_n = int(hook.get_first("SELECT COUNT(*) FROM landing.stock_daily_price")[0] or 0)
    ch = _clickhouse_client()
    cur_n = _scalar_ch(ch, "SELECT count() FROM olap.cur_stock_daily_price")
    elapsed_s = round(time.perf_counter() - started, 3)
    payload = {
        "landing_stock_daily_rows": landing_n,
        "olap_cur_stock_daily_rows": cur_n,
        "gate_duration_seconds": elapsed_s,
    }
    LOGGER.info("[validate_landing_stock_implies_cur_daily] %s", payload)
    if landing_n > 0 and cur_n == 0:
        raise AirflowFailException(
            "Quality gate: landing.stock_daily_price tiene datos pero olap.cur_stock_daily_price está vacío. "
            f"Detalle: {payload}"
        )
    return payload


def validate_cur_daily_implies_monthly_mart() -> dict[str, int]:
    """
    Fail if curated daily prices exist in-window but mart_monthly_stock_summary is empty.
    Window matches dbt vars mart_monthly_* (defaults 2024-01-01 .. 2025-12-31).
    """
    start = os.getenv("PIPELINE_MART_START_DATE", "2024-01-01")
    end = os.getenv("PIPELINE_MART_END_DATE", "2025-12-31")
    started = time.perf_counter()
    ch = _clickhouse_client()
    cur_n = _scalar_ch(
        ch,
        "SELECT count() FROM olap.cur_stock_daily_price "
        f"WHERE price_date >= toDate('{start}') AND price_date <= toDate('{end}')",
    )
    mart_n = _scalar_ch(ch, "SELECT count() FROM olap.mart_monthly_stock_summary")
    sym_n = _scalar_ch(ch, "SELECT uniqExact(symbol) FROM olap.mart_monthly_stock_summary")
    elapsed_s = round(time.perf_counter() - started, 3)
    payload = {
        "cur_stock_daily_rows_in_window": cur_n,
        "mart_monthly_rows": mart_n,
        "mart_distinct_symbols": sym_n,
        "window_start": start,
        "window_end": end,
        "gate_duration_seconds": elapsed_s,
    }
    LOGGER.info("[validate_cur_daily_implies_monthly_mart] %s", payload)
    if cur_n > 0 and mart_n == 0:
        raise AirflowFailException(
            "Quality gate: olap.cur_stock_daily_price tiene datos en la ventana del mart pero "
            "olap.mart_monthly_stock_summary está vacío. Ejecute dbt run --target olap para el mart. "
            f"Detalle: {payload}"
        )
    return payload
