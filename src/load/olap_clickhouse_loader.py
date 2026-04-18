"""
Incremental sync: PostgreSQL landing.* -> ClickHouse olap.stg_*, then rebuild olap.cur_*.

Mirrors sql/olap/V002__curated_dedup_policies.sql for the curated layer (truncate + full reselect).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

import clickhouse_connect
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

LOGGER = logging.getLogger(__name__)

LANDING_INGESTED_AT_WATERMARK_KEY = "bank_market_pipeline__landing_ingested_at_watermark"

STG_BANK_BASIC_COLS = (
    "symbol",
    "snapshot_date",
    "company_name",
    "industry",
    "sector",
    "employee_count",
    "city",
    "phone",
    "state",
    "country",
    "website",
    "address",
    "market_cap",
    "currency",
    "exchange",
    "ingested_at",
    "source_system",
    "batch_id",
)

STG_STOCK_COLS = (
    "symbol",
    "price_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adjusted_close",
    "volume",
    "ingested_at",
    "source_system",
    "batch_id",
)

STG_FUND_COLS = (
    "symbol",
    "statement_date",
    "period_type",
    "total_assets",
    "total_debt",
    "invested_capital",
    "shares_issued",
    "currency",
    "ingested_at",
    "source_system",
    "batch_id",
)

STG_HOLDERS_COLS = (
    "symbol",
    "holder_type",
    "holder_name",
    "holdings_date",
    "shares",
    "market_value",
    "pct_outstanding",
    "ingested_at",
    "source_system",
    "batch_id",
)

STG_RATINGS_COLS = (
    "symbol",
    "rating_date",
    "firm_name",
    "to_grade",
    "from_grade",
    "rating_action",
    "recommendation_score",
    "ingested_at",
    "source_system",
    "batch_id",
)

# Keep in lockstep with sql/olap/V002__curated_dedup_policies.sql (one statement per entry for HTTP driver).
_REBUILD_CURATED_STATEMENTS: tuple[str, ...] = (
    "TRUNCATE TABLE IF EXISTS olap.cur_bank_basic_info",
    "TRUNCATE TABLE IF EXISTS olap.cur_stock_daily_price",
    "TRUNCATE TABLE IF EXISTS olap.cur_bank_fundamentals",
    "TRUNCATE TABLE IF EXISTS olap.cur_holders",
    "TRUNCATE TABLE IF EXISTS olap.cur_ratings",
    """
INSERT INTO olap.cur_bank_basic_info
SELECT
    symbol,
    snapshot_date,
    company_name,
    industry,
    sector,
    employee_count,
    city,
    phone,
    state,
    country,
    website,
    address,
    market_cap,
    currency,
    exchange,
    ingested_at,
    source_system,
    batch_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, snapshot_date
            ORDER BY ingested_at DESC, batch_id DESC, source_system DESC
        ) AS _rn
    FROM olap.stg_bank_basic_info
) AS ranked
WHERE _rn = 1
""".strip(),
    """
INSERT INTO olap.cur_stock_daily_price
SELECT
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close,
    volume,
    ingested_at,
    source_system,
    batch_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, price_date
            ORDER BY ingested_at DESC, batch_id DESC, source_system DESC
        ) AS _rn
    FROM olap.stg_stock_daily_price
) AS ranked
WHERE _rn = 1
""".strip(),
    """
INSERT INTO olap.cur_bank_fundamentals
SELECT
    symbol,
    statement_date,
    period_type,
    total_assets,
    total_debt,
    invested_capital,
    shares_issued,
    currency,
    ingested_at,
    source_system,
    batch_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, statement_date, period_type
            ORDER BY ingested_at DESC, batch_id DESC, source_system DESC
        ) AS _rn
    FROM olap.stg_bank_fundamentals
) AS ranked
WHERE _rn = 1
""".strip(),
    """
INSERT INTO olap.cur_holders
SELECT
    symbol,
    holder_type,
    holder_name,
    holdings_date,
    shares,
    market_value,
    pct_outstanding,
    ingested_at,
    source_system,
    batch_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, holder_type, holder_name, holdings_date
            ORDER BY ingested_at DESC, batch_id DESC, source_system DESC
        ) AS _rn
    FROM olap.stg_holders
) AS ranked
WHERE _rn = 1
""".strip(),
    """
INSERT INTO olap.cur_ratings
SELECT
    symbol,
    rating_date,
    firm_name,
    to_grade,
    from_grade,
    rating_action,
    recommendation_score,
    ingested_at,
    source_system,
    batch_id
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY symbol, rating_date, firm_name
            ORDER BY ingested_at DESC, batch_id DESC, source_system DESC
        ) AS _rn
    FROM olap.stg_ratings
) AS ranked
WHERE _rn = 1
""".strip(),
)


def _parse_watermark(raw: str) -> datetime:
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _ch_session_settings() -> dict[str, int]:
    """
    ReplacingMergeTree tables partition by YYYYMM; wide date ranges in one INSERT exceed the
    server default max_partitions_per_insert_block (100). Raise via env for large backfills.
    """
    raw = os.getenv("CLICKHOUSE_MAX_PARTITIONS_PER_INSERT_BLOCK", "10000").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 10000
    return {"max_partitions_per_insert_block": max(n, 101)}


def _ch_client():
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    # clickhouse-connect uses the HTTP interface (not native TCP 9000).
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


def clickhouse_staging_total_rows() -> int:
    """Row counts across all OLAP staging tables (detect empty ClickHouse after landing was filled)."""
    ch = _ch_client()
    result = ch.query(
        "SELECT "
        "(SELECT count() FROM olap.stg_bank_basic_info)"
        " + (SELECT count() FROM olap.stg_stock_daily_price)"
        " + (SELECT count() FROM olap.stg_bank_fundamentals)"
        " + (SELECT count() FROM olap.stg_holders)"
        " + (SELECT count() FROM olap.stg_ratings)"
    )
    return int(result.result_rows[0][0])


def _rows_after_watermark(
    hook: PostgresHook, sql: str, watermark: datetime
) -> list[dict[str, Any]]:
    conn = hook.get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (watermark,))
            return list(cur.fetchall())
    finally:
        conn.close()


def _as_utc_naive(ts: datetime) -> datetime:
    """DateTime64(3, 'UTC'): driver accepts naive UTC."""
    if ts.tzinfo is None:
        aware = ts.replace(tzinfo=timezone.utc)
    else:
        aware = ts.astimezone(timezone.utc)
    return aware.replace(tzinfo=None)


def _row_tuple_bank_basic(r: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(r["symbol"]),
        r["snapshot_date"],
        r.get("company_name"),
        r.get("industry"),
        r.get("sector"),
        int(r["employee_count"]) if r.get("employee_count") is not None else None,
        r.get("city"),
        r.get("phone"),
        r.get("state"),
        r.get("country"),
        r.get("website"),
        r.get("address"),
        Decimal(str(r["market_cap"])) if r.get("market_cap") is not None else None,
        r.get("currency"),
        r.get("exchange"),
        _as_utc_naive(r["ingested_at"]),
        str(r["source_system"]),
        str(r["batch_id"]),
    )


def _row_tuple_stock(r: Mapping[str, Any]) -> tuple[Any, ...]:
    vol = r["volume"]
    if vol is None:
        raise ValueError("stock_daily_price.volume is required")
    return (
        str(r["symbol"]),
        r["price_date"],
        Decimal(str(r["open_price"])),
        Decimal(str(r["high_price"])),
        Decimal(str(r["low_price"])),
        Decimal(str(r["close_price"])),
        Decimal(str(r["adjusted_close"])) if r.get("adjusted_close") is not None else None,
        int(vol),
        _as_utc_naive(r["ingested_at"]),
        str(r["source_system"]),
        str(r["batch_id"]),
    )


def _row_tuple_fundamentals(r: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(r["symbol"]),
        r["statement_date"],
        str(r["period_type"]),
        Decimal(str(r["total_assets"])) if r.get("total_assets") is not None else None,
        Decimal(str(r["total_debt"])) if r.get("total_debt") is not None else None,
        Decimal(str(r["invested_capital"])) if r.get("invested_capital") is not None else None,
        Decimal(str(r["shares_issued"])) if r.get("shares_issued") is not None else None,
        r.get("currency"),
        _as_utc_naive(r["ingested_at"]),
        str(r["source_system"]),
        str(r["batch_id"]),
    )


def _row_tuple_holders(r: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(r["symbol"]),
        str(r["holder_type"]),
        str(r["holder_name"]),
        r["holdings_date"],
        Decimal(str(r["shares"])) if r.get("shares") is not None else None,
        Decimal(str(r["market_value"])) if r.get("market_value") is not None else None,
        Decimal(str(r["pct_outstanding"])) if r.get("pct_outstanding") is not None else None,
        _as_utc_naive(r["ingested_at"]),
        str(r["source_system"]),
        str(r["batch_id"]),
    )


def _row_tuple_ratings(r: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        str(r["symbol"]),
        r["rating_date"],
        str(r["firm_name"]),
        r.get("to_grade"),
        r.get("from_grade"),
        r.get("rating_action"),
        Decimal(str(r["recommendation_score"])) if r.get("recommendation_score") is not None else None,
        _as_utc_naive(r["ingested_at"]),
        str(r["source_system"]),
        str(r["batch_id"]),
    )


def sync_landing_incremental_to_clickhouse(
    *,
    landing_conn_id: str,
    previous_watermark_raw: str,
) -> dict[str, int]:
    """
    Pull landing rows with ingested_at strictly after the stored watermark, append to ClickHouse staging,
    then truncate curated tables and rebuild them from full staging (same logic as V002).
    """
    watermark = _parse_watermark(previous_watermark_raw)

    hook = PostgresHook(postgres_conn_id=landing_conn_id)
    ch = _ch_client()
    counts: dict[str, int] = {}

    specs: Sequence[tuple[str, str, tuple[str, ...], Any]] = (
        (
            "stg_bank_basic_info",
            "SELECT * FROM landing.bank_basic_info WHERE ingested_at > %s ORDER BY ingested_at",
            STG_BANK_BASIC_COLS,
            _row_tuple_bank_basic,
        ),
        (
            "stg_stock_daily_price",
            "SELECT * FROM landing.stock_daily_price WHERE ingested_at > %s ORDER BY ingested_at",
            STG_STOCK_COLS,
            _row_tuple_stock,
        ),
        (
            "stg_bank_fundamentals",
            "SELECT * FROM landing.bank_fundamentals WHERE ingested_at > %s ORDER BY ingested_at",
            STG_FUND_COLS,
            _row_tuple_fundamentals,
        ),
        (
            "stg_holders",
            "SELECT * FROM landing.holders WHERE ingested_at > %s ORDER BY ingested_at",
            STG_HOLDERS_COLS,
            _row_tuple_holders,
        ),
        (
            "stg_ratings",
            "SELECT * FROM landing.ratings WHERE ingested_at > %s ORDER BY ingested_at",
            STG_RATINGS_COLS,
            _row_tuple_ratings,
        ),
    )

    for table, sql, columns, row_fn in specs:
        rows = _rows_after_watermark(hook, sql, watermark)
        if rows:
            payload = [row_fn(r) for r in rows]
            ch.insert(table, payload, column_names=columns, database="olap")
        counts[table] = len(rows)
        LOGGER.info("OLAP staging %s: inserted %s rows (watermark %s)", table, len(rows), watermark.isoformat())

    for stmt in _REBUILD_CURATED_STATEMENTS:
        ch.command(stmt)
    LOGGER.info("OLAP curated tables rebuilt from staging after incremental load.")

    return counts
