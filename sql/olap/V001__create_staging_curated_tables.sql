-- V001__create_staging_curated_tables.sql
-- ClickHouse DDL for staging + curated layers.
-- Deduplication policy:
--   - staging tables use ReplacingMergeTree(ingested_at): retain most recent version per ORDER BY key.
--   - curated tables are fed from staging using argMax(..., ingested_at) aggregation.
--   - both layers keep audit fields for traceability.

CREATE DATABASE IF NOT EXISTS olap;

/* =============================
   STAGING TABLES
   ============================= */

CREATE TABLE IF NOT EXISTS olap.stg_bank_basic_info
(
    symbol String,
    snapshot_date Date,
    company_name Nullable(String),
    industry Nullable(String),
    sector Nullable(String),
    employee_count Nullable(Int64),
    city Nullable(String),
    phone Nullable(String),
    state Nullable(String),
    country Nullable(String),
    website Nullable(String),
    address Nullable(String),
    market_cap Nullable(Decimal(20, 2)),
    currency Nullable(String),
    exchange Nullable(String),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (symbol, snapshot_date)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.stg_stock_daily_price
(
    symbol String,
    price_date Date,
    open_price Decimal(18, 6),
    high_price Decimal(18, 6),
    low_price Decimal(18, 6),
    close_price Decimal(18, 6),
    adjusted_close Nullable(Decimal(18, 6)),
    volume UInt64,
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(price_date)
ORDER BY (symbol, price_date)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.stg_bank_fundamentals
(
    symbol String,
    statement_date Date,
    period_type LowCardinality(String),
    total_assets Nullable(Decimal(22, 2)),
    total_debt Nullable(Decimal(22, 2)),
    invested_capital Nullable(Decimal(22, 2)),
    shares_issued Nullable(Decimal(22, 2)),
    currency Nullable(String),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(statement_date)
ORDER BY (symbol, statement_date, period_type)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.stg_holders
(
    symbol String,
    holder_type LowCardinality(String),
    holder_name String,
    holdings_date Date,
    shares Nullable(Decimal(22, 2)),
    market_value Nullable(Decimal(22, 2)),
    pct_outstanding Nullable(Decimal(9, 6)),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(holdings_date)
ORDER BY (symbol, holdings_date, holder_type, holder_name)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.stg_ratings
(
    symbol String,
    rating_date Date,
    firm_name String,
    to_grade Nullable(String),
    from_grade Nullable(String),
    rating_action Nullable(String),
    recommendation_score Nullable(Decimal(8, 4)),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(rating_date)
ORDER BY (symbol, rating_date, firm_name)
SETTINGS index_granularity = 8192;

/* =============================
   CURATED TABLES (deduplicated)
   ============================= */

CREATE TABLE IF NOT EXISTS olap.cur_bank_basic_info
(
    symbol String,
    snapshot_date Date,
    company_name Nullable(String),
    industry Nullable(String),
    sector Nullable(String),
    employee_count Nullable(Int64),
    city Nullable(String),
    phone Nullable(String),
    state Nullable(String),
    country Nullable(String),
    website Nullable(String),
    address Nullable(String),
    market_cap Nullable(Decimal(20, 2)),
    currency Nullable(String),
    exchange Nullable(String),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (symbol, snapshot_date)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.cur_stock_daily_price
(
    symbol String,
    price_date Date,
    open_price Decimal(18, 6),
    high_price Decimal(18, 6),
    low_price Decimal(18, 6),
    close_price Decimal(18, 6),
    adjusted_close Nullable(Decimal(18, 6)),
    volume UInt64,
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(price_date)
ORDER BY (symbol, price_date)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.cur_bank_fundamentals
(
    symbol String,
    statement_date Date,
    period_type LowCardinality(String),
    total_assets Nullable(Decimal(22, 2)),
    total_debt Nullable(Decimal(22, 2)),
    invested_capital Nullable(Decimal(22, 2)),
    shares_issued Nullable(Decimal(22, 2)),
    currency Nullable(String),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(statement_date)
ORDER BY (symbol, statement_date, period_type)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.cur_holders
(
    symbol String,
    holder_type LowCardinality(String),
    holder_name String,
    holdings_date Date,
    shares Nullable(Decimal(22, 2)),
    market_value Nullable(Decimal(22, 2)),
    pct_outstanding Nullable(Decimal(9, 6)),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(holdings_date)
ORDER BY (symbol, holdings_date, holder_type, holder_name)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS olap.cur_ratings
(
    symbol String,
    rating_date Date,
    firm_name String,
    to_grade Nullable(String),
    from_grade Nullable(String),
    rating_action Nullable(String),
    recommendation_score Nullable(Decimal(8, 4)),
    ingested_at DateTime64(3, 'UTC'),
    source_system LowCardinality(String),
    batch_id String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(rating_date)
ORDER BY (symbol, rating_date, firm_name)
SETTINGS index_granularity = 8192;