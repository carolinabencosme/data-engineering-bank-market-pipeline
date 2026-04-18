-- V002__curated_dedup_policies.sql
-- Deduplication/load policy: staging -> curated, latest row per natural key (max ingested_at).
-- ClickHouse 24+ rejects several argMax(_, ingested_at) together with max/argMax on ingested_at
-- (ILLEGAL_AGGREGATION). We pick the winning row with row_number() instead of mixed aggregates.

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
WHERE _rn = 1;

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
WHERE _rn = 1;

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
WHERE _rn = 1;

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
WHERE _rn = 1;

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
WHERE _rn = 1;
