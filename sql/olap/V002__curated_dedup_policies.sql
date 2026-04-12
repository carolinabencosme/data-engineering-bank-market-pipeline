-- V002__curated_dedup_policies.sql
-- Deduplication/load policy from staging -> curated.
-- Rule: for each business key, keep the row with max(ingested_at).
-- Execution mode: full refresh or incremental per batch_id/date partition.

INSERT INTO olap.cur_bank_basic_info
SELECT
    symbol,
    snapshot_date,
    argMax(company_name, ingested_at) AS company_name,
    argMax(industry, ingested_at) AS industry,
    argMax(sector, ingested_at) AS sector,
    argMax(employee_count, ingested_at) AS employee_count,
    argMax(city, ingested_at) AS city,
    argMax(phone, ingested_at) AS phone,
    argMax(state, ingested_at) AS state,
    argMax(country, ingested_at) AS country,
    argMax(website, ingested_at) AS website,
    argMax(address, ingested_at) AS address,
    argMax(market_cap, ingested_at) AS market_cap,
    argMax(currency, ingested_at) AS currency,
    argMax(exchange, ingested_at) AS exchange,
    max(ingested_at) AS ingested_at,
    argMax(source_system, ingested_at) AS source_system,
    argMax(batch_id, ingested_at) AS batch_id
FROM olap.stg_bank_basic_info
GROUP BY symbol, snapshot_date;

INSERT INTO olap.cur_stock_daily_price
SELECT
    symbol,
    price_date,
    argMax(open_price, ingested_at) AS open_price,
    argMax(high_price, ingested_at) AS high_price,
    argMax(low_price, ingested_at) AS low_price,
    argMax(close_price, ingested_at) AS close_price,
    argMax(adjusted_close, ingested_at) AS adjusted_close,
    argMax(volume, ingested_at) AS volume,
    max(ingested_at) AS ingested_at,
    argMax(source_system, ingested_at) AS source_system,
    argMax(batch_id, ingested_at) AS batch_id
FROM olap.stg_stock_daily_price
GROUP BY symbol, price_date;

INSERT INTO olap.cur_bank_fundamentals
SELECT
    symbol,
    statement_date,
    period_type,
    argMax(total_assets, ingested_at) AS total_assets,
    argMax(total_debt, ingested_at) AS total_debt,
    argMax(invested_capital, ingested_at) AS invested_capital,
    argMax(shares_issued, ingested_at) AS shares_issued,
    argMax(currency, ingested_at) AS currency,
    max(ingested_at) AS ingested_at,
    argMax(source_system, ingested_at) AS source_system,
    argMax(batch_id, ingested_at) AS batch_id
FROM olap.stg_bank_fundamentals
GROUP BY symbol, statement_date, period_type;

INSERT INTO olap.cur_holders
SELECT
    symbol,
    holder_type,
    holder_name,
    holdings_date,
    argMax(shares, ingested_at) AS shares,
    argMax(market_value, ingested_at) AS market_value,
    argMax(pct_outstanding, ingested_at) AS pct_outstanding,
    max(ingested_at) AS ingested_at,
    argMax(source_system, ingested_at) AS source_system,
    argMax(batch_id, ingested_at) AS batch_id
FROM olap.stg_holders
GROUP BY symbol, holder_type, holder_name, holdings_date;

INSERT INTO olap.cur_ratings
SELECT
    symbol,
    rating_date,
    firm_name,
    argMax(to_grade, ingested_at) AS to_grade,
    argMax(from_grade, ingested_at) AS from_grade,
    argMax(rating_action, ingested_at) AS rating_action,
    argMax(recommendation_score, ingested_at) AS recommendation_score,
    max(ingested_at) AS ingested_at,
    argMax(source_system, ingested_at) AS source_system,
    argMax(batch_id, ingested_at) AS batch_id
FROM olap.stg_ratings
GROUP BY symbol, rating_date, firm_name;