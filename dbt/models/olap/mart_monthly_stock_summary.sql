{{
  config(
    materialized='table',
    engine="MergeTree()",
    order_by='(symbol, year, month)',
    partition_by="toYYYYMM(month_start_date)",
  )
}}

{# TABLE en ClickHouse: rebuild determinista; volumen acotado. Solo target `olap` (ver dbt_project olap +enabled). #}

WITH filtered_daily AS (
    SELECT
        symbol,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume
    FROM {{ source('olap_curated', 'cur_stock_daily_price') }}
    WHERE
        price_date >= toDate('{{ var("mart_monthly_start_date") }}')
        AND price_date <= toDate('{{ var("mart_monthly_end_date") }}')
        AND open_price >= 0
        AND close_price >= 0
        AND volume >= 0
        AND high_price >= low_price
        AND high_price >= open_price
        AND high_price >= close_price
        AND low_price <= open_price
        AND low_price <= close_price
),
basics AS (
    SELECT
        symbol,
        argMax(company_name, ingested_at) AS company_name
    FROM {{ source('olap_curated', 'cur_bank_basic_info') }}
    GROUP BY symbol
)
SELECT
    f.symbol AS symbol,
    b.company_name AS company_name,
    toYear(f.price_date) AS year,
    toMonth(f.price_date) AS month,
    toStartOfMonth(f.price_date) AS month_start_date,
    toLastDayOfMonth(f.price_date) AS month_end_date,
    avg(f.open_price) AS avg_open_price,
    avg(f.close_price) AS avg_close_price,
    avg(f.volume) AS avg_volume,
    min(f.open_price) AS min_open_price,
    max(f.close_price) AS max_close_price,
    toUInt64(uniqExact(f.price_date)) AS trading_days_count,
    min(f.price_date) AS min_price_date,
    max(f.price_date) AS max_price_date,
    toUInt64(count()) AS source_row_count,
    now64(3, 'UTC') AS loaded_at
FROM filtered_daily AS f
LEFT JOIN basics AS b ON f.symbol = b.symbol
GROUP BY
    f.symbol,
    b.company_name,
    toYear(f.price_date),
    toMonth(f.price_date),
    toStartOfMonth(f.price_date),
    toLastDayOfMonth(f.price_date)
