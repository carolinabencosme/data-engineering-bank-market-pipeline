{% if target.type == 'clickhouse' %}
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(symbol, month_date)',
    partition_by='toYYYYMM(month_date)'
) }}
{% else %}
{{ config(materialized='table') }}
{% endif %}

with quality_filtered as (
    select
        symbol,
        cast({{ dbt.date_trunc('month', 'price_date') }} as date) as month_date,
        open_price,
        close_price,
        volume
    from {{ ref('fct_stock_daily') }}
    where
        symbol is not null
        and price_date is not null
        and open_price is not null
        and close_price is not null
        and volume is not null
        and open_price >= 0
        and close_price >= 0
        and volume >= 0
)

select
    symbol,
    month_date,
    coalesce(avg(open_price), 0) as avg_open_price,
    coalesce(avg(close_price), 0) as avg_close_price,
    coalesce(avg(volume), 0) as avg_volume
from quality_filtered
group by
    symbol,
    month_date