{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price
from {{ source("olap_curated", "cur_stock_daily_price") }}
where
    price_date >= toDate('{{ var("mart_monthly_start_date") }}')
    and price_date <= toDate('{{ var("mart_monthly_end_date") }}')
    and (
        high_price < low_price
        or high_price < open_price
        or high_price < close_price
        or low_price > open_price
        or low_price > close_price
    )
limit 50

{% endif %}
