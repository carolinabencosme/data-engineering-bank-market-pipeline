{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select
    symbol,
    year,
    month,
    avg_open_price,
    avg_close_price,
    avg_volume
from {{ ref("mart_monthly_stock_summary") }}
where
    avg_open_price < 0
    or avg_close_price < 0
    or avg_volume < 0
limit 20

{% endif %}
