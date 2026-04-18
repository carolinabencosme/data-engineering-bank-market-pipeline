{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select symbol, year, month, min_price_date, max_price_date, month_start_date, month_end_date
from {{ ref("mart_monthly_stock_summary") }}
where
    min_price_date > max_price_date
    or month_start_date > month_end_date
limit 20

{% endif %}
