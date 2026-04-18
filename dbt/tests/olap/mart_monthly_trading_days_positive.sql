{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select symbol, year, month, trading_days_count
from {{ ref("mart_monthly_stock_summary") }}
where trading_days_count <= 0
limit 20

{% endif %}
