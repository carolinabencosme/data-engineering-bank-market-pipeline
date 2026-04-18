{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select symbol, year, month, source_row_count, trading_days_count
from {{ ref("mart_monthly_stock_summary") }}
where source_row_count < trading_days_count
limit 20

{% endif %}
