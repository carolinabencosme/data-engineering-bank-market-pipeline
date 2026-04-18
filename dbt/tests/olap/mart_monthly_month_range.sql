{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select symbol, year, month
from {{ ref("mart_monthly_stock_summary") }}
where month < 1 or month > 12
limit 20

{% endif %}
