{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select symbol, year, month, count(*) as duplicate_rows
from {{ ref("mart_monthly_stock_summary") }}
group by symbol, year, month
having duplicate_rows > 1

{% endif %}
