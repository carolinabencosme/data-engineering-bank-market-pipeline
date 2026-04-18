{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select year
from {{ ref("mart_monthly_stock_summary") }}
where year not in (2024, 2025)
limit 5

{% endif %}
