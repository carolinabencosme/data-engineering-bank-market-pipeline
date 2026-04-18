{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

with years as (
    select distinct toYear(price_date) as y
    from {{ source("olap_curated", "cur_stock_daily_price") }}
    where
        price_date >= toDate('{{ var("mart_monthly_start_date") }}')
        and price_date <= toDate('{{ var("mart_monthly_end_date") }}')
)
select 1 as coverage_gate_failed
from numbers(1)
where (select countIf(y = 2024) from years) = 0
   or (select countIf(y = 2025) from years) = 0

{% endif %}
