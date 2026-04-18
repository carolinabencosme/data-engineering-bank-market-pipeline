{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

with cur_syms as (
    select distinct symbol
    from {{ source("olap_curated", "cur_stock_daily_price") }}
    where
        price_date >= toDate('{{ var("mart_monthly_start_date") }}')
        and price_date <= toDate('{{ var("mart_monthly_end_date") }}')
),
mart_syms as (
    select distinct symbol
    from {{ ref("mart_monthly_stock_summary") }}
)
select c.symbol as symbol_missing_from_mart
from cur_syms as c
left join mart_syms as m on c.symbol = m.symbol
where m.symbol is null
limit 50

{% endif %}
