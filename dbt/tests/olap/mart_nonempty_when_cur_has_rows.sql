{% if target.type != 'clickhouse' %}
select 1 where 1 = 0
{% else %}

select 1 as quality_gate_failed
from numbers(1)
where (
        select count()
        from {{ source("olap_curated", "cur_stock_daily_price") }}
        where
            price_date >= toDate('{{ var("mart_monthly_start_date") }}')
            and price_date <= toDate('{{ var("mart_monthly_end_date") }}')
    ) > 0
    and (select count() from {{ ref("mart_monthly_stock_summary") }}) = 0

{% endif %}
