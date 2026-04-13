-- Falla si el último dato cargado tiene más de 3 días de antigüedad.
with max_load as (
    select max(ingested_at) as max_ingested_at
    from {{ ref('fct_stock_daily') }}
)
select
    max_ingested_at
from max_load
where max_ingested_at < (current_timestamp - interval '3 day')
   or max_ingested_at is null