-- Falla si el volumen diario es negativo.
select
    symbol,
    price_date,
    volume
from {{ ref('fct_stock_daily') }}
where volume < 0