-- Falla si high/low/open/close incumplen la lógica de rangos OHLC.
select
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('fct_stock_daily') }}
where
    high_price < low_price
    or high_price < greatest(open_price, close_price)
    or low_price > least(open_price, close_price)
    or open_price < 0
    or high_price < 0
    or low_price < 0
    or close_price < 0