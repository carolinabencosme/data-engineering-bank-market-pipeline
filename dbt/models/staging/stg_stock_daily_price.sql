select
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    adjusted_close,
    volume,
    ingested_at,
    source_system,
    batch_id
from {{ source('landing', 'stock_daily_price') }}