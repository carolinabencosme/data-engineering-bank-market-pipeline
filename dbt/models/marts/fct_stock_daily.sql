{{ config(
    unique_key='stock_daily_sk'
) }}

select
    md5(symbol || '|' || cast(price_date as text)) as stock_daily_sk,
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
from {{ ref('stg_stock_daily_price') }}
{% if is_incremental() %}
where ingested_at >= (
    select coalesce(max(ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}