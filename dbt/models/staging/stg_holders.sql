select
    symbol,
    holder_type,
    holder_name,
    holdings_date,
    shares,
    market_value,
    pct_outstanding,
    ingested_at,
    source_system,
    batch_id
from {{ source('landing', 'holders') }}