select
    symbol,
    case
        when lower(holder_type) = 'fund' then 'mutual_fund'
        else lower(holder_type)
    end as holder_type,
    holder_name,
    holdings_date,
    shares,
    market_value,
    pct_outstanding,
    ingested_at,
    source_system,
    batch_id
from {{ source('landing', 'holders') }}