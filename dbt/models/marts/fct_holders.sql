{{ config(
    unique_key='holders_sk'
) }}

select
    md5(symbol || '|' || coalesce(holder_type, '') || '|' || holder_name || '|' || cast(holdings_date as text)) as holders_sk,
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
from {{ ref('stg_holders') }}
{% if is_incremental() %}
where ingested_at >= (
    select coalesce(max(ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}