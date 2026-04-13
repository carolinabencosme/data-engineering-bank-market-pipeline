{{ config(
    unique_key='fundamentals_sk'
) }}

select
    md5(symbol || '|' || cast(statement_date as text) || '|' || period_type) as fundamentals_sk,
    symbol,
    statement_date,
    period_type,
    total_assets,
    total_debt,
    invested_capital,
    shares_issued,
    currency,
    ingested_at,
    source_system,
    batch_id
from {{ ref('stg_bank_fundamentals') }}
{% if is_incremental() %}
where ingested_at >= (
    select coalesce(max(ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}