{{ config(
    unique_key='symbol'
) }}

with ranked as (
    select
        symbol,
        snapshot_date,
        company_name,
        industry,
        sector,
        employee_count,
        city,
        phone,
        state,
        country,
        website,
        address,
        market_cap,
        currency,
        exchange,
        ingested_at,
        source_system,
        batch_id,
        row_number() over (partition by symbol order by snapshot_date desc, ingested_at desc) as rn
    from {{ ref('stg_bank_basic_info') }}
    {% if is_incremental() %}
    where ingested_at >= (
        select coalesce(max(ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
    )
    {% endif %}
)

select
    symbol,
    snapshot_date,
    company_name,
    industry,
    sector,
    employee_count,
    city,
    phone,
    state,
    country,
    website,
    address,
    market_cap,
    currency,
    exchange,
    ingested_at,
    source_system,
    batch_id
from ranked
where rn = 1