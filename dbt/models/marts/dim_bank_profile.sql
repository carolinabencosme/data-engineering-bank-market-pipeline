{{ config(
    unique_key='symbol'
) }}

with symbols_from_basic as (
    select distinct symbol
    from {{ ref('stg_bank_basic_info') }}
    where symbol is not null
),

symbols_from_prices as (
    select distinct symbol
    from {{ ref('stg_stock_daily_price') }}
    where symbol is not null
),

symbols_from_fundamentals as (
    select distinct symbol
    from {{ ref('stg_bank_fundamentals') }}
    where symbol is not null
),

symbols_from_holders as (
    select distinct symbol
    from {{ ref('stg_holders') }}
    where symbol is not null
),

symbols_from_ratings as (
    select distinct symbol
    from {{ ref('stg_ratings') }}
    where symbol is not null
),

symbol_universe as (
    select symbol from symbols_from_basic
    union
    select symbol from symbols_from_prices
    union
    select symbol from symbols_from_fundamentals
    union
    select symbol from symbols_from_holders
    union
    select symbol from symbols_from_ratings
),

latest_basic as (
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
),

latest_basic_per_symbol as (
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
    from latest_basic
    where rn = 1
),

latest_symbol_activity as (
    select
        symbol,
        max(ingested_at) as latest_ingested_at
    from (
        select symbol, ingested_at from {{ ref('stg_bank_basic_info') }}
        union all
        select symbol, ingested_at from {{ ref('stg_stock_daily_price') }}
        union all
        select symbol, ingested_at from {{ ref('stg_bank_fundamentals') }}
        union all
        select symbol, ingested_at from {{ ref('stg_holders') }}
        union all
        select symbol, ingested_at from {{ ref('stg_ratings') }}
    ) all_activity
    where symbol is not null
    group by symbol
)

select
    u.symbol,
    coalesce(b.snapshot_date, cast(a.latest_ingested_at as date), cast('1900-01-01' as date)) as snapshot_date,
    coalesce(b.company_name, 'UNKNOWN') as company_name,
    coalesce(b.industry, 'UNKNOWN') as industry,
    coalesce(b.sector, 'UNKNOWN') as sector,
    b.employee_count,
    coalesce(b.city, 'UNKNOWN') as city,
    b.phone,
    b.state,
    coalesce(b.country, 'UNKNOWN') as country,
    b.website,
    b.address,
    b.market_cap,
    b.currency,
    b.exchange,
    coalesce(b.ingested_at, a.latest_ingested_at, current_timestamp) as ingested_at,
    coalesce(b.source_system, 'UNKNOWN') as source_system,
    coalesce(b.batch_id, 'UNKNOWN') as batch_id
from symbol_universe u
left join latest_basic_per_symbol b
    on b.symbol = u.symbol
left join latest_symbol_activity a
    on a.symbol = u.symbol