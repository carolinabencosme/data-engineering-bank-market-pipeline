{% snapshot snap_dim_bank_profile %}

{{
    config(
      target_schema='snapshots',
      unique_key='symbol',
      strategy='timestamp',
      updated_at='ingested_at',
      invalidate_hard_deletes=True
    )
}}

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
from {{ ref('dim_bank_profile') }}

{% endsnapshot %}