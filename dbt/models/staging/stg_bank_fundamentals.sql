select
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
from {{ source('landing', 'bank_fundamentals') }}