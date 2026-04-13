select
    symbol,
    rating_date,
    firm_name,
    to_grade,
    from_grade,
    rating_action,
    recommendation_score,
    ingested_at,
    source_system,
    batch_id
from {{ source('landing', 'ratings') }}