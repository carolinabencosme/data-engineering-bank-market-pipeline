{{ config(
    unique_key='ratings_sk'
) }}

select
    md5(symbol || '|' || cast(rating_date as text) || '|' || firm_name) as ratings_sk,
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
from {{ ref('stg_ratings') }}
{% if is_incremental() %}
where ingested_at >= (
    select coalesce(max(ingested_at), cast('1900-01-01' as timestamp)) from {{ this }}
)
{% endif %}