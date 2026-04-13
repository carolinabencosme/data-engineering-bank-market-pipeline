-- Falla si existe más de una fila por combinación symbol + month_date.
select
    symbol,
    month_date,
    count(*) as row_count
from {{ ref('monthly_bank_stock_summary') }}
group by
    symbol,
    month_date
having count(*) > 1