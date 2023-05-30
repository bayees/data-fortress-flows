with 
unpivoted as (
    unpivot {{ ref('stg_notion__budget') }}
    on "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
    into
        name month
        value amount
), 
calendar_mapping as (
    select distinct
        lower(month_name_short) as month_name_short,
        month_zero_added
    from {{ ref('stg_calendar') }}
) 

select
    {{ date_to_int("last_day((unpivoted.year || '-' || calendar_mapping.month_zero_added || '-01')::date)") }} as calendar_id,
    {{ dbt_utils.generate_surrogate_key(["unpivoted.budget_category"]) }} as category_id,
    unpivoted.transaction_type,
    unpivoted.amount,
from unpivoted
left join calendar_mapping
    on unpivoted.month = calendar_mapping.month_name_short