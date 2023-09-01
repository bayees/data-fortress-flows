
select 
    {{ date_to_int("effective_date::date") }} as calendar_id
from {{ ref('stg_kapacity_bonus__vacation_hours_detail') }}