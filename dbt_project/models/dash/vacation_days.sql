{{ 
	config(materialized='external', 
	location='s3://curated/dash/vacation_days.parquet') 
}}

with result as (
    select
        date_actual,
        weekday_actual,
        weekday_name_short,
        weekday_name_long,
        month_actual,
        month_name_short,
        month_name_long,
        quarter_name,
        year,
    from {{ ref("fct_vacation_days") }} as vacation_days
    left join {{ ref("dim_calendar") }} as calendar
        on vacation_days.calendar_id = calendar.calendar_id
)

select
    *
from result
where month_name_long = 'December'
order by date_actual
