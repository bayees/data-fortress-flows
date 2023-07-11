{{ 
	config(materialized='external', 
	location='s3://curated/dash/budget.parquet') 
}}

select
 	-- dimensions
 	category.category,
	transaction_type AS category_type,
 	category.expense_type,
	category.main_category,
	
	
	calendar.year::int as year,
	calendar.month_actual::int as month_actual,
	calendar.month_name_long,
	calendar.month_name_short,
	calendar.month_zero_added,
 	cast(calendar.year as varchar) || '-' || cast(calendar.month_zero_added as varchar) as month,
	
	-- metrics
	budget.amount::decimal(10, 2) as amount,
from {{ ref("fct_budget") }} as budget
left join {{ ref("dim_calendar") }} as calendar
	on budget.calendar_id = calendar.calendar_id
left join {{ ref("dim_category") }} as category
	on budget.category_id = category.category_id