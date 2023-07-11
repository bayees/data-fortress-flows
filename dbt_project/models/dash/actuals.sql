{{ 
	config(materialized='external', 
	location='s3://curated/dash/actuals.parquet') 
}}

select
	-- Dimensions
	calendar.date_actual,
	calendar.year::varchar || '-' || calendar.month_zero_added::varchar as month,
	
	postings.description,
	
	category.main_category,
	postings.account_name,
    postings.counter_account_name,
    case
		when counter_account_name = 'Lønkonto' then 'Income'
		when counter_account_name = 'Byggekonto' then 'Expense'
		when category.category_type = 'Saving' then 'Expense'
		else category.category_type
	end as category_type,
	category.category,	
	
	-- Metrics
	postings.amount::decimal(10,2) as amount,
	postings.balance::decimal(10,2) as balance,
from {{ ref("fct_postings") }} as postings
left join {{ ref("dim_calendar") }} as calendar
 	on postings.calendar_posting_id = calendar.calendar_id
left join {{ ref("dim_category") }} as category
 	on postings.category_id = category.category_id
where account_name = 'C&V Budget'