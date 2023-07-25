{{ 
	config(materialized='external', 
	location='s3://curated/dash/actuals.parquet') 
}}

with 

actuals_with_corrections as (
	select
		calendar.date_actual,
		calendar.year::varchar || '-' || calendar.month_zero_added::varchar as month,

		postings.description,

		postings.account_name,
		postings.counter_account_name,

		category.main_category,
		case
			when counter_account_name = 'Lønkonto' then 'Income'
			when counter_account_name = 'Byggekonto' then 'Expense'
			when category.category_type = 'Saving' then 'Expense'
			else category.category_type
		end as category_type,
		category.category,

		postings.amount::decimal(10,2) as amount,

	from {{ ref("fct_postings") }} as postings
	left join {{ ref("dim_calendar") }} as calendar
		on postings.calendar_posting_id = calendar.calendar_id
	left join {{ ref("dim_category") }} as category
 		on postings.category_id = category.category_id
	where account_name = 'C&V Budget'

	union

	select
		cast('1999-01-01' as datetime) as date_actual,
		1900 || '-' || 01::varchar as month,

		'balance correction' as description,

		'C&V budget' as account_name,
		null as counter_account_name,

		'Vis ikke' as main_category,
		'Income' as category_type,
		'Kontooverførsel' as category,

		7042.32 as amount

), 
ordered_actuals_with_corrections as (
	select
		*,
		row_number() over (order by date_actual) as row_number
	from actuals_with_corrections
)

SELECT
	date_actual,
	month,

	description,

	account_name,
	counter_account_name,

	main_category,
	category_type,
	category,

	amount,
	SUM(amount) OVER (
		ORDER BY row_number
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) as balance,
from ordered_actuals_with_corrections
order by row_number desc