{{ 
	config(materialized='external', 
	location='s3://dash/balance.parquet') 
}}

with a AS (
	SELECT
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

		case 
			when row_number() over (PARTITION BY date_trunc('month', date_actual) order by date_actual) = 1 then balance - amount
			else 0
		end as _initial_balance,
		row_number() over (order by date_actual) as row_number
	FROM {{ ref("fct_postings") }} as postings
	LEFT JOIN {{ ref("dim_calendar") }} as calendar
		ON postings.calendar_posting_id = calendar.calendar_id
	left join {{ ref("dim_category") }} as category
 		on postings.category_id = category.category_id
	WHERE account_name = 'C&V Budget'
	order by date_actual desc
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
	_initial_balance,
	SUM(amount + _initial_balance) OVER (
		PARTITION BY date_trunc('month', date_actual)
		ORDER BY row_number
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) as balance,
from a
order by row_number desc