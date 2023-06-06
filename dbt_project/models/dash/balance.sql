{{ 
	config(materialized='external', 
	location='s3://dash/balance.parquet') 
}}

with a AS (
	SELECT
		date_actual,
		amount,
		case 
			when row_number() over (PARTITION BY date_trunc('month', date_actual) order by date_actual) = 1 then balance - amount
			else 0
		end as initial_balance,
		row_number() over (order by date_actual) as row_number
	FROM {{ ref("fct_postings") }} as postings
	LEFT JOIN {{ ref("dim_calendar") }} as calendar
		ON postings.calendar_posting_id = calendar.calendar_id
	WHERE account_name = 'C&V Budget'
	order by date_actual desc
)

SELECT
	date_trunc('month', date_actual) as month,
	date_actual,
	amount,
	initial_balance,
	SUM(amount + initial_balance) OVER (
		PARTITION BY date_trunc('month', date_actual)
		ORDER BY row_number
		ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	) as accumulated_amount,
	row_number
from a
order by row_number desc