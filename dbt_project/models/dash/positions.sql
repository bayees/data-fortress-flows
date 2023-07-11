{{ 
	config(materialized='external', 
	location='s3://curated/dash/positions.parquet') 
}}

select
	location.latitude_degrees as latitude,
	location.longitude_degrees as longitude,
	
	positions.known_location as location_of_interest,
	
	calendar.date_actual,

	time.second_string,
	
	-- Metrics
	positions.duration_minutes,
	0 as distance_meter
	
from {{ ref("fct_positions") }} as positions
left join {{ ref("dim_location") }} as location
 	on positions.location_id = location.location_id
left join {{ ref("dim_calendar") }} as calendar
	on positions.event_date_id = calendar.calendar_id
left join {{ ref("dim_time") }} as time
 	on positions.event_time_id = time.time_id