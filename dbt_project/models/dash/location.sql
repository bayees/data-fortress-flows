{{ 
	config(materialized='external', 
	location='s3://dash/location.parquet') 
}}

select
	*
from {{ ref("dim_location") }}