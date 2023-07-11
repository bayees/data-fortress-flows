{{ 
	config(materialized='external', 
	location='s3://curated/dash/location.parquet') 
}}

select
	*
from {{ ref("dim_location") }}