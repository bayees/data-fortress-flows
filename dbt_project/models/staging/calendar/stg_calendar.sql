{{
  config(
    materialized='incremental',
    unique_key='date_actual'
  )
}}

with source as (
      select * from {{ source('external_source', 'calendar') }}
),
renamed as (
    select
        *
    from source
)
select 
  * 
from renamed
