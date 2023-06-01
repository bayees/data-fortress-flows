{{
    config(
        materialized='incremental'
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
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where date_actual > (select max(date_actual) from {{ this }})
{% endif %}