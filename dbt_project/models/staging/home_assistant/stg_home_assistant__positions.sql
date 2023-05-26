{{
  config(
    materialized='incremental',
  )
}}

with source_positions as (

    select * from {{ source('external_source', 'home_assistant__states') }}
    where entity_id like 'device_tracker.chriphone'

)
, unwrapped_positions as (

    select 
        last_updated::timestamp as create_at,
        state as location,
        attributes['latitude'][1]::double as latitude_degrees,
        attributes['longitude'][1]::double as longitude_degrees,
        attributes['course'][1]::double as course_degrees,
        attributes['speed'][1]::double as speed_meters_per_second,
        attributes['altitude'][1]::double as altitude_meters,
        attributes['gps_accuracy'][1] as gps_accuracy_meters,
        attributes['vertical_accuracy'][1] as vertical_accuracy_meters,
    FROM source_positions

)

select *
from unwrapped_positions
where latitude_degrees is not null
and longitude_degrees is not null
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and create_at > (select max(create_at) from {{ this }})

{% endif %}