with positions as (

    SELECT * FROM {{ ref('stg_home_assistant__positions') }}

)
, prepared_positions as (

    select 
        {{ dbt_utils.generate_surrogate_key(['latitude_degrees', 'longitude_degrees']) }} as location_id,
        gps_accuracy_meters,
        create_at,
        lag(create_at, 1) over (order by create_at) as last_create_at,
        latitude_degrees,
        longitude_degrees,
        lag(latitude_degrees, 1) over (order by create_at)  AS last_latitude_degrees,
        lag(longitude_degrees, 1) over (order by create_at)  AS last_latitude_degrees,
        speed_meters_per_second,
        course_degrees,
        location
    from positions

)
, calculated_durations as (
    
    select
        location_id,
        gps_accuracy_meters,
        {{ date_to_int('create_at') }}  as event_date_id,
        {{ time_to_int('create_at') }} as event_time_id,
        date_diff('minute', last_create_at, create_at) as duration_minutes,
        speed_meters_per_second * 3.6 as speed_kilometers_per_hour,
        course_degrees,
        case
            when location = 'not_home' then 'Unknown'
            when location = 'home' then 'Home'
            else location
        end as known_location,
        -- Currently not implemented in duckdb - IIF(last_latitude IS NOT NULL AND last_longitude IS NOT NULL, geography::Point(last_latitude, last_longitude, 4326).STDistance(geography::Point(latitude, longitude, 4326)), NULL) AS distance_meter
    from prepared_positions
    
)

select *
from calculated_durations