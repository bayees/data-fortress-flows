with home_assistant_positions as (

    select * from {{ ref('stg_home_assistant__positions') }}

), 

dawa_adresses as (

    SELECT * FROM  {{ ref('stg_dawa__addresses')  }}

),

-- location_of_interest as (
--     SELECT * FROM  source('src', 'manual__location_of_interest') 
-- ), 

dawa_enriched_locations as (

    select distinct
        {{ dbt_utils.generate_surrogate_key(['home_assistant_positions.latitude_degrees', 'home_assistant_positions.longitude_degrees']) }} as location_id

        ,home_assistant_positions.latitude_degrees 
        ,home_assistant_positions.longitude_degrees

        ,case 
            when dawa_adresses.latitude_degrees is null
                and dawa_adresses.longitude_degrees is null
            then 0
            else 1
        end as dawa_enriched
        , dawa_adresses.adressebetegnelse                                    AS dawa_adressebetegnelse
        , dawa_adresses.vejstykke_navn                                     AS dawa_vejstykke_navn
        , dawa_adresses.husnr                                                AS dawa_husnummer
        , dawa_adresses.postnummer_nr                                      AS dawa_postnummer_nr
        , dawa_adresses.postnummer_navn                                    AS dawa_postnummer_navn
        , dawa_adresses.adgangspunkt_højde                                 AS dawa_højde
        , dawa_adresses.sogn_navn                                          AS dawa_sogn_navn
        , dawa_adresses.region_navn                                        AS dawa_region_navn
        , dawa_adresses.landsdel_navn                                      AS dawa_landsdel_navn
        , dawa_adresses.retskreds_navn                                     AS dawa_retskreds_navn
        , dawa_adresses.politikreds_navn                                   AS dawa_politikreds_navn
        , dawa_adresses.storkreds_navn                                     AS dawa_storkreds_navn
        , dawa_adresses.valglandsdel_navn                                  AS dawa_valglandsdel_navn
        --, ISNULL(location_of_interest.name, '?')                             AS location_of_interest
    FROM home_assistant_positions  
    LEFT JOIN dawa_adresses
      ON home_assistant_positions.latitude_degrees = dawa_adresses.latitude_degrees
      AND home_assistant_positions.longitude_degrees = dawa_adresses.longitude_degrees
    -- left join location_of_interest
    -- on geography::Point(home_assistant_positions.latitude, home_assistant_positions.longitude, 4326).STDistance(geography::Point(location_of_interest.latitude, location_of_interest.longitude, 4326)) < 100
)

select *
from dawa_enriched_locations