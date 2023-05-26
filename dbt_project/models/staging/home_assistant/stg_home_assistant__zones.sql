with source as (
      select * from {{ source('external_source', 'home_assistant__zones') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
  