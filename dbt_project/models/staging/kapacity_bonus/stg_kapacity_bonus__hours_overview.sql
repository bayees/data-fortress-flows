

with source as (
      select * from {{ source('external_source', 'kapacity_bonus__hours_overview') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
