

with source as (
      select * from {{ source('external_source', 'kapacity_bonus__vacation_hours_detail') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
  