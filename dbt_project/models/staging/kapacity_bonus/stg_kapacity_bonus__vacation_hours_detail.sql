with source as (
      select * from {{ source('external_source', 'kapacity_bonus__vacation_hours_detail') }}
),
renamed as (
    select
        "date" as effective_date,
        "Vacation Hours" as vacation_hours
    from source
)
select * from renamed
  