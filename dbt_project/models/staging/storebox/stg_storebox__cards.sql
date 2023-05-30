
with source as (
      select * from {{ source('external_source', 'storebox__cards') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
  