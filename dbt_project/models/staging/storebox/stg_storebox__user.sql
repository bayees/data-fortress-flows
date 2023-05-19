{{
  config(
    materialized='incremental',
    unique_key='userId'
  )
}}

with source as (
      select * from {{ source('external_source', 'storebox__user') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed
  