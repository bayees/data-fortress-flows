{{
  config(
    materialized='incremental'
  )
}}

with source as (
      select * from {{ source('external_source', 'dawa__addresses') }}
),
renamed as (
  select
    *
  from source
)
select * from renamed