/*
    This model maps receipts to postings. 
*/

{{ 
    config(
        materialized='view'
    ) 
}}

with receipt as (
    select
        receipt_id,
        purchase_at as purchase_at,
        price_amount as amount
    from {{ ref('stg_storebox__receipts') }}
), 
posting as (
    select
        posting_id
        ,posting_at
        ,posting_at - INTERVAL 5 DAY AS posting_start_at
        ,posting_at + INTERVAL 1 DAY AS posting_end_at
        ,amount
    from {{ ref('stg_spiir__postings') }}
)
, ranked as (
    select 
        receipt.receipt_id
        ,posting.posting_id
        ,row_number() over(partition by receipt_id order by date_diff('day', purchase_at, posting_at) asc) as row_num
    from receipt
    left join posting
        on receipt.amount = -posting.amount
        and purchase_at between cast(posting_start_at as date) and posting_end_at
)

select
    receipt_id
    ,posting_id
from ranked
where row_num = 1
and posting_id is not null