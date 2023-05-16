with
postings as (

    SELECT 
        parent.*,
        child.account_name as counter_account_name 
    FROM {{ ref('stg_spiir__postings') }} AS parent
    LEFT JOIN {{ ref('stg_spiir__postings') }} AS child
        ON parent.counter_posting_id = child.posting_id

),
receipts_mapping_to_posting as (

    SELECT * FROM {{ ref('int_receipts_mapping_to_posting') }}

),
receipts as ( 
    select 
        receipt_id,
        replace(merchant_store_id, 'None', null),
        case 
            when replace(merchant_store_id, 'None', null) is null then merchant_id || '|' || merchant_address
            else merchant_store_id
        end as merchant_key
    from {{ ref('stg_storebox__receipts') }}
)
, prepared_postings as (

    select
        {{ date_to_int('postings.posting_at') }}                                    as calendar_posting_id,
        {{ time_to_int('postings.posting_at') }}                                    as time_posting_id,
        {{ dbt_utils.generate_surrogate_key(['postings.category']) }}                        as category_id,
        {{ dbt_utils.generate_surrogate_key(['receipts_mapping_to_posting.receipt_id']) }}   as receipt_id,
        {{ dbt_utils.generate_surrogate_key(['receipts.merchant_key']) }} as merchant_id,
        postings.account_name,
        postings.description,
        postings.amount,
        postings.balance,
        postings.currency,
        postings.comment,
        postings.counter_account_name
    from postings
    left join  receipts_mapping_to_posting
        on postings.posting_id = receipts_mapping_to_posting.posting_id
    left join receipts
        on receipts_mapping_to_posting.receipt_id = receipts.receipt_id

)

select *
from prepared_postings