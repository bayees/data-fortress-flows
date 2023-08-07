with
unnested as (
    select 
        tags,
        "Merchant.Name" as merchant_name,
        ReceiptId AS receipt_id,
        PurchaseDatetimeString::datetime AS purchase_datetime,
        unnest(receiptlines) as receipt_line
    from {{ source('external_source', 'storebox__receipts') }}
)

select
    receipt_id,
    purchase_datetime,
    merchant_name,
    receipt_line.lineNumber AS line_number,
    lower(receipt_line.name) as product_name,
    receipt_line.count as quantity,
    receipt_line.itemPrice.amount as price,
    receipt_line.totalPrice.amount as total_price,
    receipt_line
from unnested
order by purchase_datetime desc
