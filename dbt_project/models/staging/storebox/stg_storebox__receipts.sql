{{
  config(
    materialized='incremental',
    unique_key='receipt_id'
  )
}}


with source as (
      select * from {{ source('external_source', 'storebox__receipts') }}
),
renamed as (
    select
        id::bigint as id,
        type::varchar(20) as type,
        receiptId::varchar(32) as receipt_id,
        purchaseDateTimeString::datetime as purchase_at,
        orderNumber::varchar(17) as order_number,
        headerText::varchar(512) as header_text,
        footerText::varchar(512) as footer_text,
        "price.amount"::decimal(18,2) as price_amount,
        "price.vat"::decimal(18,2) as price_vat,
        "price.currency"::varchar(3) as price_currency,
        "price.exchangeRate"::decimal(18,2) as price_exchange_rate,
        "price.vatPercentage"::int as price_vat_percentage,
        "price.vatRates"::varchar(512) as price_vat_rates,
        "merchant.merchantId"::varchar(32) as merchant_id,
        "merchant.storeId"::varchar(32) as merchant_store_id,
        "merchant.name"::varchar(128) as merchant_name,
        "merchant.address"::varchar(128) as merchant_address,
        "merchant.address2"::varchar(128) as merchant_adress2,
        "merchant.zipCode"::varchar(32) as merchant_zip_code,
        "merchant.city"::varchar(128) as merchant_city,
        "merchant.phoneNumber"::varchar(32) as merchant_phone_number,
        "merchant.registrationNumber"::varchar(32) as merchant_registration_number,
        "merchant.logo"::varchar(128) as merchant_logo,
        "merchant.country"::varchar(2) as merchant_country,
        "barcode.type"::varchar(32) as barcode_type,
        "barcode.value"::varchar(128) as barcode_value,
        "barcode.displayValue"::varchar(128) as barcode_display_value,
        "receiptImage.contentType"::varchar(32) as receipt_image_content_type,
        "receiptImage.bytes"::varchar(128) as receipt_image_bytes,
        "receiptImage.imageType"::varchar(32) as receipt_image_image_type,
        "receiptImage.columns"::int as receipt_image_columns,
        "receiptImage.text"::varchar(512) as receipt_image_text,
    from source
)
select * from renamed
  