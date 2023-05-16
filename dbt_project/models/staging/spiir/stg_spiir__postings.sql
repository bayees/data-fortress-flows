with source as (
      select * from {{ source('external_source', 'spiir__postings') }}
),
renamed as (
    select
        Id::varchar(20) as posting_id,
        AccountName::varchar(20) as account_name,
        case 
            when CustomDate <> 'None' then CustomDate 
            else Date 
        end::timestamp as posting_at,
        Description::varchar(512) as description,
        MainCategoryName::varchar(128) as main_category,
        CategoryName::varchar(128) as category,
        CategoryType::varchar(128) as category_type,
        ExpenseType::varchar(128) as expense_type,
        Amount::decimal(19, 2) as amount,
        case when Balance <> 'nan' then Balance end::decimal(19, 2) as balance,
        Currency::varchar(3) as currency,
        case when CounterEntryId='' then null else CounterEntryId end::varchar(20) as counter_posting_id,
        case when Comment <> 'None' then Comment end::varchar(512) as comment
    from source
)
select * from renamed
  