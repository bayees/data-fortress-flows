with categories as (

    SELECT * FROM {{ ref('stg_spiir__postings') }}

)
, prepared_categories as (

    select distinct     
        {{ dbt_utils.generate_surrogate_key(['category']) }} as category_id,
        category,
        main_category,
        category_type,
        expense_type,
    from categories
    WHERE category <> 'None'

)

select *
from prepared_categories