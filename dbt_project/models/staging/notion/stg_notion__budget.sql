{{ config(enabled = false) }}

select
        id,
        created_time::datetime as created_at,
        last_edited_time::datetime as modified_at,
        replace("properties.Category.select.name", '´', ',') as budget_category,
        "properties.Type.select.name" as transaction_type,
        "properties.Year.select.name" as year,
        url,
        "properties.Oct.number" as oct,
        "properties.Jan.number" as jan,
        "properties.Jun.number" as jun,
        "properties.Aug.number" as aug,
        "properties.Feb.number" as feb,
        "properties.May.number" as may,
        "properties.Mar.number" as mar,
        "properties.Nov.number" as nov,
        "properties.Apr.number" as apr,
        "properties.Dec.number" as dec,
        "properties.Sep.number" as sep,
        "properties.Jul.number" as jul
from {{ source('external_source', 'notion__budget') }}
