select
        Category as budget_category,
        Type as transaction_type,
        "Running Budget"::boolean as running_budget,
        Year::int as year,
        Jan::decimal(9,2) as jan,
        Feb::decimal(9,2) as feb,
        Mar::decimal(9,2) as mar,
        Apr::decimal(9,2) as apr,
        May::decimal(9,2) as may,
        Jun::decimal(9,2) as jun,
        Jul::decimal(9,2) as jul,
        Aug::decimal(9,2) as aug,
        Sep::decimal(9,2) as sep,
        Oct::decimal(9,2) as oct,
        Nov::decimal(9,2) as nov,
        Dec::decimal(9,2) as dec
from {{ source('external_source', 'google_sheets__budget') }}
