
select 
    * 
from {{ source('external_source', 'dawa__addresses') }}
