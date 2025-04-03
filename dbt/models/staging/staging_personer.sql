-- staging_personer

select 
    *,
    cast(last_modified_date as timestamp) as last_modified_timestamp,
from {{ source('teamkatalogen', 'Personer') }}
