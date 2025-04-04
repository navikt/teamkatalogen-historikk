-- staging_teams

-- obs! Denne modellen blir brukt av dbt snapshot, så derfor må det være et timestamp
select
    *,
    cast(last_modified_date as timestamp) as last_modified_timestamp,
from {{ source('teamkatalogen', 'Klynger') }}
