select * 
from {{ ref('team_players_silver_distinct_ingest') }}
where id is not null