select * 
from {{ ref('ranking_teams_silver_distinct_ingest') }}
where rank is not null