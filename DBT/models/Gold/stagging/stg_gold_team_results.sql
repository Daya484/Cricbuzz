select * 
from {{ ref('team_results_silver_distinct_ingest') }}
where match_id is not null