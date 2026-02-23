select * 
from {{ ref('ranking_allrounders_silver_distinct_ingest') }}
where rank is not null