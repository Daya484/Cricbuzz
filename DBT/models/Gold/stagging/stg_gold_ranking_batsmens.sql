select * 
from {{ ref('ranking_batsmens_silver_distinct_ingest') }}
where rank is not null