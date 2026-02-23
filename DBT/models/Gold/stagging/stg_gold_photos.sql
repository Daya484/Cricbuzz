select * 
from {{ ref('photo_silver_distinct_ingest') }}
where imageid is not null