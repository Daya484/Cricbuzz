select * 
from {{ ref('photo_galary_silver_distinct_ingest') }}
where image_id is not null