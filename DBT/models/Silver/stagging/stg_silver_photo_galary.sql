{{ config(materialized='view') }}

with bronze as (
select 
caption,
gallery_intro,	
safe_cast(imageId as numeric) as image_id,
gallery_published_time,
imageHash,
safe_cast(gallery_id as numeric) as gallery_id,
gallery_state,
gallery_headline,
file_name,
max(update_timestamp) as update_timestamp
from {{ ref('photo_galary_bronze_ingest') }}
group by 1,2,3,4,5,6,7,8,9
),
silver as (
  select *
  from {{ source('src_team_ext','photo_galary_silver_distinct_ingest') }}
),
deduplication as (
  select b.*
  from bronze b
  left join silver s
    on b.file_name = s.file_name
  where s.file_name is null
     or b.update_timestamp > s.update_timestamp
)
select * from deduplication 