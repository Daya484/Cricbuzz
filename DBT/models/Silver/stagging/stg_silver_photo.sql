--with bronze as (
select 
  safe_cast(caption as numeric) as caption,
  gallery_intro,
  gallery_published_time,
  imageId,   
  file_name,
  max(update_timestamp) as update_timestamp
  from {{ ref('photo_bronze_ingest') }}
  group by 1,2,3,4,5
-- ),
-- silver as (
--   select *
--   from {{ source('src_team_ext','team_international_silver_distinct_ingest') }}
-- ),
-- deduplication as (
--   select b.*
--   from bronze b
--   left join silver s
--     on b.file_name = s.file_name
--   where s.file_name is null
--      or b.update_timestamp > s.update_timestamp
-- )
-- select * from deduplication 