with bronze as (
select 
  safe_cast(caption as numeric) as caption,
  gallery_intro,
  imageId as gallery_published_time,
  gallery_published_time as imageId,   
  file_name,
  update_timestamp
  from {{ ref('photo_bronze_ingest') }}
  QUALIFY rank() OVER (PARTITION BY file_name ORDER BY update_timestamp DESC) = 1
),
silver as (
  select *
  from {{ source('src_team_ext','photo_silver_distinct_ingest') }}
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