
with bronze as (
select 
name,
battingStyle as batting_style,
safe_cast(id as numeric) as id,           
bowlingStyle  as bowling_style,
safe_cast(imageId as numeric) as image_id,      
file_name,
update_timestamp
from {{ ref('team_players_bronze_ingest') }}
QUALIFY rank() OVER (PARTITION BY file_name ORDER BY update_timestamp DESC) = 1
),
silver as (
  select *
  from {{ source('src_team_ext','team_players_silver_distinct_ingest') }}
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