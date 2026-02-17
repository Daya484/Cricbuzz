{{ config(materialized='view') }}

--with bronze as (
select 
safe_cast(rank as numeric) as rank,
name,
safe_cast(rating as numeric) as rating,
safe_cast(points as numeric) as points,
safe_cast(difference as numeric) as difference,
trend,
lastUpdatedOn as last_updated_on,
file_name,
max(update_timestamp) as update_timestamp
from {{ ref('ranking_teams_bronze_ingest') }}
group by 1,2,3,4,5,6,7,8
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