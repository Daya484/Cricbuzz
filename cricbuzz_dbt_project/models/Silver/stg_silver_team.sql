{{ config(materialized='view') }}

with bronze as (
select 
  teamSName as market_code,
  cast(teamid as numeric) as team_id,
  coalesce(countryName, teamName, '-1') as country_name,
  cast(imageid as numeric) as image_id,
  file_name,
  max(update_timestamp) as update_timestamp
  from {{ ref('bronze_team_ingest') }}
  group by 1,2,3,4,5
),
silver as (
  select *
  from {{ source('src_team_ext','silver_team_distinct_ingest') }}
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