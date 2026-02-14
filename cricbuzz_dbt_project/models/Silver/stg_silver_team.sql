{{ config(materialized='view') }}

with bronze as (
select 
  teamSName as maket_code,
  cast(teamid as numeric) as team_id,
  coalesce(countryName, teamName, '-1') as country_name,
  cast(imageid as numeric) as image_id,
  file_name,
  update_timestamp
  from {{ ref('bronze_team_ingest') }}
  qualify row_number() over (partition by file_name order by update_timestamp desc) = 1 
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