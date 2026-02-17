
with bronze as (
select 
series_name,
match_id,
match_desc,
match_format,
match_status,
state,
team1_name,
team1_short ,
team2_name,
team2_short,
venue_ground,
venue_city,
start_date,
end_date,
team1_runs,
team1_wickets,
team1_overs,
team2_runs,
team2_wickets,
team2_overs ,
file_name,
max(update_timestamp) as update_timestamp
from {{ ref('team_result_bronze_ingest') }}
group by 1,2,3,4,5,6
),
silver as (
  select *
  from {{ source('src_team_ext','team_player_silver_distinct_ingest') }}
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