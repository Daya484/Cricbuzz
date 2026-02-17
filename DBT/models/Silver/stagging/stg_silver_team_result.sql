
with bronze as (
select 
series_name,
safe_cast(match_id as numeric) as match_id,
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
safe_cast(team1_runs as numeric) as team1_runs,
safe_cast(team1_wickets as numeric) as team1_wickets,
safe_cast(team1_overs as numeric) as eam1_overs,
safe_cast(team2_runs as numeric) as team2_runs,
safe_cast(team2_wickets as numeric) as team2_wicket ,
safe_cast(team2_overs as numeric) as team2_overs,
file_name,
max(update_timestamp) as update_timestamp
from {{ ref('team_result_bronze_ingest') }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),
silver as (
  select *
  from {{ source('src_team_ext','team_results_silver_distinct_ingest') }}
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