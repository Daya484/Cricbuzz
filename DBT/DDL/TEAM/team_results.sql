create or replace external table  `project-75b24ad0-af2b-4b2b-863.dayasagar.team_results_ext`
(
series_name	     STRING,
match_id	       STRING,
match_desc	     STRING,
match_format	   STRING,
match_status	   STRING,
state	           STRING,
team1_name	     STRING,
team1_short      STRING,
team2_name	     STRING,
team2_short	     STRING,
venue_ground	   STRING,
venue_city	     STRING,
start_date	     STRING,
end_date	       STRING,
team1_runs	     STRING,
team1_wickets	   STRING,
team1_overs	     STRING,
team2_runs	     STRING,
team2_wickets	   STRING,
team2_overs      STRING
 
)

OPTIONS ( 
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE,
    allow_jagged_rows = TRUE,
    uris = ['gs://team-team_result/*'] 
    
    );