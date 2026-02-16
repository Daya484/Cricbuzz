create or replace external table  `project-75b24ad0-af2b-4b2b-863.dayasagar.team_players_ext`
(
  name               STRING  
  ,battingStyle      STRING  
  ,id                STRING  
  ,bowlingStyle      STRING  
  ,imageId           STRING   
 
)

OPTIONS ( 
    format = 'CSV',
    field_delimiter = ',',
    skip_leading_rows = 1,
    ignore_unknown_values = TRUE,
    allow_jagged_rows = TRUE,
    uris = ['gs://team-team_players/*'] 
    
    );