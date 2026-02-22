with final_results as(
-- 1. TEAM INTERNATIONAL
SELECT 
    CAST(market_code AS STRING) AS market_code, CAST(market_code2 AS STRING) AS market_code2, CAST(market_code3 AS STRING) AS market_code3,
    CAST(team_id AS STRING) AS team_id, CAST(country_name AS STRING) AS country_name, CAST(image_id AS STRING) AS image_id,
    CAST(file_name AS STRING) AS file_name, CAST(update_timestamp AS STRING) AS update_timestamp,
    CAST(NULL AS STRING) AS caption, CAST(NULL AS STRING) AS gallery_intro, CAST(NULL AS STRING) AS gallery_published_time, 
    CAST(NULL AS STRING) AS imageid, CAST(NULL AS STRING) AS imagehash, CAST(NULL AS STRING) AS gallery_id, 
    CAST(NULL AS STRING) AS gallery_state, CAST(NULL AS STRING) AS gallery_headline, CAST(NULL AS STRING) AS rank, 
    CAST(NULL AS STRING) AS id, CAST(NULL AS STRING) AS name, CAST(NULL AS STRING) AS country, 
    CAST(NULL AS STRING) AS country_id, CAST(NULL AS STRING) AS rating, CAST(NULL AS STRING) AS points, 
    CAST(NULL AS STRING) AS difference, CAST(NULL AS STRING) AS trend, CAST(NULL AS STRING) AS last_updated_on, 
    CAST(NULL AS STRING) AS face_image_id, CAST(NULL AS STRING) AS batting_style, CAST(NULL AS STRING) AS bowling_style, 
    CAST(NULL AS STRING) AS series_name, CAST(NULL AS STRING) AS match_id, CAST(NULL AS STRING) AS match_desc, 
    CAST(NULL AS STRING) AS match_format, CAST(NULL AS STRING) AS match_status, CAST(NULL AS STRING) AS state, 
    CAST(NULL AS STRING) AS team1_name, CAST(NULL AS STRING) AS team1_short, CAST(NULL AS STRING) AS team2_name, 
    CAST(NULL AS STRING) AS team2_short, CAST(NULL AS STRING) AS venue_ground, CAST(NULL AS STRING) AS venue_city, 
    CAST(NULL AS STRING) AS start_date, CAST(NULL AS STRING) AS end_date, CAST(NULL AS STRING) AS team1_runs, 
    CAST(NULL AS STRING) AS team1_wickets, CAST(NULL AS STRING) AS team1_overs, CAST(NULL AS STRING) AS team2_runs, 
    CAST(NULL AS STRING) AS team2_wicket, CAST(NULL AS STRING) AS team2_overs,
    'stg_gold_team_international' AS source_table
FROM  {{ ref('stg_gold_team_international') }}

UNION ALL

-- 2. TEAM PLAYERS
SELECT 
    NULL, NULL, NULL, -- market codes
    NULL, NULL, CAST(image_id AS STRING),
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(id AS STRING), CAST(name AS STRING), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    CAST(batting_style AS STRING), CAST(bowling_style AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_team_players'
FROM {{ ref('stg_gold_team_players') }}

UNION ALL

-- 3. TEAM RESULTS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(series_name AS STRING), CAST(match_id AS STRING), CAST(match_desc AS STRING), CAST(match_format AS STRING), 
    CAST(match_status AS STRING), CAST(state AS STRING), CAST(team1_name AS STRING), CAST(team1_short AS STRING), 
    CAST(team2_name AS STRING), CAST(team2_short AS STRING), CAST(venue_ground AS STRING), CAST(venue_city AS STRING), 
    CAST(start_date AS STRING), CAST(end_date AS STRING), CAST(team1_runs AS STRING), CAST(team1_wickets AS STRING), 
    CAST(team1_overs AS STRING), CAST(team2_runs AS STRING), CAST(team2_wicket AS STRING), CAST(team2_overs AS STRING),
    'stg_gold_team_results'
FROM {{ ref('stg_gold_team_results') }} 

UNION ALL

-- 4. PHOTOS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    CAST(caption AS STRING), CAST(gallery_intro AS STRING), CAST(gallery_published_time AS STRING), 
    CAST(imageid AS STRING), NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_photos'
FROM {{ ref('stg_gold_photos') }} 

UNION ALL

-- 5. PHOTOS GALLERY
SELECT 
    NULL, NULL, NULL, NULL, NULL, CAST(image_id AS STRING),
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    CAST(caption AS STRING), CAST(gallery_intro AS STRING), CAST(gallery_published_time AS STRING), 
    NULL, CAST(imagehash AS STRING), CAST(gallery_id AS STRING), CAST(gallery_state AS STRING), 
    CAST(gallery_headline AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_photos_galary'
FROM {{ ref('stg_gold_photos_galary') }}

UNION ALL

-- 6. RANKING ALLROUNDERS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(rank AS STRING), CAST(id AS STRING), CAST(name AS STRING), CAST(country AS STRING), 
    CAST(country_id AS STRING), CAST(rating AS STRING), CAST(points AS STRING), CAST(difference AS STRING), 
    CAST(trend AS STRING), CAST(last_updated_on AS STRING), CAST(face_image_id AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_ranking_allrounders'
FROM {{ ref('stg_gold_ranking_allrounders') }}

UNION ALL

-- 7. RANKING BATSMENS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(rank AS STRING), CAST(id AS STRING), CAST(name AS STRING), CAST(country AS STRING), 
    CAST(country_id AS STRING), CAST(rating AS STRING), CAST(points AS STRING), CAST(difference AS STRING), 
    CAST(trend AS STRING), CAST(last_updated_on AS STRING), CAST(face_image_id AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_ranking_batsmens'
FROM {{ ref('stg_gold_ranking_batsmens') }}

UNION ALL

-- 8. RANKING BOWLERS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(rank AS STRING), CAST(id AS STRING), CAST(name AS STRING), CAST(country AS STRING), 
    CAST(country_id AS STRING), CAST(rating AS STRING), CAST(points AS STRING), CAST(difference AS STRING), 
    CAST(trend AS STRING), CAST(last_updated_on AS STRING), CAST(face_image_id AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_ranking_bowlers'
FROM {{ ref('stg_gold_ranking_bowlers') }}

UNION ALL

-- 9. RANKING TEAMS
SELECT 
    NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(file_name AS STRING), CAST(update_timestamp AS STRING),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    CAST(rank AS STRING), NULL, CAST(name AS STRING), NULL, 
    NULL, CAST(rating AS STRING), CAST(points AS STRING), CAST(difference AS STRING), 
    CAST(trend AS STRING), CAST(last_updated_on AS STRING), NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    'stg_gold_ranking_teams'
FROM {{ ref('stg_gold_ranking_teams') }}
)
SELECT 
STRUCT(
country_name, 
market_code,
market_code2,
market_code3) AS GEOGRAPHY,

STRUCT(
safe_cast(country_id as numeric) as country_id,
safe_cast(id as numeric) as id,
name,
country,
safe_cast(rank as numeric) as rank,
safe_cast(rating as numeric) as rating,
safe_cast(points as numeric) as points,
safe_cast(difference as numeric) as difference,
trend,
last_updated_on,
safe_cast(face_image_id as numeric) as face_image_id,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp) AS ICCRANKING_ALLROUNDER,

STRUCT(
safe_cast(rank as numeric) as rank,
safe_cast(id as numeric) as id,
name,
country,
safe_cast(country_id as numeric) as country_id,
safe_cast(rating as numeric) as rating,
safe_cast(points as numeric) as points,
safe_cast(difference as numeric) as difference,
trend,
last_updated_on,
safe_cast(face_image_id as numeric) as face_image_id,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp) AS ICCRANKING_BATSMENS,

STRUCT(
safe_cast(rank as numeric) as rank,
safe_cast(id as numeric) as id,
name,
country,
safe_cast(country_id as numeric) as country_id,
safe_cast(rating as numeric) as rating,
safe_cast(points as numeric) as points,
safe_cast(difference as numeric) as difference,
trend,
last_updated_on,
safe_cast(face_image_id as numeric) as face_image_id,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp) AS ICCRANKING_BOWLERS,

STRUCT (
safe_cast(rank as numeric) as rank,
name,
safe_cast(rating as numeric) as rating,
safe_cast(points as numeric) as points,
safe_cast(difference as numeric) as difference,
trend,
last_updated_on,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS ICCRANKING_TEAMS,

STRUCT(
  market_code,
  safe_cast(team_id as numeric) as team_id,
  country_name,
  safe_cast(image_id as numeric) as image_id,
  file_name,
  CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS INTERNATIONAL_TEAM,

STRUCT(
name,
  batting_style,
  safe_cast(id as numeric) as id,           
  bowling_style,
  safe_cast(image_id as numeric) as image_id,      
  file_name,
  CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS INTERNATIONAL_TEAM_PLAYERS,

STRUCT(
name,
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
safe_cast(team1_overs as numeric) as team1_overs,
safe_cast(team2_runs as numeric) as team2_runs,
safe_cast(team2_wicket as numeric) as team2_wicket,
safe_cast(team2_overs as numeric) as team2_overs,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS INTERNATIONAL_TEAM_RESULTS,

STRUCT(
  safe_cast(caption as numeric) as caption,
  gallery_intro,
  gallery_published_time,
  imageId,   
  file_name,
  CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS PHOTOS,

STRUCT (
caption,
gallery_intro,	
safe_cast(image_id as numeric) as image_id,
gallery_published_time,
imageHash,
safe_cast(gallery_id as numeric) as gallery_id,
gallery_state,
gallery_headline,
file_name,
CAST(update_timestamp AS TIMESTAMP) as update_timestamp
) AS PHOTO_GALARY,
source_table


    -- CAST(team_id AS STRING) AS team_id, 
    -- CAST(image_id AS STRING) AS image_id,
    -- caption, 
    -- gallery_intro, 
    -- gallery_published_time, 
    -- imageid, 
    -- imagehash, 
    -- gallery_id, 
    -- gallery_state, 
    -- gallery_headline, 
    -- rank, 
    -- id, 
    -- name, 
    -- country, 
    -- country_id, 
    -- rating, 
    -- points, 
    -- difference, 
    -- trend, 
    -- last_updated_on, 
    -- face_image_id, 
    -- batting_style, 
    -- bowling_style, 
    -- series_name, 
    -- match_id, 
    -- match_desc, 
    -- match_format, 
    -- match_status, 
    -- state, 
    -- team1_name, 
    -- team1_short, 
    -- team2_name, 
    -- team2_short, 
    -- venue_ground, 
    -- venue_city, 
    -- start_date, 
    -- end_date, 
    -- team1_runs, 
    -- team1_wickets, 
    -- team1_overs, 
    -- team2_runs, 
    -- team2_wicket, 
    -- team2_overs,
    -- 'stg_gold_team_international' AS source_table,
    -- CAST(file_name AS STRING) AS file_name, 
    -- CAST(update_timestamp AS STRING) AS update_timestamp,  
  FROM final_results 