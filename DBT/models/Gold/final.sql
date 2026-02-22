-- -- WITH cte AS (
-- --   SELECT
-- --     ARRAY(
-- --       SELECT AS STRUCT i.*
-- --       from {{ ref('stg_gold_team_international') }}  as i
-- --     ) AS team_rows,
-- --     ARRAY(
-- --       SELECT AS STRUCT p.*
-- --       from {{ ref('stg_gold_team_players') }} as p
-- --     ) AS player_rows,
-- --     ARRAY(
-- --       SELECT AS STRUCT r.*
-- --       from {{ ref('stg_gold_team_results')}} as r
-- --     ) AS team_results_rows
-- -- )
-- -- SELECT i.*
-- -- FROM cte,
-- -- UNNEST(cte.team_rows)


WITH cte AS (
  SELECT
    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT i.*
      FROM {{ ref('stg_gold_team_international') }} AS i
    )) AS team_rows_string,

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT p.*
      FROM {{ ref('stg_gold_team_players') }} AS p
    )) AS player_rows_string,

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT r.*
      FROM {{ ref('stg_gold_team_results') }} AS r
    )) AS team_results_rows_string,
-----------------------------------------------------------------------------------------------------------------------    

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT ph.*
      FROM {{ ref('stg_gold_photos') }} AS ph
    )) AS photos_rows_string,

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT pg.*
      FROM {{ ref('stg_gold_photos_galary') }} AS pg
    )) AS photos_galary_rows_string,
----------------------------------------------------------------------------------------------------------------------

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT ra.*
      FROM {{ ref('stg_gold_ranking_allrounders') }} AS ra
    )) AS ranking_allrounders_rows_string,


    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT rba.*
      FROM {{ ref('stg_gold_ranking_batsmens') }} AS rba
    )) AS ranking_batsmens_rows_string,

    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT rbo.*
      FROM {{ ref('stg_gold_ranking_bowlers') }} AS  rbo
    )) AS ranking_bowlers_rows_string,


    TO_JSON_STRING(ARRAY(
      SELECT AS STRUCT rt.*
      FROM {{ ref('stg_gold_ranking_teams') }} AS rt
    )) AS ranking_teams_rows_string
)
-- -- Example: query each separately
-- SELECT *
-- FROM cte, UNNEST(team_rows);
select * from cte