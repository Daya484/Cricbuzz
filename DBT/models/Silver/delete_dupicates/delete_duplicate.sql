{% macro del_data_team() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_team_international') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_team_players() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_team_players') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_team_results() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_team_result') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_photo() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_photo') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}

------------------------------------------------------------------------------------------------------------

{% macro del_data_photo_galary() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_photo_galary') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}

------------------------------------------------------------------------------------------------------------

{% macro del_data_ranking_allrounder() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_ranking_allrounders') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_ranking_batsmens() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_ranking_batsmens') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_ranking_bowlers() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_ranking_bowlers') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}
------------------------------------------------------------------------------------------------------------

{% macro del_data_ranking_teams() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_ranking_teams') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}