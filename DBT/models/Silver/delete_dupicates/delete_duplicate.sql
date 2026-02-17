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