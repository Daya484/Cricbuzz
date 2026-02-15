{% macro del_data_team() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  FROM {{ ref('bronze_team_ingest') }} ext
  where ext.file_name=ingest.file_name
  and ingest.update_timestamp<ext.update_timestamp
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}