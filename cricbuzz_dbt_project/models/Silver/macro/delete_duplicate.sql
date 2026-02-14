{% macro del_data_team() %}
 {% set delete_sql %}
DELETE FROM {{this}} ingest
WHERE EXISTS (
  SELECT 1
  from {{ ref('stg_silver_team') }} ext
  where ext.file_name=ingest.file_name
)
{% endset %}
  {{ return(delete_sql) }}

{% endmacro %}