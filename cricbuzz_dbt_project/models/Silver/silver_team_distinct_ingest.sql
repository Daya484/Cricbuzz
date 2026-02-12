{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_team()}}']
    )
}}

SELECT 
teamSName as maket_code,
cast(teamid as numeric) as team_id,
coalesce(countryName,teamName,'-1') as country_name,
cast(imageid as numeric) as image_id,
file_name,
update_timestamp
from {{ ref('bronze_team_ingest') }}