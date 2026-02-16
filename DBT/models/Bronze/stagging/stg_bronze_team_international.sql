{{ config(materialized='view') }}

select *,
SPLIT(ARRAY_REVERSE(SPLIT(_FILE_NAME, '/'))[SAFE_OFFSET(0)],'.' )[SAFE_OFFSET(0)] AS file_name,
CURRENT_TIMESTAMP() AS update_timestamp
from {{ source('src_team_ext','team_ext') }}