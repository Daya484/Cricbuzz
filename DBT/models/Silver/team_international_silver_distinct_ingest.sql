{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_team()}}']
    )
}}

select * from {{ ref('stg_silver_team_international') }}