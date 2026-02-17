{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_team_results()}}']
    )
}}

select * from {{ ref('stg_silver_team_players') }}