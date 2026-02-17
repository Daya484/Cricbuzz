{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_ranking_teams()}}']
    )
}}

select * from {{ ref('stg_silver_ranking_teams') }}