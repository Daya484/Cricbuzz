{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_ranking_bowlers()}}']
    )
}}

select * from {{ ref('stg_silver_ranking_bowlers') }}