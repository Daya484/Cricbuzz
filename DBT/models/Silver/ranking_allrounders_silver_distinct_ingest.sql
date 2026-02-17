{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_ranking_allrounder()}}']
    )
}}

select * from {{ ref('stg_silver_ranking_allrounders') }}