{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_ranking_batsmens()}}']
    )
}}

select * from {{ ref('stg_silver_ranking_batsmens') }}