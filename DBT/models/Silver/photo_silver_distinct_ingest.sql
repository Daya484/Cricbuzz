{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_photo()}}']
    )
}}

select * from {{ ref('stg_silver_photo') }}