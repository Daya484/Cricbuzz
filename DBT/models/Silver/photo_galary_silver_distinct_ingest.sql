{{
    config(
        materialized='incremental',
        pre_hook=['{{del_data_photo_galary()}}']
    )
}}

select * from {{ ref('stg_silver_photo_galary') }}