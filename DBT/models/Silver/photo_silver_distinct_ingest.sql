{{
    config(
        materialized='table'
    )
}}
select * from {{ ref('stg_silver_photo') }}