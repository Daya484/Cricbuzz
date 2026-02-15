{{
    config(
        materialized='incremental'
    )
}}

SELECT * from {{ ref('stg_bronze_team') }}