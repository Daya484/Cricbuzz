
{{ config(materialized='table') }}

select * from {{ ref('stg_silver_team_result') }}