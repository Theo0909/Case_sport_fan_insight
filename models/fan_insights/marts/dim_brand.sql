{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(["c_brand"]) }} AS brand_key,
    c_brand AS brand
FROM {{ref('stg_fan_behavior')}}

