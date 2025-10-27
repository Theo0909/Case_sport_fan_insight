-- It necessary to build a bridge between subcategory and brand because there is a many to many relationship between brand and subcategory 

{{ config(materialized='table') }}

WITH base AS (
    SELECT distinct
        {{ dbt_utils.generate_surrogate_key(['c_brand']) }} AS brand_key,
        {{ dbt_utils.generate_surrogate_key(['c_subcategory']) }} AS subcategory_key
    FROM {{ ref('stg_fan_behavior') }} 
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['brand_key','subcategory_key']) }} AS brand_subcategory_key,
    brand_key,
    subcategory_key
FROM base

