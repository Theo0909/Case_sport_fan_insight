{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['TRIM(LOWER(brand))', 'category', 'subcategory']) }} AS brand_key,
    INITCAP(TRIM(LOWER(brand))) AS brand, 
    category,
    subcategory
FROM {{ ref('stg_fan_behavior') }}
GROUP BY 1,2,3,4


