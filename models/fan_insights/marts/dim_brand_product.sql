{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['stg.c_brand', 'stg.c_subcategory', 'stg.c_category']) }} AS brand_product_key,
    stg.c_brand AS brand,
    stg.c_subcategory AS subcategory,
    stg.c_category AS category,

FROM {{ ref('stg_fan_behavior') }} AS stg

