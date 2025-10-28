{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['stg.c_subcategory', 'stg.c_category']) }} AS subcategory_key,
    stg.c_subcategory AS subcategory,
    stg.c_category AS category,
    c.category_key

FROM {{ ref('stg_fan_behavior') }} AS stg
JOIN {{ ref('dim_category') }} AS c 
    ON stg.c_category = c.category
