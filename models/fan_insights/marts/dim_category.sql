{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['category', 'subcategory']) }} AS category_key,
    category,
    subcategory,
    subcategory_rank
FROM {{ ref('stg_fan_behavior') }}
GROUP BY 1, 2, 3, 4
