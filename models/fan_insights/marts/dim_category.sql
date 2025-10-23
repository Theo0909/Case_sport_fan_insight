{{ config(materialized='table') }}

WITH cat_base AS(
SELECT
    {{ dbt_utils.generate_surrogate_key(['category']) }} AS category_key,
    category,
    subcategory,
FROM {{ ref('stg_fan_behavior') }}
GROUP BY 1, 2, 3
)

SELECT * FROM cat_base