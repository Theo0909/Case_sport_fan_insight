{{ config(materialized='table') }}

WITH brands AS (
    SELECT DISTINCT
        c_brand AS brand,
        c_category AS category
    FROM {{ ref('stg_fan_behavior') }}
),

categories AS (
    SELECT DISTINCT
        c_category,
        {{ dbt_utils.generate_surrogate_key(["c_category"]) }} AS category_key
    FROM {{ ref('stg_fan_behavior') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["brand"]) }} AS brand_key,
    brand,
    c.category_key
FROM brands b
LEFT JOIN categories AS c ON b.category = c.c_category
