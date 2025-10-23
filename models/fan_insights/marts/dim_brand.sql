{{ config(materialized='table') }}

WITH base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['brand']) }} AS brand_key,
        brand, 
    FROM {{ ref('stg_fan_behavior') }}
    GROUP BY 1, 2
),

enriched AS (
    SELECT
        b.brand_key,
        b.brand,
        s.sport,
        s.sponsorship_type,
        s.sponsorship_tier,
        s.spend_million_usd,
        s.region AS sponsorship_region
    FROM base b
    LEFT JOIN {{ ref('brand_sponsorships') }} s
        ON b.brand = s.brand
)

SELECT * 
FROM enriched
