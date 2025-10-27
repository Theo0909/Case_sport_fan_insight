{{ config(materialized='view') }}

SELECT
    -- Aligning each brand name
    LOWER(REPLACE(brand,' ','')) AS c_brand,
    sport,
    sponsorship_type,
    sponsorship_tier,
    year,
    SUM(spend_million_usd * 1000000) AS investment

FROM {{ ref('brand_sponsorships') }}
GROUP BY 1,2,3,4,5
