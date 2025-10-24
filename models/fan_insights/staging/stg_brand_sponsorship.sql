{{ config(materialized='view') }}

SELECT
    lower(
        trim(
            regexp_replace(
                translate(brand, chr(160), ' '),
                '\\s+',
                ''
            )
        )
    ) as brand,

    sport,
    sponsorship_type,
    sponsorship_tier,
    year,
    SUM(spend_million_usd * 1000000) AS investment

FROM {{ ref('brand_sponsorships') }}
GROUP BY 1,2,3,4,5
