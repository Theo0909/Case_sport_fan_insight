WITH spons AS (
    SELECT DISTINCT
        c_brand AS sponsorship_brand,
        year,
        sport,
        sponsorship_type,
        sponsorship_tier,
        investment
    FROM {{ ref('stg_brand_sponsorship') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "sponsorship_brand",
        "sport",
        "sponsorship_type",
        "sponsorship_tier",
        "year",
        "investment"
    ]) }} as sponsorship_key,
    brand_key,
    year,
    sport,
    sponsorship_type,
    sponsorship_tier,
    investment

FROM spons
LEFT JOIN {{ref('dim_brand')}} ON spons.sponsorship_brand = dim_brand.brand
