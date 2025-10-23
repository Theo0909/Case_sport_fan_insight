{{ config(materialized='table') }}

-- Step 1: Extracting one row pr. brand using the window function qualify
WITH brand_info AS (
    SELECT
        brand AS brand_clean,
        category,
        subcategory,
        brand_key
    FROM {{ ref('dim_brand') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY brand
        ORDER BY category, subcategory
    ) = 1
),

-- Step 2: aggregate sponsorship data (unique per brand-deal)
aggregated AS (
    SELECT
        s.brand AS brand_clean,
        s.sport,
        s.sponsorship_type,
        s.sponsorship_tier,
        s.year,
        ROUND(SUM(s.spend_million_usd) * 1000000, 0) AS sponsorship_invest
    FROM {{ ref('brand_sponsorships') }} AS s
    WHERE s.spend_million_usd > 0
    GROUP BY 1,2,3,4,5
),

-- Step 3: join (1:1 per brand after deduplication)
joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'a.brand_clean',
            'a.sport',
            'a.sponsorship_type',
            'a.sponsorship_tier',
            'a.year'
        ]) }} AS sponsorship_key,
        b.brand_key,
        a.brand_clean AS brand,
        b.category,
        b.subcategory,
        a.sport,
        a.sponsorship_type,
        a.sponsorship_tier,
        a.year,
        SUM(a.sponsorship_invest) AS sponsorship_invest
    FROM aggregated AS a
    LEFT JOIN brand_info AS b
        ON a.brand_clean = b.brand_clean
    GROUP BY 1,2,3,4,5,6,7,8,9
)

-- Step 4: final output
SELECT
    sponsorship_key,
    brand_key,
    brand
    sport,
    sponsorship_type,
    sponsorship_tier,
    sponsorship_invest,
    year
FROM joined
WHERE brand_key IS NOT NULL
