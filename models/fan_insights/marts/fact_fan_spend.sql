{{ config(materialized='table') }}

WITH base AS (
    SELECT
        brand,
        audience,
        region,
        category,
        subcategory,
        spend,
        percent_composition,
        population_index,
        transaction_date
    FROM {{ ref('stg_fan_behavior') }}
),

joined AS (
    SELECT
        f.brand,
        f.audience,
        f.region,
        f.category,
        f.subcategory,
        f.spend,
        f.percent_composition,
        f.population_index,
        f.transaction_date,
        b.brand_key,
        a.audience_key,
        r.region_key
    FROM base f
    LEFT JOIN {{ ref('dim_brand') }} b
    ON TRIM(LOWER(f.brand)) = TRIM(LOWER(b.brand))
    AND TRIM(LOWER(f.category)) = TRIM(LOWER(b.category))
    AND TRIM(LOWER(f.subcategory)) = TRIM(LOWER(b.subcategory))

    LEFT JOIN {{ ref('dim_audience') }} a
        ON f.audience = a.audience
    LEFT JOIN {{ ref('dim_region') }} r
        ON f.region = r.region
)



SELECT
    {{ dbt_utils.generate_surrogate_key([
        'brand_key',
        'audience_key',
        'region_key',
        'transaction_date',
        'spend',
        'percent_composition',
        'population_index'
    ]) }} AS fact_fan_spend_key,

    brand_key,
    audience_key,
    region_key,
    spend,
    percent_composition,
    population_index,
    transaction_date
FROM joined
