{{ config(materialized='table') }}

WITH spons AS (
    SELECT DISTINCT
        brand,
        year,
        sport,
        sponsorship_type,
        sponsorship_tier,
        investment
    FROM {{ ref('stg_brand_sponsorship') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "lower(trim(brand))",
        "sport",
        "sponsorship_type",
        "sponsorship_tier",
        "year",
        "investment"
    ]) }} as sponsorship_key,

    {{ dbt_utils.generate_surrogate_key([
        "lower(trim(brand))"
    ]) }} as brand_key,

    year,
    sport,
    sponsorship_type,
    sponsorship_tier,
    investment

FROM spons
