{{ config(materialized='table') }}

SELECT
    --  Unique Fact Key per transaction
    {{ dbt_utils.generate_surrogate_key(['brand', 'audience', 'region', 'category', 'subcategory', 'spend', 'percent_composition', 'population_index']) }} AS fact_fan_spend_key,

    --  Foreign Keys to dimension tables
    {{ dbt_utils.generate_surrogate_key(['brand']) }} AS brand_key,
    {{ dbt_utils.generate_surrogate_key(['audience']) }} AS audience_key,
    {{ dbt_utils.generate_surrogate_key(['region']) }} AS region_key,
    {{ dbt_utils.generate_surrogate_key(['category']) }} AS category_key,

    --  Measures
    spend,
    percent_composition,
    population_index,

    --  Attributes
    transaction_date

FROM {{ ref('stg_fan_behavior') }}
