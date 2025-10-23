{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['brand', 'audience', 'region', 'category', 'spend', 'transaction_date']) }} as fan_behavior_id,
    brand,
    audience,
    region,
    category,
    subcategory,
    subcategory_rank,
    spend,
    percent_composition,
    population_index,
    transaction_date
FROM {{ source('sports_fan_behavior', 'SPORTS_FAN_BEHAVIOR') }}
