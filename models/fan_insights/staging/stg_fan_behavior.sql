SELECT
    audience,
    category,
    comparison_population,
    percent_composition,
    population_index,
    subcategory,
    subcategory_rank,
    CAST(transaction_date AS DATE) AS transaction_date,
    brand,
    region,
    gender,
    age_group,
    spend
FROM {{ source('sports_fan_behavior', 'SPORTS_FAN_BEHAVIOR') }}
