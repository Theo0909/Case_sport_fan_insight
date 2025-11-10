SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['region']) }} AS region_key,
    region
FROM {{ ref('stg_fan_behavior') }}

