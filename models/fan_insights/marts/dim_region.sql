{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['region']) }} AS region_key,
    region
FROM {{ ref('stg_fan_behavior') }}
GROUP BY 1, 2
