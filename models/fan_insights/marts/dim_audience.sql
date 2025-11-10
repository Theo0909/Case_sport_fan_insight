{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['audience']) }} AS audience_key,
    audience
FROM {{ ref('stg_fan_behavior') }}

