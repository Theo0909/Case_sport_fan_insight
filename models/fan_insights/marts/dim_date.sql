{{ config(materialized='table') }}

-- Generating a dates from 2025 to 2030 (the oldest date in the data source is from january 2025)
WITH date_spine AS (
    {{dbt_utils.date_spine(datepart="day", start_date="CAST('2025-01-01' AS date)", end_date="CAST('2030-12-31' AS date)")}}
)

SELECT
{{dbt_utils.generate_surrogate_key(['date_day'])}} AS date_key,
date_day AS date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month
FROM date_spine
ORDER BY date_day 