{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key,
    c_category AS category
from {{ ref('stg_fan_behavior') }}
