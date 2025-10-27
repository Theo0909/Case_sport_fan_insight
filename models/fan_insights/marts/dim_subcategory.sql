{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['subcategory']) }} as subcategory_key,
    subcategory,
    {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key
from {{ ref('stg_fan_behavior') }}
