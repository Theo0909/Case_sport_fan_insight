{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['c_subcategory','c_category']) }} as subcategory_key,
    c_subcategory AS Subcategory,
    {{ dbt_utils.generate_surrogate_key(['c_category']) }} as category_key,
    c_category AS Category
from {{ ref('stg_fan_behavior') }}
