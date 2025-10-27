{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key,
    category
from {{ ref('stg_fan_behavior') }}
