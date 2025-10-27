{{ config(materialized='table') }}

with brands as (
    select distinct
        brand,
        category
    from {{ ref('stg_fan_behavior') }}
),

categories as (
    select distinct
        category,
        {{ dbt_utils.generate_surrogate_key(["category"]) }} as category_key
    from {{ ref('stg_fan_behavior') }}
)

select
    {{ dbt_utils.generate_surrogate_key(["brand"]) }} as brand_key,
    brand,
    c.category_key
from brands b
left join categories c using (category)
