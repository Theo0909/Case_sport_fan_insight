{{ config(materialized='view') }}

with cleaned as (

    select
        -- Clean brand
        lower(replace(brand,' ','')) as c_brand,

        -- Clean category, override Under Armour
        case
            when lower(replace(brand,' ','')) = 'underarmour'
                then 'apparel/sportsequipment'
            else lower(replace(category,' ',''))
        end as c_category,

        -- Clean subcategory
        lower(replace(subcategory,' ','')) as c_subcategory,

        audience,
        region,
        spend,
        transaction_date,
        population_index,
        percent_composition

    from {{ source('sports_fan_behavior', 'SPORTS_FAN_BEHAVIOR') }}
)

select
    {{ dbt_utils.generate_surrogate_key([
        "c_brand",
        "audience",
        "region",
        "c_category",
        "c_subcategory",
        "spend",
        "population_index",
        "percent_composition",
        "transaction_date"
    ]) }} as fan_behavior_id,

    c_brand,
    audience,
    region,
    c_category,
    c_subcategory,
    spend,
    transaction_date,
    population_index,
    percent_composition

from cleaned
