{{ config(materialized='table') }}

WITH base AS (
    SELECT
        fan_behavior_id,
        spend,
        transaction_date,
        audience,
        region,
        c_brand,
        c_subcategory,
        c_category
    FROM {{ ref('stg_fan_behavior') }}
),

joined as (
    SELECT
        base.fan_behavior_id,
        base.spend,
        base.transaction_date,
        b.brand_key,
        a.audience_key,
        r.region_key,
        d.date_key,
        p.brand_product_key
    FROM base 
    LEFT JOIN {{ ref('dim_brand') }} AS b ON base.c_brand = b.brand
    LEFT JOIN {{ ref('dim_audience') }} AS a ON base.audience = a.audience
    LEFT JOIN {{ ref('dim_region') }} AS r ON base.region = r.region
    LEFT JOIN {{ref('dim_date')}} AS d ON base.transaction_date = d.date
    LEFT JOIN {{ ref('dim_brand_product') }} AS p ON base.c_brand = p.brand AND base.c_subcategory = p.subcategory AND base.c_category = p.category
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'brand_product_key',
        'audience_key',
        'region_key',
        'date_key',
        'transaction_date',
        'fan_behavior_id'
    ]) }} AS fact_fan_spend_key,
    fan_behavior_id,
    brand_key,
    audience_key,
    region_key,
    date_key,
    brand_product_key,
    spend,
    transaction_date
FROM joined
