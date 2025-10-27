{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "lower(trim(regexp_replace(translate(brand, chr(160), ' '), '\\\\s+', ' ')))",
        "audience",
        "region",
        "lower(trim(regexp_replace(translate(category, chr(160), ' '), '\\\\s+', ' ')))",
        "lower(trim(regexp_replace(translate(subcategory, chr(160), ' '), '\\\\s+', ' ')))",
        "spend",
        "population_index",
        "percent_composition",
        "transaction_date"
    ]) }} as fan_behavior_id,

    lower(
      trim(
        regexp_replace(
          translate(brand, chr(160), ' '),
          '\\s+',
          ''
        )
      )
    ) as brand,

    audience,
    region,

    case
        when lower(trim(regexp_replace(translate(brand, chr(160), ' '), '\\s+', ' ')))
            = 'under armour' then 'apparel / sports equipment'
        else lower(
              trim(
                regexp_replace(
                  translate(category, chr(160), ' '),
                  '\\s+',
                  ' '
                )
              )
            )
    end as category,

    lower(
  trim(
    regexp_replace(
      translate(
        regexp_replace(subcategory, '[^[:print:]]', ''),  -- remove invisible unicode
        chr(160),                                         -- replace NBSP
        ' '
      ),
      '\\s+',
      ' '
    )
  )
) as subcategory,


    spend,
    transaction_date,
    population_index,
    percent_composition

FROM {{ source('sports_fan_behavior', 'SPORTS_FAN_BEHAVIOR') }}
