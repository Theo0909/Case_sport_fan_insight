SELECT
    s.brand_key,
    b.brand,
    s.sport,
    s.year AS year_of_investment,
    ROUND(SUM(s.investment),0 ) AS total_investment, 
    ROUND(SUM(f.spend),0) AS total_fan_spend, 
    ROUND(SUM(f.spend) - SUM(s.investment), 2) AS roi,
    CASE
        WHEN SUM(f.spend) > SUM(s.investment) THEN 'Positive ROI'
        WHEN SUM(f.spend) = SUM(s.investment) THEN 'Break-even'
        ELSE 'Negative ROI'
    END AS roi_status

FROM {{ ref('fact_fan_spend') }} AS f
LEFT JOIN {{ ref('dim_brand') }} AS b ON f.brand_key = b.brand_key
INNER JOIN {{ ref('dim_sponsorship') }} AS s ON f.brand_key = s.brand_key
LEFT JOIN {{ ref('dim_date') }} AS d ON f.date_key = d.date_key AND s.year = d.year  -- align sponsorship to spend year

GROUP BY
    s.brand_key,
    b.brand,
    s.sport,
    s.year
ORDER BY roi DESC
