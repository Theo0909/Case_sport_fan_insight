WITH audience_brand_spend AS (
    SELECT
        a.audience,
        b.brand,
        SUM(f.spend) AS total_spend
    FROM {{ ref('fact_fan_spend') }} AS f
    LEFT JOIN {{ ref('dim_audience') }} AS a ON f.audience_key = a.audience_key
    LEFT JOIN {{ ref('dim_brand') }} AS b ON f.brand_key = b.brand_key
    GROUP BY a.audience, b.brand
),

ranked AS (
    SELECT
        audience,
        brand,
        total_spend,
        RANK() OVER (PARTITION BY audience ORDER BY total_spend DESC) AS spend_rank_desc,
        RANK() OVER (PARTITION BY audience ORDER BY total_spend ASC)  AS spend_rank_asc
    FROM audience_brand_spend
)

SELECT
    a.audience,
    CASE WHEN spend_rank_desc = 1 THEN brand END AS most_spent_brand,
    CASE WHEN spend_rank_asc = 1 THEN brand END AS least_spent_brand,
    SUM(total_spend) AS total_audience_spend
FROM ranked AS a
GROUP BY all
ORDER BY total_audience_spend DESC