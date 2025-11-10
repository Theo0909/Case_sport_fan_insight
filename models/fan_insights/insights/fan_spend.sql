WITH audience_spend AS (
    SELECT
        a.audience_key,
        a.audience,
        ROUND(SUM(f.spend), 0) AS total_spend,
        COUNT(DISTINCT f.fan_behavior_id) AS unique_fans,
        ROUND(AVG(f.spend), 2) AS avg_spend_per_fan,
        MAX(f.spend) AS highest_purchased
    FROM {{ ref('fact_fan_spend') }} AS f
    LEFT JOIN {{ ref('dim_audience') }} AS a ON f.audience_key = a.audience_key
    GROUP BY a.audience_key, a.audience
)

SELECT
    audience,
    total_spend,
    avg_spend_per_fan,
    highest_purchased,
    RANK() OVER (ORDER BY total_spend DESC) AS spend_rank

FROM audience_spend
ORDER BY total_spend DESC
