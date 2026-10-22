WITH review_with_sentiment AS (
    SELECT
        r.review_id,
        r.product_id,
        r.review_date,
        sd.compound_score,
        sd.risk_flag
    FROM {{ ref('stg_reviews') }} AS r
    LEFT JOIN {{ source('external_intermediate', 'int_review_sentiment_detail') }} AS sd
        ON r.review_id = sd.review_id
),

review_with_category AS (
SELECT
    rws.*,
    p.category
FROM review_with_sentiment AS rws
LEFT JOIN {{ ref('stg_products') }} AS p ON rws.product_id = p.product_id
),

ref_date AS (
SELECT MAX(review_date) AS max_date FROM review_with_category
)

SELECT
COALESCE(category, 'Unknown') AS category,

-- 7d
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 7 DAY)
    THEN compound_score
END) AS avg_sentiment_score_7d,
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 7 DAY)
    THEN CAST(risk_flag AS FLOAT64)
END) AS risk_review_ratio_7d,
COUNTIF(review_date >= DATE_SUB(ref_date.max_date, INTERVAL 7 DAY)) AS review_count_7d,

-- 30d
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 30 DAY)
    THEN compound_score
END) AS avg_sentiment_score_30d,
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 30 DAY)
    THEN CAST(risk_flag AS FLOAT64)
END) AS risk_review_ratio_30d,
COUNTIF(review_date >= DATE_SUB(ref_date.max_date, INTERVAL 30 DAY)) AS review_count_30d,

-- 60d
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 60 DAY)
    THEN compound_score
END) AS avg_sentiment_score_60d,
AVG(CASE
WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 60 DAY)
    THEN CAST(risk_flag AS FLOAT64)
END) AS risk_review_ratio_60d,
COUNTIF(review_date >= DATE_SUB(ref_date.max_date, INTERVAL 60 DAY)) AS review_count_60d,

-- Alert level
  {{ get_health_alert_level(
      "AVG(CASE WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 7 DAY) THEN compound_score END)",
      "AVG(CASE WHEN review_date >= DATE_SUB(ref_date.max_date, INTERVAL 7 DAY) THEN CAST(risk_flag AS FLOAT64) END)"
  ) }} AS alert_level

FROM review_with_category, ref_date
GROUP BY COALESCE(category, 'Unknown'), ref_date.max_date
ORDER BY
CASE alert_level
WHEN 'High Risk: Immediate Intervention' THEN 0
WHEN 'Medium Risk: Monitor Closely' THEN 1
ELSE 2
END,
avg_sentiment_score_7d ASC
