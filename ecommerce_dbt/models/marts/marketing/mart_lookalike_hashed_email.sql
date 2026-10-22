WITH export_candidates AS (
    SELECT
        seed.user_id,
        seed.segment,
        seed.recency,
        seed.frequency,
        seed.monetary,
        seed.predicted_profit_90_days,
        u.hashed_email
    FROM {{ ref('mart_lookalike_seed') }} AS seed
    INNER JOIN {{ ref('stg_users_hashed') }} AS u ON seed.user_id = u.user_id
    WHERE u.hashed_email IS NOT NULL
)

-- Different customer records can legitimately share an email address. Keep
-- staging faithful to the source, but make the outbound activation audience
-- deterministic and unique by hashed identity.
SELECT *
FROM export_candidates
QUALIFY row_number() OVER (
    PARTITION BY hashed_email
    ORDER BY predicted_profit_90_days DESC, user_id
) = 1
