WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_reviews') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY review_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        review_id,
        order_id,
        user_id,
        product_id,
        cast(rating AS float64) AS rating,
        coalesce(review_text, '') AS review_text,
        date(cast(review_date AS timestamp)) AS review_date
    FROM deduplicated
)

SELECT * FROM renamed
