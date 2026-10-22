WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_orders') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY order_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        order_id,
        user_id,
        cast(total_amount AS float64) AS total_amount,
        order_status,
        date(cast(order_date AS timestamp)) AS order_date
    FROM deduplicated
)

SELECT * FROM renamed
