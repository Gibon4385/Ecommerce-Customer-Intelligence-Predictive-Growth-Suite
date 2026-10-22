WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_order_items') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY order_item_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        user_id,
        cast(quantity AS float64) AS quantity,
        cast(item_price AS float64) AS item_price,
        cast(item_total AS float64) AS item_total
    FROM deduplicated
)

SELECT * FROM renamed
