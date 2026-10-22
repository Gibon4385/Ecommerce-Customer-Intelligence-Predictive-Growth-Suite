WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_products') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY product_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        cast(price AS float64) AS price,
        cast(rating AS float64) AS rating
    FROM deduplicated
)

SELECT * FROM renamed
