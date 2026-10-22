WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_events') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY event_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        event_id,
        user_id,
        event_type,
        product_id,
        cast(event_timestamp AS timestamp) AS event_timestamp
    FROM deduplicated
    WHERE user_id IS NOT null
)

SELECT * FROM renamed
