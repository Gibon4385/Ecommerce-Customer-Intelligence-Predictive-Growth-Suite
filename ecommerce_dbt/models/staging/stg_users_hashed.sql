WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'raw_users') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY user_id
        ORDER BY _ingested_at DESC NULLS LAST, _kafka_offset DESC NULLS LAST
    ) = 1
),

renamed AS (
    SELECT
        user_id,
        to_hex(sha256(lower(trim(email)))) AS hashed_email
    FROM deduplicated
)

SELECT * FROM renamed
