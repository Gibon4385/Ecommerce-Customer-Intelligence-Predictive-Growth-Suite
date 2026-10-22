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
        name,
        gender,
        city,
        date(cast(signup_date AS timestamp)) AS signup_date
    FROM deduplicated
)

SELECT * FROM renamed
-- test
