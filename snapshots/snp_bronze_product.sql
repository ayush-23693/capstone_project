{% snapshot snp_bronze_product %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__product_data') }}

),

unwrapped AS (

    SELECT
        prod.value AS product_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__product_data') }} b
    CROSS JOIN latest_file lf
    CROSS JOIN LATERAL FLATTEN(
        input => b.RAW_DATA:products_data
    ) prod

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        product_json:product_id::STRING AS product_id,

        TRY_TO_TIMESTAMP_NTZ(
            product_json:last_modified_date::STRING
        ) AS last_modified_date,

        product_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE product_id IS NOT NULL
  AND TRIM(product_id) <> ''

{% endsnapshot %}