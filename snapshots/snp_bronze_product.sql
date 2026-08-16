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

WITH unwrapped AS (

    SELECT

        prod.value AS product_json,

        b.LOADED_AT,
        b.SOURCE_FILE,
        b.ROW_NUMBER,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__product_data') }} b

    CROSS JOIN LATERAL FLATTEN(
        INPUT => b.RAW_DATA:products_data
    ) prod

),

prepared AS (

    SELECT

        UPPER(
            TRIM(
                product_json:product_id::STRING
            )
        ) AS product_id,

        TRY_TO_TIMESTAMP_NTZ(
            product_json:last_modified_date::STRING
        ) AS last_modified_date,

        product_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        ROW_NUMBER,
        BATCH_ID

    FROM unwrapped

),

latest_product_version AS (

    SELECT *

    FROM prepared

    WHERE product_id IS NOT NULL
      AND TRIM(product_id) <> ''

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY product_id

        ORDER BY
            last_modified_date DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

SELECT *

FROM latest_product_version

{% endsnapshot %}