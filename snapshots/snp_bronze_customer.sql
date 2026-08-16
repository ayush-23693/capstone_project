{% snapshot snp_bronze_customer %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__customer_data') }}

),

unwrapped AS (

    SELECT
        cust.value AS customer_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__customer_data') }} b

    CROSS JOIN latest_file lf

    , LATERAL FLATTEN(
        input => b.RAW_DATA:customers_data
    ) cust

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        customer_json:customer_id::STRING AS customer_id,

        TRY_TO_TIMESTAMP_NTZ(
            customer_json:last_modified_date::STRING
        ) AS last_modified_date,

        customer_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE customer_id IS NOT NULL
  AND TRIM(customer_id) <> ''

{% endsnapshot %}