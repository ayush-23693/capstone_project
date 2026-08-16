{% snapshot snp_bronze_store %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='store_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__store_data') }}

),

unwrapped AS (

    SELECT
        store.value AS store_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__store_data') }} b

    CROSS JOIN latest_file lf,

    LATERAL FLATTEN(
        input => b.RAW_DATA:stores_data
    ) store

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        store_json:store_id::STRING AS store_id,

        TRY_TO_TIMESTAMP_NTZ(
            store_json:last_modified_date::STRING
        ) AS last_modified_date,

        store_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE store_id IS NOT NULL
  AND TRIM(store_id) <> ''

{% endsnapshot %}