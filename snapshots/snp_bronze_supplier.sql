{% snapshot snp_bronze_supplier %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='supplier_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__supplier_data') }}

),

unwrapped AS (

    SELECT
        supplier.value AS supplier_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__supplier_data') }} b
    CROSS JOIN latest_file lf
    CROSS JOIN LATERAL FLATTEN(
        input => b.RAW_DATA:suppliers_data
    ) supplier

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        supplier_json:supplier_id::STRING AS supplier_id,

        TRY_TO_TIMESTAMP_NTZ(
            supplier_json:last_modified_date::STRING
        ) AS last_modified_date,

        supplier_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE supplier_id IS NOT NULL
  AND TRIM(supplier_id) <> ''

{% endsnapshot %}