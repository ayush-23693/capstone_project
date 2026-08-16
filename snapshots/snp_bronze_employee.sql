{% snapshot snp_bronze_employee %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='employee_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__employee_data') }}

),

unwrapped AS (

    SELECT
        emp.value AS employee_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__employee_data') }} b

    CROSS JOIN latest_file lf

    , LATERAL FLATTEN(
        input => b.RAW_DATA:employees_data
    ) emp

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        employee_json:employee_id::STRING AS employee_id,

        TRY_TO_TIMESTAMP_NTZ(
            employee_json:last_modified_date::STRING
        ) AS last_modified_date,

        employee_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE employee_id IS NOT NULL
  AND TRIM(employee_id) <> ''

{% endsnapshot %}