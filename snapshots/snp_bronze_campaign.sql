{% snapshot snp_bronze_campaign %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='campaign_id',
        strategy='timestamp',
        updated_at='last_modified_date',
        invalidate_hard_deletes=True
    )
}}

WITH latest_file AS (

    SELECT
        MAX(SOURCE_FILE) AS SOURCE_FILE
    FROM {{ ref('stg_bronze__campaign_data') }}

),

unwrapped AS (

    SELECT
        campaign.value AS campaign_json,
        b.LOADED_AT,
        b.SOURCE_FILE,
        b.BATCH_ID

    FROM {{ ref('stg_bronze__campaign_data') }} b
    CROSS JOIN latest_file lf
    CROSS JOIN LATERAL FLATTEN(
        input => b.RAW_DATA:campaigns_data
    ) campaign

    WHERE b.SOURCE_FILE = lf.SOURCE_FILE

),

prepared AS (

    SELECT
        campaign_json:campaign_id::STRING AS campaign_id,

        TRY_TO_DATE(
            campaign_json:last_modified_date::STRING
        ) AS last_modified_date,

        campaign_json AS raw_data,

        LOADED_AT,
        SOURCE_FILE,
        BATCH_ID

    FROM unwrapped

)

SELECT *
FROM prepared

WHERE campaign_id IS NOT NULL
  AND TRIM(campaign_id) <> ''

{% endsnapshot %}