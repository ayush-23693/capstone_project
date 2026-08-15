{{ config(
    materialized='table'
) }}

WITH source_data AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__campaign_data') }}

),


--   FLATTEN CAMPAIGNS ARRAY

flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                s.SOURCE_FILE,
                '\\d{4}-\\d{2}-\\d{2}'
            )
        ) AS SNAPSHOT_DATE,

        campaign.value AS CAMPAIGN_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:campaigns_data
    ) campaign

),


--    EXTRACT + CLEAN + STANDARDIZE


cleaned AS (

    SELECT

        
        --   Audit  metadata
            

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        
        --   Campaign ID
            

        NULLIF(
            TRIM(CAMPAIGN_DATA:campaign_id::STRING),
            ''
        ) AS CAMPAIGN_ID,

        
        --   Campaign descriptive fields
            

        INITCAP(
            TRIM(CAMPAIGN_DATA:campaign_name::STRING)
        ) AS CAMPAIGN_NAME,

        INITCAP(
            TRIM(CAMPAIGN_DATA:campaign_type::STRING)
        ) AS CAMPAIGN_TYPE,

        INITCAP(
            TRIM(CAMPAIGN_DATA:channel::STRING)
        ) AS CHANNEL,

        NULLIF(
            TRIM(CAMPAIGN_DATA:description::STRING),
            ''
        ) AS DESCRIPTION,

        INITCAP(
            TRIM(CAMPAIGN_DATA:target_audience::STRING)
        ) AS TARGET_AUDIENCE_SEGMENT,

        
        --   Campaign dates
            

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(CAMPAIGN_DATA:start_date::STRING),
                ''
            )
        ) AS START_DATE,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(CAMPAIGN_DATA:end_date::STRING),
                ''
            )
        ) AS END_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(CAMPAIGN_DATA:last_modified_date::STRING),
                ''
            )
        ) AS LAST_MODIFIED_DATE,

        
        --   Monetary values
            

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(CAMPAIGN_DATA:budget::STRING),
                        '$',
                        ''
                    ),
                    ',',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS BUDGET,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(CAMPAIGN_DATA:total_cost::STRING),
                        '$',
                        ''
                    ),
                    ',',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS TOTAL_COST,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(CAMPAIGN_DATA:total_revenue::STRING),
                        '$',
                        ''
                    ),
                    ',',
                    ''
                ),
                ''
            ),
            18,
            2
        ) AS TOTAL_REVENUE,

        
        -- ROI (Numeric value only) haven't performed ROI validation here.


        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(CAMPAIGN_DATA:roi_calculation::STRING),
                ''
            ),
            18,
            4
        ) AS ROI

    FROM flattened

),


--   CAMPAIGN-SPECIFIC DERIVED ATTRIBUTES


derived AS (

    SELECT
        c.*,

        
        --   Campaign duration
        

        CASE
            WHEN c.START_DATE IS NOT NULL
             AND c.END_DATE IS NOT NULL
            THEN DATEDIFF(
                DAY,
                TO_DATE(c.START_DATE),
                TO_DATE(c.END_DATE)
            )
            ELSE NULL
        END AS CAMPAIGN_DURATION_DAYS

    FROM cleaned c

),


   -- DEDUPLICATION


deduplicated AS (

    SELECT *
    FROM derived

    WHERE CAMPAIGN_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY CAMPAIGN_ID

        ORDER BY
            LAST_MODIFIED_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)


--   FINAL SILVER CAMPAIGN TABLE

SELECT
    CAMPAIGN_ID,

    CAMPAIGN_NAME,
    CAMPAIGN_TYPE,
    CHANNEL,
    DESCRIPTION,
    TARGET_AUDIENCE_SEGMENT,

    START_DATE,
    END_DATE,
    CAMPAIGN_DURATION_DAYS,
    LAST_MODIFIED_DATE,

    BUDGET,
    TOTAL_COST,
    TOTAL_REVENUE,
    ROI,

    SNAPSHOT_DATE,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM deduplicated