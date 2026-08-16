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
    FROM {{ ref('stg_bronze__store_data') }}

),




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

        store.value AS STORE_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:stores_data
    ) store

),


   -- EXTRACT + CLEAN + STANDARDIZE

cleaned AS (

    SELECT

       
        --   Audit metadata
        

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        

        NULLIF(
            TRIM(STORE_DATA:store_id::STRING),
            ''
        ) AS STORE_ID,

        
        --store name

        INITCAP(
            REGEXP_REPLACE(
                TRIM(STORE_DATA:store_name::STRING),
                '[^A-Za-z0-9 ''&-]',
                ''
            )
        ) AS STORE_NAME,

        
        

        INITCAP(
            TRIM(STORE_DATA:store_type::STRING)
        ) AS STORE_TYPE,

        

        INITCAP(
            TRIM(STORE_DATA:region::STRING)
        ) AS REGION,

        
        --   Address
         

        INITCAP(
            TRIM(STORE_DATA:address:street::STRING)
        ) AS STREET,

        INITCAP(
            TRIM(STORE_DATA:address:city::STRING)
        ) AS CITY,

        UPPER(
            TRIM(STORE_DATA:address:state::STRING)
        ) AS STATE,

        UPPER(
            TRIM(STORE_DATA:address:country::STRING)
        ) AS COUNTRY,

        CASE
            WHEN REGEXP_LIKE(
                TRIM(STORE_DATA:address:zip_code::STRING),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN TRIM(STORE_DATA:address:zip_code::STRING)
            ELSE NULL
        END AS ZIP_CODE,

        CASE
            WHEN NULLIF(
                TRIM(STORE_DATA:address:zip_code::STRING),
                ''
            ) IS NULL
            THEN NULL

            WHEN REGEXP_LIKE(
                TRIM(STORE_DATA:address:zip_code::STRING),
                '^[0-9]{5}(-[0-9]{4})?$'
            )
            THEN FALSE

            ELSE TRUE
        END AS INVALID_ZIP_CODE_FLAG,

        CONCAT_WS(
            ', ',
            NULLIF(
                INITCAP(
                    TRIM(STORE_DATA:address:street::STRING)
                ),
                ''
            ),
            NULLIF(
                INITCAP(
                    TRIM(STORE_DATA:address:city::STRING)
                ),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(STORE_DATA:address:state::STRING)
                ),
                ''
            ),
            NULLIF(
                TRIM(STORE_DATA:address:zip_code::STRING),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(STORE_DATA:address:country::STRING)
                ),
                ''
            )
        ) AS STANDARDIZED_ADDRESS,

        
          -- Store dates
        

        TRY_TO_DATE(
            NULLIF(
                TRIM(STORE_DATA:opening_date::STRING),
                ''
            )
        ) AS OPENING_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(STORE_DATA:last_modified_date::STRING),
                ''
            )
        ) AS LAST_MODIFIED_DATE,

    
        --   Store measures
        

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(STORE_DATA:size_sq_ft::STRING),
                ''
            )
        ) AS SIZE_SQ_FT,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(STORE_DATA:employee_count::STRING),
                ''
            )
        ) AS EMPLOYEE_COUNT,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(STORE_DATA:current_sales::STRING),
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
        ) AS CURRENT_SALES,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(STORE_DATA:sales_target::STRING),
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
        ) AS SALES_TARGET

    FROM flattened

),


   -- STORE-SPECIFIC DERIVED ATTRIBUTES

derived AS (

    SELECT
        c.*,

        
           --Store size category
        

        CASE
            WHEN c.SIZE_SQ_FT < 5000
                THEN 'Small'

            WHEN c.SIZE_SQ_FT <= 10000
                THEN 'Medium'

            WHEN c.SIZE_SQ_FT > 10000
                THEN 'Large'

            ELSE NULL
        END AS SIZE_CATEGORY,

        
           --Store age
        

        CASE
            WHEN c.OPENING_DATE IS NOT NULL
            THEN
                DATEDIFF(
                    YEAR,
                    c.OPENING_DATE,
                    CURRENT_DATE()
                )
                -
                CASE
                    WHEN DATEADD(
                        YEAR,
                        DATEDIFF(
                            YEAR,
                            c.OPENING_DATE,
                            CURRENT_DATE()
                        ),
                        c.OPENING_DATE
                    ) > CURRENT_DATE()
                    THEN 1
                    ELSE 0
                END
            ELSE NULL
        END AS STORE_AGE,

        
           --Sales target achievement %
        

        CAST(
            CASE
                WHEN c.SALES_TARGET > 0
                THEN (
                    c.CURRENT_SALES
                    / c.SALES_TARGET
                ) * 100.0
                ELSE NULL
            END AS DECIMAL(18,2)
        )AS SALES_TARGET_ACHIEVEMENT_PERCENTAGE,

        
        --   Revenue per square foot
        

        CAST(
            CASE
                WHEN c.SIZE_SQ_FT > 0
                THEN c.CURRENT_SALES / c.SIZE_SQ_FT
                ELSE NULL
            END AS DECIMAL(18,2)
        )AS REVENUE_PER_SQ_FT,

        
           --Sales / employee
        

        CAST(
            CASE
                WHEN c.EMPLOYEE_COUNT > 0
                THEN c.CURRENT_SALES / c.EMPLOYEE_COUNT
                ELSE NULL
            END AS DECIMAL(18,2)
        )AS SALES_PER_EMPLOYEE

    FROM cleaned c

),


   -- PERFORMANCE ISSUE FLAG

performance AS (

    SELECT
        d.*,

        CASE
            WHEN d.SALES_TARGET_ACHIEVEMENT_PERCENTAGE < 90
                THEN TRUE
            ELSE FALSE
        END AS PERFORMANCE_ISSUE_FLAG

    FROM derived d

),


   --5. DEDUPLICATION


deduplicated AS (

    SELECT *
    FROM performance

    WHERE STORE_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY STORE_ID

        ORDER BY
            LAST_MODIFIED_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)


   --FINAL SILVER STORE TABLE


SELECT
    STORE_ID,
    STORE_NAME,
    STORE_TYPE,
    REGION,

    STREET,
    CITY,
    STATE,
    COUNTRY,
    ZIP_CODE,
    INVALID_ZIP_CODE_FLAG,
    STANDARDIZED_ADDRESS,

    OPENING_DATE,
    STORE_AGE,

    SIZE_SQ_FT,
    SIZE_CATEGORY,

    EMPLOYEE_COUNT,
    CURRENT_SALES,
    SALES_TARGET,

    SALES_TARGET_ACHIEVEMENT_PERCENTAGE,
    REVENUE_PER_SQ_FT,
    SALES_PER_EMPLOYEE,

    PERFORMANCE_ISSUE_FLAG,

    LAST_MODIFIED_DATE,
    SNAPSHOT_DATE,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM deduplicated