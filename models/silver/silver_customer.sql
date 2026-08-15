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
    FROM {{ ref('stg_bronze__customer_data') }}

),

   
   -- FLATTEN CUSTOMER ARRAY


flattened AS (

    SELECT
        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        --Source snapshot date derived from filename
        TRY_TO_DATE(
            REGEXP_SUBSTR(
                s.SOURCE_FILE,
                '\\d{4}-\\d{2}-\\d{2}'
            )
        ) AS SNAPSHOT_DATE,

        customer.value AS CUSTOMER_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:customers_data
    ) customer

),


   -- EXTRACT + CLEAN + STANDARDIZE
    

cleaned AS (

    SELECT

         
           --Audit  metadata
            

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

         
           --Customer ID
            

        NULLIF(
            TRIM(CUSTOMER_DATA:customer_id::STRING),
            ''
        ) AS CUSTOMER_ID,

        /* 
           Names
           Trim
           Remove unwanted characters
           Standardize capitalization
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(CUSTOMER_DATA:first_name::STRING),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS FIRST_NAME,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(CUSTOMER_DATA:last_name::STRING),
                '[^A-Za-z0-9 ''-]',
                ''
            )
        ) AS LAST_NAME,

        /* 
           Email Normalize + validate
           Invalid values become NULL
            */

        CASE
            WHEN REGEXP_LIKE(
                LOWER(TRIM(CUSTOMER_DATA:email::STRING)),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'
            )
            THEN LOWER(TRIM(CUSTOMER_DATA:email::STRING))
            ELSE NULL
        END AS EMAIL,

        CASE
            WHEN NULLIF(
                TRIM(CUSTOMER_DATA:email::STRING),
                ''
            ) IS NULL
            THEN NULL

            WHEN REGEXP_LIKE(
                LOWER(TRIM(CUSTOMER_DATA:email::STRING)),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'
            )
            THEN FALSE

            ELSE TRUE
        END AS INVALID_EMAIL_FLAG,

        
           --Phone Normalize to 10 digits
        

        CASE
            WHEN REGEXP_LIKE(
                REGEXP_REPLACE(
                    TRIM(CUSTOMER_DATA:phone::STRING),
                    '[^0-9]',
                    ''
                ),
                '^[0-9]{10}$'
            )
            THEN REGEXP_REPLACE(
                TRIM(CUSTOMER_DATA:phone::STRING),
                '[^0-9]',
                ''
            )
            ELSE NULL
        END AS PHONE_NUMBER,

        CASE
            WHEN NULLIF(
                TRIM(CUSTOMER_DATA:phone::STRING),
                ''
            ) IS NULL
            THEN NULL

            WHEN REGEXP_LIKE(
                REGEXP_REPLACE(
                    TRIM(CUSTOMER_DATA:phone::STRING),
                    '[^0-9]',
                    ''
                ),
                '^[0-9]{10}$'
            )
            THEN FALSE

            ELSE TRUE
        END AS INVALID_PHONE_FLAG,

        
           --Address standardization
        

        INITCAP(
            TRIM(CUSTOMER_DATA:address:street::STRING)
        ) AS STREET,

        INITCAP(
            TRIM(CUSTOMER_DATA:address:city::STRING)
        ) AS CITY,

        UPPER(
            TRIM(CUSTOMER_DATA:address:state::STRING)
        ) AS STATE,

        UPPER(
            TRIM(CUSTOMER_DATA:address:country::STRING)
        ) AS COUNTRY,

        NULLIF(
            TRIM(CUSTOMER_DATA:address:zip_code::STRING),
            ''
        ) AS ZIP_CODE,

        CONCAT_WS(
            ', ',
            NULLIF(
                INITCAP(
                    TRIM(CUSTOMER_DATA:address:street::STRING)
                ),
                ''
            ),
            NULLIF(
                INITCAP(
                    TRIM(CUSTOMER_DATA:address:city::STRING)
                ),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(CUSTOMER_DATA:address:state::STRING)
                ),
                ''
            ),
            NULLIF(
                TRIM(CUSTOMER_DATA:address:zip_code::STRING),
                ''
            ),
            NULLIF(
                UPPER(
                    TRIM(CUSTOMER_DATA:address:country::STRING)
                ),
                ''
            )
        ) AS STANDARDIZED_ADDRESS,

         
           --Customer attributes
        

        UPPER(
            TRIM(CUSTOMER_DATA:income_bracket::STRING)
        ) AS INCOME_BRACKET,

        INITCAP(
            TRIM(CUSTOMER_DATA:occupation::STRING)
        ) AS OCCUPATION,

        UPPER(
            TRIM(CUSTOMER_DATA:loyalty_tier::STRING)
        ) AS LOYALTY_TIER,

        INITCAP(
            TRIM(CUSTOMER_DATA:preferred_communication::STRING)
        ) AS PREFERRED_COMMUNICATION,

        INITCAP(
            TRIM(CUSTOMER_DATA:preferred_payment_method::STRING)
        ) AS PREFERRED_PAYMENT_METHOD,

        COALESCE(
            TRY_TO_BOOLEAN(
                CUSTOMER_DATA:marketing_opt_in::STRING
            ),
            FALSE
        ) AS MARKETING_OPT_IN,

        
           --Dates
        

        COALESCE(
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'YYYY-MM-DD'
            ),
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'MM-DD-YYYY'
            ),
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'DD-MM-YYYY'
            ),
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'YYYY/MM/DD'
            ),
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'MM/DD/YYYY'
            ),
            TRY_TO_DATE(
                NULLIF(
                    TRIM(CUSTOMER_DATA:birth_date::STRING),
                    ''
                ),
                'DD/MM/YYYY'
            )
        ) AS BIRTH_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(CUSTOMER_DATA:registration_date::STRING),
                ''
            )
        ) AS REGISTRATION_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(CUSTOMER_DATA:last_purchase_date::STRING),
                ''
            )
        ) AS LAST_PURCHASE_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(CUSTOMER_DATA:last_modified_date::STRING),
                ''
            )
        ) AS LAST_MODIFIED_DATE,

        
           --Numeric values Preserve NULL when unknown
            

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(CUSTOMER_DATA:total_purchases::STRING),
                ''
            )
        ) AS TOTAL_PURCHASES,

        /* Monetary value*/

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(CUSTOMER_DATA:total_spend::STRING),
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
        ) AS TOTAL_SPEND

    FROM flattened

),


   -- CUSTOMER-SPECIFIC DERIVED ATTRIBUTES
    

derived AS (

    SELECT
        c.*,

        /* Full name */

        CONCAT_WS(
            ' ',
            NULLIF(c.FIRST_NAME, ''),
            NULLIF(c.LAST_NAME, '')
        ) AS FULL_NAME,

        /* age */

        CASE
            WHEN c.BIRTH_DATE IS NULL
            THEN NULL

            ELSE
                DATEDIFF(
                    YEAR,
                    c.BIRTH_DATE,
                    CURRENT_DATE()
                )
                -
                CASE
                    WHEN DATEADD(
                        YEAR,
                        DATEDIFF(
                            YEAR,
                            c.BIRTH_DATE,
                            CURRENT_DATE()
                        ),
                        c.BIRTH_DATE
                    ) > CURRENT_DATE()
                    THEN 1
                    ELSE 0
                END
        END AS CUSTOMER_AGE

    FROM cleaned c

),

/* 
    CUSTOMER SEGMENT
*/

segmented AS (

    SELECT
        d.*,

        CASE
            WHEN CUSTOMER_AGE BETWEEN 18 AND 35
                THEN 'Young'

            WHEN CUSTOMER_AGE BETWEEN 36 AND 55
                THEN 'Middle-aged'

            WHEN CUSTOMER_AGE >= 56
                THEN 'Senior'

            ELSE NULL
        END AS CUSTOMER_SEGMENT

    FROM derived d

),

/* 
    DEDUPLICATION
*/

deduplicated AS (

    SELECT *
    FROM segmented

    WHERE CUSTOMER_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY CUSTOMER_ID

        ORDER BY
            LAST_MODIFIED_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/* 
   FINAL SILVER CUSTOMER TABLE
*/

SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,

    BIRTH_DATE,
    CUSTOMER_AGE,
    CUSTOMER_SEGMENT,

    EMAIL,
    INVALID_EMAIL_FLAG,

    PHONE_NUMBER,
    INVALID_PHONE_FLAG,

    STREET,
    CITY,
    STATE,
    COUNTRY,
    ZIP_CODE,
    STANDARDIZED_ADDRESS,

    OCCUPATION,
    LOYALTY_TIER,
    INCOME_BRACKET,
    MARKETING_OPT_IN,
    PREFERRED_COMMUNICATION,
    PREFERRED_PAYMENT_METHOD,

    REGISTRATION_DATE,
    LAST_PURCHASE_DATE,
    LAST_MODIFIED_DATE,

    TOTAL_PURCHASES,
    TOTAL_SPEND,

    SNAPSHOT_DATE,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM deduplicated