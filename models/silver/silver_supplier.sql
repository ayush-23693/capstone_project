{{ config(materialized='table') }}

WITH source_data AS (

    SELECT
        SOURCE_FILE,
        ROW_NUMBER,
        RAW_DATA,
        LOADED_AT,
        BATCH_ID
    FROM {{ ref('stg_bronze__supplier_data') }}

),


   -- FLATTEN SUPPLIERS ARRAY


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

        supplier.value AS SUPPLIER_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:suppliers_data
    ) supplier

),


   -- EXTRACT + CLEAN + STANDARDIZE


cleaned AS (


    SELECT

         --Audit / lineage metadata

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,

        SNAPSHOT_DATE,

         --Supplier identity

        NULLIF(
            TRIM(SUPPLIER_DATA:supplier_id::STRING),
            ''
        ) AS SUPPLIER_ID,

        INITCAP(
            REGEXP_REPLACE(
                TRIM(SUPPLIER_DATA:supplier_name::STRING),
                '[^A-Za-z0-9 ''&.,-]',
                ''
            )
        ) AS SUPPLIER_NAME,

        INITCAP(
            TRIM(SUPPLIER_DATA:supplier_type::STRING)
        ) AS SUPPLIER_TYPE,

        UPPER(
            TRIM(SUPPLIER_DATA:credit_rating::STRING)
        ) AS CREDIT_RATING,

        COALESCE(
            TRY_TO_BOOLEAN(
                SUPPLIER_DATA:is_active::STRING
            ),
            FALSE

        ) AS IS_ACTIVE,

         --Contact information

        INITCAP(
            TRIM(
                SUPPLIER_DATA:contact_information:contact_person::STRING
            )
        ) AS CONTACT_PERSON,

        CASE
            WHEN NULLIF(
                TRIM(
                    SUPPLIER_DATA:contact_information:email::STRING
                ),
                ''
            ) IS NULL
            THEN NULL

            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        SUPPLIER_DATA:contact_information:email::STRING
                    )
                ),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'
            )
            THEN LOWER(
                TRIM(
                    SUPPLIER_DATA:contact_information:email::STRING
                )
            )

            ELSE NULL

        END AS EMAIL,

        CASE
            WHEN NULLIF(
                TRIM(
                    SUPPLIER_DATA:contact_information:email::STRING
                ),
                ''
            ) IS NULL
            THEN NULL

            WHEN REGEXP_LIKE(
                LOWER(
                    TRIM(
                        SUPPLIER_DATA:contact_information:email::STRING
                    )
                ),
                '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'
            )
            THEN FALSE

            ELSE TRUE

        END AS INVALID_EMAIL_FLAG,

        
         -- Supplier phone: supports US country prefix +1.
         
        

        CASE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN REGEXP_REPLACE(
                TRIM(
                    SUPPLIER_DATA:contact_information:phone::STRING
                ),
                '[^0-9]',
                ''
            )

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN SUBSTR(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                ),
                2
            )

            ELSE NULL

        END AS PHONE_NUMBER,

        CASE

            WHEN NULLIF(
                TRIM(
                    SUPPLIER_DATA:contact_information:phone::STRING
                ),
                ''
            ) IS NULL

            THEN NULL

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 10

            THEN FALSE

            WHEN LENGTH(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                )
            ) = 11

            AND LEFT(
                REGEXP_REPLACE(
                    TRIM(
                        SUPPLIER_DATA:contact_information:phone::STRING
                    ),
                    '[^0-9]',
                    ''
                ),
                1
            ) = '1'

            THEN FALSE

            ELSE TRUE

        END AS INVALID_PHONE_FLAG,

        
         --Address supplied as one string
        

        NULLIF(
            TRIM(
                REGEXP_REPLACE(
                    SUPPLIER_DATA:contact_information:address::STRING,
                    '[[:space:]]+',
                    ' '
                )
            ),
            ''

        ) AS ADDRESS,

         -- Contract information

        NULLIF(
            TRIM(
                SUPPLIER_DATA:contract_details:contract_id::STRING
            ),
            ''
        ) AS CONTRACT_ID,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:contract_details:start_date::STRING
                ),
                ''
            )
        ) AS CONTRACT_START_DATE,

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:contract_details:end_date::STRING
                ),
                ''
            )
        ) AS CONTRACT_END_DATE,

        TRY_TO_BOOLEAN(
            SUPPLIER_DATA:contract_details:exclusivity::STRING
        ) AS CONTRACT_EXCLUSIVITY,

        TRY_TO_BOOLEAN(
            SUPPLIER_DATA:contract_details:renewal_option::STRING

        ) AS CONTRACT_RENEWAL_OPTION
        ,
         --Supplier operating fields

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:last_order_date::STRING
                ),
                ''
            )
        ) AS LAST_ORDER_DATE,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:lead_time_days::STRING
                ),
                ''
            )
        ) AS LEAD_TIME_DAYS,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:minimum_order_quantity::STRING
                ),
                ''
            )
        ) AS MINIMUM_ORDER_QUANTITY,

        INITCAP(
            TRIM(SUPPLIER_DATA:payment_terms::STRING)
        ) AS PAYMENT_TERMS,

        INITCAP(
            TRIM(SUPPLIER_DATA:preferred_carrier::STRING)
        ) AS PREFERRED_CARRIER,

        NULLIF(
            TRIM(SUPPLIER_DATA:tax_id::STRING),
            ''
        ) AS TAX_ID,

        NULLIF(
            TRIM(SUPPLIER_DATA:website::STRING),
            ''
        ) AS WEBSITE,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(SUPPLIER_DATA:year_established::STRING),
                ''
            )

        ) AS YEAR_ESTABLISHED,

         --Performance metrics

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:performance_metrics:average_delay_days::STRING
                ),
                ''
            ),
            18,
            2
        ) AS AVERAGE_DELAY_DAYS,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:performance_metrics:defect_rate::STRING
                ),
                ''
            ),
            18,
            2
        ) AS DEFECT_RATE,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:performance_metrics:on_time_delivery_rate::STRING
                ),
                ''
            ),
            18,
            2
        ) AS ON_TIME_DELIVERY_RATE,

        INITCAP(
            TRIM(
                SUPPLIER_DATA:performance_metrics:quality_rating::STRING
            )
        ) AS QUALITY_RATING,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:performance_metrics:response_time_hours::STRING
                ),
                ''
            ),
            18,
            2
        ) AS RESPONSE_TIME_HOURS,

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:performance_metrics:returns_percentage::STRING
                ),
                ''
            ),
            18,
            2

        ) AS RETURNS_PERCENTAGE,

         --Categories supplied

        SUPPLIER_DATA:categories_supplied AS CATEGORIES_SUPPLIED,


         --Last modified

        TRY_TO_DATE(
            NULLIF(
                TRIM(
                    SUPPLIER_DATA:last_modified_date::STRING
                ),
                ''
            )
        ) AS LAST_MODIFIED_DATE

    FROM flattened

),

cleaned_contact_enriched AS (

    SELECT
        c.*,

        RTRIM(
            LTRIM(
                CONCAT(
                    CASE
                        WHEN c.CONTACT_PERSON IS NOT NULL
                        THEN c.CONTACT_PERSON || ' | '
                        ELSE ''
                    END,

                    CASE
                        WHEN c.EMAIL IS NOT NULL
                        THEN c.EMAIL || ' | '
                        ELSE ''
                    END,

                    CASE
                        WHEN c.PHONE_NUMBER IS NOT NULL
                        THEN c.PHONE_NUMBER || ' | '
                        ELSE ''
                    END,

                    CASE
                        WHEN c.ADDRESS IS NOT NULL
                        THEN c.ADDRESS
                        ELSE ''
                    END
                )
            ),
            ' | '
        ) AS CONTACT_INFORMATION

    FROM cleaned c

),


   -- DEDUPLICATION


deduplicated AS (

    SELECT *
    FROM cleaned_contact_enriched

    WHERE SUPPLIER_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY SUPPLIER_ID

        ORDER BY
            LAST_MODIFIED_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)


   --FINAL SILVER SUPPLIER TABLE


SELECT

    SUPPLIER_ID,
    SUPPLIER_NAME,
    SUPPLIER_TYPE,
    CREDIT_RATING,
    IS_ACTIVE,

    CONTACT_INFORMATION,

    CONTRACT_ID,
    CONTRACT_START_DATE,
    CONTRACT_END_DATE,
    CONTRACT_EXCLUSIVITY,
    CONTRACT_RENEWAL_OPTION,

    LAST_ORDER_DATE,
    LEAD_TIME_DAYS,
    MINIMUM_ORDER_QUANTITY,
    PAYMENT_TERMS,
    PREFERRED_CARRIER,

    TAX_ID,
    WEBSITE,
    YEAR_ESTABLISHED,

    AVERAGE_DELAY_DAYS,
    DEFECT_RATE,
    ON_TIME_DELIVERY_RATE,
    QUALITY_RATING,
    RESPONSE_TIME_HOURS,
    RETURNS_PERCENTAGE,

    CATEGORIES_SUPPLIED,

    LAST_MODIFIED_DATE,
    SNAPSHOT_DATE,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM deduplicated