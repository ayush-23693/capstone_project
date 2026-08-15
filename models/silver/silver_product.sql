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
    FROM {{ ref('stg_bronze__product_data') }}

),

/*
  FLATTEN PRODUCTS ARRAY
 */

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

        product.value AS PRODUCT_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:products_data
    ) product

),

/*
   EXTRACT + CLEAN + STANDARDIZE
*/

cleaned AS (

    SELECT

        /* 
           Audit  metadata
         */

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        /* 
           Product ID
        */

        NULLIF(
            TRIM(PRODUCT_DATA:product_id::STRING),
            ''
        ) AS PRODUCT_ID,

        /* 
           Product name Trim + unwanted character removal
           + Pascal  Case
        */

        INITCAP(
            REGEXP_REPLACE(
                TRIM(PRODUCT_DATA:name::STRING),
                '[^A-Za-z0-9 &''-]',
                ''
            )
        ) AS PRODUCT_NAME,

        /* 
           Description fields
        */

        NULLIF(
            TRIM(PRODUCT_DATA:short_description::STRING),
            ''
        ) AS SHORT_DESCRIPTION,

        NULLIF(
            TRIM(PRODUCT_DATA:technical_specs::STRING),
            ''
        ) AS TECHNICAL_SPECS,

        /* 
           Product hierarchy fields
        */

        INITCAP(
            TRIM(PRODUCT_DATA:category::STRING)
        ) AS CATEGORY,

        INITCAP(
            TRIM(PRODUCT_DATA:subcategory::STRING)
        ) AS SUBCATEGORY,

        INITCAP(
            TRIM(PRODUCT_DATA:product_line::STRING)
        ) AS PRODUCT_LINE,

        /* 
           Other descriptive fields
        */

        INITCAP(
            TRIM(PRODUCT_DATA:brand::STRING)
        ) AS BRAND,

        INITCAP(
            TRIM(PRODUCT_DATA:color::STRING)
        ) AS COLOR,

        TRIM(
            PRODUCT_DATA:size::STRING
        ) AS SIZE,

        /* 
           Monetary values
            */

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(PRODUCT_DATA:unit_price::STRING),
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
        ) AS UNIT_PRICE,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(PRODUCT_DATA:cost_price::STRING),
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
        ) AS COST_PRICE,

        /* 
           Inventory fields
            */

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(PRODUCT_DATA:stock_quantity::STRING),
                ''
            )
        ) AS STOCK_QUANTITY,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(PRODUCT_DATA:reorder_level::STRING),
                ''
            )
        ) AS REORDER_LEVEL,

        /* 
           Supplier relationship
            */

        NULLIF(
            TRIM(PRODUCT_DATA:supplier_id::STRING),
            ''
        ) AS SUPPLIER_ID,

        /* 
           Last modified date
            */

        TRY_TO_DATE(
            NULLIF(
                TRIM(PRODUCT_DATA:last_modified_date::STRING),
                ''
            )
        ) AS LAST_MODIFIED_DATE

    FROM flattened

),

/*
   PRODUCT-SPECIFIC DERIVED ATTRIBUTES
   */

derived AS (

    SELECT
        c.*,

        /* 
           Product full description
           name + short_description
           + technical_specs
            */

        CONCAT_WS(
            ' | ',
            NULLIF(c.PRODUCT_NAME, ''),
            NULLIF(c.SHORT_DESCRIPTION, ''),
            NULLIF(c.TECHNICAL_SPECS, '')
        ) AS PRODUCT_FULL_DESCRIPTION,

        /* 
           Product hierarchy
            */

        CONCAT_WS(
            ' > ',
            NULLIF(c.CATEGORY, ''),
            NULLIF(c.SUBCATEGORY, ''),
            NULLIF(c.PRODUCT_LINE, '')
        ) AS PRODUCT_HIERARCHY,

        /* 
           Profit margin percentage, Guarded against divide-by-zero.
            */

        CAST(
            CASE
                WHEN c.UNIT_PRICE > 0
                THEN ((c.UNIT_PRICE - c.COST_PRICE) / c.UNIT_PRICE) * 100.0
                ELSE NULL
            END AS DECIMAL(18,2)
        ) AS PROFIT_MARGIN_PERCENTAGE,

        /* 
           Low-stock flag
            */

        CASE
            WHEN c.STOCK_QUANTITY IS NOT NULL
             AND c.REORDER_LEVEL IS NOT NULL
             AND c.STOCK_QUANTITY < c.REORDER_LEVEL
            THEN TRUE
            ELSE FALSE
        END AS LOW_STOCK_FLAG

    FROM cleaned c

),

/*
   4. DEDUPLICATION
   */

deduplicated AS (

    SELECT *
    FROM derived

    WHERE PRODUCT_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY PRODUCT_ID

        ORDER BY
            LAST_MODIFIED_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/*
   FINAL SILVER PRODUCT TABLE
   */

SELECT
    PRODUCT_ID,

    PRODUCT_NAME,
    SHORT_DESCRIPTION,
    TECHNICAL_SPECS,
    PRODUCT_FULL_DESCRIPTION,

    CATEGORY,
    SUBCATEGORY,
    PRODUCT_LINE,
    PRODUCT_HIERARCHY,

    BRAND,
    COLOR,
    SIZE,

    UNIT_PRICE,
    COST_PRICE,
    PROFIT_MARGIN_PERCENTAGE,

    STOCK_QUANTITY,
    REORDER_LEVEL,
    LOW_STOCK_FLAG,

    SUPPLIER_ID,

    LAST_MODIFIED_DATE,
    SNAPSHOT_DATE,

    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM deduplicated