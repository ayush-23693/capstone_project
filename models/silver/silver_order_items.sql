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
    FROM {{ ref('stg_bronze__orders_data') }}

),

/*
    FLATTEN ORDERS
 */

orders_flattened AS (

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

        ord.index AS ORDER_ARRAY_INDEX,
        ord.value AS ORDER_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:orders_data
    ) ord

),

/*
    FLATTEN ORDER ITEMS
 */

items_flattened AS (

    SELECT
        o.SOURCE_FILE,
        o.ROW_NUMBER,
        o.LOADED_AT,
        o.BATCH_ID,
        o.SNAPSHOT_DATE,

        /* Natural item identifier within an order */
        item.index + 1 AS ORDER_ITEM_NUMBER,

        /* Order-level attributes -> item grain */
        NULLIF(
            TRIM(o.ORDER_DATA:order_id::STRING),
            ''
        ) AS ORDER_ID,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(o.ORDER_DATA:order_date::STRING),
                ''
            )
        ) AS ORDER_DATE,

        INITCAP(
            TRIM(o.ORDER_DATA:order_status::STRING)
        ) AS ORDER_STATUS,

        NULLIF(
            TRIM(o.ORDER_DATA:store_id::STRING),
            ''
        ) AS STORE_ID,

        NULLIF(
            TRIM(o.ORDER_DATA:customer_id::STRING),
            ''
        ) AS CUSTOMER_ID,

        NULLIF(
            TRIM(o.ORDER_DATA:campaign_id::STRING),
            ''
        ) AS CAMPAIGN_ID,


        item.value AS ITEM_DATA

    FROM orders_flattened o

    CROSS JOIN LATERAL FLATTEN(
        INPUT => o.ORDER_DATA:order_items
    ) item

),

/*
    CLEAN ORDER-ITEM FIELDS
 */

cleaned AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        ORDER_ID,
        ORDER_ITEM_NUMBER,
        ORDER_DATE,
        ORDER_STATUS,
        STORE_ID,
        CUSTOMER_ID,
        CAMPAIGN_ID,

        NULLIF(
            TRIM(ITEM_DATA:product_id::STRING),
            ''
        ) AS PRODUCT_ID,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(ITEM_DATA:quantity::STRING),
                ''
            )
        ) AS QUANTITY,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(ITEM_DATA:unit_price::STRING),
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
                        TRIM(ITEM_DATA:cost_price::STRING),
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

        /* Discount percentage -> fraction */
        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(ITEM_DATA:discount_amount::STRING),
                ''
            ),
            10,
            6
        )/100.0 AS ITEM_DISCOUNT_RATE

    FROM items_flattened

),

/*
  DEDUPLICATION
   One row per order + order-item number
 */

deduplicated AS (

    SELECT *
    FROM cleaned

    WHERE ORDER_ID IS NOT NULL
      AND PRODUCT_ID IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            ORDER_ID,
            ORDER_ITEM_NUMBER

        ORDER BY
            SNAPSHOT_DATE DESC NULLS LAST,
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

)

/*
   FINAL SILVER ORDER ITEMS
 */

SELECT
    ORDER_ID,
    ORDER_ITEM_NUMBER,

    PRODUCT_ID,
    CUSTOMER_ID,
    STORE_ID,
    CAMPAIGN_ID,

    ORDER_DATE,
    ORDER_STATUS,

    QUANTITY,
    UNIT_PRICE,
    COST_PRICE,
    ITEM_DISCOUNT_RATE,

    SOURCE_FILE,
    SNAPSHOT_DATE,
    LOADED_AT,
    BATCH_ID

FROM deduplicated