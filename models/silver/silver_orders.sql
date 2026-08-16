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

        ord.value AS ORDER_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:orders_data
    ) ord

),

/*
   EXTRACT + CLEAN ORDER HEADER
 */

cleaned AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        
           --Natural key
        

        NULLIF(
            TRIM(ORDER_DATA:order_id::STRING),
            ''
        ) AS ORDER_ID,

        
           --Related IDs
        

        NULLIF(
            TRIM(ORDER_DATA:customer_id::STRING),
            ''
        ) AS CUSTOMER_ID,

        NULLIF(
            TRIM(ORDER_DATA:employee_id::STRING),
            ''
        ) AS EMPLOYEE_ID,

        NULLIF(
            TRIM(ORDER_DATA:store_id::STRING),
            ''
        ) AS STORE_ID,

        NULLIF(
            TRIM(ORDER_DATA:campaign_id::STRING),
            ''
        ) AS CAMPAIGN_ID,

        
           --Dates / timestamps
        

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(ORDER_DATA:created_at::STRING),
                ''
            )
        ) AS CREATED_AT,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(ORDER_DATA:order_date::STRING),
                ''
            )
        ) AS ORDER_DATE,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(ORDER_DATA:shipping_date::STRING),
                ''
            )
        ) AS SHIPPING_DATE,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(ORDER_DATA:delivery_date::STRING),
                ''
            )
        ) AS DELIVERY_DATE,

        TRY_TO_TIMESTAMP_NTZ(
            NULLIF(
                TRIM(ORDER_DATA:estimated_delivery_date::STRING),
                ''
            )
        ) AS ESTIMATED_DELIVERY_DATE,

        
           --Status / categorical fields
        

        INITCAP(
            TRIM(ORDER_DATA:order_status::STRING)
        ) AS ORDER_STATUS,

        INITCAP(
            TRIM(ORDER_DATA:order_source::STRING)
        ) AS ORDER_SOURCE,

        INITCAP(
            TRIM(ORDER_DATA:payment_method::STRING)
        ) AS PAYMENT_METHOD,

        INITCAP(
            TRIM(ORDER_DATA:shipping_method::STRING)
        ) AS SHIPPING_METHOD,

        /*
           Discount given is in percentage , converting in fraction
         */

        TRY_TO_DECIMAL(
            NULLIF(
                TRIM(ORDER_DATA:discount_amount::STRING),
                ''
            ),
            10,
            6
        )/100.0 AS ORDER_DISCOUNT_RATE,

        
           --Monetary values
        

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(ORDER_DATA:shipping_cost::STRING),
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
        ) AS SHIPPING_COST,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(ORDER_DATA:tax_amount::STRING),
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
        ) AS TAX_AMOUNT,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REPLACE(
                        TRIM(ORDER_DATA:total_amount::STRING),
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
        ) AS SOURCE_TOTAL_AMOUNT,

        
           --Billing address
        

        INITCAP(
            TRIM(ORDER_DATA:billing_address:street::STRING)
        ) AS BILLING_STREET,

        INITCAP(
            TRIM(ORDER_DATA:billing_address:city::STRING)
        ) AS BILLING_CITY,

        UPPER(
            TRIM(ORDER_DATA:billing_address:state::STRING)
        ) AS BILLING_STATE,

        TRIM(
            ORDER_DATA:billing_address:zip_code::STRING
        ) AS BILLING_ZIP_CODE,

        
           --Shipping address
        

        INITCAP(
            TRIM(ORDER_DATA:shipping_address:street::STRING)
        ) AS SHIPPING_STREET,

        INITCAP(
            TRIM(ORDER_DATA:shipping_address:city::STRING)
        ) AS SHIPPING_CITY,

        UPPER(
            TRIM(ORDER_DATA:shipping_address:state::STRING)
        ) AS SHIPPING_STATE,

        TRIM(
            ORDER_DATA:shipping_address:zip_code::STRING
        ) AS SHIPPING_ZIP_CODE

    FROM orders_flattened

),

/*
 ORDER ITEM AGGREGATIONS
 */

item_aggregates AS (

    SELECT
        ORDER_ID,

        COUNT(PRODUCT_ID) AS TOTAL_ITEMS,

        SUM(QUANTITY) AS TOTAL_QUANTITY,

        SUM(
            QUANTITY * UNIT_PRICE
        ) AS TOTAL_AMOUNT,

        SUM(
            QUANTITY * COST_PRICE
        ) AS TOTAL_COST,

        SUM(
            QUANTITY * COST_PRICE
        ) AS LINE_COST,

        SUM(
            QUANTITY
            * UNIT_PRICE
            * (1 - COALESCE(ITEM_DISCOUNT_RATE, 0))
        ) AS LINE_REVENUE,

        SUM(
            QUANTITY * UNIT_PRICE * ITEM_DISCOUNT_RATE
        ) AS TOTAL_DISCOUNT

    FROM {{ ref('silver_order_items') }}

    GROUP BY ORDER_ID

),

/*
    ORDER-LEVEL DERIVED CALCULATIONS
 */

derived AS (

    SELECT

        c.*,

        
           --Order-level aggregation
        

        COALESCE(i.TOTAL_ITEMS, 0) AS TOTAL_ITEMS,

        COALESCE(i.TOTAL_QUANTITY, 0) AS TOTAL_QUANTITY,

        COALESCE(i.LINE_REVENUE, 0) AS LINE_REVENUE,

        COALESCE(i.LINE_COST, 0) AS LINE_COST,

        COALESCE(i.TOTAL_AMOUNT, 0) AS TOTAL_AMOUNT,

        COALESCE(i.TOTAL_COST, 0) AS TOTAL_COST,

        COALESCE(i.TOTAL_DISCOUNT, 0) AS TOTAL_DISCOUNT,

        /*
           Profit amount

           (line revenue * (1-order discount rate))
           - line cost
           - shipping cost
           - tax amount
         */

        (
            COALESCE(i.LINE_REVENUE, 0)
            * (1 - COALESCE(c.ORDER_DISCOUNT_RATE, 0))
        )
        - COALESCE(i.LINE_COST, 0)
        - COALESCE(c.SHIPPING_COST, 0)
        - COALESCE(c.TAX_AMOUNT, 0)
        AS PROFIT_AMOUNT

    FROM cleaned c

    LEFT JOIN item_aggregates i
        ON c.ORDER_ID = i.ORDER_ID

),

/*
   PROFIT MARGIN + TIME / CALENDAR / SHIPPING METRICS
 */

calculated AS (

    SELECT

        d.*,

        /*
           Profit margin percentage
         */

        CAST(
            CASE
                WHEN d.LINE_REVENUE > 0
                THEN (
                    d.PROFIT_AMOUNT
                    / d.LINE_REVENUE
                ) * 100.0
                ELSE NULL
            END
            AS DECIMAL(18,2)
        ) AS PROFIT_MARGIN_PERCENTAGE,

        /*
           Order hour
         */

        EXTRACT(
            HOUR FROM d.ORDER_DATE
        ) AS ORDER_HOUR,

        /*
           Calendar attributes
         */

        EXTRACT(
            WEEK FROM d.ORDER_DATE
        ) AS ORDER_WEEK,

        EXTRACT(
            MONTH FROM d.ORDER_DATE
        ) AS ORDER_MONTH,

        EXTRACT(
            QUARTER FROM d.ORDER_DATE
        ) AS ORDER_QUARTER,

        EXTRACT(
            YEAR FROM d.ORDER_DATE
        ) AS ORDER_YEAR,

        /*
           Processing days
         */

        DATEDIFF(
            DAY,
            TO_DATE(d.ORDER_DATE),
            TO_DATE(d.SHIPPING_DATE)
        ) AS PROCESSING_DAYS,

        /*
           Shipping days
         */

        DATEDIFF(
            DAY,
            TO_DATE(d.SHIPPING_DATE),
            TO_DATE(d.DELIVERY_DATE)
        ) AS SHIPPING_DAYS,

        /*
           Standardized addresses
         */

        CONCAT_WS(
            ', ',
            NULLIF(d.BILLING_STREET, ''),
            NULLIF(d.BILLING_CITY, ''),
            NULLIF(d.BILLING_STATE, ''),
            NULLIF(d.BILLING_ZIP_CODE, '')
        ) AS STANDARDIZED_BILLING_ADDRESS,

        CONCAT_WS(
            ', ',
            NULLIF(d.SHIPPING_STREET, ''),
            NULLIF(d.SHIPPING_CITY, ''),
            NULLIF(d.SHIPPING_STATE, ''),
            NULLIF(d.SHIPPING_ZIP_CODE, '')
        ) AS STANDARDIZED_SHIPPING_ADDRESS

    FROM derived d

),

/*
    ORDER TIME OF DAY
 */

time_classified AS (

    SELECT
        c.*,

        CASE

            WHEN c.ORDER_HOUR >= 5
             AND c.ORDER_HOUR < 12
                THEN 'Morning'

            WHEN c.ORDER_HOUR >= 12
             AND c.ORDER_HOUR < 17
                THEN 'Afternoon'

            WHEN c.ORDER_HOUR >= 17
             AND c.ORDER_HOUR < 22
                THEN 'Evening'

            ELSE 'Night'

        END AS ORDER_TIME_OF_DAY

    FROM calculated c

),

/*
    DELIVERY STATUS
 */

delivery_classified AS (

    SELECT
        t.*,

        CASE

            WHEN t.DELIVERY_DATE IS NOT NULL
             AND t.DELIVERY_DATE <= t.ESTIMATED_DELIVERY_DATE
                THEN 'On Time'

            WHEN t.DELIVERY_DATE IS NOT NULL
             AND t.DELIVERY_DATE > t.ESTIMATED_DELIVERY_DATE
                THEN 'Delayed'

            WHEN t.DELIVERY_DATE IS NULL
             AND CURRENT_DATE() > TO_DATE(t.ESTIMATED_DELIVERY_DATE)
                THEN 'Potentially Delayed'

            ELSE 'In Transit'

        END AS DELIVERY_STATUS

    FROM time_classified t

)

/*
   8. DEDUPLICATION
   Natural key = ORDER_ID
 */

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    EMPLOYEE_ID,
    STORE_ID,
    CAMPAIGN_ID,

    CREATED_AT,
    ORDER_DATE,
    SHIPPING_DATE,
    DELIVERY_DATE,
    ESTIMATED_DELIVERY_DATE,

    ORDER_STATUS,
    ORDER_SOURCE,
    PAYMENT_METHOD,
    SHIPPING_METHOD,

    ORDER_DISCOUNT_RATE,

    SHIPPING_COST,
    TAX_AMOUNT,
    SOURCE_TOTAL_AMOUNT,

    LINE_REVENUE,
    LINE_COST,
    TOTAL_ITEMS,
    TOTAL_QUANTITY,
    TOTAL_AMOUNT,
    TOTAL_DISCOUNT,
    TOTAL_COST,

    PROFIT_AMOUNT,
    PROFIT_MARGIN_PERCENTAGE,

    ORDER_HOUR,
    ORDER_TIME_OF_DAY,

    ORDER_WEEK,
    ORDER_MONTH,
    ORDER_QUARTER,
    ORDER_YEAR,

    PROCESSING_DAYS,
    SHIPPING_DAYS,
    DELIVERY_STATUS,

    BILLING_STREET,
    BILLING_CITY,
    BILLING_STATE,
    BILLING_ZIP_CODE,
    STANDARDIZED_BILLING_ADDRESS,

    SHIPPING_STREET,
    SHIPPING_CITY,
    SHIPPING_STATE,
    SHIPPING_ZIP_CODE,
    STANDARDIZED_SHIPPING_ADDRESS,

    SNAPSHOT_DATE,
    SOURCE_FILE,
    ROW_NUMBER,
    LOADED_AT,
    BATCH_ID

FROM delivery_classified

WHERE ORDER_ID IS NOT NULL

QUALIFY ROW_NUMBER() OVER (

    PARTITION BY ORDER_ID

    ORDER BY
        SNAPSHOT_DATE DESC NULLS LAST,
        LOADED_AT DESC,
        SOURCE_FILE DESC,
        ROW_NUMBER DESC

) = 1