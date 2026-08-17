{{ config(
    materialized='table'
) }}

WITH sales_base AS (

    SELECT
        oi.ORDER_ID,
        oi.ORDER_ITEM_NUMBER,

        oi.CUSTOMER_ID,
        oi.PRODUCT_ID,
        oi.STORE_ID,
        oi.CAMPAIGN_ID,

        TO_DATE(oi.ORDER_DATE) AS SALES_DATE,

        oi.QUANTITY,
        oi.UNIT_PRICE,
        oi.ITEM_DISCOUNT_RATE,

        o.ORDER_DISCOUNT_RATE,

        
         -- Item revenue after item-level discount
        
        
        oi.QUANTITY
        * oi.UNIT_PRICE
        * (
            1 - COALESCE(oi.ITEM_DISCOUNT_RATE,0)
        ) AS ITEM_NET_REVENUE,

        oi.ORDER_STATUS

    FROM {{ ref('silver_order_items') }} oi

    INNER JOIN {{ ref('silver_orders') }} o
        ON oi.ORDER_ID = o.ORDER_ID

    WHERE LOWER(oi.ORDER_STATUS) = 'completed'

),

sales_with_keys AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'ORDER_ID',
            'ORDER_ITEM_NUMBER'
        ]) }} AS SALES_KEY,

        s.*,

        dc.CUSTOMER_KEY,
        dp.PRODUCT_KEY,
        ds.STORE_KEY,
        dm.CAMPAIGN_KEY,
        dd.DATE_KEY

    FROM sales_base s

    LEFT JOIN {{ ref('dim_customer') }} dc
        ON s.CUSTOMER_ID = dc.CUSTOMER_ID

    LEFT JOIN {{ ref('dim_product') }} dp
        ON s.PRODUCT_ID = dp.PRODUCT_ID

    LEFT JOIN {{ ref('dim_store') }} ds
        ON s.STORE_ID = ds.STORE_ID

    LEFT JOIN {{ ref('dim_marketing_campaign') }} dm
        ON s.CAMPAIGN_ID = dm.CAMPAIGN_ID

    LEFT JOIN {{ ref('dim_date') }} dd
        ON s.SALES_DATE = dd.FULL_DATE

)

SELECT
    SALES_KEY,

    ORDER_ID,
    ORDER_ITEM_NUMBER,

    CUSTOMER_KEY,
    PRODUCT_KEY,
    STORE_KEY,
    CAMPAIGN_KEY,
    DATE_KEY,

    QUANTITY,
    UNIT_PRICE,
    ITEM_DISCOUNT_RATE,
    ORDER_DISCOUNT_RATE,

    QUANTITY * UNIT_PRICE AS TOTAL_GROSS_AMOUNT,

    
    -- Final sales after both discount levels.

    
        ITEM_NET_REVENUE
        * (
            1 - COALESCE(
                ORDER_DISCOUNT_RATE,
                0
            )
        )
        AS TOTAL_SALES_AMOUNT

FROM sales_with_keys