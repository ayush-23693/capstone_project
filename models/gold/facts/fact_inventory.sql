{{ config(
    materialized='table'
) }}

/*

   GRAIN: One row per PRODUCT × STORE × DATE.

   SOURCE ASSUMPTION:
       Inventory source data, available at PRODUCT level,
       does not contain STORE_ID.

       Therefore, product-level inventory is allocated to
       stores using each store's share of completed sales
       for that product across the inventory window.

       Average Inventory =
           (Beginning Inventory + Ending Inventory) / 2

 */


WITH inventory AS (

    SELECT

        PRODUCT_ID,
        SNAPSHOT_DATE,

        BEGINNING_STOCK,
        ENDING_STOCK,
        SOLD_QUANTITY,
        PURCHASED_QUANTITY,

        REORDER_LEVEL,

        LOW_STOCK_FLAG,
        STALE_SNAPSHOT_FLAG,
        NEGATIVE_BALANCE_FLAG

    FROM {{ ref('silver_inventory') }}

),


   -- SALES BY PRODUCT / STORE / DATE

completed_sales AS (

    SELECT

        UPPER(
            TRIM(oi.PRODUCT_ID)
        ) AS PRODUCT_ID,

        UPPER(
            TRIM(o.STORE_ID)
        ) AS STORE_ID,

        TO_DATE(
            o.ORDER_DATE
        ) AS SALES_DATE,

        SUM(
            TRY_TO_NUMBER(oi.QUANTITY)
        ) AS SOLD_QUANTITY

    FROM {{ ref('silver_order_items') }} oi

    INNER JOIN {{ ref('silver_orders') }} o

        ON oi.ORDER_ID = o.ORDER_ID

    WHERE LOWER(
        TRIM(o.ORDER_STATUS)
    ) = 'completed'

      AND oi.PRODUCT_ID IS NOT NULL

      AND o.STORE_ID IS NOT NULL

      AND TRY_TO_NUMBER(oi.QUANTITY) IS NOT NULL

    GROUP BY
        UPPER(TRIM(oi.PRODUCT_ID)),
        UPPER(TRIM(o.STORE_ID)),
        TO_DATE(o.ORDER_DATE)

),


   -- PRODUCT / STORE TOTAL SALES

product_store_sales AS (

    SELECT

        PRODUCT_ID,
        STORE_ID,

        SUM(
            SOLD_QUANTITY
        ) AS TOTAL_SOLD_QUANTITY

    FROM completed_sales

    GROUP BY
        PRODUCT_ID,
        STORE_ID

),


   -- PRODUCT TOTAL SALES


product_sales_totals AS (

    SELECT

        PRODUCT_ID,

        SUM(
            TOTAL_SOLD_QUANTITY
        ) AS PRODUCT_TOTAL_SOLD_QUANTITY

    FROM product_store_sales

    GROUP BY PRODUCT_ID

),


   -- STORE SALES SHARE


store_sales_share AS (

    SELECT

        pss.PRODUCT_ID,
        pss.STORE_ID,

        pss.TOTAL_SOLD_QUANTITY,

        pss.TOTAL_SOLD_QUANTITY
        / NULLIF(
            pst.PRODUCT_TOTAL_SOLD_QUANTITY,
            0
        ) AS STORE_SALES_SHARE

    FROM product_store_sales pss

    INNER JOIN product_sales_totals pst

        ON pss.PRODUCT_ID = pst.PRODUCT_ID

),


   -- PRODUCT × STORE × DATE


inventory_store_dates AS (

    SELECT

        i.PRODUCT_ID,
        sss.STORE_ID,
        i.SNAPSHOT_DATE,

        i.BEGINNING_STOCK,
        i.ENDING_STOCK,

        i.REORDER_LEVEL,

        i.STALE_SNAPSHOT_FLAG,

        i.NEGATIVE_BALANCE_FLAG,

        sss.STORE_SALES_SHARE

    FROM inventory i

    INNER JOIN store_sales_share sss

        ON i.PRODUCT_ID = sss.PRODUCT_ID

),


   -- ADD ACTUAL STORE-LEVEL COMPLETED SALES

with_store_sales AS (

    SELECT

        isd.*,

        COALESCE(
            cs.SOLD_QUANTITY,
            0
        ) AS SOLD_QUANTITY

    FROM inventory_store_dates isd

    LEFT JOIN completed_sales cs

        ON isd.PRODUCT_ID = cs.PRODUCT_ID

       AND isd.STORE_ID = cs.STORE_ID

       AND isd.SNAPSHOT_DATE = cs.SALES_DATE

),


   -- ALLOCATE PRODUCT-LEVEL INVENTORY TO STORES


allocated_inventory AS (

    SELECT

        PRODUCT_ID,
        STORE_ID,
        SNAPSHOT_DATE,

        BEGINNING_STOCK
            * STORE_SALES_SHARE
            AS ALLOCATED_BEGINNING_STOCK,

        ENDING_STOCK
            * STORE_SALES_SHARE
            AS ALLOCATED_ENDING_STOCK,

        SOLD_QUANTITY,

        REORDER_LEVEL
            * STORE_SALES_SHARE
            AS ALLOCATED_REORDER_LEVEL,

        STALE_SNAPSHOT_FLAG,
        NEGATIVE_BALANCE_FLAG,

        STORE_SALES_SHARE

    FROM with_store_sales

),


   -- DERIVE STORE-LEVEL INVENTORY MEASURES


inventory_measures AS (

    SELECT

        ai.*,

        
        --   Inferred purchased quantity
        

        ai.ALLOCATED_ENDING_STOCK
        - ai.ALLOCATED_BEGINNING_STOCK
        + ai.SOLD_QUANTITY
            AS PURCHASED_QUANTITY

    FROM allocated_inventory ai

),


  -- Supplier contribution: measured at STORE + DATE level.


with_supplier_contribution AS (

    SELECT

        im.*,

        100.0
        * im.PURCHASED_QUANTITY
        / NULLIF(
            SUM(
                im.PURCHASED_QUANTITY
            ) OVER (
                PARTITION BY
                    im.STORE_ID,
                    im.SNAPSHOT_DATE
            ),
            0
        ) AS SUPPLIER_CONTRIBUTION_PERCENTAGE

    FROM inventory_measures im

)


   -- FINAL FACT INVENTORY


SELECT

    
    -- SURROGATE KEY
    

    {{ dbt_utils.generate_surrogate_key([
        'isc.PRODUCT_ID',
        'isc.STORE_ID',
        'isc.SNAPSHOT_DATE'
    ]) }} AS INVENTORY_KEY,

    
    --   DIMENSION KEYS
    

    dp.PRODUCT_KEY,

    ds.STORE_KEY,

    dsup.SUPPLIER_KEY,

    dd.DATE_KEY,

    
    --   BUSINESS KEYS
    

    isc.PRODUCT_ID,
    isc.STORE_ID,
    isc.SNAPSHOT_DATE,

    
    --   INVENTORY MEASURES
    

    ROUND(
        isc.ALLOCATED_BEGINNING_STOCK,
        2
    ) AS BEGINNING_INVENTORY,

    ROUND(
        isc.PURCHASED_QUANTITY,
        2
    ) AS PURCHASED_QUANTITY,

    isc.SOLD_QUANTITY,

    ROUND(
        isc.ALLOCATED_ENDING_STOCK,
        2
    ) AS ENDING_INVENTORY,

    
    --   INVENTORY VALUE:Ending Inventory × Product Cost Price

    ROUND(
        isc.ALLOCATED_ENDING_STOCK
        * dp.COST_PRICE,
        2
    ) AS INVENTORY_VALUE,

    
    --   STOCK TURNOVER RATIO

    CASE

        WHEN (
            (
                isc.ALLOCATED_BEGINNING_STOCK
                + isc.ALLOCATED_ENDING_STOCK
            ) / 2
        ) > 0

        THEN ROUND(

            isc.SOLD_QUANTITY
            /
            (
                (
                    isc.ALLOCATED_BEGINNING_STOCK
                    + isc.ALLOCATED_ENDING_STOCK
                ) / 2
            ),

            2

        )

        ELSE NULL

    END AS STOCK_TURNOVER_RATIO,

    
    --   SUPPLIER CONTRIBUTION
    

    ROUND(
        isc.SUPPLIER_CONTRIBUTION_PERCENTAGE,
        2
    ) AS SUPPLIER_CONTRIBUTION_PERCENTAGE,


FROM with_supplier_contribution isc

LEFT JOIN {{ ref('dim_product') }} dp

    ON isc.PRODUCT_ID = dp.PRODUCT_ID

LEFT JOIN {{ ref('dim_store') }} ds

    ON isc.STORE_ID = ds.STORE_ID

LEFT JOIN {{ ref('dim_supplier') }} dsup

    ON dp.SUPPLIER_ID = dsup.SUPPLIER_ID

LEFT JOIN {{ ref('dim_date') }} dd

    ON isc.SNAPSHOT_DATE = dd.FULL_DATE