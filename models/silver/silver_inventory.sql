{{ config(
    materialized='table',
    schema='SILVER'
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

   -- FLATTEN ALL PRODUCT SNAPSHOTS


flattened AS (

    SELECT

        s.SOURCE_FILE,
        s.ROW_NUMBER,
        s.LOADED_AT,
        s.BATCH_ID,

        TRY_TO_DATE(
            REGEXP_SUBSTR(
                s.SOURCE_FILE,
                '[0-9]{4}-[0-9]{2}-[0-9]{2}'
            )
        ) AS SNAPSHOT_DATE,

        product.value AS PRODUCT_DATA

    FROM source_data s

    CROSS JOIN LATERAL FLATTEN(
        INPUT => s.RAW_DATA:products_data
    ) product

),


   -- CLEAN INVENTORY ATTRIBUTES


cleaned AS (

    SELECT

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID,
        SNAPSHOT_DATE,

        UPPER(
            TRIM(
                PRODUCT_DATA:product_id::STRING
            )
        ) AS PRODUCT_ID,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    PRODUCT_DATA:stock_quantity::STRING
                ),
                ''
            )
        ) AS STOCK_QUANTITY,

        TRY_TO_NUMBER(
            NULLIF(
                TRIM(
                    PRODUCT_DATA:reorder_level::STRING
                ),
                ''
            )
        ) AS REORDER_LEVEL

    FROM flattened

),

/*
    DEDUPLICATE PRODUCT / SNAPSHOT DATE
   Grain: PRODUCT_ID + SNAPSHOT_DATE
*/

snapshot_records AS (

    SELECT *

    FROM cleaned

    WHERE PRODUCT_ID IS NOT NULL
      AND SNAPSHOT_DATE IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY
            PRODUCT_ID,
            SNAPSHOT_DATE

        ORDER BY
            LOADED_AT DESC,
            SOURCE_FILE DESC,
            ROW_NUMBER DESC

    ) = 1

),


--  PRODUCT DATE BOUNDS


product_bounds AS (

    SELECT

        PRODUCT_ID,

        MIN(SNAPSHOT_DATE) AS FIRST_SNAPSHOT_DATE,
        MAX(SNAPSHOT_DATE) AS LAST_SNAPSHOT_DATE

    FROM snapshot_records

    GROUP BY PRODUCT_ID

),

/*
  DAILY DATE SPINE
   PS DATA WINDOW:
       2024-04-01 through 2024-09-27
 */

date_spine AS (

    SELECT
        TO_DATE('2024-04-01') AS INVENTORY_DATE

    UNION ALL

    SELECT
        DATEADD(
            DAY,
            1,
            INVENTORY_DATE
        )

    FROM date_spine

    WHERE INVENTORY_DATE < TO_DATE('2024-09-27')

),

/*
   One row per product per date across the complete
   PS inventory window
*/

product_dates AS (

    SELECT

        pb.PRODUCT_ID,
        ds.INVENTORY_DATE

    FROM product_bounds pb

    CROSS JOIN date_spine ds

    WHERE ds.INVENTORY_DATE >= pb.FIRST_SNAPSHOT_DATE
      AND ds.INVENTORY_DATE <= TO_DATE('2024-09-27')

),

    -- JOIN ACTUAL SNAPSHOT OBSERVATIONS
 

daily_observations AS (

    SELECT

        pd.PRODUCT_ID,
        pd.INVENTORY_DATE,

        sr.STOCK_QUANTITY AS OBSERVED_STOCK_QUANTITY,
        sr.REORDER_LEVEL AS OBSERVED_REORDER_LEVEL,

        sr.SNAPSHOT_DATE AS ACTUAL_SNAPSHOT_DATE,

        sr.SOURCE_FILE,
        sr.ROW_NUMBER,
        sr.LOADED_AT,
        sr.BATCH_ID

    FROM product_dates pd

    LEFT JOIN snapshot_records sr

        ON pd.PRODUCT_ID = sr.PRODUCT_ID

       AND pd.INVENTORY_DATE = sr.SNAPSHOT_DATE

),

/*
   CARRY FORWARD LAST OBSERVED STOCK

   If no snapshot for current day, the most
   recent observed stock position is used.

   implemented the PS's "carry-forward"
   option for the snapshot gap.
 */

carried_forward AS (

    SELECT

        PRODUCT_ID,
        INVENTORY_DATE,

        LAST_VALUE(
            OBSERVED_STOCK_QUANTITY
        IGNORE NULLS) OVER (

            PARTITION BY PRODUCT_ID

            ORDER BY INVENTORY_DATE

            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND CURRENT ROW

        ) AS ENDING_STOCK,

        LAST_VALUE(
            OBSERVED_REORDER_LEVEL
        IGNORE NULLS) OVER (

            PARTITION BY PRODUCT_ID

            ORDER BY INVENTORY_DATE

            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND CURRENT ROW

        ) AS REORDER_LEVEL,

        ACTUAL_SNAPSHOT_DATE,

        DATEDIFF(
                DAY,
                ACTUAL_SNAPSHOT_DATE,
                INVENTORY_DATE
                ) AS DAYS_SINCE_LAST_ACTUAL_SNAPSHOT,

        CASE
            WHEN DAYS_SINCE_LAST_ACTUAL_SNAPSHOT > 0
            THEN TRUE
            ELSE FALSE
        END AS STALE_SNAPSHOT_FLAG,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID

    FROM daily_observations

),


 --  Beginning stock = previous day's ending stock


with_beginning_stock AS (

    SELECT

        PRODUCT_ID,
        INVENTORY_DATE,

        LAG(ENDING_STOCK) OVER (

            PARTITION BY PRODUCT_ID

            ORDER BY INVENTORY_DATE

        ) AS BEGINNING_STOCK,

        ENDING_STOCK,
        REORDER_LEVEL,

        ACTUAL_SNAPSHOT_DATE,

        SOURCE_FILE,
        ROW_NUMBER,
        LOADED_AT,
        BATCH_ID

    FROM carried_forward

),


   -- COMPLETED SALES PER PRODUCT PER DAY


completed_sales AS (

    SELECT

        UPPER(
            TRIM(
                oi.PRODUCT_ID
            )
        ) AS PRODUCT_ID,

        TO_DATE(
            o.ORDER_DATE
        ) AS SALES_DATE,

        SUM(
            TRY_TO_NUMBER(
                oi.QUANTITY
            )
        ) AS SOLD_QUANTITY

    FROM {{ ref('silver_order_items') }} oi

    INNER JOIN {{ ref('silver_orders') }} o

        ON oi.ORDER_ID = o.ORDER_ID

    WHERE LOWER(
        TRIM(
            o.ORDER_STATUS
        )
    ) = 'completed'

      AND oi.PRODUCT_ID IS NOT NULL

      AND TRY_TO_NUMBER(
            oi.QUANTITY
          ) IS NOT NULL

    GROUP BY

        UPPER(TRIM(oi.PRODUCT_ID)),
        TO_DATE(o.ORDER_DATE)

),


   -- ADDED DAILY SALES

daily_inventory AS (

    SELECT

        w.PRODUCT_ID,
        w.INVENTORY_DATE,

        w.BEGINNING_STOCK,
        w.ENDING_STOCK,

        COALESCE(
            cs.SOLD_QUANTITY,
            0
        ) AS SOLD_QUANTITY,

        w.REORDER_LEVEL,

        w.ACTUAL_SNAPSHOT_DATE,

        w.SOURCE_FILE,
        w.ROW_NUMBER,
        w.LOADED_AT,
        w.BATCH_ID

    FROM with_beginning_stock w

    LEFT JOIN completed_sales cs

        ON w.PRODUCT_ID = cs.PRODUCT_ID

       AND w.INVENTORY_DATE = cs.SALES_DATE

)


   -- FINAL SILVER INVENTORY


SELECT

    PRODUCT_ID,

    INVENTORY_DATE AS SNAPSHOT_DATE,

    BEGINNING_STOCK,
    ENDING_STOCK,

    SOLD_QUANTITY,

      -- Purchased Quantity

    CASE

        WHEN BEGINNING_STOCK IS NOT NULL
         AND ENDING_STOCK IS NOT NULL

        THEN
            ENDING_STOCK
            - BEGINNING_STOCK
            + SOLD_QUANTITY

        ELSE NULL

    END AS PURCHASED_QUANTITY,

    REORDER_LEVEL,

    
    --   Stale / carried-forward inventory
    

    CASE

        WHEN ACTUAL_SNAPSHOT_DATE IS NULL
        THEN TRUE

        ELSE FALSE

    END AS STALE_SNAPSHOT_FLAG,

    
    --   Low stock
    

    CASE

        WHEN ENDING_STOCK IS NOT NULL
         AND REORDER_LEVEL IS NOT NULL
         AND ENDING_STOCK < REORDER_LEVEL

        THEN TRUE

        ELSE FALSE

    END AS LOW_STOCK_FLAG,

    
    --   Negative balance
    

    CASE

        WHEN ENDING_STOCK < 0
          OR BEGINNING_STOCK < 0

        THEN TRUE

        ELSE FALSE

    END AS NEGATIVE_BALANCE_FLAG,

    
    --   Lineage
     
    -- SOURCE_FILE,
    -- ROW_NUMBER,
    -- LOADED_AT,
    -- BATCH_ID

FROM daily_inventory