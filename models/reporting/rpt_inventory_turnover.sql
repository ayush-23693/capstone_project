{{ config(
    materialized='view'
) }}

WITH product_daily AS (

    SELECT

        PRODUCT_KEY,
        PRODUCT_ID,
        SNAPSHOT_DATE,

        SUM(SOLD_QUANTITY) AS DAILY_SOLD_QUANTITY,

        SUM(BEGINNING_INVENTORY) AS DAILY_BEGINNING_INVENTORY,

        SUM(ENDING_INVENTORY) AS DAILY_ENDING_INVENTORY,

        SUM(INVENTORY_VALUE) AS DAILY_INVENTORY_VALUE

    FROM {{ ref('fact_inventory') }}

    GROUP BY
        PRODUCT_KEY,
        PRODUCT_ID,
        SNAPSHOT_DATE

),

product_summary AS (

    SELECT

        PRODUCT_KEY,
        PRODUCT_ID,

        SUM(
            DAILY_SOLD_QUANTITY
        ) AS TOTAL_SOLD_QUANTITY,

        AVG(
            (
                DAILY_BEGINNING_INVENTORY
                + DAILY_ENDING_INVENTORY
            ) / 2
        ) AS AVERAGE_INVENTORY,

        AVG(
            DAILY_INVENTORY_VALUE
        ) AS AVERAGE_INVENTORY_VALUE

    FROM product_daily

    GROUP BY
        PRODUCT_KEY,
        PRODUCT_ID

)

SELECT
    -- PRODUCT_KEY,
    PRODUCT_ID,

    TOTAL_SOLD_QUANTITY,

    ROUND(
        AVERAGE_INVENTORY,
        2
    ) AS AVERAGE_INVENTORY,

    CASE

        WHEN AVERAGE_INVENTORY > 0

        THEN ROUND(
            TOTAL_SOLD_QUANTITY
            / AVERAGE_INVENTORY,
            2
        )

        ELSE NULL

    END AS STOCK_TURNOVER_RATIO

FROM product_summary