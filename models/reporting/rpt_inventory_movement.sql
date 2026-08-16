{{ config(
    materialized='view',
) }}

WITH turnover AS (

    SELECT

        -- PRODUCT_KEY,
        PRODUCT_ID,
        TOTAL_SOLD_QUANTITY,
        AVERAGE_INVENTORY,
        STOCK_TURNOVER_RATIO

    FROM {{ ref('rpt_inventory_turnover') }}

),

ranked AS (

    SELECT

        *,

        NTILE(4) OVER (
            ORDER BY STOCK_TURNOVER_RATIO
        ) AS TURNOVER_QUARTILE

    FROM turnover

    WHERE STOCK_TURNOVER_RATIO IS NOT NULL

)

SELECT

    -- PRODUCT_KEY,
    PRODUCT_ID,

    TOTAL_SOLD_QUANTITY,
    AVERAGE_INVENTORY,
    STOCK_TURNOVER_RATIO,

    CASE

        WHEN TURNOVER_QUARTILE = 1
        THEN 'Slow-moving'

        WHEN TURNOVER_QUARTILE = 4
        THEN 'Fast-moving'

        ELSE 'Normal-moving'

    END AS MOVEMENT_CATEGORY

FROM ranked