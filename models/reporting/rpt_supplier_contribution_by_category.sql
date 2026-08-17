{{ config(
    materialized='view'
) }}

WITH supplier_category_purchases AS (

    SELECT

        fi.SUPPLIER_KEY,
        ds.SUPPLIER_ID,

        dp.CATEGORY AS PRODUCT_CATEGORY,

        SUM(
            fi.PURCHASED_QUANTITY
        ) AS PURCHASED_QUANTITY

    FROM {{ ref('fact_inventory') }} fi

    INNER JOIN {{ ref('dim_supplier') }} ds
        ON fi.SUPPLIER_KEY = ds.SUPPLIER_KEY

    INNER JOIN {{ ref('dim_product') }} dp
        ON fi.PRODUCT_KEY = dp.PRODUCT_KEY

    GROUP BY

        fi.SUPPLIER_KEY,
        ds.SUPPLIER_ID,
        dp.CATEGORY

),

category_totals AS (

    SELECT

        PRODUCT_CATEGORY,

        SUM(
            PURCHASED_QUANTITY
        ) AS TOTAL_CATEGORY_PURCHASED

    FROM supplier_category_purchases

    GROUP BY PRODUCT_CATEGORY

)

SELECT

    scp.SUPPLIER_KEY,
    scp.SUPPLIER_ID,

    scp.PRODUCT_CATEGORY,

    scp.PURCHASED_QUANTITY,

    ROUND(
        100.0
        * scp.PURCHASED_QUANTITY
        / NULLIF(
            ct.TOTAL_CATEGORY_PURCHASED,
            0
        ),
        2
    ) AS SUPPLIER_CONTRIBUTION_PERCENTAGE

FROM supplier_category_purchases scp

INNER JOIN category_totals ct

    ON scp.PRODUCT_CATEGORY = ct.PRODUCT_CATEGORY