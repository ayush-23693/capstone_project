{{ config(
    materialized='view',
) }}

SELECT

    -- fi.PRODUCT_KEY,
    fi.PRODUCT_ID,

    dp.PRODUCT_NAME AS PRODUCT_NAME,
    dp.CATEGORY,
    dp.SUBCATEGORY,

    SUM(
        fi.ENDING_INVENTORY
    ) AS TOTAL_ENDING_INVENTORY,

    ROUND(
        AVG(
            fi.ENDING_INVENTORY
        ),
        2
    ) AS AVERAGE_INVENTORY,

    ROUND(
        SUM(
            fi.INVENTORY_VALUE
        ),
        2
    ) AS TOTAL_INVENTORY_VALUE,

    ROUND(
        AVG(
            fi.INVENTORY_VALUE
        ),
        2
    ) AS AVERAGE_INVENTORY_VALUE

FROM {{ ref('fact_inventory') }} fi

LEFT JOIN {{ ref('dim_product') }} dp

    ON fi.PRODUCT_KEY = dp.PRODUCT_KEY

GROUP BY

    fi.PRODUCT_KEY,
    fi.PRODUCT_ID,

    dp.PRODUCT_NAME,
    dp.CATEGORY,
    dp.SUBCATEGORY