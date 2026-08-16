{{ config(
    materialized='view'
) }}

WITH supplier_purchases AS (

    SELECT

        SUPPLIER_KEY,

        SUM(
            GREATEST(
                PURCHASED_QUANTITY,
                0
            )
        ) AS PURCHASED_QUANTITY

    FROM {{ ref('fact_inventory') }}

    GROUP BY SUPPLIER_KEY

),

supplier_share AS (

    SELECT

        SUPPLIER_KEY,

        PURCHASED_QUANTITY,

        100.0
        * PURCHASED_QUANTITY
        / NULLIF(
            SUM(PURCHASED_QUANTITY) OVER (),
            0
        ) AS CONTRIBUTION_PERCENTAGE

    FROM supplier_purchases

)

SELECT

    ss.SUPPLIER_KEY,

    ds.SUPPLIER_ID,

    ss.PURCHASED_QUANTITY,

    ROUND(
        ss.CONTRIBUTION_PERCENTAGE,
        2
    ) AS CONTRIBUTION_PERCENTAGE,

    ROUND(
        POWER(
            ss.CONTRIBUTION_PERCENTAGE,
            2
        ),
        2
    ) AS HHI_CONTRIBUTION,

    CASE

        WHEN ss.CONTRIBUTION_PERCENTAGE > 25
            THEN 'High'

        WHEN ss.CONTRIBUTION_PERCENTAGE > 10
            THEN 'Medium'

        ELSE 'Low'

    END AS CONCENTRATION_RISK_FLAG

FROM supplier_share ss

LEFT JOIN {{ ref('dim_supplier') }} ds

    ON ss.SUPPLIER_KEY = ds.SUPPLIER_KEY

ORDER BY
    ss.CONTRIBUTION_PERCENTAGE DESC