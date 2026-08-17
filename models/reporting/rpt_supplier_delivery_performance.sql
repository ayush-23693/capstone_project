{{ config(
    materialized='view',
) }}

WITH supplier_delivery AS (

    SELECT

        SUPPLIER_ID,

        ON_TIME_DELIVERY_RATE,

        AVERAGE_DELAY_DAYS

    FROM {{ ref('silver_supplier') }}

)

SELECT

    ds.SUPPLIER_KEY,
    sd.SUPPLIER_ID,
    sd.ON_TIME_DELIVERY_RATE,

    ROUND(
        100 - sd.ON_TIME_DELIVERY_RATE,
        2
    ) AS DELAYED_DELIVERY_RATE,

    sd.AVERAGE_DELAY_DAYS

FROM supplier_delivery sd

LEFT JOIN {{ ref('dim_supplier') }} ds
    ON sd.SUPPLIER_ID = ds.SUPPLIER_ID