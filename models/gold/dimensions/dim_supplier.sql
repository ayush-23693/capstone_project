{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['SUPPLIER_ID']) }} AS SUPPLIER_KEY,
    SUPPLIER_ID,
    SUPPLIER_NAME,
    CONTACT_INFORMATION,
    PAYMENT_TERMS,
    SUPPLIER_TYPE,
    ON_TIME_DELIVERY_RATE,
    AVERAGE_DELAY_DAYS
FROM {{ ref('silver_supplier') }}