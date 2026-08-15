{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['SUPPLIER_ID']) }} AS SUPPLIER_KEY,
    SUPPLIER_ID,
    SUPPLIER_NAME,
    CONTACT_INFORMATION,
    PAYMENT_TERMS,
    SUPPLIER_TYPE
FROM {{ ref('silver_supplier') }}