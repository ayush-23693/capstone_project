{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['PRODUCT_ID']) }} AS PRODUCT_KEY,
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    SUBCATEGORY,
    BRAND,
    COLOR,
    SIZE,
    UNIT_PRICE,
    COST_PRICE,
    SUPPLIER_ID
FROM {{ ref('silver_product') }}