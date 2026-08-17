{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['STORE_ID']) }} AS STORE_KEY,
    STORE_ID,
    STORE_NAME,
    STANDARDIZED_ADDRESS AS ADDRESS,
    REGION,
    STORE_TYPE,
    OPENING_DATE,
    SIZE_CATEGORY,
FROM {{ ref('silver_store') }}