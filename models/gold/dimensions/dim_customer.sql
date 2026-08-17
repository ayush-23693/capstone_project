{{ config(
    materialized='table'
) }}


SELECT
    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} AS CUSTOMER_KEY,
    CUSTOMER_ID,
    FULL_NAME,
    CUSTOMER_SEGMENT,
    EMAIL,
    PHONE_NUMBER,
    STANDARDIZED_ADDRESS AS ADDRESS,
    OCCUPATION,
    LOYALTY_TIER,
    INCOME_BRACKET,
    MARKETING_OPT_IN,
    PREFERRED_COMMUNICATION,
    PREFERRED_PAYMENT_METHOD,
    REGISTRATION_DATE,
    LAST_PURCHASE_DATE,
    TOTAL_PURCHASES,
    TOTAL_SPEND
FROM {{ ref('silver_customer') }}