{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['CAMPAIGN_ID']) }} AS CAMPAIGN_KEY,
    CAMPAIGN_ID,
    TARGET_AUDIENCE_SEGMENT,
    BUDGET,
    CAMPAIGN_DURATION_DAYS AS DURATION,
    ROI,
    START_DATE,
    END_DATE,
    TOTAL_COST
FROM {{ ref('silver_marketing_campaign') }}