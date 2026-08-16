{{ config(
    materialized='view'
) }}

WITH campaign_engagement AS (

    SELECT
        fmp.CAMPAIGN_KEY,

        MAX(
            fmp.NEW_CUSTOMERS_ACQUIRED
        ) AS LAST_DAILY_NEW_CUSTOMERS,

        SUM(
            fmp.NEW_CUSTOMERS_ACQUIRED
        ) AS TOTAL_NEW_CUSTOMERS_ACQUIRED,

        MAX_BY(
            fmp.REPEAT_PURCHASE_RATE,
            fmp.DATE_KEY
        ) AS FINAL_REPEAT_PURCHASE_RATE

    FROM {{ ref('fact_marketing_performance') }} fmp

    GROUP BY
        fmp.CAMPAIGN_KEY

)

SELECT
    ce.CAMPAIGN_KEY,
    dmc.CAMPAIGN_ID,
    dmc.CAMPAIGN_NAME,
    dmc.CAMPAIGN_TYPE,
    dmc.TARGET_AUDIENCE_SEGMENT,

    ce.TOTAL_NEW_CUSTOMERS_ACQUIRED,
    ce.FINAL_REPEAT_PURCHASE_RATE

FROM campaign_engagement ce

INNER JOIN {{ ref('dim_marketing_campaign') }} dmc
    ON ce.CAMPAIGN_KEY = dmc.CAMPAIGN_KEY