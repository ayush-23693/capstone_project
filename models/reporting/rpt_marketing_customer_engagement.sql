{{ config(
    materialized='view'
) }}

WITH campaign_metrics AS (

    SELECT

        CAMPAIGN_KEY,

        SUM(
            NEW_CUSTOMERS_ACQUIRED
        ) AS TOTAL_NEW_CUSTOMERS_ACQUIRED

    FROM {{ ref('fact_marketing_performance') }}

    GROUP BY CAMPAIGN_KEY

),

final_campaign_rate AS (

    SELECT

        CAMPAIGN_KEY,

        REPEAT_PURCHASE_RATE AS FINAL_REPEAT_PURCHASE_RATE

    FROM {{ ref('fact_marketing_performance') }}

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY CAMPAIGN_KEY

        ORDER BY DATE_KEY DESC

    ) = 1

)

SELECT

    cm.CAMPAIGN_KEY,

    dmc.CAMPAIGN_ID,
    dmc.CAMPAIGN_NAME,
    dmc.CAMPAIGN_TYPE,
    dmc.TARGET_AUDIENCE_SEGMENT,

    cm.TOTAL_NEW_CUSTOMERS_ACQUIRED,

    fcr.FINAL_REPEAT_PURCHASE_RATE

FROM campaign_metrics cm

INNER JOIN final_campaign_rate fcr

    ON cm.CAMPAIGN_KEY = fcr.CAMPAIGN_KEY

INNER JOIN {{ ref('dim_marketing_campaign') }} dmc

    ON cm.CAMPAIGN_KEY = dmc.CAMPAIGN_KEY