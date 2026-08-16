
SELECT
    fmp.CAMPAIGN_KEY,
    dmc.CAMPAIGN_ID,
    dmc.CAMPAIGN_NAME,
    dmc.CAMPAIGN_TYPE,
    dmc.CHANNEL,

    SUM(
        fmp.TOTAL_SALES_INFLUENCED
    ) AS TOTAL_SALES_INFLUENCED,

    MAX(
        dmc.TOTAL_COST
    ) AS CAMPAIGN_COST,

    SUM(
        fmp.NEW_CUSTOMERS_ACQUIRED
    ) AS NEW_CUSTOMERS_ACQUIRED,

    CASE
        WHEN MAX(dmc.TOTAL_COST) > 0
        THEN ROUND(
            (
                SUM(fmp.TOTAL_SALES_INFLUENCED)
                - MAX(dmc.TOTAL_COST)
            )
            / MAX(dmc.TOTAL_COST)
            * 100.0,
            2
        )
        ELSE NULL
    END AS ROI_PERCENTAGE

FROM {{ ref('fact_marketing_performance') }} fmp

INNER JOIN {{ ref('dim_marketing_campaign') }} dmc
    ON fmp.CAMPAIGN_KEY = dmc.CAMPAIGN_KEY

GROUP BY
    fmp.CAMPAIGN_KEY,
    dmc.CAMPAIGN_ID,
    dmc.CAMPAIGN_NAME,
    dmc.CAMPAIGN_TYPE,
    dmc.CHANNEL