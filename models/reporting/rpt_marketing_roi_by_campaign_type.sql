{{ config(
    materialized='view'
) }}

WITH campaign_totals AS (

    SELECT
        fmp.CAMPAIGN_KEY,

        dmc.CAMPAIGN_TYPE,

        SUM(
            fmp.TOTAL_SALES_INFLUENCED
        ) AS CAMPAIGN_SALES_INFLUENCED,

        MAX(
            dmc.TOTAL_COST
        ) AS CAMPAIGN_COST

    FROM {{ ref('fact_marketing_performance') }} fmp

    INNER JOIN {{ ref('dim_marketing_campaign') }} dmc
        ON fmp.CAMPAIGN_KEY = dmc.CAMPAIGN_KEY

    GROUP BY
        fmp.CAMPAIGN_KEY,
        dmc.CAMPAIGN_TYPE

),

campaign_type_metrics AS (

    SELECT
        CAMPAIGN_TYPE,

        COUNT(
            DISTINCT CAMPAIGN_KEY
        ) AS TOTAL_CAMPAIGNS,

        SUM(
            CAMPAIGN_SALES_INFLUENCED
        ) AS TOTAL_SALES_INFLUENCED,

        SUM(
            CAMPAIGN_COST
        ) AS TOTAL_CAMPAIGN_COST

    FROM campaign_totals

    GROUP BY CAMPAIGN_TYPE

)

SELECT
    CAMPAIGN_TYPE,
    TOTAL_CAMPAIGNS,
    TOTAL_SALES_INFLUENCED,
    TOTAL_CAMPAIGN_COST,

    CASE
        WHEN TOTAL_CAMPAIGN_COST > 0
        THEN ROUND(
            (
                TOTAL_SALES_INFLUENCED
                - TOTAL_CAMPAIGN_COST
            )
            / TOTAL_CAMPAIGN_COST
            * 100.0,
            2
        )
        ELSE NULL
    END AS ROI_PERCENTAGE

FROM campaign_type_metrics