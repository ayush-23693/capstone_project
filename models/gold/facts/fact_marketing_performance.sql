{{ config(
    materialized='table'
) }}

/*
GRAIN: One row per campaign per date.
ATTRIBUTION RULE:
    A completed sale is attributed to the campaign whose
    CAMPAIGN_KEY is carried on FACT_SALES from the source order.

    campaign attribution is already present
    on the source order.
*/



   -- COMPLETE CAMPAIGN × DATE GRID


WITH campaign_dates AS (

    SELECT
        c.CAMPAIGN_KEY,
        c.CAMPAIGN_ID,
        TO_DATE(c.START_DATE) AS START_DATE,
        TO_DATE(c.END_DATE) AS END_DATE,
        c.TOTAL_COST AS CAMPAIGN_COST,

        d.DATE_KEY,
        d.FULL_DATE AS PERFORMANCE_DATE

    FROM {{ ref('dim_marketing_campaign') }} c

    INNER JOIN {{ ref('dim_date') }} d
        ON d.FULL_DATE >= TO_DATE(c.START_DATE)
       AND d.FULL_DATE <= TO_DATE(c.END_DATE)

),


/*
    ORDER-LEVEL SALES

   FACT_SALES is at order-item grain, so aggregated it to
   order grain before doing customer purchase analysis.

 */

order_sales AS (

    SELECT

        ORDER_ID,
        CUSTOMER_KEY,
        CAMPAIGN_KEY,
        DATE_KEY,

        MIN(
            DATE_KEY
        ) AS ORDER_DATE_KEY,

        SUM(
            TOTAL_SALES_AMOUNT
        ) AS ORDER_SALES_AMOUNT

    FROM {{ ref('fact_sales') }}

    WHERE CAMPAIGN_KEY IS NOT NULL
      AND CUSTOMER_KEY IS NOT NULL
      AND DATE_KEY IS NOT NULL

    GROUP BY
        ORDER_ID,
        CUSTOMER_KEY,
        CAMPAIGN_KEY,
        DATE_KEY

),



   -- FIRST PURCHASE FOR EVERY CUSTOMER


customer_purchase_history AS (

    SELECT

        ORDER_ID,
        CUSTOMER_KEY,
        CAMPAIGN_KEY,
        DATE_KEY,
        ORDER_SALES_AMOUNT,

        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_KEY
            ORDER BY
                DATE_KEY,
                ORDER_ID
        ) AS PURCHASE_NUMBER

    FROM order_sales

),


   -- CAMPAIGN-ATTRIBUTED ORDERS



campaign_orders AS (

    SELECT

        oph.ORDER_ID,
        oph.CUSTOMER_KEY,
        oph.CAMPAIGN_KEY,
        oph.DATE_KEY,
        oph.ORDER_SALES_AMOUNT,
        oph.PURCHASE_NUMBER,

        CASE
            WHEN oph.PURCHASE_NUMBER = 1
            THEN TRUE
            ELSE FALSE
        END AS IS_FIRST_PURCHASE,

        CASE
            WHEN oph.PURCHASE_NUMBER > 1
            THEN TRUE
            ELSE FALSE
        END AS IS_REPEAT_PURCHASE

    FROM customer_purchase_history oph

    INNER JOIN {{ ref('dim_marketing_campaign') }} c
        ON oph.CAMPAIGN_KEY = c.CAMPAIGN_KEY

    INNER JOIN {{ ref('dim_date') }} d
        ON oph.DATE_KEY = d.DATE_KEY

    WHERE d.FULL_DATE >= TO_DATE(c.START_DATE)
      AND d.FULL_DATE <= TO_DATE(c.END_DATE)

),


   -- DAILY SALES


daily_sales AS (

    SELECT

        CAMPAIGN_KEY,
        DATE_KEY,

        SUM(
            ORDER_SALES_AMOUNT
        ) AS TOTAL_SALES_INFLUENCED

    FROM campaign_orders

    GROUP BY
        CAMPAIGN_KEY,
        DATE_KEY

),



   -- DAILY NEW CUSTOMERS


daily_new_customers AS (

    SELECT

        CAMPAIGN_KEY,
        DATE_KEY,

        COUNT(
            DISTINCT CUSTOMER_KEY
        ) AS NEW_CUSTOMERS_ACQUIRED

    FROM campaign_orders

    WHERE IS_FIRST_PURCHASE = TRUE

    GROUP BY
        CAMPAIGN_KEY,
        DATE_KEY

),


   -- CUMULATIVE CUSTOMER COUNTS

campaign_customer_dates AS (

    SELECT DISTINCT

        CAMPAIGN_KEY,
        CUSTOMER_KEY,
        DATE_KEY,
        IS_FIRST_PURCHASE,
        IS_REPEAT_PURCHASE

    FROM campaign_orders

),


cumulative_customer_metrics AS (

    SELECT

        cd.CAMPAIGN_KEY,
        cd.DATE_KEY,

        /*
         * Customers whose first campaign-attributed purchase
         * has occurred on or before this date.
         */
        COUNT(
            DISTINCT CASE
                WHEN cd.IS_FIRST_PURCHASE = TRUE
                THEN cd.CUSTOMER_KEY
            END
        ) AS CUMULATIVE_FIRST_PURCHASE_CUSTOMERS,

        /*
         * Customers who have made a subsequent
         * campaign-attributed purchase by this date.
         */
        COUNT(
            DISTINCT CASE
                WHEN cd.IS_REPEAT_PURCHASE = TRUE
                THEN cd.CUSTOMER_KEY
            END
        ) AS CUMULATIVE_REPEAT_CUSTOMERS

    FROM campaign_customer_dates cd

    GROUP BY
        cd.CAMPAIGN_KEY,
        cd.DATE_KEY

),



   -- COMBINED TO REQUIRED CAMPAIGN × DATE GRAIN


daily_metrics AS (

    SELECT

        cd.CAMPAIGN_KEY,
        cd.CAMPAIGN_ID,
        cd.DATE_KEY,
        cd.PERFORMANCE_DATE,
        cd.CAMPAIGN_COST,

        COALESCE(
            ds.TOTAL_SALES_INFLUENCED,
            0
        ) AS TOTAL_SALES_INFLUENCED,

        COALESCE(
            dnc.NEW_CUSTOMERS_ACQUIRED,
            0
        ) AS NEW_CUSTOMERS_ACQUIRED,

        COALESCE(
            ccm.CUMULATIVE_FIRST_PURCHASE_CUSTOMERS,
            0
        ) AS CUMULATIVE_FIRST_PURCHASE_CUSTOMERS,

        COALESCE(
            ccm.CUMULATIVE_REPEAT_CUSTOMERS,
            0
        ) AS CUMULATIVE_REPEAT_CUSTOMERS

    FROM campaign_dates cd

    LEFT JOIN daily_sales ds
        ON cd.CAMPAIGN_KEY = ds.CAMPAIGN_KEY
       AND cd.DATE_KEY = ds.DATE_KEY

    LEFT JOIN daily_new_customers dnc
        ON cd.CAMPAIGN_KEY = dnc.CAMPAIGN_KEY
       AND cd.DATE_KEY = dnc.DATE_KEY

    LEFT JOIN cumulative_customer_metrics ccm
        ON cd.CAMPAIGN_KEY = ccm.CAMPAIGN_KEY
       AND cd.DATE_KEY = ccm.DATE_KEY

)



--   FINAL FACT


SELECT

    {{ dbt_utils.generate_surrogate_key([
        'CAMPAIGN_KEY',
        'DATE_KEY'
    ]) }} AS MARKETING_PERFORMANCE_KEY,

    CAMPAIGN_KEY,
    DATE_KEY,

    TOTAL_SALES_INFLUENCED,

    NEW_CUSTOMERS_ACQUIRED,

    CASE
        WHEN CUMULATIVE_FIRST_PURCHASE_CUSTOMERS > 0
        THEN ROUND(
            100.0
            * CUMULATIVE_REPEAT_CUSTOMERS
            / CUMULATIVE_FIRST_PURCHASE_CUSTOMERS,
            2
        )
        ELSE NULL
    END AS REPEAT_PURCHASE_RATE,

    CASE
        WHEN CAMPAIGN_COST > 0
        THEN ROUND(
            (
                SUM(TOTAL_SALES_INFLUENCED) OVER (
                    PARTITION BY CAMPAIGN_KEY
                    ORDER BY DATE_KEY
                    ROWS BETWEEN UNBOUNDED PRECEDING
                         AND CURRENT ROW
                )
                - CAMPAIGN_COST
            )
            / CAMPAIGN_COST
            * 100.0,
            2
        )
        ELSE NULL
    END AS ROI

FROM daily_metrics