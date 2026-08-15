{{ config(materialized='table') }}

WITH date_spine AS (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="TO_DATE('2024-04-01')",
            end_date="TO_DATE('2024-09-28')"
        )
    }}

),

dates AS (

    SELECT
        date_day AS FULL_DATE
    FROM date_spine

),

calendar_attributes AS (

    SELECT
         --DateKey

        {{ dbt_utils.generate_surrogate_key(['FULL_DATE']) }} AS DATE_KEY,

        FULL_DATE,

        -- CALENDAR

        YEAR(FULL_DATE) AS YEAR,

        QUARTER(FULL_DATE) AS QUARTER,
    
        MONTH (FULL_DATE) AS MONTH,
        
        WEEK (FULL_DATE) AS WEEK,

        DAYOFWEEKISO(
            FULL_DATE
        ) AS DAY_OF_WEEK,

        
         --US holiday flag

        CASE
            WHEN
                
                -- New Year's Day
                 
                (MONTH(FULL_DATE) = 1
                 AND DAY(FULL_DATE) = 1)

                OR
                -- Independence Day
                 
                (MONTH(FULL_DATE) = 7
                 AND DAY(FULL_DATE) = 4)

                OR

                -- Veterans Day
                 
                (MONTH(FULL_DATE) = 11
                 AND DAY(FULL_DATE) = 11)

                OR

                -- Christmas Day
                 
                (MONTH(FULL_DATE) = 12
                 AND DAY(FULL_DATE) = 25)

            THEN TRUE
            ELSE FALSE
        END AS HOLIDAY_FLAG,

        
         -- Season
        
        CASE
            WHEN MONTH(FULL_DATE) IN (12, 1, 2)
                THEN 'Winter'

            WHEN MONTH(FULL_DATE) IN (3, 4, 5)
                THEN 'Spring'

            WHEN MONTH(FULL_DATE) IN (6, 7, 8)
                THEN 'Summer'

            ELSE 'Fall'
        END AS SEASON

    FROM dates

)

SELECT *
FROM calendar_attributes
ORDER BY FULL_DATE