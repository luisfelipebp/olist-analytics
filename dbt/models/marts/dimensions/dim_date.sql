WITH date_spine AS (

    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2016-01-01' as date)",
            end_date="cast('2019-01-01' as date)"
        )
    }}

),

date_clean AS (

    SELECT
        CAST(date_day AS DATE) AS clean_date
    FROM date_spine

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key(['clean_date']) }} AS date_sk,

        clean_date AS date_day,

        EXTRACT(YEAR FROM clean_date) AS year,
        EXTRACT(MONTH FROM clean_date) AS month,
        EXTRACT(QUARTER FROM clean_date) AS quarter,

        TO_CHAR(clean_date, 'Day') AS day_of_week,

        CASE
            WHEN EXTRACT(DOW FROM clean_date) IN (0, 6)
                THEN TRUE
            ELSE FALSE
        END AS is_weekend

    FROM date_clean

)

SELECT *
FROM final