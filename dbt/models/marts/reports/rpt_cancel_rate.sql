WITH orders AS (
    SELECT
        TO_CHAR(date_day, 'YYYY-MM') AS month_date,
        order_status
    FROM {{ ref('mart_orders') }}

),
    
final AS (

    SELECT

        month_date,
        COUNT(*) AS total_orders,
        SUM(
            CASE
                WHEN order_status = 'canceled'
                    THEN 1
                ELSE 0
            END
        ) AS canceled_orders,
        ROUND(
            SUM(
                CASE
                    WHEN order_status = 'canceled'
                        THEN 1
                    ELSE 0
                END
            ) * 100.0 / COUNT(*),
            2
        ) AS cancel_rate_pct

    FROM orders

    GROUP BY month_date
    HAVING COUNT(*) >= 100


)

SELECT *
FROM final
ORDER BY month_date