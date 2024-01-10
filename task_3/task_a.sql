WITH cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('month', signup_date) AS cohort_month
    FROM Users
),
retention AS (
    SELECT
        cohorts.cohort_month,  -- Added table reference
        EXTRACT(MONTH FROM AGE(orders.order_date, cohorts.cohort_month)) AS period,
        COUNT(DISTINCT orders.user_id) AS retained_users  -- Corrected reference
    FROM cohorts
    JOIN orders ON cohorts.user_id = orders.user_id
    WHERE orders.order_date >= cohorts.cohort_month  -- Corrected reference
    GROUP BY 1, 2
),
initial_counts AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS initial_users
    FROM cohorts
    GROUP BY 1
)
SELECT
    cohorts.cohort_month,
    initial_counts.initial_users,
    retention.period,
    retention.retained_users,
    ROUND(100.0 * retention.retained_users / initial_counts.initial_users, 2) AS retention_rate
FROM cohorts
JOIN initial_counts ON cohorts.cohort_month = initial_counts.cohort_month
LEFT JOIN retention ON cohorts.cohort_month = retention.cohort_month
ORDER BY cohort_month, period;


