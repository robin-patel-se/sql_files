SELECT
    MIN(ac.event_month) OVER (PARTITION BY ac.user_id)              AS min_month,
    ac.event_month,
    ua.territory,
    SUM(ac.sessions) OVER (PARTITION BY ac.user_id, ac.event_month) AS total_session
FROM collab.analytics_engineer_task.user_attributes ua
    JOIN collab.analytics_engineer_task.fact_user_activity ac
         ON ua.user_id = ac.user_id
WHERE ac.event_month >= '2021-01-01';

WITH cohort_items AS (
    SELECT
        CAST(signup_tstamp AS date) AS cohort_month,
        user_id,
        territory
    FROM collab.analytics_engineer_task.user_attributes ua
),
     user_activities AS (
         SELECT
             DATEDIFF(MONTH, cohort_month, event_month) AS month_number,
             tr.user_id,
             tr.bookings
         FROM collab.analytics_engineer_task.fact_user_transactions tr
             JOIN cohort_items ci
                  ON tr.user_id = ci.user_id
         GROUP BY tr.user_id,
                  DATEDIFF(MONTH, cohort_month, event_month),
                  bookings
     ),
     retention_table AS (
         SELECT
             format(ci.cohort_month, 'yyyy-MM') AS cohort_month,
             ac.month_number,
             SUM(bookings)                      AS count_booking,
             territory
         FROM collab.analytics_engineer_task.fact_user_activity ac
             JOIN cohort_items ci ON ac.user_id = ci.user_id
         GROUP BY format(ci.cohort_month, 'yyyy-MM'),
                  ac.month_number,
                  territory
     )
SELECT
    cohort_month,
    month_number,
    count_booking,
    territory
FROM retention_table
WHERE month_number BETWEEN 0 AND 12
