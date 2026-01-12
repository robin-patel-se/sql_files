-- i think it would be interesting to understand a general behavioural pattern of users who transact across sessions
-- outcome could be something like: users who book, have on average 4 sessions in the week before the booking
-- or
-- maybe even some cross device kind of insight - users who book actually do research on mobile but end up booking on desktop
USE WAREHOUSE pipe_xlarge;
WITH user_trx_date AS (
    SELECT f.shiro_user_id,
           MAX(f.booking_completed_date) AS last_transaction_date
    FROM se.data.fact_complete_booking f
    WHERE f.booking_completed_date >= '2021-02-01'
    GROUP BY 1
),
     minus_4_weeks_sessions AS (
         SELECT *
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN user_trx_date utd ON stba.attributed_user_id = utd.shiro_user_id::VARCHAR
         WHERE stba.touch_start_tstamp >= DATEADD(WEEK, -1, utd.last_transaction_date)
     )
SELECT mws.shiro_user_id,
       COUNT(*)
FROM minus_4_weeks_sessions mws
GROUP BY 1;
