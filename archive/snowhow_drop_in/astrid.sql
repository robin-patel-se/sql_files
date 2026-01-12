-- What is the % of users looking first on mobile and then booking on desktop vs users who have just looked at desktop and then booked?

WITH web_transaction_users AS (
--list of users that transacted on the web
    SELECT DISTINCT
           sb.shiro_user_id,
           sb.booking_completed_date::DATE AS booking_date
    FROM se.data.scv_touched_transactions stt
             INNER JOIN se.data.se_booking sb ON stt.booking_id = sb.booking_id
    WHERE stt.event_tstamp >= current_date - 8
      AND sb.booking_status = 'COMPLETE'
      AND sb.device_platform = 'web'
)


SELECT COUNT(DISTINCT stba.attributed_user_id) AS users_with_non_web_sessions
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN web_transaction_users wtu
                    ON stba.attributed_user_id = wtu.shiro_user_id::VARCHAR AND stba.touch_start_tstamp < wtu.booking_date
                           AND stba.touch_start_tstamp >= wtu.booking_date - 15
                        AND stba.touch_experience  IS DISTINCT FROM 'web';
--4904

-- SELECT COUNT(DISTINCT wtu.shiro_user_id)
-- FROM web_transaction_users wtu
--6189