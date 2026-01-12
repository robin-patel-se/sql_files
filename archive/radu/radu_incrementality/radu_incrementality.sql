-- Date user joined -- se.data.se_user_attributes - signup_tstamp
-- BrowseCount30Days --number of sessions a user has has within 30 days
-- PreviousBookingDate -- penultimate booking date
-- Datebooked -- most recent booking date
-- Transaction ID -- most recent transaction id

--user level grain table

WITH users_bookings AS (
    SELECT fcb.shiro_user_id,
           fcb.booking_completed_date,
           COALESCE(sb.transaction_id, tb.se_sale_id || '-' || tb.reference_id)                        AS transaction_id,
           ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) AS rank
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_booking sb ON fcb.booking_id = sb.booking_id
             LEFT JOIN se.data.tb_booking tb ON fcb.booking_id = tb.booking_id
    WHERE fcb.booking_completed_date IS NOT NULL --because deposit bookings in catalogue aren't yet complete
      AND fcb.shiro_user_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date DESC) < 3
),
     last_30_day_sessions AS (
         SELECT stba.attributed_user_id,
                count(*) AS sessions
         FROM se.data_pii.scv_touch_basic_attributes stba
         WHERE stba.stitched_identity_type = 'se_user_id'
           AND stba.touch_start_tstamp >= CURRENT_DATE - 31
         GROUP BY 1
     )
SELECT sua.shiro_user_id,
       sua.signup_tstamp::DATE          AS date_user_joined,
       COALESCE(lds.sessions, 0)        AS sessions_last_30_days,
       ub2.booking_completed_date::DATE AS prev_booking_date,   -- penultimate booking date
       ub2.transaction_id               AS prev_transaction_id, -- penultimate transaction id
       ub1.booking_completed_date::DATE AS last_booking_date,   -- last booking date
       ub1.transaction_id               AS last_transaction_id, -- last transaction id
       CASE
           WHEN prev_booking_date IS NULL THEN 1
           WHEN prev_booking_date < CURRENT_DATE - 365 THEN 1
           ELSE 0
           END                          AS previous_booking     -- one time booker OR previous booking was more than a year ago
FROM se.data.se_user_attributes sua
         LEFT JOIN users_bookings ub1 ON sua.shiro_user_id = ub1.shiro_user_id AND ub1.rank = 1
         LEFT JOIN users_bookings ub2 ON sua.shiro_user_id = ub2.shiro_user_id AND ub2.rank = 2
         LEFT JOIN last_30_day_sessions lds ON sua.shiro_user_id = lds.attributed_user_id
WHERE sessions_last_30_days > 0
  AND sessions_last_30_days < 100
  AND last_booking_date - date_user_joined >= 3 --their last booking wasn't within 3 days of them joining
;

