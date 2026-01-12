--Q1: Show me for the last 6 months by week (se.data.sale_active), how many sales went active each week. Each row on this table can be considered a view_date where a se_sale_id was active.


--explain table, row for every active sale on a week
--task where you don't know the dataset, how to aggregate to give insight,
SELECT * FROM se.data.sale_active sa;

--Q2: se_credit, shows credits associated to users (shiro_user_id)
--For users that have signed up from 1st Jan 2019 (se_user_attributes), how many weeks each user has been signed up for and
--for each of the possible credit statuses (se_credit) show how many credits each user has and how much value they equate to in GBP (credit_amount_gbp)
--Expected output to be one row per user and if a user has no credits they should still be included in the list

-- Bonus Question, I'm looking for a user's email address, but can't find it in this table se.data.se_user_attributes, WHY?

------------------------------------------------------------------------------------------------------------------------


WITH app_catchment_users AS (
    --input users query, adjust this query to adjust the segment of users you want
    --to return data on
    --list of users that have signed up outside 180 days of their first app activity
    SELECT sua.shiro_user_id,
           TO_DATE(sua.first_app_activity_tstamp) AS app_installation_date
    FROM se.data.se_user_attributes sua
    WHERE DATE_TRUNC(MONTH, sua.first_app_activity_tstamp) <= '2020-11-01'
),
     users_plus_minus_180 AS (
         --create a grain of plus and minus 180 days from each user's first app event
         SELECT acu.shiro_user_id,
                acu.app_installation_date,
                sc.date_value                                           AS date,
                DATEDIFF(DAY, acu.app_installation_date, sc.date_value) AS since_app_download
         FROM app_catchment_users acu
                  INNER JOIN se.data.se_calendar sc ON acu.app_installation_date - 180 <= sc.date_value
             AND acu.app_installation_date + 180 >= sc.date_value
     ),
     user_opens AS (
         SELECT ceo.shiro_user_id,
                upm.date,
                COUNT(*) AS opens
         FROM se.data.crm_events_opens ceo
                  INNER JOIN users_plus_minus_180 upm ON ceo.shiro_user_id = upm.shiro_user_id AND ceo.event_date = upm.date
         GROUP BY 1, 2
     ),
     user_clicks AS (
         SELECT cec.shiro_user_id,
                upm.date,
                COUNT(*) AS clicks
         FROM se.data.crm_events_clicks cec
                  INNER JOIN users_plus_minus_180 upm ON cec.shiro_user_id = upm.shiro_user_id AND cec.event_date = upm.date
         GROUP BY 1, 2
     ),
     user_spvs AS (
         SELECT stba.attributed_user_id::INT AS shiro_user_id,
                sts.event_tstamp::DATE       AS date,
                COUNT(*)                     AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
                  INNER JOIN users_plus_minus_180 upm
                             ON stba.attributed_user_id = upm.shiro_user_id::VARCHAR AND sts.event_tstamp::DATE = upm.date
         WHERE stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2
     ),
     user_sessions AS (
         SELECT s.attributed_user_id::INT                                                      AS shiro_user_id,
                s.touch_start_tstamp::DATE                                                     AS date,
                COUNT(*)                                                                       AS sessions,
                SUM(IFF(s.touch_experience IN ('native app ios', 'native app android'), 1, 0)) AS app_sessions,
                SUM(IFF(s.touch_experience = 'native app ios', 1, 0))                          AS app_ios_sessions,
                SUM(IFF(s.touch_experience = 'native app android', 1, 0))                      AS app_android_sessions
         FROM se.data_pii.scv_touch_basic_attributes s
                  INNER JOIN users_plus_minus_180 upm
                             ON s.attributed_user_id = upm.shiro_user_id::VARCHAR AND s.touch_start_tstamp::DATE = upm.date
         WHERE s.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2
     ),
     user_bookings AS (
         SELECT fcb.shiro_user_id,
                fcb.booking_completed_date                                                      AS date,
                COUNT(*)                                                                        AS bookings,
                SUM(IFF(fcb.device_platform IN ('native app ios', 'native app android'), 1, 0)) AS app_bookings,
                SUM(IFF(fcb.device_platform = 'native app ios', 1, 0))                          AS app_ios_bookings,
                SUM(IFF(fcb.device_platform = 'native app android', 1, 0))                      AS app_android_bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                             AS margin,
                SUM(IFF(fcb.device_platform IN ('native app ios', 'native app android'),
                        fcb.margin_gross_of_toms_gbp_constant_currency, 0))                     AS app_margin,
                SUM(IFF(fcb.device_platform = 'native app ios', fcb.margin_gross_of_toms_gbp_constant_currency,
                        0))                                                                     AS app_ios_margin,
                SUM(IFF(fcb.device_platform = 'native app android', fcb.margin_gross_of_toms_gbp_constant_currency,
                        0))                                                                     AS app_android_margin
         FROM se.data.fact_complete_booking fcb
                  INNER JOIN users_plus_minus_180 upm
                             ON fcb.shiro_user_id = upm.shiro_user_id AND fcb.booking_completed_date = upm.date
         GROUP BY 1, 2
     ),
     combine_data AS (
         --combine the datasources together at user level
         SELECT upm.shiro_user_id,
                upm.app_installation_date,
                upm.since_app_download,
                upm.date,
                uo.opens,
                uc.clicks,
                us.spvs,
                uss.sessions,
                uss.app_sessions,
                uss.app_ios_sessions,
                uss.app_android_sessions,
                ub.bookings,
                ub.app_bookings,
                ub.app_ios_bookings,
                ub.app_android_bookings,
                ub.margin,
                ub.app_margin,
                ub.app_ios_margin,
                ub.app_android_margin
         FROM users_plus_minus_180 upm
                  LEFT JOIN user_opens uo ON upm.shiro_user_id = uo.shiro_user_id AND upm.date = uo.date
                  LEFT JOIN user_clicks uc ON upm.shiro_user_id = uc.shiro_user_id AND upm.date = uc.date
                  LEFT JOIN user_spvs us ON upm.shiro_user_id = us.shiro_user_id AND upm.date = us.date
                  LEFT JOIN user_sessions uss ON upm.shiro_user_id = uss.shiro_user_id AND upm.date = uss.date
                  LEFT JOIN user_bookings ub ON upm.shiro_user_id = ub.shiro_user_id AND upm.date = ub.date
     )
--aggregate to since download
SELECT cd.since_app_download,
       COUNT(DISTINCT cd.shiro_user_id) AS users,
       SUM(cd.opens)                    AS opens,
       SUM(cd.clicks)                   AS clicks,
       SUM(cd.spvs)                     AS spvs,
       AVG(cd.spvs)                     AS avg_spvs,
       SUM(cd.sessions)                 AS sessions,
       SUM(cd.app_sessions)             AS app_sessions,
       SUM(cd.app_ios_sessions)         AS app_ios_sessions,
       SUM(cd.app_android_sessions)     AS app_android_sessions,
       SUM(bookings)                    AS bookings,
       SUM(app_bookings)                AS app_bookings,
       SUM(app_ios_bookings)            AS app_ios_bookings,
       SUM(app_android_bookings)        AS app_android_bookings,
       SUM(margin)                      AS margin,
       SUM(app_margin)                  AS app_margin,
       SUM(app_ios_margin)              AS app_ios_margin,
       SUM(app_android_margin)          AS app_android_margin
FROM combine_data cd
GROUP BY 1;