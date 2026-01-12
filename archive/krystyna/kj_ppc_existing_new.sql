-- max date of ppc session in a month
WITH users_with_sessions AS (
    SELECT ua.shiro_user_id:: VARCHAR    AS user_id,
           stba.touch_start_tstamp::DATE AS session_date,
           stba.touch_id                 AS touch_id,
           ua.signup_tstamp::DATE        AS user_join_date,
           stba.touch_hostname_territory AS territory
    FROM se.data_pii.scv_touch_basic_attributes stba
             JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
             JOIN se.data.se_user_attributes ua ON ua.shiro_user_id:: VARCHAR = stba.attributed_user_id
    WHERE stba.touch_start_tstamp :: DATE >= '2020-08-01'
      AND stba.touch_start_tstamp :: DATE <= '2020-08-30'
      AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'
    GROUP BY 1, 2, 3, 4, 5
),
     users_with_bookings AS (
         SELECT fcb.booking_created_date                            AS booking_date,
                fcb.booking_id                                      AS booking_id,
                ua.shiro_user_id:: VARCHAR                          AS user_id,
                stba.touch_id                                       AS touch_id,
                stba.touch_hostname_territory                       AS territory,
                sum(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
         FROM se.data_pii.scv_touch_basic_attributes stba
                  JOIN se.data.scv_touch_attribution ta
                       ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
                  JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
                  JOIN se.data.se_user_attributes ua ON ua.shiro_user_id::VARCHAR = stba.attributed_user_id
                  JOIN se.data.scv_touched_transactions stt ON stt.touch_id = stba.touch_id
                  JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         WHERE fcb.booking_created_date >= '2020-08-01'
           AND fcb.booking_created_date <= '2020-08-30'
           AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'

         GROUP BY 1, 2, 3, 4, 5
     )
SELECT uws.session_date,
       uws.user_join_date,
       uws.user_id,
       uwb.booking_date,
       uwb.booking_id,
       uws.territory,
       uwb.margin,
       CASE WHEN uws.user_join_date < '2020-08-01' THEN 'Existing' ELSE 'New' END AS user_status
FROM users_with_sessions uws
         LEFT JOIN users_with_bookings uwb ON uwb.user_id = uws.user_id AND uwb.touch_id = uws.touch_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8



SELECT ua.shiro_user_id:: VARCHAR                          AS user_id,
       stba.touch_start_tstamp::DATE                       AS session_date,
       stba.touch_id                                       AS touch_id,
       ua.signup_tstamp::DATE                              AS user_join_date,
       CASE
           WHEN user_join_date < '2020-08-01'
               THEN 'Existing'
           ELSE 'New'
           END                                             AS user_status,
       stba.touch_hostname_territory                       AS territory,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data_pii.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_attribution ta
                   ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
         LEFT JOIN se.data.se_user_attributes ua ON ua.shiro_user_id::VARCHAR = stba.attributed_user_id
         LEFT JOIN se.data.scv_touched_transactions stt ON stt.touch_id = stba.touch_id
         LEFT JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
WHERE stba.touch_start_tstamp::DATE >= '2020-08-01'
  AND stba.touch_start_tstamp::DATE <= '2020-08-30'
  AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'
GROUP BY 1, 2, 3, 4, 5, 6;



SELECT stba.touch_start_tstamp::DATE                                                 AS date,
       COUNT(DISTINCT stba.attributed_user_id)                                       AS users,
       COUNT(DISTINCT ua.shiro_user_id)                                              AS members,
       COUNT(DISTINCT
             IFF(ua.signup_tstamp::DATE < $view_date::DATE, ua.shiro_user_id, NULL)) AS existing_users,
       COUNT(DISTINCT IFF(DATE_TRUNC(MONTH, ua.signup_tstamp) = DATE_TRUNC(MONTH, $view_date::DATE), ua.shiro_user_id,
                          NULL))                                                     AS new_users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution ta
                    ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
         LEFT JOIN se.data.se_user_attributes ua ON ua.shiro_user_id::VARCHAR = stba.attributed_user_id
WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) = DATE_TRUNC(MONTH, $view_date::DATE)
  AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'
GROUP BY 1



SET view_date = '2020-08-01'; --date of the month you want to look at

WITH user_sessions_spvs AS (
    --calculate ppc non brand cpl sessions and spvs for all users in the month of 'view date'
    SELECT stba.attributed_user_id,
           COUNT(DISTINCT stba.touch_id)  AS sessions,
           COUNT(DISTINCT sts.se_sale_id) AS spvs

    FROM se.data_pii.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_attribution ta
                        ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) =
          DATE_TRUNC(MONTH, $view_date::DATE) --to set the session started in same month view date
      AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'
    GROUP BY 1
),
     user_bookings AS (
         --calculate ppc non brand cpl bookings for all users in the month of 'view date'
         SELECT stba.attributed_user_id,
                COUNT(DISTINCT fcb.booking_id)                      AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin

         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_attribution ta
                             ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
                  INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) =
               DATE_TRUNC(MONTH, $view_date::DATE) --to set the session started in same month view date
           AND stmc.touch_mkt_channel = 'PPC - Non Brand CPL'
         GROUP BY 1
     ),
     user_join_status AS (
         --calc the join status of each user
         SELECT uss.attributed_user_id,
                sua.signup_tstamp::DATE AS member_join_date,
                IFF(member_join_date BETWEEN DATE_TRUNC(MONTH, $view_date::DATE)
                        AND LAST_DAY($view_date::DATE), --user joined within the month of 'view date'
                    'New', 'Existing')  AS user_status,
                uss.sessions,
                uss.spvs,
                ub.bookings,
                ub.margin
         FROM user_sessions_spvs uss
                  LEFT JOIN user_bookings ub ON uss.attributed_user_id = ub.attributed_user_id
                  LEFT JOIN se.data.se_user_attributes sua ON uss.attributed_user_id = sua.shiro_user_id::VARCHAR
     )
SELECT ujs.user_status,
       SUM(ujs.sessions) AS sessions,
       SUM(ujs.spvs)     AS spvs,
       SUM(ujs.bookings) AS bookings,
       SUM(ujs.margin)   AS margin
FROM user_join_status ujs
WHERE ujs.member_join_date IS NOT NULL --remove sessions for non members
GROUP BY 1;