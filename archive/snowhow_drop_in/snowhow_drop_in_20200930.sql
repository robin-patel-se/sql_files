-- What is the % of users who have booked a hotel multiple times?
-- What interaction do users first when interacting with the map on the sale page? Difference between mobile and desktop?
-- What is the % of users interacting with the calendar of the non-default vs the default offer? Difference between mobile and desktop?
-- What is the % of users looking first on mobile and then booking on desktop vs users who have just looked at desktop and then booked?
-- What is the % of bookings that didn’t select the cheapest offer?
-- What is the % of sales that don’t have the cheapest offer as the default offer?
-- What is the average revenue per user?

-- What is the % of users who have booked a hotel multiple times?
SELECT COUNT(DISTINCT shiro_user_id) AS multiple_bookers,
       (
           SELECT COUNT(DISTINCT shiro_user_id)
           FROM se.data.se_booking sb
           WHERE sb.shiro_user_id IS NOT NULL
             AND sb.booking_status = 'COMPLETE'
       )                             AS bookers,
       multiple_bookers / bookers    AS multiple_booker_perc
FROM (
         SELECT sb.shiro_user_id,
                ssa.company_name,
                count(*)
         FROM se.data.se_booking sb
                  INNER JOIN se.data.se_sale_attributes ssa ON sb.sale_id = ssa.se_sale_id
         WHERE sb.shiro_user_id IS NOT NULL
           AND sb.booking_status = 'COMPLETE'
         GROUP BY 1, 2
         HAVING count(*) > 1
     );

-- What is the % of users looking first on mobile and then booking on desktop vs users who have just looked at desktop and then booked?
SELECT *
FROM se.data.scv_touch_basic_attributes stba;

WITH web_booking_sessions AS (
    SELECT stt.touch_id
    FROM se.data.scv_touched_transactions stt
             LEFT JOIN se.data.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
    WHERE stt.event_tstamp::DATE >= CURRENT_DATE - 7
      AND stba.touch_experience = 'web'
)
SELECT s.attributed_user_id,
       SUM(IFF(s.touch_experience = 'mobile web', 1, 0)) AS mobile_web_sessions,
       SUM(IFF(s.touch_experience = 'mobile web', 0, 1)) AS non_mobile_web_session,
       COUNT(*)                                          AS sessions
FROM se.data.scv_touch_basic_attributes s
         INNER JOIN web_bookers AND S.touch_start_tstamp >= CURRENT_DATE - 10
GROUP BY 1;

-- What is the % of sales that don’t have the cheapest offer as the default offer?


-- What is the average revenue per user?

SELECT AVG(avg_margin),
       AVG(avg_customer_total_price)
FROM (
         SELECT sb.shiro_user_id,
                AVG(sb.margin_gross_of_toms_gbp_constant_currency) AS avg_margin,
                AVG(sb.customer_total_price_gbp_constant_currency) AS avg_customer_total_price
         FROM se.data.se_booking sb
         WHERE sb.booking_status = 'COMPLETE'
         GROUP BY 1
     );


USE WAREHOUSE pipe_xlarge;

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
         SELECT sua.shiro_user_id,
                sua.original_affiliate_territory,
                sua.signup_tstamp::DATE        AS member_join_date,
                sua.member_original_affiliate_classification,
                CASE
                    WHEN DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) = 0
                        AND sua.member_original_affiliate_classification = 'PPC Non Brand'
                        THEN 'new_ppc_members'
                    WHEN DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) = 0
                        AND sua.member_original_affiliate_classification NOT LIKE 'PPC Non Brand'
                        THEN 'new_other_members'
                    WHEN DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) >= 1
                        THEN 'old_members'
                    ELSE '0'
                    END                        AS members_type,
                CASE
                    WHEN DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) <= 6 THEN '1'
                    ELSE '0' END               AS six_months_overlap,
                CASE
                    WHEN uss.attributed_user_id IN (
                        SELECT attributed_user_id
                        FROM user_bookings
                    ) THEN 'PPC Bookers'
                    ELSE 'PPC Non Bookers' END AS booker_status,
                uss.sessions,
                uss.spvs,
                ub.bookings,
                ub.margin
         FROM se.data.se_user_attributes sua
                  LEFT JOIN user_sessions_spvs uss ON uss.attributed_user_id = sua.shiro_user_id::VARCHAR
                  LEFT JOIN user_bookings ub ON uss.attributed_user_id = ub.attributed_user_id
     )
SELECT ujs.original_affiliate_territory,
/*ujs.MEMBER_ORIGINAL_AFFILIATE_CLASSIFICATION, */
       ujs.six_months_overlap,
       ujs.booker_status,
       ujs.members_type,
       CASE
           WHEN ujs.members_type = 'new_ppc_members' THEN SUM(ujs.margin) + SUM(ujs.margin) * 0.5
           ELSE '0' END                                                           AS six_m_ltv_inc_m1,
/*SUM(ujs.margin) - (case when ujs.six_months_overlap = 1 then SUM(ujs.margin) else '0' end ) as discounted_six_M_LTV, */
       (six_m_ltv_inc_m1) +
       (SUM(ujs.margin) -
        (CASE WHEN ujs.six_months_overlap = 1 THEN SUM(ujs.margin) ELSE '0' END)) AS total_margin,
       SUM(ujs.sessions)                                                          AS sessions,
       SUM(ujs.spvs)                                                              AS spvs,
       SUM(ujs.bookings)                                                          AS bookings,
       SUM(ujs.margin)                                                            AS margin,
       COUNT(DISTINCT IFF(DATE_TRUNC(MONTH, ujs.member_join_date::DATE)
                              = DATE_TRUNC(MONTH, $view_date::DATE),
                          ujs.shiro_user_id,
                          NULL))                                                  AS users,
       COUNT(DISTINCT IFF(ujs.sessions > 0, ujs.shiro_user_id, NULL))             AS session_users
FROM user_join_status ujs
WHERE ujs.member_join_date IS NOT NULL --remove sessions for non members
  AND ujs.original_affiliate_territory IN ('UK', 'DE', 'IT')
GROUP BY 1, 2, 3, 4;


USE WAREHOUSE pipe_xlarge;
SET view_date = '2020-08-01'; --date of the month you want to look at
WITH user_sessions_spvs AS (
    --calculate ppc non brand cpl sessions and spvs for all users in the month of 'view date'
    SELECT stba.attributed_user_id,
           COUNT(DISTINCT stba.touch_id)  AS sessions,
           COUNT(DISTINCT IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL',
                              stba.touch_id,
                              NULL))      AS ppc_sessions,
           COUNT(DISTINCT sts.se_sale_id) AS spvs,
           COUNT(DISTINCT IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL',
                              sts.se_sale_id,
                              NULL))      AS ppc_spvs
    FROM se.data_pii.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touch_attribution ta
                        ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) =
          DATE_TRUNC(MONTH, $view_date::DATE) --to set the session started in same month view date
    GROUP BY 1
),
     user_bookings AS (
         --calculate ppc non brand cpl bookings for all users in the month of 'view date'
         SELECT stba.attributed_user_id,
                COUNT(DISTINCT fcb.booking_id)                      AS bookings,
                COUNT(DISTINCT
                      IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL',
                          fcb.booking_id,
                          NULL))                                    AS ppc_bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin,
                SUM(IFF(stmc.touch_mkt_channel = 'PPC - Non Brand CPL',
                        fcb.margin_gross_of_toms_gbp_constant_currency,
                        NULL))                                      AS ppc_margin
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_attribution ta
                             ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
                  INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) =
               DATE_TRUNC(MONTH, $view_date::DATE) --to set the session started in same month view date
         GROUP BY 1
     ),
     user_join_status AS (
         --calc the join status of each user
         SELECT sua.shiro_user_id,
                sua.original_affiliate_territory,
                sua.signup_tstamp::DATE                                                               AS member_join_date,
                IFF(member_join_date BETWEEN DATE_TRUNC(MONTH, $view_date::DATE)
                        AND LAST_DAY($view_date::DATE), --user joined within the month of 'view date'
                    'New', 'Existing')                                                                AS user_join_status,
                IFF(DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) <= 6, 1, 0) AS six_months_overlap,
                IFF(sua.member_original_affiliate_classification = 'PPC Non Brand'
                    , 'ppc_member', 'non_ppc_member')                                                 AS member_affiliate_type,
                IFF(ub.bookings >= 1, 'PPC Booker', 'Other')                                          AS ppc_booker_status,
                uss.sessions,
                uss.spvs,
                uss.ppc_sessions,
                uss.ppc_spvs,
                ub.bookings,
                ub.margin,
                ub.ppc_bookings,
                ub.ppc_margin
         FROM se.data.se_user_attributes sua
                  LEFT JOIN user_sessions_spvs uss ON uss.attributed_user_id = sua.shiro_user_id::VARCHAR
                  LEFT JOIN user_bookings ub ON uss.attributed_user_id = ub.attributed_user_id
     )
SELECT ujs.original_affiliate_territory,
       ujs.six_months_overlap,
       ujs.ppc_booker_status,
       ujs.user_join_status,
       ujs.member_affiliate_type,
       COUNT(DISTINCT shiro_user_id)                                  AS users,
       COUNT(DISTINCT IFF(ujs.sessions > 0, ujs.shiro_user_id, NULL)) AS ppc_session_users,
       SUM(ujs.sessions)                                              AS sessions,
       SUM(ujs.spvs)                                                  AS spvs,
       SUM(ujs.bookings)                                              AS bookings,
       SUM(ujs.margin)                                                AS margin,
       SUM(ujs.ppc_sessions)                                          AS ppc_sessions,
       SUM(ujs.ppc_spvs)                                              AS ppc_spvs,
       SUM(ujs.ppc_bookings)                                          AS ppc_bookings,
       SUM(ujs.ppc_margin)                                            AS ppc_margin
FROM user_join_status ujs
WHERE ujs.member_join_date IS NOT NULL --remove sessions for non members
GROUP BY 1, 2, 3, 4, 5;

------------------------------------------------------------------------------------------------------------------------

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

SELECT disTiNCT touch_experience FROm se.data.scv_touch_basic_attributes stba;

