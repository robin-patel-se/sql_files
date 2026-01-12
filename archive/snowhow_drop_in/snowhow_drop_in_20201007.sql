WITH sess_bookings AS (

    SELECT stt.touch_id,
           COUNT(DISTINCT fcb.booking_id)                      AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin

    FROM se.data_pii.scv_touch_basic_attributes stba
             INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
             INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id

    WHERE stba.touch_start_tstamp >= '2020-09-01'
      AND stba.touch_start_tstamp <= '2020-09-30'

    GROUP BY 1
)
   , sess_spvs AS (
    SELECT stba.touch_id,
           COUNT(*)
               AS spvs

    FROM se.data_pii.scv_touch_basic_attributes stba
             LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id

    WHERE stba.touch_start_tstamp >= '2020-09-01'
      AND stba.touch_start_tstamp <= '2020-09-30'

    GROUP BY 1
)
SELECT stba.touch_start_tstamp::DATE                                                                         AS day,
       stmc.touch_mkt_channel,
       stmc.affiliate,
       stmc.utm_campaign,
       stba.touch_experience,
       stmc.touch_affiliate_territory                                                                        AS touch_hostname_territory,
       COALESCE(SUM(s.spvs), 0)                                                                              AS spvs,
       COUNT(DISTINCT CASE WHEN stba.stitched_identity_type = 'se_user_id' THEN stba.attributed_user_id END) AS logged_in_users,
       COALESCE(SUM(b.bookings), 0)                                                                          AS bookings,
       COALESCE(SUM(b.margin), 0)                                                                            AS margin

FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution a ON stba.touch_id = a.touch_id
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON a.attributed_touch_id = stmc.touch_id
         LEFT JOIN sess_bookings b ON stba.touch_id = b.touch_id
         LEFT JOIN sess_spvs s ON stba.touch_id = s.touch_id


WHERE stba.touch_start_tstamp >= '2020-09-01'
  AND stba.touch_start_tstamp <= '2020-09-30'
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT')
  AND a.attribution_model = 'last non direct'

GROUP BY 1, 2, 3, 4, 5, 6;

------------------------------------------------------------------------------------------------------------------------


SET view_date = '2020-09-01'; --date of the month you want to look at
USE WAREHOUSE pipe_xlarge;

WITH user_sessions_spvs AS (
    --calculate ppc non brand cpl sessions and spvs for all users in the month of 'view date'
    SELECT stba.attributed_user_id,
           COUNT(DISTINCT stba.touch_id)  AS sessions,

           COUNT(DISTINCT sts.se_sale_id) AS spvs,
           COUNT(DISTINCT
                 IFF(stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all'), stba.touch_id,
                     NULL))               AS facebook_sessions,
           COUNT(DISTINCT
                 IFF(stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all'), sts.se_sale_id,
                     NULL))               AS facebook_spvs


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
                COUNT(DISTINCT fcb.booking_id)                                 AS bookings,
                COUNT(DISTINCT IFF((stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all')),
                                   fcb.booking_id, NULL))                      AS facebook_bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency)            AS margin,
                SUM(IFF((stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all')),
                        fcb.margin_gross_of_toms_gbp_constant_currency, NULL)) AS facebook_margin

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
                sua.original_affiliate_id          AS affiliate,
                sua.signup_tstamp::DATE            AS member_join_date,
                IFF(member_join_date BETWEEN DATE_TRUNC(MONTH, $view_date::DATE) AND LAST_DAY($view_date::DATE), 'New',
                    'Existing')                    AS user_join_status,
                IFF(DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) <= 6, 1,
                    0)                             AS six_months_overlap,
                CASE
                    WHEN sua.member_original_affiliate_classification = 'Paid Social' AND
                         sua.original_affiliate_id IN (3034, 2619) THEN 'facebook_member_cpl'
                    WHEN sua.member_original_affiliate_classification = 'Paid Social' THEN 'facebook_member_cpa'
                    ELSE 'non_facebook_member' END AS member_affiliate_type,
                uss.sessions,
                uss.spvs,
                uss.facebook_sessions,
                uss.facebook_spvs,
                ub.bookings,
                ub.margin,
                ub.facebook_bookings,
                ub.facebook_margin

         FROM se.data.se_user_attributes sua
                  LEFT JOIN user_sessions_spvs uss
                            ON uss.attributed_user_id = sua.shiro_user_id::VARCHAR
                  LEFT JOIN user_bookings ub ON uss.attributed_user_id = ub.attributed_user_id
     )

SELECT ujs.original_affiliate_territory,
       ujs.six_months_overlap,
       ujs.user_join_status,
       ujs.member_affiliate_type,

       COUNT(DISTINCT shiro_user_id)                                  AS users,
       COUNT(DISTINCT IFF(ujs.sessions > 0, ujs.shiro_user_id, NULL)) AS facebook_session_users,
       SUM(ujs.sessions)                                              AS sessions,
       SUM(ujs.spvs)                                                  AS spvs,
       SUM(ujs.bookings)                                              AS bookings,
       SUM(ujs.margin)                                                AS margin,
       SUM(ujs.facebook_sessions)                                     AS facebook_sessions,
       SUM(ujs.facebook_spvs)                                         AS facebook_spvs,
       SUM(ujs.facebook_bookings)                                     AS facebook_bookings,
       SUM(ujs.facebook_margin)                                       AS facebook_margin

FROM user_join_status ujs
WHERE ujs.member_join_date IS NOT NULL --remove sessions for non members
  AND ujs.original_affiliate_territory IN ('UK', 'DE')

GROUP BY 1, 2, 3, 4;



WITH user_sessions_spvs AS (
    --calculate ppc non brand cpl sessions and spvs for all users in the month of 'view date'
    SELECT stba.attributed_user_id,
           COUNT(DISTINCT stba.touch_id)  AS sessions,

           COUNT(DISTINCT sts.se_sale_id) AS spvs,
           COUNT(DISTINCT
                 IFF(stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all'), stba.touch_id,
                     NULL))               AS facebook_sessions,
           COUNT(DISTINCT
                 IFF(stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all'), sts.se_sale_id,
                     NULL))               AS facebook_spvs


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
                COUNT(DISTINCT fcb.booking_id)                                 AS bookings,
                COUNT(DISTINCT IFF((stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all')),
                                   fcb.booking_id, NULL))                      AS facebook_bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency)            AS margin,
                SUM(IFF((stmc.touch_mkt_channel = 'Paid Social' AND stmc.affiliate NOT IN ('fbla-de', 'fb-uk-all')),
                        fcb.margin_gross_of_toms_gbp_constant_currency, NULL)) AS facebook_margin

         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_attribution ta
                             ON stba.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON ta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data.scv_touched_transactions stt ON stba.touch_id = stt.touch_id
                  INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id

         WHERE DATE_TRUNC(MONTH, stba.touch_start_tstamp) =
               DATE_TRUNC(MONTH, $view_date::DATE) --to set the session started in same month view date


         GROUP BY 1
     )
     --calc the join status of each user
SELECT sua.shiro_user_id,
       sua.original_affiliate_territory,
       sua.original_affiliate_id          AS affiliate,
       sua.original_affiliate_name,
       sua.signup_tstamp::DATE            AS member_join_date,
       sua.member_original_affiliate_classification,
       IFF(member_join_date BETWEEN DATE_TRUNC(MONTH, $view_date::DATE) AND LAST_DAY($view_date::DATE), 'New',
           'Existing')                    AS user_join_status,
       IFF(DATEDIFF(MONTHS, sua.signup_tstamp::DATE, LAST_DAY($view_date::DATE)) <= 6, 1,
           0)                             AS six_months_overlap,
       CASE
           WHEN sua.member_original_affiliate_classification = 'Paid Social' AND
                sua.original_affiliate_id IN ('1281', '3034') THEN 'facebook_member_cpl'
           WHEN sua.member_original_affiliate_classification = 'Paid Social' THEN 'facebook_member_cpa'
           ELSE 'non_facebook_member' END AS member_affiliate_type,
       uss.sessions,
       uss.spvs,
       uss.facebook_sessions,
       uss.facebook_spvs,
       ub.bookings,
       ub.margin,
       ub.facebook_bookings,
       ub.facebook_margin

FROM se.data.se_user_attributes sua
         LEFT JOIN user_sessions_spvs uss
                   ON uss.attributed_user_id = sua.shiro_user_id::VARCHAR
         LEFT JOIN user_bookings ub ON uss.attributed_user_id = ub.attributed_user_id
WHERE member_affiliate_type = 'facebook_member_cpa'
  AND user_join_status = 'New';



SELECT *
FROM se.data.se_affiliate sa
WHERE id IN (3034, 2619);

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
SELECT DISTINCT
       sts.event_tstamp::DATE,
       stmc.touch_mkt_channel,
       COUNT(sts.touch_id) AS spv
FROM se.data.scv_touch_marketing_channel AS stmc
         LEFT JOIN se.data.scv_touched_spvs AS sts ON stmc.touch_id = sts.touch_id
WHERE sts.event_tstamp >= '2020-01-01'
GROUP BY 1, 2