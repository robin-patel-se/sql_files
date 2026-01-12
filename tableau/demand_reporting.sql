------------------------------------------------------------------------------------------------------------------------
--member recency status function
CREATE OR REPLACE FUNCTION se_dev_robin.data.member_recency_status(signup_tstamp TIMESTAMP, comparison_tstamp TIMESTAMP
                                                                  )
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
    SELECT
        --     IFF(signup_tstamp >= comparison_tstamp::DATE - 30, 'new', 'old')
        CASE
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) < 0
                THEN '6. Pre-membership' --for activity that happens prior to sign up
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) > 365 THEN '5. 365d+'
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) > 180 THEN '4. 181-365d'
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) > 90 THEN '3. 91-180d'
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) > 30 THEN '2. 31-90d'
            WHEN DATEDIFF(DAY, signup_tstamp, comparison_tstamp) >= 0 THEN '1. <30d'
            ELSE '7. Other'
            END

$$
;

SELECT DATEDIFF(DAY, CURRENT_DATE - 30, CURRENT_DATE)

-- <30d (members joined 30d or less ago)
-- 31-90d (members joined more than 30d but equal or less than 90d ago)
-- 91-180d (members joined more than 90d but equal or less than 180d ago)
-- 181-365d (members joined more than 180d but equal or less than 365d ago)
-- 365d+ (members joined more than 365d ago)

GRANT USAGE ON FUNCTION se_dev_robin.data.member_recency_status(TIMESTAMP, TIMESTAMP) TO ROLE se_basic;
GRANT USAGE ON SCHEMA se_dev_robin.data TO ROLE se_basic;

SELECT DISTINCT
       se_dev_robin.data.member_recency_status(sua.signup_tstamp, ua.date::TIMESTAMP) AS member_recency_status,
       sua.current_affiliate_territory,
       sua.original_affiliate_territory,
       ua.date
FROM se.data.user_activity ua
         INNER JOIN se.data.se_user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
WHERE ua.web_sessions_1d > 0
   OR ua.app_sessions_1d > 0
   OR ua.emails_1d > 0
    AND ua.date >= '2018-01-01'

------------------------------------------------------------------------------------------------------------------------

--create a grain based on web/app/email activity and bookings
--aggregate up to user signup_group which is a grouping of users that
--are classified as either new or old at the time of the activity/booking
WITH activity AS (
    SELECT DISTINCT
           ua.shiro_user_id,
           ua.date
    FROM se.data.user_activity ua
    WHERE ua.web_sessions_1d > 0
       OR ua.app_sessions_1d > 0
       OR ua.emails_1d > 0
        AND ua.date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           fcb.shiro_user_id,
           fcb.booking_completed_date
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           es.shiro_user_id,
           es.event_date
    FROM se.data.crm_events_sends es
    WHERE es.event_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           eo.shiro_user_id,
           eo.event_date
    FROM se.data.crm_events_opens eo
    WHERE eo.event_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           ec.shiro_user_id,
           ec.event_date
    FROM se.data.crm_events_clicks ec
    WHERE ec.event_date >= '2018-01-01'
)
SELECT se.data.member_recency_status(sua.signup_tstamp, a.date::TIMESTAMP)    AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       a.date
FROM activity a
         INNER JOIN se.data.se_user_attributes sua ON a.shiro_user_id = sua.shiro_user_id
WHERE a.date < CURRENT_DATE
GROUP BY 1, 2, 3, 4


------------------------------------------------------------------------------------------------------------------------
--scv
USE WAREHOUSE pipe_xlarge;

WITH bookings AS (
    SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
           se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
           se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                             AS date,
           se.data.channel_category(stmc.touch_mkt_channel)                             AS channel, -- last click channel
           stba.touch_experience,
           se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
           se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
           ds.product_configuration,
           ds.travel_type,
           COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                           AS no_nights,
           SUM(fcb.rooms)                                                               AS rooms
    FROM se.data.fact_complete_booking fcb
             INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                        ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
             INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
             INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE fcb.booking_completed_date >= '2018-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
     spvs AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)     AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
                sts.event_tstamp::DATE                                                 AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                       AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)          AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)   AS posa_category,
                ds.product_configuration,
                ds.travel_type,
                COUNT(DISTINCT sts.event_hash)                                         AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                             ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                             AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
                d.product_configuration,
                d.travel_type,
                COUNT(DISTINCT stba.touch_id)                                             AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                   AS users
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                  LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
         WHERE stba.touch_start_tstamp >= '2018-01-01'
           AND stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     )

SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
       COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                ss.current_affiliate_territory)                                             AS current_affiliate_territory,
       COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                ss.original_affiliate_territory)                                            AS original_affiliate_territory,
       COALESCE(b.date, s.date, ss.date)                                                    AS date,
       COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
       INITCAP(
               COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
       COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
       COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
       COALESCE(b.product_configuration, s.product_configuration,
                ss.product_configuration)                                                   AS product_configuration,
       COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
       COALESCE(b.bookings, 0)                                                              AS bookings,
       COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
       COALESCE(b.no_nights, 0)                                                             AS no_nights,
       COALESCE(b.rooms, 0)                                                                 AS rooms,
       COALESCE(s.spvs, 0)                                                                  AS spvs,
       COALESCE(ss.sessions, 0)                                                             AS sessions,
       COALESCE(ss.users, 0)                                                                AS users
FROM bookings b
         FULL OUTER JOIN spvs s ON
        b.member_recency_status = s.member_recency_status AND
        b.current_affiliate_territory = s.current_affiliate_territory AND
        b.original_affiliate_territory = s.original_affiliate_territory AND
        b.date = s.date AND
        b.channel = s.channel AND
        b.touch_experience = s.touch_experience AND
        b.platform = s.platform AND
        b.posa_category = s.posa_category AND
        b.product_configuration = s.product_configuration AND
        b.travel_type = s.travel_type
         FULL OUTER JOIN sessions ss ON
            COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
            COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
            COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
            COALESCE(b.date, s.date) = ss.date AND
            COALESCE(b.channel, s.channel) = ss.channel AND
            COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
            COALESCE(b.platform, s.platform) = ss.platform AND
            COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
            COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
            COALESCE(b.travel_type, s.travel_type) = ss.travel_type;
------------------------------------------------------------------------------------------------------------------------
--sessions and users

SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
       stba.touch_experience,
       se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.touch_start_tstamp >= '2018-01-01'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;



------------------------------------------------------------------------------------------------------------------------
--sign up events
SELECT se.data.member_recency_status(sua.signup_tstamp, sua.signup_tstamp::TIMESTAMP) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)          AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)         AS original_affiliate_territory,
       sua.signup_tstamp::DATE                                                        AS signup_date,
       COUNT(DISTINCT sua.shiro_user_id)                                              AS signups
FROM se.data.se_user_attributes sua
GROUP BY 1, 2, 3, 4


------------------------------------------------------------------------------------------------------------------------

SELECT se.data.member_recency_status(sua.signup_tstamp, es.event_date::TIMESTAMP) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)      AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)     AS original_affiliate_territory,
       es.event_date                                                              AS send_date,
       COUNT(*)                                                                   AS sends
FROM se.data.crm_events_sends es
         INNER JOIN se.data.se_user_attributes sua ON es.shiro_user_id = sua.shiro_user_id
WHERE es.event_date >= '2018-01-01'
GROUP BY 1, 2, 3, 4;


SELECT se.data.member_recency_status(sua.signup_tstamp, eo.event_date::TIMESTAMP) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)      AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)     AS original_affiliate_territory,
       eo.event_date                                                              AS open_date,
       COUNT(*)                                                                   AS opens
FROM se.data.crm_events_opens eo
         INNER JOIN se.data.se_user_attributes sua ON eo.shiro_user_id = sua.shiro_user_id
WHERE eo.event_date >= '2018-01-01'
GROUP BY 1, 2, 3, 4;


SELECT se.data.member_recency_status(sua.signup_tstamp, ec.event_date::TIMESTAMP) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)      AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)     AS original_affiliate_territory,
       ec.event_date                                                              AS click_date,
       COUNT(*)                                                                   AS clicks
FROM se.data.crm_events_clicks ec
         INNER JOIN se.data.se_user_attributes sua ON ec.shiro_user_id = sua.shiro_user_id
WHERE ec.event_date >= '2018-01-01'
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------
--wau

SELECT se.data.member_recency_status(sua.signup_tstamp, ua.date::TIMESTAMP)   AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       ua.date,
       COUNT(*)                                                               AS wau,
       SUM(IFF(ua.app_sessions_7d > 0, 1, 0))                                 AS app_wau,
       SUM(IFF(ua.web_sessions_7d > 0, 1, 0))                                 AS web_wau
FROM se.data.user_activity ua
         INNER JOIN se.data.se_user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
WHERE ua.date >= '2018-01-01'
  AND (ua.app_sessions_7d > 0
    OR ua.web_sessions_7d > 0)
GROUP BY 1, 2, 3, 4


SELECT se.data.member_recency_status(sua.signup_tstamp, ua.date::TIMESTAMP)   AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       ua.date,
       SUM(IFF(ua.app_sessions_7d > 0 OR ua.web_sessions_7d > 0, 1, 0))     AS wau,
       SUM(IFF(ua.app_sessions_7d > 0, 1, 0))                                AS app_wau,
       SUM(IFF(ua.web_sessions_7d > 0, 1, 0))                                AS web_wau,
       SUM(IFF(ua.emails_7d > 0, 1, 0))                                      AS email_wau
FROM se.data.user_activity ua
         INNER JOIN se.data.se_user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
WHERE ua.date >= '2018-01-01'
  AND (ua.app_sessions_7d > 0
    OR ua.web_sessions_7d > 0
    OR ua.emails_7d > 0)
GROUP BY 1, 2, 3, 4

--mau

SELECT se.data.member_recency_status(sua.signup_tstamp, ua.date::TIMESTAMP)   AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       ua.date,
       SUM(IFF(ua.app_sessions_30d > 0 OR ua.web_sessions_30d > 0, 1, 0))     AS mau,
       SUM(IFF(ua.app_sessions_30d > 0, 1, 0))                                AS app_mau,
       SUM(IFF(ua.web_sessions_30d > 0, 1, 0))                                AS web_mau,
       SUM(IFF(ua.emails_30d > 0, 1, 0))                                      AS email_mau
FROM se.data.user_activity ua
         INNER JOIN se.data.se_user_attributes sua ON ua.shiro_user_id = sua.shiro_user_id
WHERE ua.date >= '2018-01-01'
  AND (ua.app_sessions_30d > 0
    OR ua.web_sessions_30d > 0
    OR ua.emails_30d > 0)
GROUP BY 1, 2, 3, 4


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

--  SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
--         se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
--         se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
--         stba.touch_start_tstamp::DATE                                             AS date,
--         se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
--         stba.touch_experience,
--         se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
--         se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
--         d.product_configuration,
--         d.travel_type,
--         COUNT(DISTINCT stba.touch_id)                                             AS sessions,
--         COUNT(DISTINCT stba.attributed_user_id)                                   AS users
--  FROM se.data_pii.scv_touch_basic_attributes stba
--           INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
--           INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
--           LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
--           LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
--  WHERE stba.touch_start_tstamp >= '2018-01-01'
--    AND stba.stitched_identity_type = 'se_user_id'
--  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10


SELECT DATE_TRUNC(WEEK, stba.touch_start_tstamp::DATE)                           AS date,
       se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
         LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
         LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
WHERE stba.touch_start_tstamp >= '2020-11-16'
  AND stba.stitched_identity_type = 'se_user_id'
  AND posa_category = 'UK'
GROUP BY 1, 2, 3;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.demand_model_grain AS
WITH activity AS (
    SELECT DISTINCT
           ua.shiro_user_id,
           ua.date
    FROM se.data.user_activity ua
    WHERE ua.web_sessions_1d > 0
       OR ua.app_sessions_1d > 0
       OR ua.emails_1d > 0
        AND ua.date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           fcb.shiro_user_id,
           fcb.booking_completed_date
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           es.shiro_user_id,
           es.event_date
    FROM se.data.crm_events_sends es
    WHERE es.event_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           eo.shiro_user_id,
           eo.event_date
    FROM se.data.crm_events_opens eo
    WHERE eo.event_date >= '2018-01-01'

    UNION

    SELECT DISTINCT
           ec.shiro_user_id,
           ec.event_date
    FROM se.data.crm_events_clicks ec
    WHERE ec.event_date >= '2018-01-01'
)
SELECT se.data.member_recency_status(sua.signup_tstamp, a.date::TIMESTAMP)    AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       a.date
FROM activity a
         INNER JOIN se.data.se_user_attributes sua ON a.shiro_user_id = sua.shiro_user_id
WHERE a.date < CURRENT_DATE
GROUP BY 1, 2, 3, 4;


WITH scv AS (
    WITH bookings AS (
        SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
               se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
               se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
               fcb.booking_completed_date::DATE                                             AS date,
               se.data.channel_category(stmc.touch_mkt_channel)                             AS channel, -- last click channel
               stba.touch_experience,
               se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
               se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
               ds.product_configuration,
               ds.travel_type,
               COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
               SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
               SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
               SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
               SUM(fcb.no_nights)                                                           AS no_nights,
               SUM(fcb.rooms)                                                               AS rooms
        FROM se.data.fact_complete_booking fcb
                 INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
                 INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
                 INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                            ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                 INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                 INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
        WHERE fcb.booking_completed_date >= '2020-11-01'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
         spvs AS (
             SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)     AS member_recency_status,
                    se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
                    se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
                    sts.event_tstamp::DATE                                                 AS date,
                    se.data.channel_category(stmc.touch_mkt_channel)                       AS channel, -- last click channel
                    stba.touch_experience,
                    se.data.platform_from_touch_experience(stba.touch_experience)          AS platform,
                    se.data.posa_category_from_territory(stmc.touch_affiliate_territory)   AS posa_category,
                    ds.product_configuration,
                    ds.travel_type,
                    COUNT(DISTINCT sts.event_hash)                                         AS spvs
             FROM se.data.scv_touched_spvs sts
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                      INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                                 ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                      INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                      INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
             WHERE sts.event_tstamp >= '2020-11-01'
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
         ),
         sessions AS (
             SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
                    se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
                    se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
                    stba.touch_start_tstamp::DATE                                             AS date,
                    se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
                    stba.touch_experience,
                    se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
                    se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
                    d.product_configuration,
                    d.travel_type,
                    COUNT(DISTINCT stba.touch_id)                                             AS sessions,
                    COUNT(DISTINCT stba.attributed_user_id)                                   AS users
             FROM se.data_pii.scv_touch_basic_attributes stba
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                      INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                      LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                      LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
             WHERE stba.touch_start_tstamp >= '2020-11-01'
               AND stba.stitched_identity_type = 'se_user_id'
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
         )

    SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
           COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                    ss.current_affiliate_territory)                                             AS current_affiliate_territory,
           COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                    ss.original_affiliate_territory)                                            AS original_affiliate_territory,
           COALESCE(b.date, s.date, ss.date)                                                    AS date,
           COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
           INITCAP(
                   COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
           COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
           COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
           COALESCE(b.product_configuration, s.product_configuration,
                    ss.product_configuration)                                                   AS product_configuration,
           COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
           COALESCE(b.bookings, 0)                                                              AS bookings,
           COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
           COALESCE(b.no_nights, 0)                                                             AS no_nights,
           COALESCE(b.rooms, 0)                                                                 AS rooms,
           COALESCE(s.spvs, 0)                                                                  AS spvs,
           COALESCE(ss.sessions, 0)                                                             AS sessions,
           COALESCE(ss.users, 0)                                                                AS users
    FROM bookings b
             FULL OUTER JOIN spvs s ON
            b.member_recency_status = s.member_recency_status AND
            b.current_affiliate_territory = s.current_affiliate_territory AND
            b.original_affiliate_territory = s.original_affiliate_territory AND
            b.date = s.date AND
            b.channel = s.channel AND
            b.touch_experience = s.touch_experience AND
            b.platform = s.platform AND
            b.posa_category = s.posa_category AND
            b.product_configuration = s.product_configuration AND
            b.travel_type = s.travel_type
             FULL OUTER JOIN sessions ss ON
                COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
                COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
                COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
                COALESCE(b.date, s.date) = ss.date AND
                COALESCE(b.channel, s.channel) = ss.channel AND
                COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
                COALESCE(b.platform, s.platform) = ss.platform AND
                COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
                COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
                COALESCE(b.travel_type, s.travel_type) = ss.travel_type
)
SELECT *
FROM scratch.robinpatel.demand_model_grain dmg
         LEFT JOIN scv s ON dmg.date = s.date
    AND dmg.member_recency_status = s.member_recency_status
    AND dmg.current_affiliate_territory = s.current_affiliate_territory
    AND dmg.original_affiliate_territory = s.original_affiliate_territory
WHERE dmg.date >= '2020-11-16'
;


WITH bookings AS (
    SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
           se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
           se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                             AS date,
           se.data.channel_category(stmc.touch_mkt_channel)                             AS channel, -- last click channel
           stba.touch_experience,
           se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
           se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
           ds.product_configuration,
           ds.travel_type,
           COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                           AS no_nights,
           SUM(fcb.rooms)                                                               AS rooms
    FROM se.data.fact_complete_booking fcb
             INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                        ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
             INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
             INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE fcb.booking_completed_date >= '2020-11-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
     spvs AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)     AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
                sts.event_tstamp::DATE                                                 AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                       AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)          AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)   AS posa_category,
                ds.product_configuration,
                ds.travel_type,
                COUNT(DISTINCT sts.event_hash)                                         AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                             ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2020-11-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                             AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
                d.product_configuration,
                d.travel_type,
                COUNT(DISTINCT stba.touch_id)                                             AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                   AS users
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                  LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
         WHERE stba.touch_start_tstamp >= '2020-11-01'
           AND stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     )

SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
       COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                ss.current_affiliate_territory)                                             AS current_affiliate_territory,
       COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                ss.original_affiliate_territory)                                            AS original_affiliate_territory,
       COALESCE(b.date, s.date, ss.date)                                                    AS date,
       COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
       INITCAP(
               COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
       COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
       COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
       COALESCE(b.product_configuration, s.product_configuration,
                ss.product_configuration)                                                   AS product_configuration,
       COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
       COALESCE(b.bookings, 0)                                                              AS bookings,
       COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
       COALESCE(b.no_nights, 0)                                                             AS no_nights,
       COALESCE(b.rooms, 0)                                                                 AS rooms,
       COALESCE(s.spvs, 0)                                                                  AS spvs,
       COALESCE(ss.sessions, 0)                                                             AS sessions,
       COALESCE(ss.users, 0)                                                                AS users
FROM bookings b
         FULL OUTER JOIN spvs s ON
        b.member_recency_status = s.member_recency_status AND
        b.current_affiliate_territory = s.current_affiliate_territory AND
        b.original_affiliate_territory = s.original_affiliate_territory AND
        b.date = s.date AND
        b.channel = s.channel AND
        b.touch_experience = s.touch_experience AND
        b.platform = s.platform AND
        b.posa_category = s.posa_category AND
        b.product_configuration = s.product_configuration AND
        b.travel_type = s.travel_type
         FULL OUTER JOIN sessions ss ON
            COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
            COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
            COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
            COALESCE(b.date, s.date) = ss.date AND
            COALESCE(b.channel, s.channel) = ss.channel AND
            COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
            COALESCE(b.platform, s.platform) = ss.platform AND
            COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
            COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
            COALESCE(b.travel_type, s.travel_type) = ss.travel_type


SELECT dse.date,
       SUM(dse.sessions)
FROM scratch.robinpatel.demand_scv_extract dse
GROUP BY 1;


SELECT dmg.date,
       SUM(dse.sessions)
FROM scratch.robinpatel.demand_model_grain dmg
         LEFT JOIN scratch.robinpatel.demand_scv_extract dse ON dmg.date = dse.date
    AND dmg.member_recency_status = dse.member_recency_status
    AND dmg.original_affiliate_territory = dse.original_affiliate_territory
    AND dmg.current_affiliate_territory = dse.current_affiliate_territory
WHERE dmg.date >= '2020-11-16'
GROUP BY 1;



SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
       stba.touch_experience,
       se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.touch_start_tstamp >= '2020-11-01'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


SELECT DISTINCT fb.tech_platform
FROM se.data.fact_booking fb
WHERE fb.booking_status_type IN ('live', 'cancelled')



SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
       stba.touch_experience,
       se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

USE WAREHOUSE pipe_xlarge;

SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       se.data.channel_category(stmc.touch_mkt_channel)                          AS channel, -- last click channel
       INITCAP(stba.touch_experience)                                            AS touch_experience,
       INITCAP(se.data.platform_from_touch_experience(stba.touch_experience))    AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8



WITH bookings AS (
    SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
           se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
           se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                             AS date,
           se.data.channel_category(stmc.touch_mkt_channel)                             AS channel, -- last click channel
           stba.touch_experience,
           se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
           se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
           ds.product_configuration,
           se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
           COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                           AS no_nights,
           SUM(fcb.rooms)                                                               AS rooms
    FROM se.data.fact_complete_booking fcb
             INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                        ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
             INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
             INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE fcb.booking_completed_date >= '2020-11-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
     spvs AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)           AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
                sts.event_tstamp::DATE                                                       AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                             AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
                ds.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
                COUNT(DISTINCT sts.event_hash)                                               AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                             ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2020-11-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)   AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)       AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)      AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                               AS date,
                se.data.channel_category(stmc.touch_mkt_channel)                            AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)               AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)        AS posa_category,
                d.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, d.posu_country) AS travel_type,
                COUNT(DISTINCT stba.touch_id)                                               AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                     AS users
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                  LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
         WHERE stba.touch_start_tstamp >= '2020-11-01'
           AND stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     )

SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
       COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                ss.current_affiliate_territory)                                             AS current_affiliate_territory,
       COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                ss.original_affiliate_territory)                                            AS original_affiliate_territory,
       COALESCE(b.date, s.date, ss.date)                                                    AS date,
       COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
       INITCAP(
               COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
       COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
       COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
       COALESCE(b.product_configuration, s.product_configuration,
                ss.product_configuration)                                                   AS product_configuration,
       COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
       COALESCE(b.bookings, 0)                                                              AS bookings,
       COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
       COALESCE(b.no_nights, 0)                                                             AS no_nights,
       COALESCE(b.rooms, 0)                                                                 AS rooms,
       COALESCE(s.spvs, 0)                                                                  AS spvs,
       COALESCE(ss.sessions, 0)                                                             AS sessions,
       COALESCE(ss.users, 0)                                                                AS users
FROM bookings b
         FULL OUTER JOIN spvs s ON
        b.member_recency_status = s.member_recency_status AND
        b.current_affiliate_territory = s.current_affiliate_territory AND
        b.original_affiliate_territory = s.original_affiliate_territory AND
        b.date = s.date AND
        b.channel = s.channel AND
        b.touch_experience = s.touch_experience AND
        b.platform = s.platform AND
        b.posa_category = s.posa_category AND
        b.product_configuration = s.product_configuration AND
        b.travel_type = s.travel_type
         FULL OUTER JOIN sessions ss ON
            COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
            COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
            COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
            COALESCE(b.date, s.date) = ss.date AND
            COALESCE(b.channel, s.channel) = ss.channel AND
            COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
            COALESCE(b.platform, s.platform) = ss.platform AND
            COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
            COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
            COALESCE(b.travel_type, s.travel_type) = ss.travel_type

------------------------------------------------------------------------------------------------------------------------

SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       stmc.touch_mkt_channel                                                    AS channel, -- last click channel
       INITCAP(stba.touch_experience)                                            AS touch_experience,
       INITCAP(se.data.platform_from_touch_experience(stba.touch_experience))    AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


------------------------------------------------------------------------------------------------------------------------
SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp) AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)     AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory)    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                             AS date,
       stmc.touch_mkt_channel                                                    AS channel, -- last click channel
       INITCAP(stba.touch_experience)                                            AS touch_experience,
       se.data.platform_from_touch_experience(stba.touch_experience)             AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)      AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                             AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                   AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
         INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2019-01-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8



WITH bookings AS (
    SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
           se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
           se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                             AS date,
           stmc.touch_mkt_channel                                                       AS channel, -- last click channel
           stba.touch_experience,
           se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
           se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
           ds.product_configuration,
           se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
           COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                           AS no_nights,
           SUM(fcb.rooms)                                                               AS rooms
    FROM se.data.fact_complete_booking fcb
             INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
             INNER JOIN se.data.scv_touch_attribution sta
                        ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                        ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
             INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
             INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE fcb.booking_completed_date >= '2018-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
     spvs AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)           AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
                sts.event_tstamp::DATE                                                       AS date,
                stmc.touch_mkt_channel                                                       AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
                ds.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
                COUNT(DISTINCT sts.event_hash)                                               AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_attribution sta
                             ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                             ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)   AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)       AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)      AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                               AS date,
                stmc.touch_mkt_channel                                                      AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)               AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)        AS posa_category,
                d.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, d.posu_country) AS travel_type,
                COUNT(DISTINCT stba.touch_id)                                               AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                     AS users
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_attribution sta
                             ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                  LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
         WHERE stba.touch_start_tstamp >= '2018-01-01'
           AND stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     )

SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
       COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                ss.current_affiliate_territory)                                             AS current_affiliate_territory,
       COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                ss.original_affiliate_territory)                                            AS original_affiliate_territory,
       COALESCE(b.date, s.date, ss.date)                                                    AS date,
       COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
       INITCAP(
               COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
       COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
       COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
       COALESCE(b.product_configuration, s.product_configuration,
                ss.product_configuration)                                                   AS product_configuration,
       COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
       COALESCE(b.bookings, 0)                                                              AS bookings,
       COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
       COALESCE(b.no_nights, 0)                                                             AS no_nights,
       COALESCE(b.rooms, 0)                                                                 AS rooms,
       COALESCE(s.spvs, 0)                                                                  AS spvs,
       COALESCE(ss.sessions, 0)                                                             AS sessions,
       COALESCE(ss.users, 0)                                                                AS users
FROM bookings b
         FULL OUTER JOIN spvs s ON
        b.member_recency_status = s.member_recency_status AND
        b.current_affiliate_territory = s.current_affiliate_territory AND
        b.original_affiliate_territory = s.original_affiliate_territory AND
        b.date = s.date AND
        b.channel = s.channel AND
        b.touch_experience = s.touch_experience AND
        b.platform = s.platform AND
        b.posa_category = s.posa_category AND
        b.product_configuration = s.product_configuration AND
        b.travel_type = s.travel_type
         FULL OUTER JOIN sessions ss ON
            COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
            COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
            COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
            COALESCE(b.date, s.date) = ss.date AND
            COALESCE(b.channel, s.channel) = ss.channel AND
            COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
            COALESCE(b.platform, s.platform) = ss.platform AND
            COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
            COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
            COALESCE(b.travel_type, s.travel_type) = ss.travel_type

------------------------------------------------------------------------------------------------------------------------

WITH bookings AS (
    SELECT se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date) AS member_recency_status,
           se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
           se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                             AS date,
           stmc.touch_mkt_channel                                                       AS channel, -- last click channel
           stba.touch_experience,
           se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
           se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
           ds.product_configuration,
           se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
           COUNT(DISTINCT fcb.booking_id)                                               AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                          AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                          AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                            AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                           AS no_nights,
           SUM(fcb.rooms)                                                               AS rooms
    FROM se.data.fact_complete_booking fcb
             INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
             INNER JOIN se.data.scv_touch_attribution sta
                        ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                        ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
             INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
             INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE fcb.booking_completed_date >= '2018-01-01'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
     spvs AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp)           AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)        AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)       AS original_affiliate_territory,
                sts.event_tstamp::DATE                                                       AS date,
                stmc.touch_mkt_channel                                                       AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)                AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)         AS posa_category,
                ds.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, ds.posu_country) AS travel_type,
                COUNT(DISTINCT sts.event_hash)                                               AS spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_attribution sta
                             ON sts.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
                             ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)   AS member_recency_status,
                se.data.posa_category_from_territory(sua.current_affiliate_territory)       AS current_affiliate_territory,
                se.data.posa_category_from_territory(sua.original_affiliate_territory)      AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                               AS date,
                stmc.touch_mkt_channel                                                      AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)               AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)        AS posa_category,
                d.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, d.posu_country) AS travel_type,
                COUNT(DISTINCT stba.touch_id)                                               AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                     AS users
         FROM se.data_pii.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_attribution sta
                             ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
                  INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  LEFT JOIN se.data.scv_touched_spvs ts ON stba.touch_id = ts.touch_id
                  LEFT JOIN se.data.dim_sale d ON ts.se_sale_id = d.se_sale_id
         WHERE stba.touch_start_tstamp >= '2018-01-01'
           AND stba.stitched_identity_type = 'se_user_id'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     )

SELECT COALESCE(b.member_recency_status, s.member_recency_status, ss.member_recency_status) AS member_recency_status,
       COALESCE(b.current_affiliate_territory, s.current_affiliate_territory,
                ss.current_affiliate_territory)                                             AS current_affiliate_territory,
       COALESCE(b.original_affiliate_territory, s.original_affiliate_territory,
                ss.original_affiliate_territory)                                            AS original_affiliate_territory,
       COALESCE(b.date, s.date, ss.date)                                                    AS date,
       COALESCE(b.channel, s.channel, ss.channel)                                           AS channel,
       INITCAP(COALESCE(b.touch_experience, s.touch_experience, ss.touch_experience))       AS touch_experience,
       COALESCE(b.platform, s.platform, ss.platform)                                        AS platform,
       COALESCE(b.posa_category, s.posa_category, ss.posa_category)                         AS posa_category,
       COALESCE(b.product_configuration, s.product_configuration,
                ss.product_configuration)                                                   AS product_configuration,
       COALESCE(b.travel_type, s.travel_type, ss.travel_type)                               AS travel_type,
       COALESCE(b.bookings, 0)                                                              AS bookings,
       COALESCE(b.margin_gbp_constant_currency, 0)                                          AS margin_gbp_constant_currency,
       COALESCE(b.no_nights, 0)                                                             AS no_nights,
       COALESCE(b.rooms, 0)                                                                 AS rooms,
       COALESCE(s.spvs, 0)                                                                  AS spvs,
       COALESCE(ss.sessions, 0)                                                             AS sessions,
       COALESCE(ss.users, 0)                                                                AS users
FROM bookings b
         FULL OUTER JOIN spvs s ON
        b.member_recency_status = s.member_recency_status AND
        b.current_affiliate_territory = s.current_affiliate_territory AND
        b.original_affiliate_territory = s.original_affiliate_territory AND
        b.date = s.date AND
        b.channel = s.channel AND
        b.touch_experience = s.touch_experience AND
        b.platform = s.platform AND
        b.posa_category = s.posa_category AND
        b.product_configuration = s.product_configuration AND
        b.travel_type = s.travel_type
         FULL OUTER JOIN sessions ss ON
            COALESCE(b.member_recency_status, s.member_recency_status) = ss.member_recency_status AND
            COALESCE(b.current_affiliate_territory, s.current_affiliate_territory) = ss.current_affiliate_territory AND
            COALESCE(b.original_affiliate_territory, s.original_affiliate_territory) = ss.original_affiliate_territory AND
            COALESCE(b.date, s.date) = ss.date AND
            COALESCE(b.channel, s.channel) = ss.channel AND
            COALESCE(b.touch_experience, s.touch_experience) = ss.touch_experience AND
            COALESCE(b.platform, s.platform) = ss.platform AND
            COALESCE(b.posa_category, s.posa_category) = ss.posa_category AND
            COALESCE(b.product_configuration, s.product_configuration) = ss.product_configuration AND
            COALESCE(b.travel_type, s.travel_type) = ss.travel_type;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

SELECT DATE_TRUNC(MONTH, fcb.booking_completed_date)       AS month,
       COUNT(DISTINCT fcb.booking_id)                      AS bookings,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_constant_currency,
       SUM(fcb.margin_gross_of_toms_eur_constant_currency) AS margin_eur_constant_currency,
       SUM(fcb.margin_gross_of_toms_gbp)                   AS margin_gbp_reporting_currency,
       SUM(fcb.no_nights)                                  AS no_nights,
       SUM(fcb.rooms)                                      AS rooms
FROM se.data.fact_complete_booking fcb
--          INNER JOIN se.data.scv_touched_transactions stt ON fcb.booking_id = stt.booking_id
--          INNER JOIN se.data.scv_touch_attribution sta
--                     ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
--          INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
--          INNER JOIN se.data_pii.scv_touch_basic_attributes stba -- only sessions that can be attributed to a user
--                     ON stt.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
--          INNER JOIN se.data.se_user_attributes sua ON fcb.shiro_user_id = sua.shiro_user_id
         INNER JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2021-02-01'
GROUP BY 1

