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
),
     unknown_users AS (
         --add additional grain of unknown for non member activity on any date
         SELECT 'unknown' AS member_recency_status,
                'unknown' AS current_affiliate_territory,
                'unknown' AS original_affiliate_territory,
                sc.date_value
         FROM se.data.se_calendar sc
         WHERE sc.date_value BETWEEN '2018-01-01' AND CURRENT_DATE
     )

SELECT se.data.member_recency_status(sua.signup_tstamp, a.date::TIMESTAMP)    AS member_recency_status,
       se.data.posa_category_from_territory(sua.current_affiliate_territory)  AS current_affiliate_territory,
       se.data.posa_category_from_territory(sua.original_affiliate_territory) AS original_affiliate_territory,
       a.date
FROM activity a
         INNER JOIN se.data.se_user_attributes sua ON a.shiro_user_id = sua.shiro_user_id
WHERE a.date < CURRENT_DATE
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT uu.member_recency_status,
       uu.current_affiliate_territory,
       uu.original_affiliate_territory,
       uu.date_value
FROM unknown_users uu
;

--session grain

SELECT IFF(sua.shiro_user_id IS NULL, 'unknown',
           se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)) AS member_recency_status,
       IFF(sua.shiro_user_id IS NULL, 'unknown',
           se.data.posa_category_from_territory(sua.current_affiliate_territory))     AS current_affiliate_territory,
       IFF(sua.shiro_user_id IS NULL, 'unknown',
           se.data.posa_category_from_territory(sua.original_affiliate_territory))    AS original_affiliate_territory,
       stba.touch_start_tstamp::DATE                                                  AS date,
       stmc.touch_mkt_channel                                                         AS channel, -- last click channel
       INITCAP(stba.touch_experience)                                                 AS touch_experience,
       se.data.platform_from_touch_experience(stba.touch_experience)                  AS platform,
       se.data.posa_category_from_territory(stmc.touch_affiliate_territory)           AS posa_category,
       COUNT(DISTINCT stba.touch_id)                                                  AS sessions,
       COUNT(DISTINCT stba.attributed_user_id)                                        AS users
FROM se.data_pii.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
         LEFT JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
WHERE stba.touch_start_tstamp >= '2021-03-31'
GROUP BY 1,2,3,4,5,6,7,8

UNION ALL

SELECT 'unknown'                                                   AS member_recency_status,
       'unknown'                                                   AS current_affiliate_territory,
       'unknown'                                                   AS original_affiliate_territory,
       fcb.booking_completed_date::DATE                            AS date,
       'unknown'                                                   AS channel, -- last click channel
       fcb.device_platform                                         AS touch_experience,
       se.data.platform_from_touch_experience(fcb.device_platform) AS platform,
       se.data.posa_category_from_territory(fcb.territory)         AS posa_category,
       NULL                                                        AS sessions,
       NULL                                                        AS users
FROM se.data.fact_complete_booking fcb
         INNER JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id


------------------------------------------------------------------------------------------------------------------------
--event grain

WITH bookings AS (
    SELECT IFF(sua.shiro_user_id IS NULL, 'unknown',
               se.data.member_recency_status(sua.signup_tstamp, fcb.booking_completed_date))             AS member_recency_status,
           IFF(sua.shiro_user_id IS NULL, 'unknown',
               se.data.posa_category_from_territory(sua.current_affiliate_territory))                    AS current_affiliate_territory,
           IFF(sua.shiro_user_id IS NULL, 'unknown',
               se.data.posa_category_from_territory(sua.original_affiliate_territory))                   AS original_affiliate_territory,
           fcb.booking_completed_date::DATE                                                              AS date,
           IFF(stt.touch_id IS NULL, 'unknown', stmc.touch_mkt_channel)                                  AS channel, -- last click channel
           COALESCE(stba.touch_experience, fcb.device_platform)                                          AS touch_experience,
           se.data.platform_from_touch_experience(COALESCE(stba.touch_experience, fcb.device_platform))  AS platform,
           se.data.posa_category_from_territory(COALESCE(stmc.touch_affiliate_territory, fcb.territory)) AS posa_category,
           ds.product_configuration,
           se.data.se_sale_travel_type(COALESCE(stmc.touch_affiliate_territory, fcb.territory),
                                       ds.posu_country)                                                  AS travel_type,
           COUNT(DISTINCT fcb.booking_id)                                                                AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency)                                           AS margin_gbp_constant_currency,
           SUM(fcb.margin_gross_of_toms_eur_constant_currency)                                           AS margin_eur_constant_currency,
           SUM(fcb.margin_gross_of_toms_gbp)                                                             AS margin_gbp_reporting_currency,
           SUM(fcb.no_nights)                                                                            AS no_nights,
           SUM(fcb.rooms)                                                                                AS rooms
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
         SELECT IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.member_recency_status(sua.signup_tstamp, sts.event_tstamp))      AS member_recency_status,
                IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.posa_category_from_territory(sua.current_affiliate_territory))   AS current_affiliate_territory,
                IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.posa_category_from_territory(sua.original_affiliate_territory))  AS original_affiliate_territory,
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
                             ON sts.touch_id = stba.touch_id
                  LEFT JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
                  INNER JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
         WHERE sts.event_tstamp >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
     ),
     sessions AS (
         SELECT IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.member_recency_status(sua.signup_tstamp, stba.touch_start_tstamp)) AS member_recency_status,
                IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.posa_category_from_territory(sua.current_affiliate_territory))     AS current_affiliate_territory,
                IFF(sua.shiro_user_id IS NULL, 'unknown',
                    se.data.posa_category_from_territory(sua.original_affiliate_territory))    AS original_affiliate_territory,
                stba.touch_start_tstamp::DATE                                                  AS date,
                stmc.touch_mkt_channel                                                         AS channel, -- last click channel
                stba.touch_experience,
                se.data.platform_from_touch_experience(stba.touch_experience)                  AS platform,
                se.data.posa_category_from_territory(stmc.touch_affiliate_territory)           AS posa_category,
                d.product_configuration,
                se.data.se_sale_travel_type(stmc.touch_affiliate_territory, d.posu_country)    AS travel_type,
                COUNT(DISTINCT stba.touch_id)                                                  AS sessions,
                COUNT(DISTINCT stba.attributed_user_id)                                        AS users
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