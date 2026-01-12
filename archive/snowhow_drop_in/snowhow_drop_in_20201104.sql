SET from_date = '2020-11-01';
USE WAREHOUSE pipe_xlarge;
WITH canx_bookings AS (

    SELECT sb.cancellation_date::DATE                                                                     AS canx_date,
           stmc.touch_mkt_channel,
           stba.touch_experience,
           stmc.touch_affiliate_territory                                                                 AS touch_affiliate_territory,
           count(*)                                                                                       AS bookings,
           SUM(sb.margin_gross_of_toms_gbp)                                                               AS margin,
           SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN 1 ELSE 0 END)                    AS ho_bookings,
           SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN sb.margin_gross_of_toms_gbp END) AS ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                        posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN 1
                   ELSE 0 END)                                                                            AS uk_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                        posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN sb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                            AS uk_ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                       THEN 1
                   ELSE 0 END)                                                                            AS dach_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                       THEN sb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                            AS dach_ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN 1
                   ELSE 0 END)                                                                            AS it_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN sb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                            AS it_ho_margin,
           SUM(CASE WHEN ds.product_type = 'Package' THEN 1 ELSE 0 END)                                   AS p_bookings,
           SUM(CASE WHEN ds.product_type = 'Package' THEN sb.margin_gross_of_toms_gbp ELSE 0 END)         AS p_margin,
           SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN 1 ELSE 0 END)                              AS "3pp_bookings",
           SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN sb.margin_gross_of_toms_gbp ELSE 0 END)    AS "3pp_margin"


    FROM se.data.scv_touched_transactions stt
             INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = stt.touch_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
             INNER JOIN se.data.se_booking sb ON stt.booking_id = sb.booking_id
             LEFT JOIN se.data.dim_sale ds ON sb.sale_id = ds.se_sale_id
    WHERE sb.booking_status IN ('REFUNDED')
      AND sb.cancellation_date::DATE >= $from_date--)
    GROUP BY 1, 2, 3, 4
),

     live_bookings_table AS (

         SELECT sb.booking_completed_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory                                                                AS touch_affiliate_territory,

                count(*)                                                                                      AS bookings,
                SUM(sb.margin_gross_of_toms_gbp)                                                              AS margin,
                SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN 1 ELSE 0 END)                   AS ho_bookings,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN sb.margin_gross_of_toms_gbp END) AS ho_margin,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                             posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN 1
                        ELSE 0 END)                                                                           AS uk_ho_bookings,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                             posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN sb.margin_gross_of_toms_gbp
                        ELSE 0 END)                                                                           AS uk_ho_margin,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                            THEN 1
                        ELSE 0 END)                                                                           AS dach_ho_bookings,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                            THEN sb.margin_gross_of_toms_gbp
                        ELSE 0 END)                                                                           AS dach_ho_margin,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN 1
                        ELSE 0 END)                                                                           AS it_ho_bookings,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy')
                            THEN sb.margin_gross_of_toms_gbp
                        ELSE 0 END)                                                                           AS it_ho_margin,
                SUM(CASE WHEN ds.product_type = 'Package' THEN 1 ELSE 0 END)                                  AS p_bookings,
                SUM(CASE WHEN ds.product_type = 'Package' THEN sb.margin_gross_of_toms_gbp ELSE 0 END)        AS p_margin,
                SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN 1 ELSE 0 END)                             AS "3pp_bookings",
                SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN sb.margin_gross_of_toms_gbp ELSE 0 END)   AS "3pp_margin"


         FROM se.data.scv_touched_transactions stt
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = stt.touch_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  INNER JOIN se.data.fact_complete_booking sb ON stt.booking_id = sb.booking_id
                  LEFT JOIN se.data.dim_sale ds ON sb.sale_id = ds.se_sale_id
         WHERE sb.booking_completed_date::DATE >= $from_date


         GROUP BY 1, 2, 3, 4
     ),

     sess_spvs
         AS (
         SELECT s.event_tstamp::DATE                                                        AS event_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory,
                COUNT(*)                                                                    AS spvs,
                COUNT(DISTINCT s.se_sale_id)                                                AS unique_spvs,
                SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN 1 ELSE 0 END) AS ho_spvs,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                             posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN 1
                        ELSE 0 END)                                                         AS uk_ho_spvs,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                            THEN 1
                        ELSE 0 END)                                                         AS dach_ho_spvs,
                SUM(CASE
                        WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN 1
                        ELSE 0 END)                                                         AS it_ho_spvs,
                SUM(CASE WHEN ds.product_type = 'Package' THEN 1 ELSE 0 END)                AS p_spvs,
                SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN 1 ELSE 0 END)           AS "3pp_spvs"

         FROM se.data.scv_touched_spvs s
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = s.touch_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
         WHERE s.event_tstamp::DATE >= $from_date
         GROUP BY 1, 2, 3, 4
     ),
     basic_att
         AS (
         SELECT stba.touch_start_tstamp::DATE                                 AS event_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory,
                COUNT(DISTINCT stba.touch_id)                                 AS sessions,
                COUNT(DISTINCT stba.attributed_user_id_hash)                  AS users,
                COUNT(DISTINCT CASE
                                   WHEN stba.stitched_identity_type = 'se_user_id'
                                       THEN stba.attributed_user_id_hash END) AS logged_in_users
         FROM se.data.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         WHERE stba.touch_start_tstamp::DATE >= $from_date
           AND stba.touch_start_tstamp::DATE < current_date
         GROUP BY 1, 2, 3, 4
     )


SELECT ba.event_date,
       ba.touch_mkt_channel,
       ba.touch_experience,
       ba.touch_affiliate_territory,
       coalesce(ba.sessions, 0)        AS sessions,
       coalesce(ba.users, 0)           AS users,
       coalesce(ba.logged_in_users, 0) AS logged_in_users,
       coalesce(spv.spvs, 0)           AS spvs,
       coalesce(cx.bookings, 0)        AS cancelled_bookings,
       coalesce(li.bookings, 0)        AS new_bookings,
       coalesce(cx.margin, 0)          AS cancelled_margin,
       coalesce(li.margin, 0)          AS new_margin

FROM basic_att ba
         LEFT JOIN sess_spvs spv ON spv.event_date = ba.event_date
    AND spv.touch_mkt_channel = ba.touch_mkt_channel
    AND spv.touch_experience = ba.touch_experience
    AND spv.touch_affiliate_territory = ba.touch_affiliate_territory
         LEFT JOIN canx_bookings cx ON cx.canx_date = ba.event_date
    AND cx.touch_mkt_channel = ba.touch_mkt_channel
    AND cx.touch_experience = ba.touch_experience
    AND cx.touch_affiliate_territory = ba.touch_affiliate_territory

         LEFT JOIN live_bookings_table li ON li.booking_completed_date = ba.event_date
    AND li.touch_mkt_channel = ba.touch_mkt_channel
    AND li.touch_experience = ba.touch_experience
    AND li.touch_affiliate_territory = ba.touch_affiliate_territory


WHERE ba.touch_affiliate_territory IN ('UK', 'DE', 'IT');

--refactor to tidy
--canx not included in live bookings
--grain is not complete because based off of sessions
SET from_date = '2020-10-01';
WITH canx_bookings AS (
    --bookings that are only canx assigned to their cancellation date
    SELECT sb.cancellation_date::DATE       AS canx_date,
           stmc.touch_mkt_channel,
           stba.touch_experience,
           stmc.touch_affiliate_territory   AS touch_affiliate_territory,
           ds.product_type,
           ds.posu_country,
           count(*)                         AS canx_bookings,
           SUM(sb.margin_gross_of_toms_gbp) AS canx_margin

    FROM se.data.scv_touched_transactions stt
             INNER JOIN se.data.scv_touch_basic_attributes stba
                        ON stba.touch_id = stt.touch_id
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
             INNER JOIN se.data.se_booking sb ON stt.booking_id = sb.booking_id
             LEFT JOIN se.data.dim_sale ds ON sb.sale_id = ds.se_sale_id
    WHERE sb.booking_status IN ('REFUNDED')
      AND sb.cancellation_date::DATE >= $from_date
    GROUP BY 1, 2, 3, 4, 5, 6
),
     gross_bookings AS (
         --bookings that are live and canx
         SELECT sb.booking_completed_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory                                           AS touch_affiliate_territory,
                ds.product_type,
                ds.posu_country,
                count(*)                                                                 AS gross_of_canx_bookings,
                SUM(IFF(sb.booking_status = 'COMPLETE', 1, 0))                           AS live_bookings,
                SUM(sb.margin_gross_of_toms_gbp)                                         AS gross_of_canx_margin,
                SUM(IFF(sb.booking_status = 'COMPLETE', sb.margin_gross_of_toms_gbp, 0)) AS live_margin
         FROM se.data.scv_touched_transactions stt
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = stt.touch_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  INNER JOIN se.data.fact_complete_booking sb ON stt.booking_id = sb.booking_id
                  LEFT JOIN se.data.dim_sale ds ON sb.sale_id = ds.se_sale_id
         WHERE sb.booking_completed_date::DATE >= $from_date
           AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
           AND sb.booking_completed_date < current_date
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     sess_spvs AS (
         SELECT s.event_tstamp::DATE         AS event_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory,
                ds.product_type,
                ds.posu_country,
                COUNT(*)                     AS spvs,
                COUNT(DISTINCT s.se_sale_id) AS unique_spvs

         FROM se.data.scv_touched_spvs s
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = s.touch_id
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
                  LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
         WHERE s.event_tstamp::DATE >= $from_date
           AND s.event_tstamp < current_date
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     grain AS (
         --create a complete grain of date, channel, experience and territory
         SELECT COALESCE(ss.event_date, gb.booking_completed_date, cx.canx_date)        AS event_date,
                COALESCE(ss.touch_mkt_channel, gb.touch_mkt_channel,
                         cx.touch_mkt_channel)                                          AS touch_mkt_channel,
                COALESCE(ss.touch_experience, gb.touch_experience, cx.touch_experience) AS touch_experience,
                COALESCE(ss.touch_affiliate_territory, gb.touch_affiliate_territory,
                         cx.touch_affiliate_territory)                                  AS touch_affiliate_territory,
                SUM(ss.spvs)                                                            AS spvs,
                SUM(ss.unique_spvs)                                                     AS unique_spvs,
                --examples of how to pivot out spvs by product and posu, can be applied to gross bookings and canx bookings too
--        SUM(IFF(ss.product_type IN ('Hotel', 'Hotel Plus'), 1, 0))                                      AS ho_spvs,
--        SUM(IFF(ss.product_type IN ('Hotel', 'Hotel Plus')
--                    AND ss.posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland'), 1, 0)) AS uk_ho_spvs,
--        SUM(IFF(ss.product_type IN ('Hotel', 'Hotel Plus')
--                    AND ss.posu_country IN ('Germany', 'Austria', 'Switzerland'), 1, 0))                AS dach_ho_spvs,
--        SUM(IFF(ss.product_type IN ('Hotel', 'Hotel Plus') AND ss.posu_country IN ('Italy'), 1, 0))     AS it_ho_spvs,
--        SUM(IFF(ss.product_type = 'Package', 1, 0))                                                     AS p_spvs,
--        SUM(IFF(ss.product_type = 'WRD - direct', 1, 0))                                                AS "3pp_spvs",
                SUM(gb.gross_of_canx_bookings)                                          AS gross_of_canx_bookings,
                SUM(gb.live_bookings)                                                   AS live_bookings,
--        SUM(IFF(gb.product_type IN ('Hotel', 'Hotel Plus'), 1, 0))                                      AS ho_gross_of_canx_bookings,
--        SUM(IFF(gb.product_type IN ('Hotel', 'Hotel Plus')
--                    AND gb.posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland'), 1, 0)) AS uk_ho_gross_of_canx_bookings,
                SUM(gb.gross_of_canx_margin)                                            AS gross_of_canx_margin,
--        SUM(IFF(gb.product_type IN ('Hotel', 'Hotel Plus')
--                    AND gb.posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland'), gb.live_margin, 0)) AS uk_ho_gross_of_canx_margin,
                SUM(gb.live_margin)                                                     AS live_margin,
                SUM(canx_bookings)                                                      AS canx_bookings,
                SUM(canx_margin)                                                        AS canx_margin
         FROM sess_spvs ss
                  FULL OUTER JOIN gross_bookings gb ON ss.event_date = gb.booking_completed_date
             AND ss.touch_mkt_channel = gb.touch_mkt_channel
             AND ss.touch_experience = gb.touch_experience
             AND ss.touch_affiliate_territory = gb.touch_affiliate_territory
             AND ss.product_type = gb.product_type
             AND ss.posu_country = gb.posu_country
                  FULL OUTER JOIN canx_bookings cx ON ss.event_date = cx.canx_date
             AND COALESCE(ss.touch_mkt_channel, gb.touch_mkt_channel) = cx.touch_mkt_channel
             AND COALESCE(ss.touch_experience, gb.touch_experience) = cx.touch_experience
             AND COALESCE(ss.touch_affiliate_territory, gb.touch_affiliate_territory) = cx.touch_affiliate_territory
             AND COALESCE(ss.product_type, gb.product_type) = cx.product_type
             AND COALESCE(ss.posu_country, gb.posu_country) = cx.posu_country
         GROUP BY 1, 2, 3, 4
     ),
     session_metrics AS (
         SELECT stba.touch_start_tstamp::DATE                                                 AS event_date,
                stmc.touch_mkt_channel,
                stba.touch_experience,
                stmc.touch_affiliate_territory,
                COUNT(DISTINCT stba.touch_id)                                                 AS sessions,
                COUNT(DISTINCT stba.attributed_user_id_hash)                                  AS users,
                COUNT(DISTINCT IFF(stba.touch_logged_in, stba.attributed_user_id_hash, NULL)) AS logged_in_users
         FROM se.data.scv_touch_basic_attributes stba
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         WHERE stba.touch_start_tstamp::DATE >= $from_date
           AND stba.touch_start_tstamp::DATE < current_date
         GROUP BY 1, 2, 3, 4
     )
SELECT g.event_date,
       g.touch_mkt_channel,
       g.touch_experience,
       g.touch_affiliate_territory,
       sm.sessions,
       sm.users,
       sm.logged_in_users,
       g.spvs,
       g.unique_spvs,
       g.gross_of_canx_bookings,
       g.live_bookings,
       g.gross_of_canx_margin,
       g.live_margin,
       g.canx_bookings,
       g.canx_margin
FROM grain g
         LEFT JOIN session_metrics sm ON g.event_date = sm.event_date
    AND g.touch_mkt_channel = sm.touch_mkt_channel
    AND g.touch_experience = sm.touch_experience
    AND g.touch_affiliate_territory = sm.touch_affiliate_territory
WHERE g.touch_affiliate_territory IN ('UK', 'DE', 'IT')
;



--------------------------------------------------------------------------------------------------------------------------
--ashmita

WITH booking_list AS (
--create a list of bookings and compute the difference in days between this booking and the user's previous booking
    SELECT sb.shiro_user_id,
           sua.email,
           sb.booking_completed_date,
           sb.booking_status,
           LAG(sb.booking_completed_date)
               OVER (PARTITION BY sb.shiro_user_id ORDER BY sb.booking_completed_date) AS last_booking_date,
           LAG(sb.booking_status)
               OVER (PARTITION BY sb.shiro_user_id ORDER BY sb.booking_completed_date) AS last_booking_status,
           DATEDIFF(DAY, sb.booking_completed_date, last_booking_date)                 AS diff_days
    FROM se.data.se_booking sb
             LEFT JOIN se.data_pii.se_user_attributes sua ON sb.shiro_user_id = sua.shiro_user_id
    WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
      AND sb.booking_completed_date >= '2020-10-01'
      AND sb.check_in_date >= CURRENT_DATE
      AND sb.shiro_user_id IS NOT NULL
),
     user_list AS (
         --compute a list of users that match the require criteria
         SELECT DISTINCT
                bl.shiro_user_id,
                bl.email
         FROM booking_list bl
         WHERE bl.diff_days < 0
           AND bl.diff_days >= -2
           AND bl.booking_status = 'COMPLETE'
           AND bl.last_booking_status = 'REFUNDED'
     )
SELECT ul.shiro_user_id,
       ul.email,
       SUM(IFF(bl.booking_status = 'COMPLETE', 1, 0)) AS live_bookings,
       SUM(IFF(bl.booking_status = 'REFUNDED', 1, 0)) AS canx_bookings
FROM user_list ul
         LEFT JOIN booking_list bl ON ul.shiro_user_id = bl.shiro_user_id
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
SELECT stmc.touch_mkt_channel,
       stmc.touch_affiliate_territory,
       fcb.booking_completed_date,
       fcb.booking_id,
       fcb.margin_gross_of_toms_gbp,
       fcb.margin_gross_of_toms_gbp_constant_currency

FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
         INNER JOIN se.data.scv_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last paid'
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE DATE_TRUNC(MONTH, fcb.booking_completed_date) = '2020-10-01';


SELECT stmc.touch_mkt_channel,
       stmc.touch_affiliate_territory,
       stmc.touch_hostname_territory,
       stba.touch_hostname_territory,
       stba.touch_posa_territory,
       stba.useragent
FROM se.data.scv_touch_marketing_channel stmc
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt stba ON stmc.touch_id = stba.touch_id
WHERE stmc.touch_affiliate_territory IS NULL;


