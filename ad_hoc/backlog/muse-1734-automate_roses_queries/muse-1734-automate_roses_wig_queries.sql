--1st bookings of the year per member gross Source. Weekly updated for WIGS scorecard
USE WAREHOUSE marketing_pipe_large
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.first_booking_of_the_year_gross AS
WITH bookings_ranked AS (
    SELECT EXTRACT(YEAR FROM booking_completed_date)                                               AS year,
           EXTRACT(MONTH FROM booking_completed_date)                                              AS month,
           CASE
               WHEN territory IN ('UK') THEN 'UK'
               WHEN territory IN ('DE', 'CH', 'AT') THEN 'DACH'
               ELSE 'ROW' END                                                                      AS territory,
           booking_completed_date,
           travel_type,
           check_in_date,
           DATE_TRUNC('month', check_in_date)                                                      AS checkin_month,
           shiro_user_id,
           booking_id,
           RANK()
                   OVER (PARTITION BY shiro_user_id,year ORDER BY booking_completed_timestamp ASC) AS order_rank_year
    FROM se.data.fact_booking fb
--where SHIRO_USER_ID in ('36942507','29469237')
    WHERE booking_status_type IN ('live', 'cancelled')
      AND territory NOT IN ('PL', 'TL')
      AND year >= 2018
)
SELECT year,
       month,
       territory,
       booking_completed_date,
       travel_type,
       check_in_date,
       checkin_month,
       shiro_user_id,
       booking_id,
       CASE WHEN order_rank_year = 1 THEN '1st' ELSE 'repeat' END AS order_rank
FROM bookings_ranked
WHERE year IS NOT NULL;



SELECT fb.booking_id,
       fb.booking_completed_timestamp,
       fb.booking_completed_date,
       fb.shiro_user_id,
       ua.original_affiliate_id,
       ua.member_original_affiliate_classification,
       EXTRACT(YEAR FROM fb.booking_completed_date)                                                        AS year,
       EXTRACT(MONTH FROM fb.booking_completed_date)                                                       AS month,
       CASE
           WHEN fb.territory IN ('UK') THEN 'UK'
           WHEN fb.territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW' END                                                                                  AS territory,
       fb.travel_type,
       fb.check_in_date,
       DATE_TRUNC('month', fb.check_in_date)                                                               AS checkin_month,
       fb.shiro_user_id,
       fb.booking_id,
       stmc.touch_mkt_channel,
       ROW_NUMBER() OVER (PARTITION BY fb.shiro_user_id, year ORDER BY fb.booking_completed_timestamp ASC) AS customer_order_rank_year,
       IFF(customer_order_rank_year = 1, '1st', 'repeat')                                                  AS customer_order_rank_year_category,
       us.member_recency_status,
       preus.booker_segment                                                                                AS previous_day_booker_segment,    -- using the previous day's segment
       preus.engagement_segment                                                                            AS previous_day_engagement_segment -- using the previous day's segment
FROM se.data.fact_booking fb
    INNER JOIN se.data.se_user_attributes ua ON fb.shiro_user_id = ua.shiro_user_id
                   -- attributed last non direct channel
    INNER JOIN se.data.scv_touched_transactions stt ON fb.booking_id = stt.booking_id
    INNER JOIN se.data.scv_touch_attribution sta ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id

    LEFT JOIN  se.data.user_segmentation us ON
            fb.shiro_user_id = us.shiro_user_id
        AND fb.booking_completed_date = us.date
    LEFT JOIN  se.data.user_segmentation preus ON
            fb.shiro_user_id = preus.shiro_user_id
        AND preus.date = DATEADD(DAY, -1, fb.booking_completed_date)

WHERE fb.booking_status_type IN ('live', 'cancelled')
  AND fb.territory NOT IN ('PL', 'TL')
  AND fb.booking_completed_date >= '2018-01-01'
  AND fb.booking_completed_date >= CURRENT_DATE - 1
;

self_describing_task --include 'dv/bi/booking/customer_yearly_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking fb;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_segmentation CLONE data_vault_mvp.dwh.user_segmentation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

DROP TABLE data_vault_mvp_dev_robin.bi.customer_yearly_booking;

self_describing_task --include 'se/bi/booking/customer_yearly_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

airflow backfill --start_date '2018-01-01 00:00:00' --end_date '2018-01-01 00:00:00' --task_regex '.*' bi__customer_yearly_booking__daily_at_04h00

------------------------------------------------------------------------------------------------------------------------


--monthly active users by channel groups (monthly update for WIGS Analysis model)


CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.monthly_active_users_segments AS
SELECT EXTRACT(YEAR FROM stba.touch_start_tstamp)  AS active_year,
       EXTRACT(MONTH FROM stba.touch_start_tstamp) AS active_month,
       CASE
           WHEN stmc.touch_affiliate_territory IN ('UK') THEN 'UK'
           WHEN stmc.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW' END                          AS territory,
       us.member_recency_status,
       preus.booker_segment,
       preus.engagement_segment,
       CASE
           WHEN touch_mkt_channel IN ('Affiliate Program',
                                      'Display CPA',
                                      'Display CPL',
                                      'Paid Social CPA',
                                      'Paid Social CPL',
                                      'PPC - Non Brand CPA',
                                      'PPC - Non Brand CPL',
                                      'PPC - Undefined'
               ) THEN 'CPA/CPL'
           WHEN stmc.touch_mkt_channel IN ('Direct',
                                           'Organic Search Brand',
                                           'Organic Search Non-Brand',
                                           'Organic Social',
                                           'Blog'
               ) THEN 'Free'
           WHEN stmc.touch_mkt_channel IN ('Email - Other',
                                           'Other',
                                           'Partner',
                                           'Media'
               ) THEN 'All Other'
           WHEN stmc.touch_mkt_channel IN ('Email - Newsletter', 'Email - Triggers') THEN 'Email - News/Trigg'
           ELSE stmc.touch_mkt_channel END         AS channel_group,
       COUNT(DISTINCT stba.attributed_user_id)     AS users

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
    LEFT JOIN  se.data.user_segmentation us ON TRY_TO_NUMBER(stba.attributed_user_id) = us.shiro_user_id AND stba.touch_start_tstamp::date = us.date
    LEFT JOIN  se.data.user_segmentation preus ON TRY_TO_NUMBER(stba.attributed_user_id) = preus.shiro_user_id AND DATEADD(DAY, -1, stba.touch_start_tstamp::date) = preus.date
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
GROUP BY 1, 2, 3, 4, 5, 6, 7;


self_describing_task --include 'dv/bi/scv/monthly_active_users_segments.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

self_describing_task --include 'se/bi/scv/monthly_active_users_segments.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

SELECT *
FROM se.bi.monthly_active_users_segments;

------------------------------------------------------------------------------------------------------------------------

SELECT EXTRACT(YEAR FROM touch_start_tstamp)  AS session_start_year,
       EXTRACT(MONTH FROM touch_start_tstamp) AS session_start_month,
       stba.touch_start_tstamp,
       stba.touch_id,
       se_year                                AS session_start_se_year,
       se_week                                AS session_start_se_week,
       touch_mkt_channel,
       CASE
           WHEN touch_mkt_channel IN (
                                      'Affiliate Program',
                                      'Display CPA',
                                      'Display CPL',
                                      'Paid Social CPA',
                                      'Paid Social CPL',
                                      'PPC - Non Brand CPA',
                                      'PPC - Non Brand CPL',
                                      'PPC - Undefined'
               ) THEN 'CPA/CPL'
           WHEN touch_mkt_channel IN (
                                      'Direct',
                                      'Organic Search Brand',
                                      'Organic Search Non-Brand',
                                      'Organic Social',
                                      'Blog'
               ) THEN 'Free'
           WHEN touch_mkt_channel IN (
                                      'Email - Other',
                                      'Other',
                                      'Partner',
                                      'Media'
               ) THEN 'All Other'
           WHEN touch_mkt_channel IN (
                                      'Email - Newsletter',
                                      'Email - Triggers') THEN 'Email - News/Trigg'
           ELSE touch_mkt_channel END         AS channel_group,
       ua.original_affiliate_territory        AS user_territory,
       CASE
           WHEN original_affiliate_territory IN ('UK') THEN 'UK'
           WHEN original_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW' END                     AS territory_category,
       TRY_TO_NUMBER(attributed_user_id)      AS shiro_user_id
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes ua
               ON stba.attributed_user_id = TO_VARCHAR(ua.shiro_user_id)
    INNER JOIN se.data.se_calendar se ON touch_start_tstamp::date = se.date_value
    INNER JOIN se.data.scv_touch_attribution sta
               ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc
               ON sta.attributed_touch_id = stmc.touch_id
WHERE stitched_identity_type = 'se_user_id'
  AND original_affiliate_territory NOT IN ('PL', 'TL')
  AND stba.touch_start_tstamp >= CURRENT_DATE - 1 -- warehousing
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attributed_user_id,year ORDER BY touch_start_tstamp) = 1

--and SHIRO_USER_ID in ('36942507', '29469237')

    self_describing_task --include 'dv/bi/scv/customer_yearly_first_session.py'  --method 'run' --start '2022-02-17 00:00:00' --end '2022-02-17 00:00:00'


SELECT EXTRACT(YEAR FROM touch_start_tstamp)  AS session_start_year,
       EXTRACT(MONTH FROM touch_start_tstamp) AS session_start_month,
       stba.touch_start_tstamp,
       stba.touch_id,
       se.se_year                             AS session_start_se_year,
       se.se_week                             AS session_start_se_week,
       stmc.touch_mkt_channel,
       CASE
           WHEN stmc.touch_mkt_channel IN (
                                           'Affiliate Program',
                                           'Display CPA',
                                           'Display CPL',
                                           'Paid Social CPA',
                                           'Paid Social CPL',
                                           'PPC - Non Brand CPA',
                                           'PPC - Non Brand CPL',
                                           'PPC - Undefined'
               ) THEN 'CPA/CPL'
           WHEN stmc.touch_mkt_channel IN (
                                           'Direct',
                                           'Organic Search Brand',
                                           'Organic Search Non-Brand',
                                           'Organic Social',
                                           'Blog'
               ) THEN 'Free'
           WHEN stmc.touch_mkt_channel IN (
                                           'Email - Other',
                                           'Other',
                                           'Partner',
                                           'Media'
               ) THEN 'All Other'
           WHEN stmc.touch_mkt_channel IN (
                                           'Email - Newsletter',
                                           'Email - Triggers'
               ) THEN 'Email - News/Trigg'
           ELSE stmc.touch_mkt_channel
           END                                AS channel_group,
       ua.original_affiliate_territory        AS user_territory,
       CASE
           WHEN ua.original_affiliate_territory = 'UK' THEN 'UK'
           WHEN ua.original_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW'
           END                                AS territory_category,
       TRY_TO_NUMBER(stba.attributed_user_id) AS shiro_user_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN data_vault_mvp_dev_robin.dwh.user_attributes ua ON stba.attributed_user_id = TO_VARCHAR(ua.shiro_user_id)
    INNER JOIN data_vault_mvp_dev_robin.dwh.se_calendar se ON touch_start_tstamp::date = se.date_value
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stitched_identity_type = 'se_user_id'
  AND original_affiliate_territory NOT IN ('PL', 'TL')
  AND stba.touch_start_tstamp >= TO_DATE('2022-02-16 04:00:00') -- get batch of data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stba.attributed_user_id, year ORDER BY touch_start_tstamp) = 1


DROP TABLE data_vault_mvp_dev_robin.bi.customer_yearly_booking;


SELECT *
FROM data_vault_mvp_dev_robin.bi.customer_yearly_first_session;


SELECT *
FROM data_vault_mvp_dev_robin.bi.customer_yearly_first_session__step01__model_data t1
    INNER JOIN data_vault_mvp_dev_robin.bi.customer_yearly_first_session t2 ON t1.shiro_user_id = t2.shiro_user_id;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.customer_yearly_booking CLONE data_vault_mvp.bi.customer_yearly_booking;
ALTER TABLE data_vault_mvp_dev_robin.bi.customer_yearly_booking
    RENAME COLUMN territory_group TO territory_category;

airflow backfill --start_date '2019-01-01 00:00:00' --end_date '2019-01-01 00:00:00' --task_regex '.*' bi__customer_yearly_first_session__daily_at_04h00


SELECT *
FROM data_vault_mvp.bi.customer_yearly_first_session
WHERE shiro_user_id = 1337884;


------------------------------------------------------------------------------------------------------------------------


SELECT ua.date                        AS email_activity_date,
       sc.se_week                     AS email_activity_se_week,
       sc.se_year                     AS email_activity_se_year,
       ua.shiro_user_id,
       u.original_affiliate_territory AS user_original_territory,
       CASE
           WHEN u.original_affiliate_territory = 'UK' THEN 'UK'
           WHEN u.original_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW'
           END                        AS user_original_territory_category,
       u.current_affiliate_territory  AS user_current_territory,
       CASE
           WHEN u.current_affiliate_territory = 'UK' THEN 'UK'
           WHEN u.current_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW'
           END                        AS user_current_territory_category,
       us.booker_segment,
       us.opt_in_status,
       COALESCE(ue.unique_sends, 0)   AS unique_sends,
       COALESCE(ue.sends, 0)          AS sends,
       COALESCE(ue.unique_opens, 0)   AS unique_opens,
       COALESCE(ue.opens, 0)          AS opens,
       COALESCE(ue.unique_clicks, 0)  AS unique_clicks,
       COALESCE(ue.clicks, 0)         AS clicks,
       ua.emails_1d,
       ua.emails_7d,
       ua.emails_14d,
       ua.emails_30d
FROM se.data.user_activity AS ua
    INNER JOIN data_vault_mvp.dwh.se_calendar sc ON ua.date = sc.date_value
                   --user emails is the date of the event
    LEFT JOIN  se.data.user_emails AS ue ON ua.shiro_user_id = ue.user_id AND ua.date = ue.date
    INNER JOIN se.data.user_segmentation AS us ON ua.shiro_user_id = us.shiro_user_id AND ua.date = us.date
    INNER JOIN se.data.se_user_attributes AS u ON ua.shiro_user_id = u.shiro_user_id
WHERE ua.date >= '2018-01-01'
  AND ua.date >= CURRENT_DATE - 1
  --remove redundant user activity rows for non email activity
  AND (ue.shiro_user_id IS NOT NULL
    OR
       ua.emails_30d > 0
    )
--and ua.shiro_user_id = '24916549'


SELECT ceo.event_tstamp::DATE,
       COUNT(*)
FROM se.data.crm_events_opens ceo
WHERE ceo.shiro_user_id = 22921864
  AND ceo.event_tstamp <= '2022-02-20'
  AND ceo.event_tstamp >= DATEADD(DAY, -30, '2022-02-20')
GROUP BY 1;

SELECT *
FROM se.data.user_emails ue
WHERE ue.shiro_user_id = 22921864;
SELECT *
FROM se.data.user_activity ua
WHERE ua.shiro_user_id = 22921864
  AND ua.date >= '2022-01-01';


self_describing_task --include 'dv/bi/email/daily_customer_email_activity.py'  --method 'run' --start '2022-02-20 00:00:00' --end '2022-02-20 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE data_vault_mvp.dwh.user_emails;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_emails ue
    self_describing_task --include 'se/bi/email/daily_customer_email_activity.py'  --method 'run' --start '2022-02-20 00:00:00' --end '2022-02-20 00:00:00'

    airflow backfill --start_date '2018-01-01 00:00:00' --end_date '2018-01-01 00:00:00' --task_regex '.*' bi__daily_customer_email_activity__daily_at_04h00

SELECT *
FROM se.bi.daily_customer_email_activity;


------------------------------------------------------------------------------------------------------------------------
SELECT EXTRACT(YEAR FROM em.date)                                                  AS year,
       EXTRACT(MONTH FROM em.date)                                                 AS month,
       CASE
           WHEN current_affiliate_territory IN ('UK') THEN 'UK'
           WHEN current_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW' END                                                          AS territory,
       em.booker_segment                                                           AS booker_segment,
       us.booker_segment                                                           AS previous_booker_segment,
       engagement_segment,
       member_recency_status,
       SUM(sends)                                                                  AS sends,
       SUM(unique_clicks)                                                          AS clicks,
       COUNT(DISTINCT CASE WHEN sends > 0 THEN em.shiro_user_id ELSE NULL END)     AS email_recipients,
       COUNT(DISTINCT CASE WHEN sends IS NULL THEN em.shiro_user_id ELSE NULL END) AS no_email_user
FROM data_vault_mvp.bi.daily_customer_email_activity em
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON em.shiro_user_id = us.shiro_user_id AND em.date = DATEADD(DAY, 1, us.date)
WHERE year >= 2019
GROUP BY 1, 2, 3, 4, 5, 6, 7

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.daily_customer_email_activity CLONE data_vault_mvp.bi.daily_customer_email_activity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_segmentation CLONE data_vault_mvp.dwh.user_segmentation;

self_describing_task --include 'dv/bi/email/monthly_sends_by_segment.py'  --method 'run' --start '2022-02-21 00:00:00' --end '2022-02-21 00:00:00'


SELECT EXTRACT(YEAR FROM em.date)                                                  AS email_activity_year,
       EXTRACT(MONTH FROM em.date)                                                 AS email_activity_month,
       em.user_current_territory_category,
       em.booker_segment                                                           AS booker_segment,
       us.booker_segment                                                           AS previous_booker_segment,
       us.engagement_segment                                                       AS previous_engagement_segment,
       us.member_recency_status                                                    AS previous_member_recency_status,
       SHA2(
                   COALESCE(email_activity_year, 0) ||
                   COALESCE(email_activity_month, 0) ||
                   COALESCE(user_current_territory_category, '') ||
                   COALESCE(previous_booker_segment, '') ||
                   COALESCE(previous_engagement_segment, '') ||
                   COALESCE(previous_member_recency_status, '') ||
           )                                                                       AS email_activity_hash,
       SUM(sends)                                                                  AS sends,
       SUM(unique_clicks)                                                          AS clicks,
       COUNT(DISTINCT CASE WHEN sends > 0 THEN em.shiro_user_id ELSE NULL END)     AS email_recipients,
       COUNT(DISTINCT CASE WHEN sends IS NULL THEN em.shiro_user_id ELSE NULL END) AS no_email_users
FROM data_vault_mvp.bi.daily_customer_email_activity em
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON em.shiro_user_id = us.shiro_user_id AND em.date = DATEADD(DAY, 1, us.date)
WHERE em.email_activity_date >= TO_DATE('2022-02-20 04:00:00') -- get batch of data
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


SELECT *
FROM data_vault_mvp_dev_robin.bi.monthly_sends_by_segment;


SELECT EXTRACT(YEAR FROM em.email_activity_date)                                      AS email_activity_year,
       EXTRACT(MONTH FROM em.email_activity_date)                                     AS email_activity_month,
       em.user_current_territory_category,
       em.booker_segment                                                              AS booker_segment,
       us.booker_segment                                                              AS previous_booker_segment,
       us.engagement_segment                                                          AS previous_engagement_segment,
       us.member_recency_status                                                       AS previous_member_recency_status,
       SHA2(
                   COALESCE(email_activity_year, 0) ||
                   COALESCE(email_activity_month, 0) ||
                   COALESCE(em.user_current_territory_category, '') ||
                   COALESCE(previous_booker_segment, '') ||
                   COALESCE(previous_engagement_segment, '') ||
                   COALESCE(previous_member_recency_status, '')
           )                                                                          AS email_activity_hash,
       SUM(em.sends)                                                                  AS sends,
       SUM(em.unique_clicks)                                                          AS clicks,
       COUNT(DISTINCT CASE WHEN em.sends > 0 THEN em.shiro_user_id ELSE NULL END)     AS email_recipients,
       COUNT(DISTINCT CASE WHEN em.sends IS NULL THEN em.shiro_user_id ELSE NULL END) AS no_email_users
FROM data_vault_mvp.bi.daily_customer_email_activity em
    LEFT JOIN data_vault_mvp.dwh.user_segmentation us ON em.shiro_user_id = us.shiro_user_id AND em.email_activity_date = DATEADD(DAY, 1, us.date)
WHERE em.email_activity_date >= TO_DATE('2022-02-20 04:00:00') -- get batch of data
  AND email_activity_year = 2022
  AND email_activity_month = 2
  AND em.user_current_territory_category = 'UK'
  AND previous_booker_segment = 'Repeat'
  AND previous_engagement_segment = 'last_active_7d'
  AND previous_member_recency_status = '5. 365+'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;

DROP TABLE data_vault_mvp_dev_robin.bi.monthly_sends_by_segment;

self_describing_task --include 'se/bi/email/monthly_sends_by_segment.py'  --method 'run' --start '2022-02-21 00:00:00' --end '2022-02-21 00:00:00'


------------------------------------------------------------------------------------------------------------------------
--de bug issue:

--prod
WITH prod AS (
    SELECT *
    FROM se.bi.customer_yearly_booking cyb
        LEFT JOIN se.data.se_calendar se ON cyb.booking_completed_date = se.date_value
    WHERE se_year = 2022
      AND se_week = 7
      AND customer_order_rank_year = 1
    ORDER BY 1
),
     query AS (

-- query
         WITH bookings_ranked AS (
             SELECT se_year,
                    se_week,
                    CASE
                        WHEN territory IN ('UK') THEN 'UK'
                        WHEN territory IN ('DE', 'CH', 'AT') THEN 'DACH'
                        ELSE 'ROW' END                                                                     AS territory,
                    booking_completed_date,
                    shiro_user_id,
                    booking_id,
                    RANK() OVER (PARTITION BY shiro_user_id,year ORDER BY booking_completed_timestamp ASC) AS order_rank_year
             FROM se.data.fact_booking fb
                 LEFT JOIN se.data.se_calendar se ON fb.booking_completed_date = se.date_value
             WHERE booking_status_type IN ('live', 'cancelled')
               AND territory NOT IN ('PL', 'TL')
         )
         SELECT se_year,
                se_week,
                booking_completed_date,
                booking_id,
                shiro_user_id
         FROM bookings_ranked
         WHERE se_year IS NOT NULL
           AND order_rank_year = 1
           AND se_year = 2022
           AND se_week = 7
         ORDER BY 3
     )


SELECT q.booking_id,
       q.booking_completed_date
FROM query q
EXCEPT
SELECT p.booking_id,
       p.booking_completed_date
FROM prod p

;


SELECT *
FROM se.data.fact_booking fb
WHERE fb.shiro_user_id = 28138054
  AND fb.booking_status_type IN ('live', 'cancelled');


SELECT *
FROM se.bi.customer_yearly_booking cyb
WHERE cyb.shiro_user_id = '28138054';
SELECT *
FROM data_vault_mvp_dev_robin.bi.customer_yearly_booking cyb
WHERE cyb.shiro_user_id = '28138054';


self_describing_task --include 'dv/bi/booking/customer_yearly_booking.py'  --method 'run' --start '2022-02-23 00:00:00' --end '2022-02-23 00:00:00'
self_describing_task --include 'dv/bi/booking/customer_yearly_booking.py'  --method 'run' --start '2022-02-15 00:00:00' --end '2022-02-15 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.customer_yearly_booking CLONE data_vault_mvp.bi.customer_yearly_booking;

SELECT *
FROM raw_vault_mvp.travelbird_mysql.offers_offer;

airflow backfill --start_date '2022-02-15 00:00:00' --end_date '2022-02-16 00:00:00' --task_regex '.*' bi__customer_yearly_booking__daily_at_04h00

SELECT *
FROM data_vault_mvp.bi.customer_yearly_booking cyb
WHERE cyb.shiro_user_id = '28138054';

DELETE
FROM data_vault_mvp.bi.customer_yearly_booking
WHERE booking_completed_date >= '2022-02-15'


--new table
SELECT *
FROM se.bi.customer_yearly_booking cyb
    LEFT JOIN se.data.se_calendar se ON cyb.booking_completed_date = se.date_value
WHERE se_year = 2022
  AND se_week = 7
  AND customer_order_rank_year = 1
ORDER BY 1;


-- old query
WITH bookings_ranked AS (
    SELECT se_year,
           se_week,
           CASE
               WHEN territory IN ('UK') THEN 'UK'
               WHEN territory IN ('DE', 'CH', 'AT') THEN 'DACH'
               ELSE 'ROW' END                                                                     AS territory,
           booking_completed_date,
           shiro_user_id,
           booking_id,
           RANK() OVER (PARTITION BY shiro_user_id,year ORDER BY booking_completed_timestamp ASC) AS order_rank_year
    FROM se.data.fact_booking fb
        LEFT JOIN se.data.se_calendar se ON fb.booking_completed_date = se.date_value
    WHERE booking_status_type IN ('live', 'cancelled')
      AND territory NOT IN ('PL', 'TL')
      AND year >= 2022
)
SELECT se_year,
       se_week,
--territory,
       booking_completed_date,
       booking_id,
       shiro_user_id
FROM bookings_ranked
WHERE se_year IS NOT NULL
  AND order_rank_year = 1
  AND se_year = 2022
  AND se_week = 7
ORDER BY 3;




SELECT *
FROM se.bi.customer_yearly_booking cyb
    LEFT JOIN se.data.se_calendar se ON cyb.booking_completed_date = se.date_value
WHERE se_year = 2022
  AND se_week = 7
  AND customer_order_rank_year = 1
ORDER BY 1;

SELECT se_year,
       se_week,
       MAX(booking_completed_date)   AS max_date,
       COUNT(DISTINCT shiro_user_id) AS bookers
FROM se.bi.customer_yearly_booking cyb
    LEFT JOIN se.data.se_calendar se ON cyb.booking_completed_date = se.date_value
WHERE se_year = 2022
GROUP BY 1, 2
ORDER BY 1, 2;



