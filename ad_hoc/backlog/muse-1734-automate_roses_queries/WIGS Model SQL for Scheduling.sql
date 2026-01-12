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
           RANK() OVER (PARTITION BY shiro_user_id,year ORDER BY booking_completed_timestamp ASC) AS order_rank_year
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

SELECT * FROM se.bi.customer_yearly_booking cyb;


-- gross 1st booking by channel plus booker detail, minimum monthly, but weekly ideal
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.first_booking_of_the_year_gross_channelplus1 AS
SELECT year,
       month,
       territory,
       booking_completed_date,
       travel_type,
       checkin_month,
       us.date    AS segment_date,
       preus.date AS segment_date2, -- sense check that for fields from the preus table, it should be for previous day's segment
       order_rank,
       --stt.TOUCH_ID,
       touch_mkt_channel,
       us.member_recency_status,
       preus.booker_segment,        -- using the previous day's segment
       preus.engagement_segment,    -- using the previous day's segment
       gb.shiro_user_id,
       ua.original_affiliate_id,
       ua.member_original_affiliate_classification,
       gb.booking_id
FROM collab.demand_shared_tables.first_booking_of_the_year_gross gb
    LEFT JOIN se.data.scv_touched_transactions stt ON gb.booking_id = stt.booking_id -- joining booking IDs to the transaction ID in the SCV table to find their session/touch ID
    LEFT JOIN se.data.scv_touch_attribution sta -- joining to the attribution table to get LND channels
    ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    LEFT JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id -- channel for the session based on the attribution model selected
    LEFT JOIN se.data.user_segmentation us ON gb.shiro_user_id = us.shiro_user_id AND gb.booking_completed_date = us.date -- getting the segment on date of booking completed date
    LEFT JOIN se.data.user_segmentation preus ON gb.shiro_user_id = preus.shiro_user_id AND gb.booking_completed_date = DATEADD(DAY, 1, preus.date) -- joining to the same segement table but getting the previous day's segment for engagement related fields
    LEFT JOIN se.data.se_user_attributes ua ON gb.shiro_user_id = ua.shiro_user_id -- getting the signup channel using original affiliate ID and classification


--monthly active users by channel groups (monthly update for WIGS Analysis model)
    use warehouse marketing_pipe_large
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.monthly_active_users_segments AS
SELECT EXTRACT(YEAR FROM touch_start_tstamp)  AS year,
       EXTRACT(MONTH FROM touch_start_tstamp) AS month,
       CASE
           WHEN touch_affiliate_territory IN ('UK') THEN 'UK'
           WHEN touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
           ELSE 'ROW' END                     AS territory,
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
           WHEN touch_mkt_channel IN ('Direct',
                                      'Organic Search Brand',
                                      'Organic Search Non-Brand',
                                      'Organic Social',
                                      'Blog'
               ) THEN 'Free'
           WHEN touch_mkt_channel IN ('Email - Other',
                                      'Other',
                                      'Partner',
                                      'Media'
               ) THEN 'All Other'
           WHEN touch_mkt_channel IN ('Email - Newsletter', 'Email - Triggers') THEN 'Email - News/Trigg'
           ELSE touch_mkt_channel END         AS channel_group,

       COUNT(DISTINCT attributed_user_id)     AS users

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_attribution sta
               ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
    LEFT JOIN  se.data.user_segmentation us ON stba.attributed_user_id = TO_VARCHAR(us.shiro_user_id) AND stba.touch_start_tstamp::date = us.date
    LEFT JOIN  se.data.user_segmentation preus ON stba.attributed_user_id = TO_VARCHAR(preus.shiro_user_id) AND stba.touch_start_tstamp::date = DATEADD(DAY, 1, preus.date)
WHERE stba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4, 5, 6, 7;

------------------------------------------------------------------------------------------------------------------------

--1st session of the year (weekly update for WIGS scorecard)
USE WAREHOUSE marketing_pipe_large
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.first_touch_of_the_year_channel AS
WITH session_ranked AS (
    SELECT EXTRACT(YEAR FROM touch_start_tstamp)                                               AS year,
           EXTRACT(MONTH FROM touch_start_tstamp)                                              AS month,
           stba.touch_start_tstamp,
           stba.touch_id,
           se_year,
           se_week,
           touch_mkt_channel,
           CASE
               WHEN original_affiliate_territory IN ('UK') THEN 'UK'
               WHEN original_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
               ELSE 'ROW' END                                                                  AS territory,
           RANK()
                   OVER (PARTITION BY attributed_user_id,year ORDER BY touch_start_tstamp ASC) AS touch_rank_year,
           attributed_user_id
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
        LEFT JOIN se.data.se_user_attributes ua
                  ON stba.attributed_user_id = TO_VARCHAR(ua.shiro_user_id)
        LEFT JOIN se.data.se_calendar se ON touch_start_tstamp::date = se.date_value
        LEFT JOIN se.data.scv_touch_attribution sta
                  ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
        LEFT JOIN se.data.scv_touch_marketing_channel stmc
                  ON sta.attributed_touch_id = stmc.touch_id
    WHERE year >= 2019
      AND stitched_identity_type = 'se_user_id'
      AND original_affiliate_territory NOT IN ('PL', 'TL')

    --and SHIRO_USER_ID in ('36942507', '29469237')
)
SELECT year,
       month,
       se_year,
       se_week,
       touch_start_tstamp,
       territory,
       CASE WHEN touch_rank_year = 1 THEN '1st' ELSE 'repeat' END AS order_rank,
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
           WHEN touch_mkt_channel IN ('Direct',
                                      'Organic Search Brand',
                                      'Organic Search Non-Brand',
                                      'Organic Social',
                                      'Blog'
               ) THEN 'Free'
           WHEN touch_mkt_channel IN ('Email - Other',
                                      'Other',
                                      'Partner',
                                      'Media'
               ) THEN 'All Other'
           WHEN touch_mkt_channel IN ('Email - Newsletter', 'Email - Triggers') THEN 'Email - News/Trigg'
           ELSE touch_mkt_channel END                             AS channel_group,
       attributed_user_id,
       touch_id
FROM session_ranked
WHERE touch_rank_year = 1;



--Email sends breakdown tables

-- 2019 sends data update, only needed data needs replacing. Monthly update at most
USE WAREHOUSE marketing_pipe_medium;
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.member_email_metrics19 AS
SELECT ua.date,
       ua.shiro_user_id,
       u.original_affiliate_territory,
       u.current_affiliate_territory,
       us.booker_segment,
       us.opt_in_status,
       ua.emails_1d,
       ua.emails_7d,
       ua.emails_14d,
       ua.emails_30d,
       ue.unique_sends,
       ue.sends,
       ue.unique_opens,
       ue.opens,
       ue.unique_clicks,
       ue.clicks
FROM se.data.user_activity AS ua
    LEFT JOIN  se.data.user_emails AS ue
               ON ua.shiro_user_id = ue.user_id AND ua.date = ue.date
    INNER JOIN se.data.user_segmentation AS us
               ON ua.shiro_user_id = us.shiro_user_id AND ua.date = us.date
    INNER JOIN se.data.se_user_attributes AS u
               ON ua.shiro_user_id = u.shiro_user_id
WHERE EXTRACT(YEAR FROM ua.date) IN (2019, 2018)
--and ua.shiro_user_id = '24916549'
ORDER BY 1;

-- 2019 sends data update, only needed data needs replacing. Monthly update at most, must run with above SQL
USE WAREHOUSE marketing_pipe_medium;
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.member_email_metrics19b AS
SELECT se.se_week,
       se.se_year,
       date,
       shiro_user_id,
       original_affiliate_territory,
       current_affiliate_territory,
       booker_segment,
       opt_in_status,
       emails_1d,
       emails_7d,
       emails_14d,
       emails_30d,
       unique_sends,
       sends,
       unique_opens,
       opens,
       unique_clicks,
       clicks
FROM collab.demand_shared_tables.member_email_metrics19
    LEFT JOIN se.data.se_calendar se ON date = se.date_value;

-- email data for 2020 onward. Monthly uppate for WIGS Analysis
USE WAREHOUSE marketing_pipe_large
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.member_email_metrics20 AS
SELECT ua.date,
       ua.shiro_user_id,
       u.original_affiliate_territory,
       u.current_affiliate_territory,
       us.booker_segment,
       us.opt_in_status,
       ua.emails_1d,
       ua.emails_7d,
       ua.emails_14d,
       ua.emails_30d,
       ue.unique_sends,
       ue.sends,
       ue.unique_opens,
       ue.opens,
       ue.unique_clicks,
       ue.clicks
FROM se.data.user_activity AS ua
    LEFT JOIN  se.data.user_emails AS ue
               ON ua.shiro_user_id = ue.user_id AND ua.date = ue.date
    INNER JOIN se.data.user_segmentation AS us
               ON ua.shiro_user_id = us.shiro_user_id AND ua.date = us.date
    INNER JOIN se.data.se_user_attributes AS u
               ON ua.shiro_user_id = u.shiro_user_id
WHERE ua.date >= '2020-01-01'
--and ua.shiro_user_id = '24916549'
ORDER BY 1;

-- 2020 onward sends data update, only needed data needs replacing. Must run with above SQL
USE WAREHOUSE marketing_pipe_medium
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.member_email_metrics21 AS
SELECT se.se_week,
       se.se_year,
       date,
       shiro_user_id,
       original_affiliate_territory,
       current_affiliate_territory,
       booker_segment,
       opt_in_status,
       emails_1d,
       emails_7d,
       emails_14d,
       emails_30d,
       unique_sends,
       sends,
       unique_opens,
       opens,
       unique_clicks,
       clicks
FROM collab.demand_shared_tables.member_email_metrics20
    LEFT JOIN se.data.se_calendar se ON date = se.date_value;

--combining existing 2019 email sends data with 2020 updated data, must run with above SQL
USE WAREHOUSE marketing_pipe_medium
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.member_email_metrics19c AS
SELECT *
FROM collab.demand_shared_tables.member_email_metrics19b
UNION ALL
SELECT *
FROM collab.demand_shared_tables.member_email_metrics21;


SELECT * FROM se.bi.daily_customer_email_activity dcea;

--breakdown by month, so monthly udpate after above is updated
USE WAREHOUSE marketing_pipe_large
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.sends_by_segment AS
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
FROM scratch.roseyin.member_email_metrics19c em
    LEFT JOIN se.data.user_segmentation us ON em.shiro_user_id = us.shiro_user_id AND em.date = DATEADD(DAY, 1, us.date)
WHERE year >= 2019
GROUP BY 1, 2, 3, 4, 5, 6, 7
;

SELECT * FROM se.bi.monthly_email_metrics_by_segment membs;


SELECT * FROM data_vault_mvp.bi.daily_customer_email_activity dcea;
SELECT * FROm collab.demand_shared_tables.member_email_metrics19c

GRANT SELECT ON TABLE collab.demand_shared_tables.member_email_metrics19c TO ROLE jor;