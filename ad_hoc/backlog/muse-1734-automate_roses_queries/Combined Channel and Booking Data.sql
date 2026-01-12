--Below SQLs were used for gsheet template for weekly weighted SPVs, need collab access, weekly update would suffice, but might not be needed if weighted SPVs can be extracted in another table.
use warehouse MARKETING_PIPE_MEDIUM -- selects warehouse power, if in doubt, ask data for more info on what's available to you
alter session set week_start = 1 -- legacy code to make sure the week starts on a monday, but less useful now we use se week
create or replace transient table COLLAB.DEMAND_SHARED_TABLES.LND_DACH_Channel_web_metrics as
SELECT SE_YEAR                                          as year,
       SE_WEEK                                          as week,
       max(to_date(stba.touch_start_tstamp))            as end_date, -- used to verify the end of week date in case of partial week data
       se.data.channel_category(stmc.touch_mkt_channel) as channel, -- high level grouping of channels, with Data owning this function
       count(distinct ATTRIBUTED_USER_ID)               as members, -- unique count of users in the time frame
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then ATTRIBUTED_USER_ID
                          else null end)                as app_members,  -- identifying app only users
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then ATTRIBUTED_USER_ID
                          else null end)                as web_members,  -- identifying web users (excluding app only users)
       COUNT(distinct stba.TOUCH_ID)                    as sessions,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then stba.TOUCH_ID
                          else null end)                as app_sessions,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then stba.TOUCH_ID
                          else null end)                as web_sessions,
       count(distinct sts.EVENT_HASH)                   as spvs,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then sts.EVENT_HASH
                          else null end)                as app_spvs,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then sts.EVENT_HASH
                          else null end)                as web_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba -- source session table
         inner JOIN SE.DATA.SCV_TOUCH_ATTRIBUTION sta -- table that allows choosing an attribution model, otherwise, joining channel table alone will be last-click attribution
                    ON stba.TOUCH_ID = sta.TOUCH_ID AND sta.attribution_model = 'last non direct'
         inner join se.data.scv_touch_marketing_channel stmc ON sta.ATTRIBUTED_TOUCH_ID = stmc.touch_id -- identifying the marketing channel based on session (touch) id
         left join se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id -- finding all the spv events associated with that session
         left join SE.DATA.SE_CALENDAR se on stba.TOUCH_START_TSTAMP::date = se.DATE_VALUE -- joining to SE calendar to match each date with se week and se year, different to calendar year and week
WHERE stba.stitched_identity_type = 'se_user_id' -- filter to member only traffic
  and TOUCH_AFFILIATE_TERRITORY IN ('DE','AT','CH') -- territory filter, grouping for DACH here
  AND stba.touch_start_tstamp::date >= '2018-12-01' -- cutting irrelevant dates out to limit data pull
GROUP BY 1, 2, 4;

--LND booking by channel
use warehouse MARKETING_PIPE_MEDIUM
alter session set week_start = 1
create or replace transient table COLLAB.DEMAND_SHARED_TABLES.LND_DACH_Channel_booking_metrics as
SELECT se_year                                                       AS year,
       se_week                                                       AS week,
       MAX(fcb.booking_completed_date)                               AS end_date,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
       COUNT(DISTINCT fcb.booking_id)                                AS bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS app_bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS web_bookings,
       ROUND(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 2) AS margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS app_margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS web_margin,
       SUM(fcb.no_nights)                                            AS nights,
       SUM(CASE
               WHEN stba.touch_experience LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS app_nights,
       SUM(CASE
               WHEN stba.touch_experience NOT LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS web_nights
FROM se.data.fact_complete_booking fcb --se.data.se_booking sb
         LEFT JOIN se.data.scv_touched_transactions stt
                   ON stt.booking_id = fcb.booking_id
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc
                    ON sta.attributed_touch_id = stmc.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
                   ON stba.touch_id = stt.touch_id
         LEFT JOIN se.data.se_calendar se ON fcb.booking_completed_date = se.date_value
WHERE touch_affiliate_territory IN ('DE','AT','CH')
  AND stba.stitched_identity_type = 'se_user_id'
  AND booking_completed_date >= '2018-12-01'
GROUP BY 1, 2, 4;

use warehouse MARKETING_PIPE_MEDIUM
alter session set week_start = 1
create or replace transient table COLLAB.DEMAND_SHARED_TABLES.LND_UK_Channel_web_metrics as
SELECT SE_YEAR                                          as year,
       SE_WEEK                                          as week,
       max(to_date(stba.touch_start_tstamp))            as end_date,
       se.data.channel_category(stmc.touch_mkt_channel) as channel,
       count(distinct ATTRIBUTED_USER_ID)               as members,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then ATTRIBUTED_USER_ID
                          else null end)                as app_members,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then ATTRIBUTED_USER_ID
                          else null end)                as web_members,
       COUNT(distinct stba.TOUCH_ID)                    as sessions,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then stba.TOUCH_ID
                          else null end)                as app_sessions,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then stba.TOUCH_ID
                          else null end)                as web_sessions,
       count(distinct sts.EVENT_HASH)                   as spvs,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE like 'native app%' then sts.EVENT_HASH
                          else null end)                as app_spvs,
       count(distinct case
                          when stba.TOUCH_EXPERIENCE not like 'native app%' then sts.EVENT_HASH
                          else null end)                as web_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
         inner JOIN SE.DATA.SCV_TOUCH_ATTRIBUTION sta
                    ON stba.TOUCH_ID = sta.TOUCH_ID AND sta.attribution_model = 'last non direct'
         inner join se.data.scv_touch_marketing_channel stmc ON sta.ATTRIBUTED_TOUCH_ID = stmc.touch_id
         left join se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
         left join SE.DATA.SE_CALENDAR se on stba.TOUCH_START_TSTAMP::date = se.DATE_VALUE
WHERE stba.stitched_identity_type = 'se_user_id'
  and TOUCH_AFFILIATE_TERRITORY IN ('UK')
  AND stba.touch_start_tstamp::date >= '2018-12-01'
GROUP BY 1, 2, 4;

--LND booking by channel
use warehouse MARKETING_PIPE_MEDIUM
alter session set week_start = 1
create or replace transient table COLLAB.DEMAND_SHARED_TABLES.LND_UK_Channel_booking_metrics as
SELECT se_year                                                       AS year,
       se_week                                                       AS week,
       MAX(fcb.booking_completed_date)                               AS end_date,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
       COUNT(DISTINCT fcb.booking_id)                                AS bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS app_bookings,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.booking_id
                          ELSE NULL END)                             AS web_bookings,
       ROUND(SUM(fcb.margin_gross_of_toms_gbp_constant_currency), 2) AS margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS app_margin,
       ROUND(SUM(CASE
                     WHEN stba.touch_experience NOT LIKE 'native app%' THEN fcb.margin_gross_of_toms_gbp_constant_currency
                     ELSE NULL END),
             2)                                                      AS web_margin,
       SUM(fcb.no_nights)                                            AS nights,
       SUM(CASE
               WHEN stba.touch_experience LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS app_nights,
       SUM(CASE
               WHEN stba.touch_experience NOT LIKE 'native app%'
                   THEN fcb.no_nights
               ELSE NULL END)                                        AS web_nights
FROM se.data.fact_complete_booking fcb --se.data.se_booking sb
         LEFT JOIN se.data.scv_touched_transactions stt
                   ON stt.booking_id = fcb.booking_id
         INNER JOIN se.data.scv_touch_attribution sta
                    ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel stmc
                    ON sta.attributed_touch_id = stmc.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
                   ON stba.touch_id = stt.touch_id
         LEFT JOIN se.data.se_calendar se ON fcb.booking_completed_date = se.date_value
WHERE touch_affiliate_territory IN ('UK')
  AND stba.stitched_identity_type = 'se_user_id'
  AND booking_completed_date >= '2018-12-01'
GROUP BY 1, 2, 4;