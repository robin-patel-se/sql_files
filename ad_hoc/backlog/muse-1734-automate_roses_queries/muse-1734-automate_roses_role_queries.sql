--Below SQLs were used for gsheet template for weekly weighted SPVs, need collab access, weekly update would suffice, but might not be needed if weighted SPVs can be extracted in another table.
USE WAREHOUSE marketing_pipe_medium -- selects warehouse power, if in doubt, ask data for more info on what's available to you
ALTER SESSION SET week_start = 1 -- legacy code to make sure the week starts on a monday, but less useful now we use se week
CREATE OR REPLACE TRANSIENT TABLE collab.demand_shared_tables.lnd_dach_channel_web_metrics AS
SELECT se_year                                          AS year,
       se_week                                          AS week,
       MAX(TO_DATE(stba.touch_start_tstamp))            AS end_date,    -- used to verify the end of week date in case of partial week data
       se.data.channel_category(stmc.touch_mkt_channel) AS channel,     -- high level grouping of channels, with Data owning this function
       COUNT(DISTINCT attributed_user_id)               AS members,     -- unique count of users in the time frame
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN attributed_user_id
                          ELSE NULL END)                AS app_members, -- identifying app only users
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN attributed_user_id
                          ELSE NULL END)                AS web_members, -- identifying web users (excluding app only users)
       COUNT(DISTINCT stba.touch_id)                    AS sessions,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN stba.touch_id
                          ELSE NULL END)                AS app_sessions,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN stba.touch_id
                          ELSE NULL END)                AS web_sessions,
       COUNT(DISTINCT sts.event_hash)                   AS spvs,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN sts.event_hash
                          ELSE NULL END)                AS app_spvs,
       COUNT(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN sts.event_hash
                          ELSE NULL END)                AS web_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba -- source session table
    INNER JOIN se.data.scv_touch_attribution sta -- table that allows choosing an attribution model, otherwise, joining channel table alone will be last-click attribution
               ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id -- identifying the marketing channel based on session (touch) id
    LEFT JOIN  se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id -- finding all the spv events associated with that session
    LEFT JOIN  se.data.se_calendar se ON stba.touch_start_tstamp::date = se.date_value -- joining to SE calendar to match each date with se week and se year, different to calendar year and week
WHERE stba.stitched_identity_type = 'se_user_id'      -- filter to member only traffic
  AND touch_affiliate_territory IN ('DE', 'AT', 'CH') -- territory filter, grouping for DACH here
  AND stba.touch_start_tstamp::date >= '2018-12-01'   -- cutting irrelevant dates out to limit data pull
GROUP BY 1, 2, 4;

SELECT *
FROM se.data.user_subscription us
    INNER JOIN se.data.se_user_attributes sua ON us.shiro_user_id = sua.shiro_user_id AND us.calendar_date = sua.signup_tstamp::DATE;

SELECT MIN(calendar_date)
FROM se.data.user_subscription us;

airflow backfill --start_date '2019-01-01 00:00:00' --end_date '2019-01-01 00:00:00' --task_regex '.*' bi__monthly_email_metrics_by_segment__daily_at_04h00


SELECT *
FROM se.data.user_subscription us
WHERE us.shiro_user_id = 62972247;


SELECT * FROM se.bi.monthly_email_metrics_by_segment;