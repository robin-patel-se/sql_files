SELECT aub.date,

       SUM(aub.active_1d)  AS email_active_1d,
       SUM(aub.active_7d)  AS email_active_7d,
       SUM(aub.active_14d) AS email_active_14d,
       SUM(aub.active_30d) AS email_active_30d,
       SUM(aub.active_90d) AS email_active_90d

FROM se.data.active_user_base aub
WHERE aub.platform = 'email_active'
GROUP BY 1
ORDER BY 1;

SELECT *
FROM se.data.user_activity ua;

SELECT date,
       SUM(sends),
       SUM(ue.opens),
       SUM(ue.clicks)
FROM se.data.user_emails ue
WHERE date >= '2020-07-01'
GROUP BY 1;

SELECT platform,

--        SUM(aub.active_1d)  AS active_1d,
       SUM(aub.active_7d)  AS active_7d,
--        SUM(aub.active_14d) AS active_14d,
       SUM(aub.active_30d) AS active_30d
--        SUM(aub.active_90d) AS active_90d
FROM se.data.active_user_base aub
WHERE date = current_date - 1
GROUP BY 1;

--last click
SELECT se.data.channel_category(stmc.touch_mkt_channel) as channel,
       COUNT(DISTINCT CASE WHEN stba.touch_start_tstamp >= current_date - 7 THEN stba.attributed_user_id_hash END) AS active_7d,
       COUNT(DISTINCT stba.attributed_user_id_hash)                                                                AS active_30d
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= current_date - 30
GROUP BY 1;

--last non direct
SELECT se.data.channel_category(stmc.touch_mkt_channel) as channel,
       COUNT(DISTINCT CASE WHEN stba.touch_start_tstamp >= current_date - 7 THEN stba.attributed_user_id_hash END) AS active_7d,
       COUNT(DISTINCT stba.attributed_user_id_hash)                                                                AS active_30d
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= current_date - 30
GROUP BY 1;

--users that have signed up in last 30 days
SELECT count(*)
FROM se.data.se_user_attributes sua
WHERE sua.signup_tstamp >= current_date - 30;

--sessions
SELECT se.data.channel_category(stmc.touch_mkt_channel) as channel,
       COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= current_date - 30
GROUP BY 1;

--spvs
SELECT se.data.channel_category(stmc.touch_mkt_channel) as channel,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= current_date - 30
GROUP BY 1;

--bookings
SELECT count(*) FROM se.data.se_booking sb WHERE sb.booking_status = 'COMPLETE' AND sb.booking_completed_date >= current_date - 30;