USE WAREHOUSE pipe_xlarge;

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
LIMIT 10;

SELECT stba.touch_experience,
       COUNT(DISTINCT sts.event_hash) AS spvs
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp::DATE = '2020-06-01'
GROUP BY 1;

SELECT *
FROM se.data.scv_touched_spvs sts;
SELECT *
FROM se.data.scv_touched_transactions stt sts;

--spvs by day by sale id,


SELECT sts.se_sale_id,
       sts.event_tstamp::DATE AS date,
       count(*)               AS spvs
FROM se.data.scv_touched_spvs sts
GROUP BY 1, 2;

--spvs by day by sale id by device platform and channel

SELECT stba.touch_experience,
       stmc.touch_mkt_channel,
       sts.event_tstamp::DATE         AS date, --this can be changed to stba.touch_start_tstamp::DATE to use the date of the session
       sts.se_sale_id,
       COUNT(DISTINCT sts.event_hash) AS spvs
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = sts.touch_id
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE sts.event_tstamp >= '2020-06-01'
GROUP BY 1, 2, 3, 4;

SELECT MAX(date) FROM se.data.active_user_base aub;