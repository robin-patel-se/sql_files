SELECT sts.event_tstamp::DATE AS date,
       stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE BETWEEN '2021-01-17' AND '2021-01-26'
GROUP BY 1, 2;

--influx in 'OTHER' channel spvs


USE WAREHOUSE pipe_xlarge;

SELECT sts.event_tstamp::DATE AS date,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE BETWEEN '2020-10-10' AND '2021-01-26'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1;


SELECT sts.event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE BETWEEN '2020-10-10' AND '2021-01-26'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1;


SELECT sts.event_tstamp::DATE AS date,
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE BETWEEN '2020-10-10' AND '2021-01-26'
  AND stmc.touch_mkt_channel = 'Other'
  AND PARSE_URL(sts.page_url):host::varchar = '95.131.252.120'
GROUP BY 1;

SELECT *
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE PARSE_URL(sts.page_url):host::VARCHAR LIKE '%.eu-west-1.compute.amazonaws.com'
LIMIT 50;


SELECT '95.131.252.120' REGEXP '[0-9]{{1,3}}\\.[0-9]{{1,3}}\\.[0-9]{{1,3}}\\.[0-9]{{1,3}}';


-- influx of 'Other' channel spvs on certain date


SELECT *,
       PARSE_URL(sts.page_url):host::varchar AS host
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE = '2021-01-22'
  AND stmc.touch_mkt_channel = 'Other';

--found loads of aws urls

SELECT PARSE_URL(sts.page_url):host::varchar AS host,
       COUNT(*)
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE = '2021-01-22'
  AND stmc.touch_mkt_channel = 'Other'
GROUP BY 1;


-- check other spvs by minute of day on the 22nd of Jan
SELECT DATE_TRUNC(MINUTE, sts.event_tstamp),
       COUNT(*)
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE = '2021-01-22'
  AND stmc.touch_mkt_channel = 'Other'
  AND PARSE_URL(sts.page_url):host::varchar IN
      ('ec2-54-73-62-134.eu-west-1.compute.amazonaws.com', 'ec2-54-75-210-204.eu-west-1.compute.amazonaws.com')
GROUP BY 1;


-- Check the ip address of events
SELECT *
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE = '2021-01-22'
  AND stmc.touch_mkt_channel = 'Other'
  AND PARSE_URL(sts.page_url):host::varchar IN
      ('ec2-54-73-62-134.eu-west-1.compute.amazonaws.com', 'ec2-54-75-210-204.eu-west-1.compute.amazonaws.com')
;


USE WAREHOUSE pipe_xlarge;


airflow backfill --start_date '2021-02-02 07:00:00' --end_date '2021-02-02 07:00:00' --task_regex '.*' se_data_pii_object_creation__daily_at_07h00