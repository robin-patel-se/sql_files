airflow backfill --start_date '2020-04-30 03:00:00' --end_date '2020-04-30 03:00:00' --task_regex '.*' single_customer_view__daily_at_03h00
airflow backfill --start_date '2020-05-01 03:00:00' --end_date '2020-05-03 03:00:00' --task_regex '.*' -m single_customer_view__daily_at_03h00

SELECT MAX(date)
FROM se.data.user_activity;
CREATE OR REPLACE TABLE se_dev_robin.data.user_activity CLONE se.data.user_activity;
CREATE OR REPLACE TABLE se_dev_robin.data.user_activity_bkup CLONE se.data.user_activity;
USE WAREHOUSE pipe_large;
CALL scratch.robinpatel.backfill_user_activity_loop(1, 5);

SELECT date, count(*)
FROM se_dev_robin.data.user_activity
WHERE date >= '2020-04-01'
GROUP BY 1;

--run on prod
CREATE OR REPLACE TABLE se.data.user_activity CLONE se_dev_robin.data.user_activity;

airflow backfill --start_date '2020-04-30 03:00:00' --end_date '2020-05-03 03:00:00' --task_regex '.*' -m dwh__user_activity__daily_at_03h00

CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base CLONE se.data.active_user_base;
CREATE OR REPLACE TABLE se_dev_robin.data.active_user_base_bkup CLONE se.data.active_user_base;
CREATE OR REPLACE TABLE se_dev_robin.data.user_activity CLONE se.data.user_activity;
CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation CLONE se.data.user_segmentation;


SELECT MAX(updated_at)
FROM se_dev_robin.data.active_user_base;
self_describing_task --include 'se/data/se_active_user_base'  --method 'run' --start '2020-04-30 00:00:00' --end '2020-04-30 00:00:00'

SELECT DISTINCT date
FROM se_dev_robin.data.active_user_base;

-- run on prod
CREATE OR REPLACE TABLE se.data.active_user_base CLONE se_dev_robin.data.active_user_base;

airflow backfill --start_date '2020-04-30 03:00:00' --end_date '2020-05-03 03:00:00' --task_regex '.*' -m active_user_base__daily_at_03h00

------------------------------------------------------------------------------------------------------------------------

SELECT touch_start_tstamp::DATE AS date, count(*) AS sessions
FROM se.data.scv_touch_basic_attributes
WHERE touch_start_tstamp >= '2020-01-01'
  AND touch_start_tstamp < CURRENT_DATE
  AND touch_experience = 'native app'
GROUP BY 1
ORDER BY 1;


SELECT b.touch_start_tstamp::DATE                                      AS date,
       c.touch_mkt_channel                                             AS last_non_direct_mkt_channel,
       b.touch_experience,
       c.touch_hostname_territory,
       c.touch_affiliate_territory,
       COUNT(DISTINCT b.attributed_user_id_hash)                       AS users,
       COUNT(DISTINCT b.touch_id)                                      AS sessions,
       COUNT(spv.event_hash)                                           AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id)                    AS unique_spvs
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         INNER JOIN se.data.scv_touched_spvs spv ON b.touch_id = spv.touch_id
         LEFT JOIN se.data.dim_sale s ON spv.se_sale_id = s.sale_id
WHERE b.touch_start_tstamp::DATE >= '2020-01-01'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1;