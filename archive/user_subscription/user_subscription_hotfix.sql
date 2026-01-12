USE WAREHOUSE pipe_large;
SELECT calendar_date,
       count(DISTINCT user_id) AS users
FROM se.data.user_subscription
WHERE calendar_date >= '2020-04-01'
GROUP BY 1;

SELECT updated_at, count(*)
FROM data_vault_mvp.dwh.user_subscription_event
GROUP BY updated_at;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.profile CLONE raw_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TABLE se_dev_robin.data.se_calendar CLONE se.data.se_calendar;

--backfill tables
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_subscription_event CLONE data_vault_mvp_dev_robin.dwh.user_subscription_event_bkup;
CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription CLONE se_dev_robin.data.user_subscription_bkup;

SELECT updated_at,
       count(*)
FROM data_vault_mvp.dwh.user_subscription_event
GROUP BY 1;

SELECT updated_at,
       count(*)
FROM se.data.user_subscription
GROUP BY 1;


SELECT MAX(updated_at)
FROM se_dev_robin.data.user_subscription;

self_describing_task --include 'dv/dwh_rec/transactional/user_subscription'  --method 'run' --start '2020-04-17 03:00:00' --end '2020-04-17 03:00:00'


SELECT sd.id              AS user_id,
       CASE
           WHEN p.receive_sales_reminders = 1 THEN 2 --daily subscription status
           WHEN p.receive_weekly_offers = 1 THEN 1 --weekly subscription status
           ELSE 0 --opted out
           END            AS subscription_type,
       --there are many rows in profile table with 1970 last_updated date
       CASE
           WHEN p.last_updated = '1970-01-01 00:00:00' THEN p.schedule_tstamp
           ELSE p.last_updated
           END            AS event_tstamp,
       event_tstamp::DATE AS event_date
FROM raw_vault_mvp_dev_robin.cms_mysql.profile p
         INNER JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot sd ON p.id = sd.profile_id
WHERE event_tstamp >= TIMESTAMPADD('day', -1, '2020-04-15 03:00:00'::TIMESTAMP);

SELECT last_updated::DATE,
       count(*)
FROM raw_vault_mvp_dev_robin.cms_mysql.profile
GROUP BY 1;


airflow backfill --start_date '2020-04-15 03:00:00' --end_date '2020-04-16 03:00:00' --task_regex '.*' -m dwh__transactional__user_subscription__daily_at_03h00
airflow backfill --start_date '2020-04-17 03:00:00' --end_date '2020-04-17 03:00:00' --task_regex '.*' dwh__transactional__user_subscription__daily_at_03h00
airflow backfill --start_date '2020-04-18 03:00:00' --end_date '2020-04-18 03:00:00' --task_regex '.*' dwh__transactional__user_subscription__daily_at_03h00
airflow backfill --start_date '2020-04-19 03:00:00' --end_date '2020-04-19 03:00:00' --task_regex '.*' dwh__transactional__user_subscription__daily_at_03h00


SELECT calendar_date, COUNT(DISTINCT user_id), count(*)
FROM se.data.user_subscription
WHERE calendar_date >= '2020-04-01'
GROUP BY 1
ORDER BY 1 DESC;


SELECT user_id,
       count(*)
FROM data_vault_mvp_dev_robin.dwh.user_subscription_event
WHERE event_tstamp >= '2020-04-01'
GROUP BY 1
ORDER BY 2 DESC;

SELECT *
FROM se_dev_robin.data.user_subscription
WHERE user_id = 72349772;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_subscription_event
WHERE user_id = 72349772;

SELECT calendar_date,
       CASE
           WHEN subscription_type = 2 THEN 'daily'
           WHEN subscription_type = 1 THEN 'weekly'
           WHEN subscription_type = 0 THEN 'opt out'
           END                 AS subscription_status,
       count(DISTINCT user_id) AS users
FROM se.data.user_subscription
WHERE calendar_date IN ('2020-03-01', '2020-04-01')
GROUP BY 1, 2;



SELECT count(*)
FROM data_vault_mvp.cms_mysql_snapshots.profile_snapshot;

SELECT COUNT(DISTINCT id)
FROM raw_vault_mvp.cms_mysql.profile;

SELECT COUNT(DISTINCT id)
FROM data_vault_mvp.cms_mysql_snapshots.profile_snapshot;