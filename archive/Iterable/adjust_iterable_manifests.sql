CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_bounce CLONE raw_vault.iterable.email_bounce;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_click CLONE raw_vault.iterable.email_click
CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_open CLONE raw_vault.iterable.email_open;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_send CLONE raw_vault.iterable.email_send;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_unsubscribe CLONE raw_vault.iterable.email_unsubscribe;

CREATE SCHEMA hygiene_vault_dev_robin.iterable;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_bounce CLONE hygiene_vault.iterable.email_bounce;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_click CLONE hygiene_vault.iterable.email_click
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_complaint CLONE hygiene_vault.iterable.email_complaint;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_open CLONE hygiene_vault.iterable.email_open;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_send CLONE hygiene_vault.iterable.email_send;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_send_skip CLONE hygiene_vault.iterable.email_send_skip;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_subscribe CLONE hygiene_vault.iterable.email_subscribe;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_unsubscribe CLONE hygiene_vault.iterable.email_unsubscribe;

-- 2021-11-03


dataset_task --include 'iterable.email_bounce' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_bounce eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_bounce eb;


dataset_task --include 'iterable.email_click' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_click eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_click eb;


dataset_task --include 'iterable.email_open' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_open eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_open eb;

dataset_task --include 'iterable.email_send' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_send eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_send eb;

dataset_task --include 'iterable.email_complaint' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_complaint eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_complaint eb;

dataset_task --include 'iterable.email_unsubscribe' --operation LatestRecordsOperation --method 'run'  --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT COUNT(*) FROM latest_vault.iterable.email_unsubscribe eb;
SELECT COUNT(*) FROM latest_vault_dev_robin.iterable.email_unsubscribe eb;



CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_bounce_20211118 CLONE latest_vault.iterable.email_bounce;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_click_20211118 CLONE latest_vault.iterable.email_click;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_complaint_20211118 CLONE latest_vault.iterable.email_complaint;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_open_20211118 CLONE latest_vault.iterable.email_open;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_send_20211118 CLONE latest_vault.iterable.email_send;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_send_skip_20211118 CLONE latest_vault.iterable.email_send_skip;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_subscribe_20211118 CLONE latest_vault.iterable.email_subscribe;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.email_unsubscribe_20211118 CLONE latest_vault.iterable.email_unsubscribe;

DROP TABLE latest_vault.iterable.email_bounce;
DROP TABLE latest_vault.iterable.email_click;
DROP TABLE latest_vault.iterable.email_complaint;
DROP TABLE latest_vault.iterable.email_open;
DROP TABLE latest_vault.iterable.email_send;
DROP TABLE latest_vault.iterable.email_send_skip;
DROP TABLE latest_vault.iterable.email_subscribe;
DROP TABLE latest_vault.iterable.email_unsubscribe;



airflow backfill --start_date '2021-09-09 16:00:00' --end_date '2021-09-09 16:00:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_bounce' incoming__iterable__email_bounce__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_click' incoming__iterable__email_click__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_complaint' incoming__iterable__email_complaint__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_open' incoming__iterable__email_open__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_send' incoming__iterable__email_send__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_send_skip' incoming__iterable__email_send_skip__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_subscribe' incoming__iterable__email_subscribe__hourly
airflow backfill --start_date '2021-11-03 00:30:00' --end_date '2021-11-03 00:30:00' --reset_dagruns --task_regex 'LatestRecordsOperation__incoming__iterable__email_unsubscribe' incoming__iterable__email_unsubscribe__hourly
