SELECT *
FROM hygiene_vault.iterable.email_open
WHERE event_created_at >= CURRENT_DATE - 1
;


/*
{
  "campaignId": 14188929,
  "city": "Frankfurt am Main",
  "contentId": 73096036,
  "country": "Germany",
  "createdAt": "2025-07-29 00:27:35 +00:00",
  "email": "drews@convestberlin.de",
  "ip": "172.226.110.22",
  "isBot": true,
  "messageId": "ca335224ac854eefa749432f5937a9d0",
  "proxySource": "Apple",
  "region": "HE",
  "templateId": 18527163,
  "userAgent": "Mozilla/5.0",
  "userAgentDevice": "Other",
  "userId": "34735281"
}
*/


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS raw_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.iterable.email_open
	CLONE raw_vault.iterable.email_open
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.email_open
	CLONE hygiene_vault.iterable.email_open
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open
	CLONE latest_vault.iterable.email_open
;

/*
dataset_task \
    --include 'incoming.iterable.email_open' \
    --kind 'incoming' \
    --operation HygieneOperation \
    --method 'run' \
    --upstream \
    --start '2025-04-2 00:00:00' \
    --end '2025-04-2 00:00:00'
  */

USE WAREHOUSE pipe_xlarge
;

SELECT
	MIN(event_created_at)
FROM hygiene_vault.iterable.email_open
WHERE record['isBot']::BOOLEAN IS NOT NULL
;

-- 2025-04-22 19:27:55.000000000;


ALTER TABLE hygiene_vault_dev_robin.iterable.email_open
	ADD COLUMN is_bot BOOLEAN
;

SELECT *
FROM hygiene_vault_dev_robin.iterable.email_open
;


./
scripts/
mwaa-cli production "dags backfill --m --start-date '2021-11-03 04:30:00' --end-date '2021-11-03 04:30:00' --donot-pickle dwh__iterable_crm_reporting__daily_at_04h30"



USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_open
	CLONE latest_vault.iterable.app_push_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open
	CLONE latest_vault.iterable.email_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_open
	CLONE latest_vault.iterable.in_app_open
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__sends
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__opens
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting__opens.py' \
    --method 'run' \
    --start '2025-07-30 00:00:00' \
    --end '2025-07-30 00:00:00'


ALTER TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
	ADD COLUMN
			email_opens_bot_same_day INTEGER,
		email_opens_bot_1d INTEGER,
		email_opens_bot_7d INTEGER,
		email_opens_bot_28d INTEGER,
		unique_email_opens_bot_same_day INTEGER,
		unique_email_opens_bot_1d INTEGER,
		unique_email_opens_bot_7d INTEGER,
		unique_email_opens_bot_28d INTEGER
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens
;



DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting
WHERE iterable_crm_reporting.email_opens_bot_1d > 0;


SELECT * FROM hygiene_vault.iterable.app_users;

USE ROLE pipelinerunner;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__opens_20250730 CLONE data_vault_mvp.dwh.iterable_crm_reporting__opens;
DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__opens;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting_20250730 CLONE data_vault_mvp.dwh.iterable_crm_reporting;

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting;