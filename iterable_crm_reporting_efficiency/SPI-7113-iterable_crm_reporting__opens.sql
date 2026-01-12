USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open
	CLONE latest_vault.iterable.email_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_open
	CLONE latest_vault.iterable.app_push_open
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_open
	CLONE latest_vault.iterable.in_app_open
;

-- CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.iterable_crm_reporting__sends
-- CLONE latest_vault.iterable.iterable_crm_reporting__sends;


SELECT
	message_id_email_hash,
	message_id,
	campaign_id,
	crm_channel_type,
	first_open_event_date,
	first_open_event_time,
	email_opens_1d,
	email_opens_7d,
	email_opens_14d,
	unique_email_opens,
	unique_email_opens_1d,
	unique_email_opens_7d,
	unique_email_opens_14d
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens__step02__model_data

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens__step02__model_data')
;

CREATE OR REPLACE TRANSIENT TABLE iterable_crm_reporting__opens__step02__model_data
(
	message_id_email_hash  VARCHAR,
	message_id             VARCHAR,
	campaign_id            NUMBER,
	crm_channel_type       VARCHAR,
	first_open_event_date  DATE,
	first_open_event_time  TIMESTAMP,
	email_opens_1d         NUMBER,
	email_opens_7d         NUMBER,
	email_opens_14d        NUMBER,
	unique_email_opens     NUMBER,
	unique_email_opens_1d  NUMBER,
	unique_email_opens_7d  NUMBER,
	unique_email_opens_14d NUMBER
)
;
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens;

SELECT MIN(event_created_at) FROM latest_vault.iterable.email_open eo;

2021-11-03 12:00:04.000000000

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__opens.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__opens.py'  --method 'run' --start '2025-07-15 00:00:00' --end '2025-07-15 00:00:00'

SELECT * FROm data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens