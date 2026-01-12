SELECT *
FROM latest_vault.iterable.email_send es
;

SELECT *
FROM latest_vault.iterable.app_push_send aps
;

SELECT *
FROM latest_vault.iterable.in_app_send ias
;

SELECT *
FROM latest_vault.iterable.web_push_send wps
;

-- campaign ids appear to have an overlap


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign
	CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_send
	CLONE latest_vault.iterable.email_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.app_push_send
	CLONE latest_vault.iterable.app_push_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_send
	CLONE latest_vault.iterable.in_app_send
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.web_push_send
	CLONE latest_vault.iterable.web_push_send
;

-- moving sends modelling into separate job.


SELECT
	crm_channel_type,
	campaign_id,
	catalog_collection_count,
	catalog_lookup_count,
	channel_id,
	content_id,
	send_event_date,
	send_event_time,
	message_id,
	email_hash,
	message_id_email_hash,
	message_type_id,
	product_recommendation_count,
	template_id,
	send_start_date
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends__step02__stack_sends

SELECT
	crm_channel_type,
	message_id_email_hash,
	message_id,
	campaign_id,
	email_hash,
	send_event_date,
	send_event_time,
	send_start_date,
	lead_event_date,
	send_end_date
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends__step03__compute_lead_date send_events
	LEFT JOIN latest_vault_dev_robin.iterable.campaign campaigns ON send_events.campaign_id = campaigns.id
WHERE campaigns.id IS NULL


SELECT
	ua.membership_account_status,
	COUNT(*)
FROM data_vault_mvp.dwh.user_attributes ua
GROUP BY 1
;

SELECT
	ua.email,
	ua.membership_account_status
FROM data_vault_mvp.dwh.user_attributes ua
WHERE ua.membership_account_status = 'DELETED'


SELECT
	crm_channel_type,
	message_id_email_hash,
	message_id,
	campaign_id,
	email_hash,
	send_event_date,
	send_event_time,
	send_start_date,
	lead_event_date,
	send_end_date,
	campaign_name,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	mapped_campaign,
	mapped_theme,
	mapped_segment,
	is_athena,
	is_automated_campaign,
	ame_calculated_campaign_name,
	shiro_user_id,
	current_affiliate_territory
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends__step04__enrich_sends

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends__step04__enrich_sends')
;


CREATE OR REPLACE TRANSIENT TABLE iterable_crm_reporting__sends__step04__enrich_sends
(
	message_id_email_hash        VARCHAR,
	email_hash                   VARCHAR,
	message_id                   VARCHAR,
	campaign_id                  NUMBER,
	crm_channel_type             VARCHAR,
	send_event_date              DATE,
	send_event_time              TIMESTAMP_NTZ,
	send_start_date              DATE,
	send_end_date                DATE,
	campaign_name                VARCHAR,
	splittable_email_name        VARCHAR,
	mapped_crm_date              VARCHAR,
	mapped_territory             VARCHAR,
	mapped_objective             VARCHAR,
	mapped_platform              VARCHAR,
	mapped_campaign              VARCHAR,
	mapped_theme                 VARCHAR,
	mapped_segment               VARCHAR,
	is_athena                    BOOLEAN,
	is_automated_campaign        BOOLEAN,
	ame_calculated_campaign_name VARCHAR,
	shiro_user_id                NUMBER,
	current_affiliate_territory  VARCHAR
)
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py'  --method 'run' --start '2025-07-15 00:00:00' --end '2025-07-15 00:00:00'


SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,
	campaign_id,
	catalog_collection_count,
	catalog_lookup_count,
	channel_id,
	content_id,
	event_created_at,
	email,
	message_bus_id,
	message_id,
	message_type_id,
	product_recommendation_count,
	template_id,
	record
FROM latest_vault_dev_robin.iterable.email_send
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
;


SELECT
	COUNT(*),
	MIN(es.event_created_at)
FROM latest_vault.iterable.email_send es
;

9,862,061,726

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends icrs

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'


SELECT COUNT(*) FROm data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends icrs;
10,171,549,792
SELECT COUNT(*) FROm data_vault_mvp.dwh.iterable_crm_reporting icr;
10,160,217,863


self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__sends.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__opens.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__opens;
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__clicks.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks;
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__unsubs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__unsubs;
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings;
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__spvs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs;
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__migration.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__migration;