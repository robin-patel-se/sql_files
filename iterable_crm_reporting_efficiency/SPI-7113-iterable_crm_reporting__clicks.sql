USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.iterable
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_click
	CLONE latest_vault.iterable.email_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.in_app_click
	CLONE latest_vault.iterable.in_app_click
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.web_push_clicks
	CLONE latest_vault.iterable.web_push_clicks
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__clicks.py'  --method 'run' --start '2025-07-09 00:00:00' --end '2025-07-09 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__clicks
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__clicks.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'

SELECT *
FROM latest_vault.travelbird_mysql.orders_orderevent
;

