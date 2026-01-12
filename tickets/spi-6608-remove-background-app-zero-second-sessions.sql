USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.05_touch_basic_attributes.01_module_touch_basic_attributes.py' \
    --method 'run' \
    --start '2024-10-15 00:00:00' \
    --end '2024-10-15 00:00:00'

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	ADD COLUMN is_app_background_session BOOLEAN;


USE ROLE pipelinerunner;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
	ADD COLUMN is_app_background_session BOOLEAN;