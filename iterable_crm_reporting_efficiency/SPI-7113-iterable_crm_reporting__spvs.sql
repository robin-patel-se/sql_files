USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;


CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;


self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting__spvs.py' \
    --method 'run' \
    --start '2025-07-16 00:00:00' \
    --end '2025-07-16 00:00:00';

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs__step05__aggregate_spvs')
;

CREATE OR REPLACE TRANSIENT TABLE iterable_crm_reporting__spvs__step05__aggregate_spvs
(
	message_id_email_hash VARCHAR,
	message_id            VARCHAR,
	campaign_id           VARCHAR,
	send_event_date       DATE,
	shiro_user_id         VARCHAR,
	spvs_same_day_lc      NUMBER,
	spvs_1d_lc            NUMBER,
	spvs_7d_lc            NUMBER,
	spvs_28d_lc           NUMBER,
	spvs_same_day_lnd     NUMBER,
	spvs_1d_lnd           NUMBER,
	spvs_7d_lnd           NUMBER,
	spvs_28d_lnd          NUMBER,
	spvs_same_day_url     NUMBER,
	spvs_1d_url           NUMBER,
	spvs_7d_url           NUMBER,
	spvs_28d_url          NUMBER
)
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
;


self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__spvs.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__spvs.py'  --method 'run' --start '2025-07-15 00:00:00' --end '2025-07-15 00:00:00'

USE ROLE pipelinerunner
;

USE ROLE personal_role__robinpatel
;

USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__spvs
WHERE send_event_date = '2025-01-09' AND message_id = '2f1c64ced4034be2a4978757c1e981f6'
;

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.send_event_date = '2025-01-09' AND
	  icr.message_id = '2f1c64ced4034be2a4978757c1e981f6' AND
	  icr.campaign_id = 3677373