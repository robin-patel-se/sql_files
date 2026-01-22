/*
./scripts/mwaa-cli production "dags backfill --start-date '2022-11-30 00:00:00' --end-date '2022-12-01 00:00:00' --donot-pickle bi__session_metrics__daily_at_03h30"
*/

SELECT
	mtpbc.voucher_transaction_flag
FROM data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks mtpbc
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_events_of_interest meoi
WHERE meoi.event_category = 'pay_button_click'



USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation
	CLONE latest_vault.cms_mysql.product_reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation
	CLONE latest_vault.cms_mysql.reservation
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
-- CLONE data_vault_mvp.bi.session_metrics__events_of_interest;

DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
;

DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics__events_of_interest.py' \
    --method 'run' \
    --start '2026-01-22 00:00:00' \
    --end '2026-01-22 00:00:00'


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
-- CLONE data_vault_mvp.bi.session_metrics__events_of_interest;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__feature_flags
	CLONE data_vault_mvp.bi.session_metrics__feature_flags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types
	CLONE data_vault_mvp.bi.session_metrics__login_types
;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.data
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics
	CLONE data_vault_mvp.bi.session_metrics
;

DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics.py' \
    --method 'run' \
    --start '2026-01-22 00:00:00' \
    --end '2026-01-22 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics
WHERE session_metrics.has_voucher_pay_button_click
;


SELECT *
FROM data_vault_mvp.bi.session_metrics sm
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_metrics_20260122 CLONE data_vault_mvp.bi.session_metrics
;

DROP TABLE data_vault_mvp.bi.session_metrics
;

SELECT
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE touch_start_tstamp::DATE >= '2026-01-01'
GROUP BY ALL
;

SELECT
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.bi.session_metrics mtba
WHERE touch_start_tstamp::DATE >= '2026-01-01'
GROUP BY ALL
;

SELECT
	sm.landing_screen_category,
	COUNT(*)
FROM se.bi.session_metrics sm
WHERE touch_landing_screen_view_name IS NOT NULL
GROUP BY ALL
;


SELECT * FROM se.bi.session_metrics sm WHERE sm.PAY_BUTTON_CLICKS_NON_VOUCHER;