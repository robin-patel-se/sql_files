CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails CLONE data_vault_mvp.dwh.user_emails
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_activity_one_day.py'  --method 'run' --start '2023-10-29 00:00:00' --end '2023-10-29 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.user_activity_daily
;

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.user_activity_daily
(

	-- (lineage) metadata for the current job
	schedule_tstamp TIMESTAMP,
	run_tstamp      TIMESTAMP,
	operation_id    VARCHAR,
	created_at      TIMESTAMP,
	updated_at      TIMESTAMP,

	date            DATE,
	shiro_user_id   INT,
	web_sessions_1d INT,
	app_sessions_1d INT,
	emails_1d       INT,

	CONSTRAINT pk_1 PRIMARY KEY (date, shiro_user_id)
)
	CLUSTER BY (date)
;

INSERT INTO data_vault_mvp_dev_robin.dwh.user_activity_daily
SELECT
	ua.schedule_tstamp,
	ua.run_tstamp,
	ua.operation_id,
	ua.created_at,
	ua.updated_at,
	ua.date,
	ua.shiro_user_id,
	ua.web_sessions_1d,
	ua.app_sessions_1d,
	ua.emails_1d
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.date IS NOT NULL
  AND ua.shiro_user_id IS NOT NULL
;

SELECT *
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.web_sessions_90d > 30
  AND ua.app_sessions_90d > 30
  AND ua.date = CURRENT_DATE - 1
;

SELECT *
FROM data_vault_mvp.dwh.user_activity ua
WHERE ua.shiro_user_id = 1914508 AND ua.date >= CURRENT_DATE - 30

SELECT
	uad.date,
	uad.shiro_user_id,
	uad.web_sessions_1d,
	SUM(uad.web_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW )   AS web_sessions_7d,
	SUM(uad.web_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW )  AS web_sessions_14d,
	SUM(uad.web_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW )  AS web_sessions_30d,
	SUM(uad.web_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW )  AS web_sessions_90d,
	SUM(uad.web_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 365 PRECEDING AND CURRENT ROW ) AS web_sessions_365d,
	uad.app_sessions_1d,
	SUM(uad.app_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW )   AS app_sessions_7d,
	SUM(uad.app_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW )  AS app_sessions_14d,
	SUM(uad.app_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW )  AS app_sessions_30d,
	SUM(uad.app_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW )  AS app_sessions_90d,
	SUM(uad.app_sessions_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 365 PRECEDING AND CURRENT ROW ) AS app_sessions_365d,
	uad.emails_1d,
	SUM(uad.emails_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW )   AS emails_7d,
	SUM(uad.emails_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW )  AS emails_14d,
	SUM(uad.emails_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW )  AS emails_30d,
	SUM(uad.emails_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW )  AS emails_90d,
	SUM(uad.emails_1d)
		OVER (PARTITION BY uad.shiro_user_id ORDER BY uad.date ROWS BETWEEN 365 PRECEDING AND CURRENT ROW ) AS emails_365d
FROM data_vault_mvp_dev_robin.dwh.user_activity_daily uad
-- WHERE uad.shiro_user_id = 1914508
WHERE uad.shiro_user_id = 10527537
;

DROP TABLE data_vault_mvp_dev_robin.dwh.user_activity
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_activity
;


USE ROLE pipelinerunner
;

UPDATE data_vault_mvp.dwh.iterable__catalogue_product target
SET target.updated_at = CURRENT_TIMESTAMP::TIMESTAMP
WHERE target.sale_active
;

SELECT *
FROM data_vault_mvp.dwh.iterable__catalogue_product icp
WHERE icp.se_sale_id = 'A38529'
;

SELECT
	record['saleActive']::VARCHAR,
	record
FROM unload_vault_mvp.iterable.catalogue_product__20231031t090000__hourly
WHERE record['seSaleId']::VARCHAR = 'A38529'

;

SELECT DISTINCT
	record['seSaleId']::VARCHAR
FROM unload_vault_mvp.iterable.catalogue_product__20231031t090000__hourly
WHERE record['deactivatedReason']::VARCHAR IS DISTINCT FROM 'none'
;

SELECT
	COUNT(*)
FROM unload_vault_mvp.iterable.catalogue_product__20231031t090000__hourly
;