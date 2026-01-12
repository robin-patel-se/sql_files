-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_first_activities.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_first_activities
	CLONE data_vault_mvp.dwh.user_first_activities
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.user_attributes.user_first_activities.py' \
    --method 'run' \
    --start '2025-10-06 00:00:00' \
    --end '2025-10-06 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_first_activities
	CLONE data_vault_mvp.dwh.user_first_activities
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_first_activities_20251003 CLONE data_vault_mvp_dev_robin.dwh.user_first_activities
;

DROP TABLE data_vault_mvp_dev_robin.dwh.user_first_activities
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_first_activities
;


SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_basic_attributes hmtba



SELECT
	mtba.attributed_user_id,
	MIN(mtai.event_tstamp)
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs mtai
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
	ON mtai.touch_id = mtba.touch_id


SELECT
	event_hash,
	touch_id,
	event_tstamp,

	app_install_context,
	idfv,
	aaid,
	attributed,
	event_name,
	existing_user,
	partner_name,
	campaign,
	campaign_type,
	channel,
	branch_ad_format,
	brand,
	model,
	os,
	os_version,
	sdk_version,
	days_from_last_attributed_touch_to_event,
	deep_linked,
	first_event_for_user,
	app_install_channel
FROM data_vault_mvp.single_customer_view_stg.module_touched_app_installs mtai


SELECT
	COALESCE(first_session.shiro_user_id, first_app_session.shiro_user_id,
			 first_app_install.shiro_user_id) AS shiro_user_id,
	first_session.first_session_activity_tstamp,
	first_app_session.first_app_activity_tstamp,
	first_app_session.app_cohort_id,
	first_app_session.app_cohort_year_month,
	first_app_install.first_app_install_tstamp,
	first_app_install.first_app_install_context
FROM data_vault_mvp_dev_robin.dwh.user_first_session first_session
FULL OUTER JOIN data_vault_mvp_dev_robin.dwh.user_first_app_session first_app_session
	ON first_session.shiro_user_id = first_app_session.shiro_user_id
FULL OUTER JOIN data_vault_mvp_dev_robin.dwh.user_first_app_session first_app_install
	ON COALESCE(first_session.shiro_user_id, first_app_session.shiro_user_id) = first_app_install.shiro_user_id


-- need to work out the difference edge cases incremental
-- if they have

------------------------------------------------------------------------------------------------------------------------
-- Post dep steps

-- back up table

-- Change source to historical table
/*
sources = {
	'module_touch_basic_attributes': DBObjectRef(
		db_name='single_customer_view_historical',
		schema_name='unioned_data',
		object_name='historical_module_touch_basic_attributes',
	),
}
*/
-- comment out incremental in step 1

-- drop dev table
/*
 DROP TABLE data_vault_mvp_dev_robin.dwh.user_first_activities
;
 */

-- run model

-- self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_activities.py'  --method 'run' --start '2025-10-02 00:00:00' --end '2025-10-02 00:00:00'

-- swap dev and prod table

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_first_activities
WHERE first_app_install_tstamp IS NOT NULL
;

DROP SCHEMA data_vault_mvp_dev_robin.dwh
;


------------------------------------------------------------------------------------------------------------------------

module=/biapp/task_catalogue/dv/dwh/user_attributes/user_first_session.py make clones
module=/biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_session.py make clones
module=/biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_install.py make clones
module=/biapp/task_catalogue/dv/dwh/user_attributes/user_first_activities.py make clones



self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_install.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_install.py'  --method 'run' --start '2025-10-05 00:00:00' --end '2025-10-05 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_session.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_app_session.py'  --method 'run' --start '2025-10-05 00:00:00' --end '2025-10-05 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_session.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_session.py'  --method 'run' --start '2025-10-05 00:00:00' --end '2025-10-05 00:00:00'

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_first_activities.py'  --method 'run' --start '2025-10-05 00:00:00' --end '2025-10-05 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_first_app_install
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_first_app_session
;


SELECT
	DATE_TRUNC(YEAR, first_app_activity_tstamp),
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_first_activities
GROUP BY 1
;

SELECT
	DATE_TRUNC(YEAR, first_app_activity_tstamp),
	COUNT(*)
FROM data_vault_mvp.dwh.user_first_activities
GROUP BY 1
;

USE ROLE pipelinerunner
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_first_activities_20251006 CLONE data_vault_mvp.dwh.user_first_activities;


SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba


./scripts/mwaa-cli production "dags backfill --start-date '1969-12-31 00:00:00' --end-date '1970-01-01 00:00:00' --donot-pickle dwh__user_first_activities__daily_at_03h00"

SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	shiro_user_id,
	first_session_activity_tstamp,
	first_app_activity_tstamp,
	app_cohort_id,
	app_cohort_year_month,
	first_app_install_tstamp,
	first_app_install_context
FROM data_vault_mvp.dwh.user_first_activities ufa;

SELECT * FROM se.bi.search_model sm