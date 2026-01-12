SELECT
	qh.query_text,
	qh.query_id,
	qh.query_type,
	qh.warehouse_name,
	qh.role_name,
	qh.execution_status,
	qh.start_time,
	qh.end_time,
	qh.total_elapsed_time,
	qh.execution_time,
	qh.execution_time / 60000 AS execution_time_mins,
	execution_time_mins / 60  AS execution_time_hours,
	qh.queued_overload_time
FROM snowflake.account_usage.query_history qh
WHERE qh.role_name = 'PIPELINERUNNER'
  AND qh.warehouse_name = 'PIPE_XSMALL'
  AND qh.query_type NOT IN ('ROLLBACK', 'DESCRIBE', 'COMMIT')
  AND qh.start_time::DATE = CURRENT_DATE - 2
ORDER BY execution_time DESC
;

USE WAREHOUSE pipe_xlarge
;


SELECT
	eb.event_tstamp::DATE,
	COUNT(DISTINCT eb.shiro_user_id) AS users
FROM hygiene_snapshot_vault_mvp.sfmc.events_bounces eb
WHERE eb.event_tstamp >= CURRENT_DATE - 15
GROUP BY 1
;


SELECT *
FROM data_vault_mvp.dwh.user_recent_activities ura
;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s

SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2')
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW collab.muse.snowflake_query_history_v2 COPY GRANTS
AS
WITH
	dedupe_uac AS (
		SELECT *
		FROM raw_vault_mvp.snowflake_uac.users u
		QUALIFY ROW_NUMBER() OVER (PARTITION BY u.snowflake_user ORDER BY u.loaded_at DESC) = 1
	)
SELECT

	--cost calcs
	2.08::decimal(10, 2)                             AS per_credit_cost, -- cost per credit
	qh.query_id,
	CASE qh.warehouse_size
		WHEN 'X-Small' THEN 1
		WHEN 'Small' THEN 2
		WHEN 'Medium' THEN 4
		WHEN 'Large' THEN 8
		WHEN 'X-Large' THEN 16
		WHEN '2X-Large' THEN 32
		WHEN '3X-Large' THEN 64
		WHEN '4X-Large' THEN 128
		ELSE 0
	END                                              AS credits_per_hour,
	qh.total_elapsed_time / 1000                     AS total_elapsed_time_sec,

	((credits_per_hour * per_credit_cost) / (60 * 60)) -- per second cost
		* total_elapsed_time_sec                     AS cost__query_duration,

	(credits_per_hour / (60 * 60)) -- per second credits
		* total_elapsed_time_sec                     AS credits_used__query_duration,

	qh.credits_used_cloud_services * per_credit_cost AS cost__credits_used_cloud_services,

	qh.query_type,
	--query text grouping
	CASE

		WHEN qh.user_name = 'SNOWPLOW' THEN 'snowplow'

		--modelling dev
		WHEN LOWER(qh.query_text) LIKE '%_dev_robin%' THEN 'warehouse development'
		WHEN LOWER(qh.query_text) LIKE '%_dev_kirsten%' THEN 'warehouse development'
		WHEN LOWER(qh.query_text) LIKE '%_dev_parastou%' THEN 'warehouse development'
		WHEN LOWER(qh.query_text) LIKE '%_dev_saur%' THEN 'warehouse development'
		WHEN LOWER(qh.query_text) LIKE '%_dev_gianni%' THEN 'warehouse development'
		WHEN LOWER(qh.query_text) LIKE '%_dev_donald%' THEN 'warehouse development'

		--dbt
		WHEN qh.role_name NOT IN ('DBT_PROD', 'PERSONAL_ROLE__DBT_PROD') AND qh.warehouse_name LIKE 'DBT_%'
			THEN 'dbt_development'
		WHEN qh.role_name IN ('DBT_PROD', 'PERSONAL_ROLE__DBT_PROD') THEN 'dbt'
		WHEN qh.warehouse_name LIKE '%DBT_%' THEN 'dbt'

		--tableau
		WHEN qh.role_name LIKE 'PERSONAL_ROLE__TABLEAU' THEN 'tableau_analytical'

		--user query
		WHEN LOWER(qh.query_text) LIKE 'create%' AND qh.role_name LIKE ANY ('PERSONAL_ROLE__%', 'QUALITY_ASSURANCE')
			THEN 'user query'
		WHEN LOWER(qh.query_text) LIKE 'select%' AND qh.role_name LIKE ANY ('PERSONAL_ROLE__%', 'QUALITY_ASSURANCE')
			THEN 'user query'
		WHEN LOWER(qh.query_text) LIKE 'with%' AND qh.role_name LIKE ANY ('PERSONAL_ROLE__%', 'QUALITY_ASSURANCE')
			THEN 'user query'
		WHEN LOWER(qh.query_text) LIKE 'set%' AND qh.role_name LIKE ANY ('PERSONAL_ROLE__%', 'QUALITY_ASSURANCE')
			THEN 'user query'

		--shared role
		WHEN qh.role_name = 'SE_BASIC' THEN 'shared role'

		--single customer view
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'dv/dwh/events/%' THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%create table if not exists data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%merge into data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%create or replace view hygiene_vault_mvp.snowplow.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%create or replace transient table hygiene_vault_mvp.snowplow.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%merge into hygiene_vault_mvp.snowplow.event_stream%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%create or replace transient table data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%insert into data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE
			 ('%create or replace view data_vault_mvp.single_customer_view_stg.module_touched_searches__filter_empty_searches%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%delete from data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE
			 ('%create or replace transient table data_vault_mvp.single_customer_view_stg_bak.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%alter table data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg.%')
			THEN 'single_customer_view'
		WHEN LOWER(qh.query_text) LIKE ('%drop table data_vault_mvp.single_customer_view_stg_bak.%')
			THEN 'single_customer_view'


		--uac
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'uac/snowflake/%' THEN 'uac'
		WHEN LOWER(qh.query_text) LIKE '%grant %' THEN 'uac'
		WHEN LOWER(qh.query_text) LIKE '%revoke %' THEN 'uac'
		WHEN LOWER(qh.query_text) LIKE '%pipeline_gsheets.snowflake%' THEN 'uac'

		--admin
		WHEN LOWER(qh.query_text) LIKE '%describe %' THEN 'admin'
		WHEN LOWER(qh.query_text) LIKE 'show %' THEN 'admin'
		WHEN LOWER(qh.query_text) LIKE '%information_schema.%' THEN 'admin'
		WHEN LOWER(qh.query_text) LIKE '%snowflake.account_usage.%' THEN 'admin'
		WHEN LOWER(qh.query_text) LIKE '%collab.muse.snowflake_%' THEN 'admin'
		WHEN LOWER(qh.query_text) LIKE '%drop user if exists%' THEN 'admin'
		WHEN LOWER(qh.query_text) = 'use role pipelinerunner' AND qh.role_name = 'SECURITYADMIN' THEN 'admin'

		--assertions
		WHEN LOWER(qh.query_text) LIKE '%fails_quality_control%' THEN 'assertions'
		WHEN LOWER(qh.query_text) LIKE '%fails___%' THEN 'assertions'

		--data science
		WHEN LOWER(qh.query_text) LIKE '%data_vault_mvp.engagement_stg%' THEN 'engagement_stg.user_snapshot'
		WHEN LOWER(qh.query_text) LIKE '%data_vault.engagement_stg%' THEN 'engagement_stg.user_snapshot'

		WHEN LOWER(qh.query_text) LIKE '%data_science.%' THEN 'data science'
		WHEN LOWER(qh.query_text) LIKE '%se-data-science%' THEN 'data science'
		WHEN qh.role_name = 'DATASCIENCERUNNER' THEN 'data science'

		-- personal ide
		WHEN qh.query_type = 'DESCRIBE' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
		WHEN LOWER(qh.query_text) LIKE 'select system$%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
		WHEN LOWER(qh.query_text) LIKE 'desc function%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
		WHEN LOWER(qh.query_text) LIKE 'use role "%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'
		WHEN LOWER(qh.query_text) LIKE 'select current_available_roles()%' AND qh.role_name LIKE 'PERSONAL_ROLE__%'
			THEN 'personal ide'
		WHEN LOWER(qh.query_text) LIKE 'select 1%' AND qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'personal ide'


		--regular jobs
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'staging/hygiene_snapshots/%' THEN 'regular jobs'
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'staging/hygiene/%' THEN 'regular jobs'
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'dv/travelbird_mysql_snapshots/%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%file format%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%stage%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create schema if not exists%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table hygiene_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table hygiene_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table hygiene_snapshot_vault_mvp.%'
			THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace table raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%truncate table if exists raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists hygiene_snapshot_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%update raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into hygiene_snapshot_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%merge into latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%count(*)%' AND qh.role_name = 'PIPELINERUNNER' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%delete from raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%delete from raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table if exists raw_vault_mvp.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table if exists raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table if exists hygiene_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table if exists latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop view if exists raw_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop view if exists latest_vault.%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%raw_vault%add primary key%' THEN 'regular jobs'
		WHEN LOWER(qh.query_text) LIKE '%unload_vault_mvp%' THEN 'regular jobs'
		WHEN qh.role_name = 'WORKSHEETS_APP_RL' THEN 'regular jobs'


		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'dv/dwh/%' THEN 'warehouse jobs'
		WHEN SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
						'task_catalogue/', -1) LIKE 'dv/finance/%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%create view if not exists data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace transient table data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%create table if not exists data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%create or replace table data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%insert into  data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%merge into data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop view if exists data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop table if exists data_vault_mvp.%' THEN 'warehouse jobs'
		WHEN LOWER(qh.query_text) LIKE '%drop view if exists data_vault_mvp.%' THEN 'warehouse jobs'

		WHEN LOWER(qh.query_text) LIKE '%create or replace view%' THEN 'views'

		WHEN LOWER(qh.query_text) IN ('commit', 'rollback') THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%plain returns%' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%unicode returns%' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%desc table /*%' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%use database%' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%use warehouse%' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE '%PUT%' AND qh.role_name = 'PIPELINERUNNER' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE 'select current_available_roles()%' AND qh.role_name = 'PUBLIC' THEN 'system'
		WHEN LOWER(qh.query_text) LIKE 'select system$list_outbound_shares_details();%' AND qh.role_name = 'PUBLIC'
			THEN 'system'

		WHEN LOWER(qh.query_text) LIKE '%merge into%' THEN 'other merge into'
		WHEN LOWER(qh.query_text) LIKE '%insert into%' THEN 'other insert into'

		--pipeline catch all
		WHEN qh.role_name = 'PIPELINERUNNER' THEN 'regular jobs'

		ELSE 'other'
	END                                              AS query_group,


	LEFT(qh.query_text, 100)                         AS query_text,
	qh.query_text                                    AS query_text_full,

	--warehouse grouping
	CASE
		WHEN qh.warehouse_name LIKE 'DATA_SCIENCE_PIPE%' THEN 'DATA_SCIENCE'
		WHEN qh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN qh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN qh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN qh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN qh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN qh.warehouse_name = 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN qh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN qh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		ELSE 'DATA_PLATFORM_OTHERS'
	END                                              AS warehouse_group,

	--role grouping
	CASE
		WHEN qh.role_name
			IN ('PIPELINERUNNER',
				'DATASCIENCERUNNER',
				'DATASCIENCEAPI',
				'SECURITYADMIN',
				'PERSONAL_ROLE__TABLEAU', 'TABLEAU') THEN qh.role_name
		WHEN qh.role_name IN ('DBT_ANALYST', 'DBT_PRODUCTION') THEN 'DBT'
		WHEN qh.role_name LIKE 'PERSONAL_ROLE__%' THEN 'PERSONAL ROLES'
		ELSE 'OTHER ROLES'
	END                                              AS role_group,

	qh.role_name,
	qh.user_name,
	qh.warehouse_name,
	qh.warehouse_type,
	qh.warehouse_size,
	qh.total_elapsed_time,
	qh.start_time::DATE - 1                          AS tableau_incremental_refresh_date,
	qh.start_time,
	qh.end_time,
	qh.credits_used_cloud_services,
	qh.execution_status,
	COALESCE(u.team, 'unclassified')                 AS team,
	u.position,
	qh.query_tag,
	SPLIT_PART(TRY_PARSE_JSON(qh.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR,
			   'task_catalogue/', -1)                AS pipeline_script_path,
	SPLIT_PART(pipeline_script_path, '/', -1)        AS pipeline_filename,
	qh.bytes_spilled_to_local_storage,
	qh.bytes_spilled_to_remote_storage
FROM snowflake.account_usage.query_history qh
	LEFT JOIN dedupe_uac u ON UPPER(u.snowflake_user) = qh.user_name
-- from first day of previous month
WHERE qh.start_time >= DATEADD(MONTH, -12, DATE_TRUNC('month', CURRENT_DATE()))
;


SELECT *
FROM collab.muse.snowflake_query_history_v2
WHERE bytes_spilled_to_remote_storage > 0