SELECT *
FROM collab.muse.snowflake_query_history_v2 s
;

SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2')
;

USE ROLE pipelinerunner
;

GRANT SELECT ON TABLE collab.muse.snowflake_query_history_v2 TO ROLE data_team_basic
;

CREATE OR REPLACE VIEW collab.muse.snowflake_query_history_v2
	COPY GRANTS AS
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
	SPLIT_PART(pipeline_script_path, '/', -1)        AS pipeline_filename
FROM snowflake.account_usage.query_history qh
	LEFT JOIN dedupe_uac u ON UPPER(u.snowflake_user) = qh.user_name
	-- from first day of previous month
WHERE qh.start_time >= DATEADD(MONTH, -12, DATE_TRUNC('month', CURRENT_DATE()))
;

SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.query_group = 'single_customer_view'
  AND s.pipeline_script_path LIKE '%trimmed%'
  AND s.start_time::DATE = CURRENT_DATE
;
------------------------------------------------------------------------------------------------------------------------

SELECT
	qh.start_time::DATE AS date,
	qh.query_group,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= CURRENT_DATE - 30
GROUP BY 1, 2
;


SELECT
	qh.start_time::DATE AS date,
	qh.query_group,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= '2023-01-01'
  AND qh.query_group = 'single_customer_view'
GROUP BY 1, 2
;

SELECT
	DATE_TRUNC(WEEK, qh.start_time) AS week,
	qh.query_group,
	SUM(qh.cost__query_duration)    AS total_cost,
	total_cost / 7                  AS avg_daily_cost
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= '2023-01-01'
  AND qh.query_group = 'single_customer_view'
GROUP BY 1, 2
;


SELECT
	mdh.service_type,
	mdh.usage_date,
	mdh.credits_used_compute,
	mdh.credits_used_cloud_services,
	mdh.credits_used, -- comparative to Snowflake
	mdh.credits_adjustment_cloud_services,
	mdh.credits_billed,
	mdh.credits_billed * 2.08 AS compute_cost
FROM snowflake.account_usage.metering_daily_history mdh
WHERE mdh.usage_date >= '2023-02-28'
ORDER BY mdh.usage_date
;



SELECT *
FROM collab.muse.snowflake_query_history_v2 s
;


SELECT
	qh.start_time::DATE AS date,
	qh.query_group,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= '2023-01-01'
GROUP BY 1, 2
;


-- spike in other insert into
SELECT *
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= '2023-03-01'
  AND qh.query_group = 'other insert into'
ORDER BY cost__query_duration DESC
;

SELECT
	TRY_PARSE_JSON(s.query_tag),
	SPLIT_PART(TRY_PARSE_JSON(s.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR, 'task_catalogue/',
			   -1),
	*
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE
  AND TRY_PARSE_JSON(s.query_tag) IS NOT NULL
  AND SPLIT_PART(TRY_PARSE_JSON(s.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR, 'task_catalogue/',
				 -1) != 'uac/snowflake/role_privilege_management.py'


SELECT
	s.start_time::DATE          AS date,
	SPLIT_PART(TRY_PARSE_JSON(s.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR, 'task_catalogue/',
			   -1)              AS path,
	s.query_group,
	SUM(s.cost__query_duration) AS cost
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time BETWEEN '2023-01-01' AND '2023-03-31'
  AND TRY_PARSE_JSON(s.query_tag) IS NOT NULL
  AND SPLIT_PART(TRY_PARSE_JSON(s.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR, 'task_catalogue/',
				 -1) != 'uac/snowflake/role_privilege_management.py'
GROUP BY 1, 2, 3
ORDER BY 4 DESC
;


------------------------------------------------------------------------------------------------------------------------


-- investigate dbt pr runs

SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 30
  AND s.query_group LIKE 'dbt%'
  AND s.query_type NOT IN (
						   'ALTER_SESSION',
						   'USE'
	)
;


SELECT
	s.start_time::DATE                                                  AS date,
	s.query_group,
	IFF(LOWER(s.query_text_full) LIKE '%dbt_cloud_pr%', 'pr', 'non-pr') AS pr_status,
	COUNT(*),
	SUM(s.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 30
  AND s.query_group LIKE 'dbt%'
GROUP BY 1, 2, 3
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	qh.start_time::DATE AS date,
--     qh.query_group,
	qh.warehouse_name,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.start_time >= '2023-01-01'
GROUP BY 1, 2--, 3
;

------------------------------------------------------------------------------------------------------------------------
-- compare metering costs with query history

WITH
	stack AS (
		SELECT
			mh.usage_date,
			'metering_daily_history' AS source,
			SUM(mh.credits_billed)   AS credits_billed
		FROM snowflake.account_usage.metering_daily_history mh
		GROUP BY 1, 2

		UNION ALL

		SELECT
			qh.start_time::DATE AS usage_date,
			'query_history'     AS source,
			SUM(
						(CASE qh.warehouse_size
							 WHEN 'X-Small' THEN 1
							 WHEN 'Small' THEN 2
							 WHEN 'Medium' THEN 4
							 WHEN 'Large' THEN 8
							 WHEN 'X-Large' THEN 16
							 WHEN '2X-Large' THEN 32
							 WHEN '3X-Large' THEN 64
							 WHEN '4X-Large' THEN 128
							 ELSE 0
						 END / (60 * 60)) * (qh.total_elapsed_time / 1000) --AS credits_per_hour,
				)               AS credits_billed
		FROM snowflake.account_usage.query_history qh
		GROUP BY 1, 2
	)

SELECT
	s.usage_date,
	SUM(IFF(s.source = 'metering_daily_history', s.credits_billed, NULL)) AS metering_daily_history_credits,
	SUM(IFF(s.source = 'query_history', s.credits_billed, NULL))          AS query_history_credits
FROM stack s
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------

-- account_usage
SELECT
	mdh.usage_date,
	SUM(mdh.credits_billed)
FROM snowflake.account_usage.metering_daily_history mdh
WHERE mdh.usage_date >= '2022-04-01'
GROUP BY 1
ORDER BY usage_date
;
-- organization_usage
SELECT
	mdh.usage_date,
	SUM(mdh.credits_billed)
FROM snowflake.organization_usage.metering_daily_history mdh
WHERE mdh.usage_date >= '2022-04-01'
GROUP BY 1
ORDER BY usage_date
;

------------------------------------------------------------------------------------------------------------------------

-- looks like increase in dbt, single customer view and user query query groups

SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE DATE_TRUNC('month', s.start_time) = '2023-01-01'
  AND s.query_group IN ('dbt', 'single_customer_view', 'user query')
ORDER BY cost__query_duration DESC
;

-- investigate tableau usage in user_query

SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE DATE_TRUNC('month', s.start_time) = '2023-01-01'
  AND s.query_group = 'user query'
  AND s.user_name = 'TABLEAU'


------------------------------------------------------------------------------------------------------------------------

SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2')
;

CREATE OR REPLACE VIEW snowflake_query_history_v2
			(
			 per_credit_cost,
			 query_id,
			 credits_per_hour,
			 total_elapsed_time_sec,
			 cost__query_duration,
			 credits_used__query_duration,
			 cost__credits_used_cloud_services,
			 query_type,
			 query_group,
			 query_text,
			 query_text_full,
			 warehouse_group,
			 role_group,
			 role_name,
			 user_name,
			 warehouse_name,
			 warehouse_type,
			 warehouse_size,
			 total_elapsed_time,
			 tableau_incremental_refresh_date,
			 start_time,
			 end_time,
			 credits_used_cloud_services,
			 execution_status,
			 team,
			 position,
			 query_tag,
			 pipeline_script_path,
			 pipeline_filename
				)
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
	SPLIT_PART(pipeline_script_path, '/', -1)        AS pipeline_filename
FROM snowflake.account_usage.query_history qh
	LEFT JOIN dedupe_uac u ON UPPER(u.snowflake_user) = qh.user_name
	-- from first day of previous month
WHERE qh.start_time >= DATEADD(MONTH, -12, DATE_TRUNC('month', CURRENT_DATE()))
;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.warehouse_group = 'DBT'

------------------------------------------------------------------------------------------------------------------------


SELECT
	DATE_TRUNC(MONTH, mdh.usage_date) AS month,
	SUM(mdh.credits_billed)           AS compute_credits,
	SUM(mdh.credits_billed * 2.08)    AS compute_cost
FROM snowflake.account_usage.metering_daily_history mdh
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, s.start_time)     AS month,
	SUM(s.credits_used__query_duration) AS compute_credits,
	SUM(s.cost__query_duration)         AS compute_cost
FROM collab.muse.snowflake_query_history_v2 s
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, mdh.usage_date) AS month,
	SUM(mdh.credits_billed)           AS compute_credits,
	SUM(mdh.credits_billed * 2.08)    AS compute_cost
FROM snowflake.account_usage.metering_daily_history mdh
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, mdh.start_time) AS month,
	SUM(mdh.credits_used)             AS compute_credits,
	SUM(mdh.credits_used * 2.08)      AS compute_cost
FROM snowflake.account_usage.warehouse_metering_history mdh
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, s.start_time)     AS month,
	SUM(s.credits_used__query_duration) AS compute_credits,
	SUM(s.cost__query_duration)         AS compute_cost
FROM collab.muse.snowflake_query_history_v2 s
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, mdh.start_time) AS month,
	SUM(mdh.credits_used)             AS compute_credits,
	SUM(mdh.credits_used * 2.08)      AS compute_cost
FROM snowflake.account_usage.warehouse_metering_history mdh
WHERE mdh.warehouse_name LIKE 'DBT%'
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, s.start_time)     AS month,
	SUM(s.credits_used__query_duration) AS compute_credits,
	SUM(s.cost__query_duration)         AS compute_cost
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.warehouse_name LIKE 'DBT%'
GROUP BY 1
;


SELECT *
FROM collab.muse.snowflake_query_history_v2 s
;

SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2')
;

SELECT
	DATE_TRUNC(MONTH, mdh.start_time) AS month,
	SUM(mdh.credits_used)             AS compute_credits,
	SUM(mdh.credits_used * 2.08)      AS compute_cost
FROM snowflake.account_usage.warehouse_metering_history mdh
WHERE UPPER(mdh.warehouse_name) LIKE '%DBT%'
GROUP BY 1
;

SELECT
	DATE_TRUNC(MONTH, s.start_time)     AS month,
	SUM(s.credits_used__query_duration) AS compute_credits,
	SUM(s.cost__query_duration)         AS compute_cost
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.query_group = 'dbt'
GROUP BY 1
;



SELECT
	wmh.credits_used,
	wmh.credits_used * 2.08 AS cost,
	wmh.credits_used_cloud_services,
	wmh.credits_used_compute,
	wmh.end_time,
	wmh.start_time,
	wmh.warehouse_id,
	wmh.warehouse_name,
	CASE
		WHEN wmh.warehouse_name LIKE 'DATA_SCIENCE%' THEN 'DATA_SCIENCE'
		WHEN wmh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN wmh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN wmh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN wmh.warehouse_name LIKE 'CUSTOMER_INSIGHT%'
			THEN 'CUSTOMER_INSIGHT' --note below DBT because CI have dbt warehouses
		WHEN wmh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN wmh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN wmh.warehouse_name = 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN wmh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'CLOUD_SERVICES_ONLY' THEN 'CLOUD_SERVICES_ONLY'
		ELSE 'DATA_PLATFORM_OTHERS'
	END                     AS warehouse_group
FROM snowflake.account_usage.warehouse_metering_history wmh
;


SELECT DISTINCT
	warehouse_name,
	CASE
		WHEN wmh.warehouse_name LIKE 'DATA_SCIENCE%' THEN 'DATA_SCIENCE'
		WHEN wmh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN wmh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN wmh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN wmh.warehouse_name LIKE 'CUSTOMER_INSIGHT%'
			THEN 'CUSTOMER_INSIGHT' --note below DBT because CI have dbt warehouses
		WHEN wmh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN wmh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN wmh.warehouse_name LIKE 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN wmh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'CLOUD_SERVICES_ONLY' THEN 'CLOUD_SERVICES_ONLY'
		ELSE 'DATA_PLATFORM_OTHERS'
	END AS warehouse_group
FROM snowflake.account_usage.warehouse_metering_history wmh
;


USE ROLE pipelinerunner
;

CREATE OR REPLACE VIEW collab.muse.snowflake_query_history_v2
			(
			 per_credit_cost,
			 query_id,
			 credits_per_hour,
			 total_elapsed_time_sec,
			 cost__query_duration,
			 credits_used__query_duration,
			 cost__credits_used_cloud_services,
			 query_type,
			 query_group,
			 query_text,
			 query_text_full,
			 warehouse_group,
			 role_group,
			 role_name,
			 user_name,
			 warehouse_name,
			 warehouse_type,
			 warehouse_size,
			 total_elapsed_time,
			 tableau_incremental_refresh_date,
			 start_time,
			 end_time,
			 credits_used_cloud_services,
			 execution_status,
			 team,
			 position,
			 query_tag,
			 pipeline_script_path,
			 pipeline_filename
				)
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
		WHEN qh.warehouse_name LIKE 'DATA_SCIENCE%' THEN 'DATA_SCIENCE'
		WHEN qh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN qh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN qh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN qh.warehouse_name LIKE 'CUSTOMER_INSIGHT%'
			THEN 'CUSTOMER_INSIGHT' --note below DBT because CI have dbt warehouses
		WHEN qh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN qh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN qh.warehouse_name LIKE 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN qh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN qh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN qh.warehouse_name = 'CLOUD_SERVICES_ONLY' THEN 'CLOUD_SERVICES_ONLY'
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
	SPLIT_PART(pipeline_script_path, '/', -1)        AS pipeline_filename
FROM snowflake.account_usage.query_history qh
	LEFT JOIN dedupe_uac u ON UPPER(u.snowflake_user) = qh.user_name
	-- from first day of previous month
WHERE qh.start_time >= DATEADD(MONTH, -12, DATE_TRUNC('month', CURRENT_DATE()))
;

USE ROLE pipelinerunner
;

GRANT SELECT ON TABLE collab.muse.warehouse_metering TO ROLE data_team_basic
;

GRANT SELECT ON TABLE collab.muse.warehouse_metering TO ROLE tableau
;

CREATE OR REPLACE VIEW collab.muse.warehouse_metering AS
(
SELECT
	wmh.credits_used,
	wmh.credits_used * 2.08 AS cost,
	wmh.credits_used_cloud_services,
	wmh.credits_used_compute,
	wmh.end_time,
	wmh.start_time,
	wmh.warehouse_id,
	wmh.warehouse_name,
	CASE
		WHEN wmh.warehouse_name LIKE 'DATA_SCIENCE%' THEN 'DATA_SCIENCE'
		WHEN wmh.warehouse_name LIKE 'MARKETING_PIPE%' THEN 'MARKETING'
		WHEN wmh.warehouse_name = 'SNOWPLOW_WH' THEN 'SNOWPLOW'
		WHEN wmh.warehouse_name LIKE '%DBT%' THEN 'DBT'
		WHEN wmh.warehouse_name LIKE 'CUSTOMER_INSIGHT%'
			THEN 'CUSTOMER_INSIGHT' --note below DBT because CI have dbt warehouses
		WHEN wmh.warehouse_name LIKE 'TABLEAU%' THEN 'TABLEAU'
		WHEN wmh.warehouse_name
			IN ('PIPE_4XLARGE',
				'PIPE_2XLARGE',
				'PIPE_XLARGE',
				'PIPE_LARGE',
				'PIPE_MEDIUM') THEN 'DATA_PLATFORM_MODELLING'
		WHEN wmh.warehouse_name LIKE 'PIPE_HYGIENE%' THEN 'DATA_PLATFORM_HYGIENE'
		WHEN wmh.warehouse_name = 'PIPE_XSMALL' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'PIPE_DEFAULT' THEN 'DATA_PLATFORM_DEFAULT'
		WHEN wmh.warehouse_name = 'CLOUD_SERVICES_ONLY' THEN 'CLOUD_SERVICES_ONLY'
		ELSE 'DATA_PLATFORM_OTHERS'
	END                     AS warehouse_group
FROM snowflake.account_usage.warehouse_metering_history wmh
	)
;

------------------------------------------------------------------------------------------------------------------------
-- investigating incremental jobs in pipeline to see if we can reduce costs on merge

SELECT
	SPLIT_PART(PARSE_JSON(s.query_tag)['data-pipeline-query-origins']['script-path']::VARCHAR, '/', -1) AS file_name,
	*
FROM collab.muse.snowflake_query_history_v2 s
WHERE s.start_time >= CURRENT_DATE - 7
  AND s.user_name = 'PIPELINERUNNER'
ORDER BY cost__query_duration DESC
-- AND file_name = 'customer_yearly_booking.py'
;

-- Touchification has large costs, could check variance between created and updated on touchification table to add a hard limit

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt