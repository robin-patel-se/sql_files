ip-10-73-3-246.eu-west-1.compute.internal
*** Reading remote log FROM Cloudwatch log_group: airflow-DATA-pipeline-production-TASK log_stream: dag_id=dwh__iterable_crm_reporting__daily_at_04h30/
run_id=scheduled__2025-07-07T04_30_00+00_00/
task_id=SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py/
attempt=1.log.
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1083}} INFO - Dependencies ALL met FOR <TaskInstance: dwh__iterable_crm_reporting__daily_at_04h30.SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py scheduled__2025-07-07T04:30:00+00:00 [queued]>
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1083}} INFO - Dependencies ALL met FOR <TaskInstance: dwh__iterable_crm_reporting__daily_at_04h30.SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py scheduled__2025-07-07T04:30:00+00:00 [queued]>
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1279}} INFO -
--------------------------------------------------------------------------------
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1280}} INFO - Starting attempt 1 OF 2
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1281}} INFO -
--------------------------------------------------------------------------------
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1300}} INFO - Executing <TASK(AirflowOperator): SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py> ON 2025-07-07 04:30:00+00:00
[2025-07-08, 04:32:04 UTC] {{standard_task_runner.py:55}} INFO - Started process 22304 TO run TASK
[2025-07-08, 04:32:04 UTC] {{standard_task_runner.py:82}} INFO - Running: ['airflow', 'tasks', 'run', 'dwh__iterable_crm_reporting__daily_at_04h30', 'SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py', 'scheduled__2025-07-07T04:30:00+00:00', '--job-id', '9253296', '--raw', '--subdir', 'DAGS_FOLDER/self_describings_dags/dwh__iterable_crm_reporting__daily_at_04h30.py', '--cfg-path', '/tmp/tmp0wye7wpj']
[2025-07-08, 04:32:04 UTC] {{standard_task_runner.py:83}} INFO - Job 9253296: Subtask SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py
[2025-07-08, 04:32:04 UTC] {{task_command.py:388}} INFO - Running <TaskInstance: dwh__iterable_crm_reporting__daily_at_04h30.SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py scheduled__2025-07-07T04:30:00+00:00 [running]> ON host ip-10-73-3-246.eu-west-1.compute.internal
[2025-07-08, 04:32:04 UTC] {{taskinstance.py:1507}} INFO - Exporting the FOLLOWING env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=dwh__iterable_crm_reporting__daily_at_04h30
AIRFLOW_CTX_TASK_ID=SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py
AIRFLOW_CTX_EXECUTION_DATE=2025-07-07T04:30:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=scheduled__2025-07-07T04:30:00+00:00
[2025-07-08, 04:32:05 UTC] {{SCHEDULE.py:217}} INFO - FIRST: 2025-07-07 04:30:00
[2025-07-08, 04:32:05 UTC] {{SCHEDULE.py:218}} INFO - LAST: 2025-07-07 04:30:00
[2025-07-08, 04:32:11 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:32:12 UTC] {{SQL.py:1909}} INFO - Query:

ALTER SESSION SET QUERY_TAG = '{"select_schema_version": "1.0.0", "app": "ScriptOperator", "workload_id": "usr.local.airflow.dags.biapp.task_catalogue.dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py", "filename": "/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting.py", "run_id": "20250707T043000__daily_at_04h30", "team": "data-pipeline", "tenant_id": "MWAA"}'
;

[2025-07-08, 04:32:12 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b30-0106-f537-0002-dd012657a45b
[2025-07-08, 04:32:12 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0666 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00059176
[2025-07-08, 04:32:12 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step01__campaign_enrichment AS (
	SELECT
		c.id                                                                  AS campaign_id,
		c.campaign_created_at,
		c.campaign_updated_at,
		c.start_at                                                            AS campaign_start_date,
		c.ended_at                                                            AS campaign_end_date,
		c.name                                                                AS campaign_name,
		c.template_id,
		c.message_medium,
		c.created_by_user_id,
		c.updated_by_user_id,
		c.campaign_state,
		c.list_ids,
		c.suppression_list_ids,
		c.send_size,
		c.labels,
		c.type                                                                AS campaign_type,
		c.splittable_email_name,
		c.mapped_crm_date,
		c.mapped_territory,
		c.mapped_objective,
		c.mapped_platform,
		c.mapped_campaign,
		c.mapped_theme,
		c.mapped_segment,
		c.record,
		IFF(LOWER(c.name) LIKE ANY ('%ame_athena%', '%core_athena%', '%partner_athena%', '%test_athena%'), TRUE,
			FALSE)                                                            AS is_athena,
		IFF(LOWER(c.type) IS NOT DISTINCT FROM 'triggered', TRUE, FALSE)      AS is_automated_campaign,
		IFF(is_automated_campaign, SPLIT_PART(c.name, '_', 3)::VARCHAR, NULL) AS ame_calculated_campaign_name
	FROM latest_vault.iterable.campaign c
)
;


[2025-07-08, 04:32:14 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b30-0106-f537-0002-dd012657a463
[2025-07-08, 04:32:14 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 2.0629 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.01833656
[2025-07-08, 04:32:14 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:32:14 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step02__model_sends AS (
	SELECT
		'email'                                          AS crm_channel_type,
		email_send.campaign_id,
		email_send.catalog_collection_count,
		email_send.catalog_lookup_count,
		email_send.channel_id,
		email_send.content_id,
		email_send.event_created_at::DATE                AS send_event_date,
		email_send.event_created_at::TIMESTAMP           AS send_event_time,
		user_attributes.shiro_user_id,
		user_attributes.current_affiliate_territory,
		email_send.message_id,
		SHA2(email_send.email)                           AS email_hash,
		SHA2(email_send.message_id || email_send.email)  AS message_id_email_hash,
		email_send.message_type_id,
		email_send.product_recommendation_count,
		email_send.template_id,
		email_send.event_created_at::DATE                AS send_start_date,
		LEAD(email_send.event_created_at::DATE) OVER (
			PARTITION BY
				email_send.campaign_id, SHA2(email_send.email)
			ORDER BY
				email_send.event_created_at::DATE ASC
			)                                            AS lead_event_date,
		-- this will be used to join behavioural data to for automated campaigns
		COALESCE(lead_event_date - 1, CURRENT_DATE + 30) AS send_end_date
	FROM latest_vault.iterable.email_send email_send
		LEFT JOIN data_vault_mvp.dwh.user_attributes user_attributes
				  ON LOWER(user_attributes.email) = LOWER(email_send.email) AND user_attributes.email IS NOT NULL
	WHERE email_send.event_created_at::DATE >= '2021-11-03'
	UNION ALL
	SELECT
		'app'                                                 AS crm_channel_type,
		app_push_send.campaign_id,
		NULL                                                  AS catalog_collection_count,
		NULL                                                  AS catalog_lookup_count,
		app_push_send.channel_id,
		NULL                                                  AS content_id,
		app_push_send.event_created_at::DATE                  AS send_event_date,
		app_push_send.event_created_at::TIMESTAMP             AS send_event_time,
		user_attributes.shiro_user_id,
		user_attributes.current_affiliate_territory,
		app_push_send.message_id,
		SHA2(app_push_send.email)                             AS email_hash,
		SHA2(app_push_send.message_id || app_push_send.email) AS message_id_email_hash,
		app_push_send.message_type_id,
		NULL                                                  AS product_recommendation_count,
		app_push_send.template_id,
		app_push_send.event_created_at::DATE                  AS send_start_date,
		LEAD(app_push_send.event_created_at::DATE) OVER (
			PARTITION BY
				app_push_send.campaign_id, SHA2(app_push_send.email)
			ORDER BY
				app_push_send.event_created_at::DATE ASC
			)                                                 AS lead_event_date,
		-- this will be used to join behavioural data to for automated campaigns
		COALESCE(lead_event_date - 1, CURRENT_DATE + 30)      AS send_end_date
	FROM latest_vault.iterable.app_push_send app_push_send
		LEFT JOIN data_vault_mvp.dwh.user_attributes user_attributes
				  ON LOWER(user_attributes.email) = LOWER(app_push_send.email) AND user_attributes.email IS NOT NULL
	UNION ALL
	SELECT
		'in-app'                                          AS crm_channel_type,
		in_app_send.campaign_id,
		NULL                                              AS catalog_collection_count,
		NULL                                              AS catalog_lookup_count,
		in_app_send.channel_id,
		NULL                                              AS content_id,
		in_app_send.event_created_at::DATE                AS send_event_date,
		in_app_send.event_created_at::TIMESTAMP           AS send_event_time,
		user_attributes.shiro_user_id,
		user_attributes.current_affiliate_territory,
		in_app_send.message_id,
		SHA2(in_app_send.email)                           AS email_hash,
		SHA2(in_app_send.message_id || in_app_send.email) AS message_id_email_hash,
		in_app_send.message_type_id,
		NULL                                              AS product_recommendation_count,
		in_app_send.template_id,
		in_app_send.event_created_at::DATE                AS send_start_date,
		LEAD(in_app_send.event_created_at::DATE) OVER (
			PARTITION BY
				in_app_send.campaign_id, SHA2(in_app_send.email)
			ORDER BY
				in_app_send.event_created_at::DATE ASC
			)                                             AS lead_event_date,
		-- this will be used to join behavioural data to for automated campaigns
		COALESCE(lead_event_date - 1, CURRENT_DATE + 30)  AS send_end_date
	FROM latest_vault.iterable.in_app_send in_app_send
		LEFT JOIN data_vault_mvp.dwh.user_attributes user_attributes
				  ON LOWER(user_attributes.email) = LOWER(in_app_send.email) AND user_attributes.email IS NOT NULL
	UNION ALL
	SELECT
		'web-push'                                            AS crm_channel_type,
		web_push_send.campaign_id,
		NULL                                                  AS catalog_collection_count,
		NULL                                                  AS catalog_lookup_count,
		NULL                                                  AS channel_id,
		NULL                                                  AS content_id,
		web_push_send.event_created_at::DATE                  AS send_event_date,
		web_push_send.event_created_at::TIMESTAMP             AS send_event_time,
		user_attributes.shiro_user_id,
		user_attributes.current_affiliate_territory,
		web_push_send.message_id,
		SHA2(web_push_send.email)                             AS email_hash,
		SHA2(web_push_send.message_id || web_push_send.email) AS message_id_email_hash,
		NULL                                                  AS message_type_id,
		NULL                                                  AS product_recommendation_count,
		NULL                                                  AS template_id,
		web_push_send.event_created_at::DATE                  AS send_start_date,
		LEAD(web_push_send.event_created_at::DATE) OVER (
			PARTITION BY
				web_push_send.campaign_id, SHA2(web_push_send.email)
			ORDER BY
				web_push_send.event_created_at::DATE ASC
			)                                                 AS lead_event_date,
		-- this will be used to join behavioural data to for automated campaigns
		COALESCE(lead_event_date - 1, CURRENT_DATE + 30)      AS send_end_date
	FROM latest_vault.iterable.web_push_send web_push_send
		LEFT JOIN data_vault_mvp.dwh.user_attributes user_attributes
				  ON LOWER(user_attributes.email) = LOWER(web_push_send.email) AND user_attributes.email IS NOT NULL
)
;


[2025-07-08, 04:40:17 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b30-0106-f537-0002-dd012657a4bf
[2025-07-08, 04:40:17 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 482.5686 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 4.28949832
[2025-07-08, 04:40:17 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:40:17 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step03__model_opens AS (
	SELECT
		'email'                         AS crm_channel_type,
		SHA2(eo.message_id || eo.email) AS message_id_email_hash,
		eo.campaign_id,
		eo.city,
		eo.country,
		eo.event_created_at::DATE       AS open_event_date,
		eo.event_created_at::TIMESTAMP  AS open_event_time,
		SHA2(eo.email)                  AS email_hash,
		SHA2(eo.ip)                     AS ip_hash,
		eo.message_id,
		eo.region,
		eo.template_id,
		eo.user_agent,
		eo.user_agent_device
	FROM latest_vault.iterable.email_open eo
	WHERE eo.event_created_at::DATE >= '2021-11-03'
	UNION ALL
	SELECT
		'app'                           AS crm_channel_type,
		SHA2(eo.message_id || eo.email) AS message_id_email_hash,
		eo.campaign_id,
		NULL                            AS city,
		NULL                            AS country,
		eo.event_created_at::DATE       AS open_event_date,
		eo.event_created_at::TIMESTAMP  AS open_event_time,
		SHA2(eo.email)                  AS email_hash,
		NULL                            AS ip_hash,
		eo.message_id,
		NULL                            AS region,
		eo.template_id,
		NULL                            AS user_agent,
		NULL                            AS user_agent_device
	FROM latest_vault.iterable.app_push_open eo
	UNION ALL
	SELECT
		'in-app'                        AS crm_channel_type,
		SHA2(eo.message_id || eo.email) AS message_id_email_hash,
		eo.campaign_id,
		NULL                            AS city,
		NULL                            AS country,
		eo.event_created_at::DATE       AS open_event_date,
		eo.event_created_at::TIMESTAMP  AS open_event_time,
		SHA2(eo.email)                  AS email_hash,
		NULL                            AS ip_hash,
		eo.message_id,
		NULL                            AS region,
		NULL                            AS template_id,
		NULL                            AS user_agent,
		NULL                            AS user_agent_device
	FROM latest_vault.iterable.in_app_open eo
)
;


[2025-07-08, 04:41:56 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b38-0106-f537-0002-dd012657b63b
[2025-07-08, 04:41:56 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 99.3427 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.88304578
[2025-07-08, 04:41:56 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:41:56 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step04__model_clicks AS (
	SELECT
		'email'                                           AS crm_channel_type,
		SHA2(email_click.message_id || email_click.email) AS message_id_email_hash,
		email_click.campaign_id,
		email_click.city,
		email_click.country,
		email_click.event_created_at::DATE                AS click_event_date,
		email_click.event_created_at::TIMESTAMP           AS click_event_time,
		SHA2(email_click.email)                           AS email_hash,
		SHA2(email_click.ip)                              AS ip_hash,
		email_click.message_id,
		email_click.region,
		email_click.template_id,
		email_click.user_agent,
		email_click.user_agent_device
	FROM latest_vault.iterable.email_click email_click
	WHERE email_click.event_created_at::DATE >= '2021-11-03'
	UNION ALL
	SELECT
		'in-app'                                            AS crm_channel_type,
		SHA2(in_app_click.message_id || in_app_click.email) AS message_id_email_hash,
		in_app_click.campaign_id,
		NULL                                                AS city,
		NULL                                                AS country,
		in_app_click.event_created_at::DATE                 AS click_event_date,
		in_app_click.event_created_at::TIMESTAMP            AS click_event_time,
		SHA2(in_app_click.email)                            AS email_hash,
		NULL                                                AS ip_hash,
		in_app_click.message_id,
		NULL                                                AS region,
		NULL                                                AS template_id,
		NULL                                                AS user_agent,
		NULL                                                AS user_agent_device
	FROM latest_vault.iterable.in_app_click in_app_click
	WHERE in_app_click.event_created_at::DATE >= '2021-11-03'
	UNION ALL
	SELECT
		'web-push'                                                AS crm_channel_type,
		SHA2(web_push_clicks.message_id || web_push_clicks.email) AS message_id_email_hash,
		web_push_clicks.campaign_id,
		NULL                                                      AS city,
		NULL                                                      AS country,
		web_push_clicks.event_created_at::DATE                    AS click_event_date,
		web_push_clicks.event_created_at::TIMESTAMP               AS click_event_time,
		SHA2(web_push_clicks.email)                               AS email_hash,
		NULL                                                      AS ip_hash,
		web_push_clicks.message_id,
		NULL                                                      AS region,
		NULL                                                      AS template_id,
		NULL                                                      AS user_agent,
		NULL                                                      AS user_agent_device
	FROM latest_vault.iterable.web_push_clicks web_push_clicks
)
;


[2025-07-08, 04:42:10 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b39-0106-f537-0002-dd012657b89f
[2025-07-08, 04:42:10 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 14.0363 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.12476713
[2025-07-08, 04:42:10 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:42:10 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step04_1__model_in_app_scv_clicks AS (
	SELECT
		'in-app-scv'                    AS crm_channel_type,
		SHA2(ec.message_id || ua.email) AS message_id_email_hash,
		ec.campaign_id,
		NULL                            AS city,
		NULL                            AS country,
		ec.event_tstamp::DATE           AS click_event_date,
		ec.event_tstamp::TIMESTAMP      AS click_event_time,
		SHA2(ua.email)                  AS email_hash,
		NULL                            AS ip_hash,
		ec.message_id,
		NULL                            AS region,
		ec.template_id,
		NULL                            AS user_agent,
		NULL                            AS user_agent_device
	FROM data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events ec
		INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
				   ON mtba.touch_id = ec.touch_id
		INNER JOIN data_vault_mvp.dwh.user_attributes ua ON ua.shiro_user_id::VARCHAR = mtba.attributed_user_id
	WHERE ec.event_tstamp::DATE >= '2021-11-03'
	  AND ec.event_subcategory = 'in_app_click'
	  AND ec.clicked_url IS DISTINCT FROM 'iterable://dismiss'
)
;


[2025-07-08, 04:42:14 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b3a-0106-f537-0002-dd012657b8cf
[2025-07-08, 04:42:14 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 4.0195 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.03572867
[2025-07-08, 04:42:14 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:42:14 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step05__model_unsubs AS (
	SELECT
		eu.event_created_at::DATE       AS unsub_event_date,
		eu.event_created_at::TIMESTAMP  AS unsub_event_time,
		SHA2(eu.message_id || eu.email) AS message_id_email_hash,
		eu.message_id,
		eu.campaign_id,
		eu.unsub_source
	FROM latest_vault.iterable.email_unsubscribe eu
	WHERE unsub_event_date >= '2021-11-03'
	  AND eu.unsub_source IN ('Complaint', 'EmailLink')
)
;


[2025-07-08, 04:42:15 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b3a-0106-f537-0002-dd012657b8e7
[2025-07-08, 04:42:15 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 1.2506 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.01111687
[2025-07-08, 04:42:15 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:42:15 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends AS (
	WITH
		aggregate_sends AS (
			SELECT
				es.crm_channel_type,
				es.message_id_email_hash,
				es.message_id,
				es.campaign_id,
				es.email_hash,
				es.shiro_user_id,
				es.current_affiliate_territory,
				es.send_event_date,
				es.send_event_time,
				es.send_start_date,
				es.send_end_date,
				COUNT(*) AS email_sends
			FROM data_vault_mvp.dwh.iterable_crm_reporting__step02__model_sends es
			GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
		)
	SELECT
		ags.*,
		c.campaign_name,
		c.splittable_email_name,
		c.mapped_crm_date,
		c.mapped_territory,
		c.mapped_objective,
		c.mapped_platform,
		c.mapped_campaign,
		c.mapped_theme,
		c.mapped_segment,
		c.is_athena,
		c.is_automated_campaign,
		c.ame_calculated_campaign_name
	FROM aggregate_sends ags
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step01__campaign_enrichment c
				  ON ags.campaign_id = c.campaign_id
)
;


[2025-07-08, 04:51:23 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b3a-0106-f537-0002-dd012657b8ef
[2025-07-08, 04:51:23 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 547.2448 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 4.86439826
[2025-07-08, 04:51:23 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 04:51:23 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step07__aggregate_opens AS (
	SELECT
		s.crm_channel_type,
		o.message_id_email_hash,
		o.campaign_id,
		o.message_id,
		MIN(o.open_event_date)                                                                                        AS first_open_event_date,
		MIN(o.open_event_time)                                                                                        AS first_open_event_time,
		COUNT(*)                                                                                                      AS email_opens,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 1, 1, 0))                                      AS email_opens_1d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 7, 1, 0))                                      AS email_opens_7d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 14, 1,
				0))                                                                                                   AS email_opens_14d,
		COUNT(DISTINCT o.message_id_email_hash)                                                                       AS unique_email_opens,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 1, o.message_id_email_hash,
						   NULL))                                                                                     AS unique_email_opens_1d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 7, o.message_id_email_hash,
						   NULL))                                                                                     AS unique_email_opens_7d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, o.open_event_date) <= 14, o.message_id_email_hash,
						   NULL))                                                                                     AS unique_email_opens_14d
	FROM data_vault_mvp.dwh.iterable_crm_reporting__step03__model_opens o
		INNER JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends s
				   ON o.message_id_email_hash = s.message_id_email_hash
	GROUP BY 1, 2, 3, 4
)
;


[2025-07-08, 05:09:33 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b43-0106-f537-0002-dd012657c6df
[2025-07-08, 05:09:33 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 1090.1994 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 9.69066118
[2025-07-08, 05:09:33 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:09:33 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step08__aggregate_clicks AS (
	SELECT
		ec.message_id_email_hash,
		ec.campaign_id,
		ec.message_id,
		MIN(ec.click_event_date)                                                                                AS first_click_event_date,
		MIN(ec.click_event_date)                                                                                AS first_click_event_time,
		COUNT(*)                                                                                                AS email_clicks,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 1, 1,
				0))                                                                                             AS email_clicks_1d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 7, 1,
				0))                                                                                             AS email_clicks_7d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 14, 1,
				0))                                                                                             AS email_clicks_14d,
		COUNT(DISTINCT ec.message_id_email_hash)                                                                AS unique_email_clicks,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 1, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_email_clicks_1d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 7, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_email_clicks_7d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 14, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_email_clicks_14d
	FROM data_vault_mvp.dwh.iterable_crm_reporting__step04__model_clicks ec
		INNER JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends s
				   ON ec.message_id_email_hash = s.message_id_email_hash
	GROUP BY 1, 2, 3
)
;


[2025-07-08, 05:10:01 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b55-0106-f537-0002-dd0126580a1b
[2025-07-08, 05:10:01 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 27.7619 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.24677258
[2025-07-08, 05:10:01 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:10:01 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step08_1__aggregate_clicks_in_app_scv AS (
	SELECT
		ec.message_id_email_hash,
		ec.campaign_id,
		ec.message_id,
		MIN(ec.click_event_date)                                                                                AS first_click_event_date,
		MIN(ec.click_event_date)                                                                                AS first_click_event_time,
		COUNT(*)                                                                                                AS in_app_scv_clicks,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 1, 1,
				0))                                                                                             AS in_app_scv_clicks_1d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 7, 1,
				0))                                                                                             AS in_app_scv_clicks_7d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 14, 1,
				0))                                                                                             AS in_app_scv_clicks_14d,
		COUNT(DISTINCT ec.message_id_email_hash)                                                                AS unique_in_app_scv_clicks,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 1, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_in_app_scv_clicks_1d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 7, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_in_app_scv_clicks_7d,
		COUNT(DISTINCT IFF(DATEDIFF(DAY, s.send_start_date, ec.click_event_date) <= 14, ec.message_id_email_hash,
						   NULL))                                                                               AS unique_in_app_scv_clicks_14d
	FROM data_vault_mvp.dwh.iterable_crm_reporting__step04_1__model_in_app_scv_clicks ec
		INNER JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends s
				   ON ec.message_id_email_hash = s.message_id_email_hash
	GROUP BY 1, 2, 3
)
;


[2025-07-08, 05:10:08 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b56-0106-f537-0002-dd0126581043
[2025-07-08, 05:10:08 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 7.2617 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.06454878
[2025-07-08, 05:10:08 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:10:08 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step09__aggregate_unsubs AS (
	SELECT
		ec.message_id_email_hash,
		ec.campaign_id,
		ec.message_id,
		MIN(ec.unsub_event_date)                                                                                      AS unsub_event_date,
		MIN(ec.unsub_event_time)                                                                                      AS unsub_event_time,
		COUNT(*)                                                                                                      AS email_unsubs,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 1, 1,
				0))                                                                                                   AS email_unsubs_1d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 7, 1,
				0))                                                                                                   AS email_unsubs_7d,
		SUM(IFF(DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 14, 1,
				0))                                                                                                   AS email_unsubs_14d,

		SUM(IFF(ec.unsub_source = 'Complaint', 1, 0))                                                                 AS email_unsubs_complaint,
		SUM(IFF(ec.unsub_source = 'Complaint' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 1, 1,
				0))                                                                                                   AS email_unsubs_complaint_1d,
		SUM(IFF(ec.unsub_source = 'Complaint' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 7, 1,
				0))                                                                                                   AS email_unsubs_complaint_7d,
		SUM(IFF(ec.unsub_source = 'Complaint' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 14, 1,
				0))                                                                                                   AS email_unsubs_complaint_14d,

		SUM(IFF(ec.unsub_source = 'EmailLink', 1, 0))                                                                 AS email_unsubs_email_link,
		SUM(IFF(ec.unsub_source = 'EmailLink' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 1, 1,
				0))                                                                                                   AS email_unsubs_email_link_1d,
		SUM(IFF(ec.unsub_source = 'EmailLink' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 7, 1,
				0))                                                                                                   AS email_unsubs_email_link_7d,
		SUM(IFF(ec.unsub_source = 'EmailLink' AND DATEDIFF(DAY, s.send_start_date, ec.unsub_event_date) <= 14, 1,
				0))                                                                                                   AS email_unsubs_email_link_14d
	FROM data_vault_mvp.dwh.iterable_crm_reporting__step05__model_unsubs ec
		INNER JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends s
				   ON ec.message_id_email_hash = s.message_id_email_hash
	GROUP BY 1, 2, 3
)
;


[2025-07-08, 05:10:14 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b56-0106-f537-0002-dd01265812ef
[2025-07-08, 05:10:14 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 5.7809 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.05138608
[2025-07-08, 05:10:14 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:10:14 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step10__model_scv_booking_data AS (
	WITH
		stack_bookings AS (
			SELECT
				'last non direct'                                  AS attribution_model,
				COALESCE(
						tmc.utm_campaign,
						tba2.app_push_open_context:dataFields:campaignId::VARCHAR
				)                                                  AS campaign_id,
				COALESCE(
						tmc.landing_page_parameters['messageId']::VARCHAR,
						tba2.app_push_open_context:dataFields:messageId::VARCHAR
				)                                                  AS message_id,
				tt.event_tstamp::DATE                              AS event_date,
				-- prioritise the booking user id over the identity stitched user id
				COALESCE(
						fb.shiro_user_id,
						tba.attributed_user_id
				)                                                  AS shiro_user_id,
				fb.booking_id                                      AS booking_id,
				fb.margin_gross_of_toms_gbp_constant_currency      AS margin_gbp,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN tt.booking_id
				END                                                AS booking_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN tt.booking_id
				END                                                AS booking_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN tt.booking_id
				END                                                AS booking_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN tt.booking_id
				END                                                AS booking_package,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_package,
				fb.gross_revenue_gbp_constant_currency             AS gross_revenue_gbp,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_package,
				DATEDIFF(DAY, fb.check_in_date, fb.check_out_date) AS los,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN los
				END                                                AS los_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN los
				END                                                AS los_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN los
				END                                                AS los_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN los
				END                                                AS los_package
			FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tt
				INNER JOIN data_vault_mvp.dwh.fact_booking fb
						   ON fb.booking_id = tt.booking_id
							   AND fb.booking_status_type IN ('live', 'cancelled')
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba
						   ON tt.touch_id = tba.touch_id
							   AND tba.stitched_identity_type = 'se_user_id'
							   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
						   ON tt.touch_id = ta.touch_id
							   AND ta.attribution_model = 'last non direct'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc
						   ON ta.attributed_touch_id = tmc.touch_id
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba2
						   ON ta.attributed_touch_id = tba2.touch_id
							   AND tba2.stitched_identity_type = 'se_user_id'
				INNER JOIN data_vault_mvp.dwh.dim_sale ds
						   ON fb.se_sale_id = ds.se_sale_id
			WHERE (
				tmc.utm_medium = 'email'
					AND tmc.utm_campaign IS NOT NULL
					AND tt.event_tstamp::DATE >= '2021-11-03'
				)
			   OR tba2.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL

			UNION ALL

			SELECT
				'last click'                                       AS attribution_model,
				COALESCE(
						tmc.utm_campaign,
						tba.app_push_open_context:dataFields:campaignId::VARCHAR
				)                                                  AS campaign_id,
				COALESCE(
						tmc.landing_page_parameters['messageId']::VARCHAR,
						tba.app_push_open_context:dataFields:messageId::VARCHAR
				)                                                  AS message_id,
				tt.event_tstamp::DATE                              AS event_date,
				COALESCE(
						fb.shiro_user_id,
						tba.attributed_user_id
				)                                                  AS shiro_user_id,
				fb.booking_id                                      AS booking_id,
				fb.margin_gross_of_toms_gbp_constant_currency      AS margin_gbp,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN tt.booking_id
				END                                                AS booking_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN tt.booking_id
				END                                                AS booking_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN tt.booking_id
				END                                                AS booking_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN tt.booking_id
				END                                                AS booking_package,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN fb.margin_gross_of_toms_gbp_constant_currency
				END                                                AS margin_gbp_package,
				fb.gross_revenue_gbp_constant_currency             AS gross_revenue_gbp,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN fb.gross_revenue_gbp_constant_currency
				END                                                AS gross_revenue_gbp_package,
				DATEDIFF(DAY, fb.check_in_date, fb.check_out_date) AS los,
				CASE
					WHEN LOWER(ds.travel_type) = 'domestic' THEN los
				END                                                AS los_domestic,
				CASE
					WHEN LOWER(ds.travel_type) = 'international' THEN los
				END                                                AS los_international,
				CASE
					WHEN LOWER(ds.product_type) = 'hotel' THEN los
				END                                                AS los_hotel,
				CASE
					WHEN LOWER(ds.product_type) = 'package' THEN los
				END                                                AS los_package
			FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tt
				INNER JOIN data_vault_mvp.dwh.fact_booking fb
						   ON fb.booking_id = tt.booking_id
							   AND fb.booking_status_type IN ('live', 'cancelled')
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba
						   ON tt.touch_id = tba.touch_id
							   AND tba.stitched_identity_type = 'se_user_id'
							   AND tba.touch_start_tstamp::DATE >= '2021-11-03'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc
						   ON tt.touch_id = tmc.touch_id
				INNER JOIN data_vault_mvp.dwh.dim_sale ds
						   ON fb.se_sale_id = ds.se_sale_id
			WHERE (
				tmc.utm_medium = 'email'
					AND tmc.utm_campaign IS NOT NULL
					AND tt.event_tstamp::DATE >= '2021-11-03'
				)
			   OR tba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
		),
		attach_send_data AS (
			SELECT
				sb.attribution_model,
				sb.campaign_id,
				COALESCE(ags.message_id, ags2.message_id)                                                             AS message_id,
				COALESCE(ags.message_id_email_hash, ags2.message_id_email_hash)                                       AS message_id_email_hash,
				sb.event_date,
				sb.shiro_user_id,

				sb.booking_id,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1, 1,
					NULL)                                                                                             AS bookings_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7, 1,
					NULL)                                                                                             AS bookings_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14, 1,
					NULL)                                                                                             AS bookings_14d,

				sb.margin_gbp,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.margin_gbp,
					0)                                                                                                AS margin_gbp_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.margin_gbp,
					0)                                                                                                AS margin_gbp_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.margin_gbp,
					0)                                                                                                AS margin_gbp_14d,

				IFF(sb.booking_domestic IS NOT NULL, 1, 0)                                                            AS booking_domestic,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1 AND
					sb.booking_domestic IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_domestic_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7 AND
					sb.booking_domestic IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_domestic_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14 AND
					sb.booking_domestic IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_domestic_14d,

				IFF(sb.booking_international IS NOT NULL, 1, 0)                                                       AS booking_international,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1 AND
					sb.booking_international IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_international_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7 AND
					sb.booking_international IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_international_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14 AND
					sb.booking_international IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_international_14d,

				IFF(sb.booking_hotel IS NOT NULL, 1, 0)                                                               AS booking_hotel,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1 AND
					sb.booking_hotel IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_hotel_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7 AND
					sb.booking_hotel IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_hotel_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14 AND
					sb.booking_hotel IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_hotel_14d,

				IFF(sb.booking_package IS NOT NULL, 1, 0)                                                             AS booking_package,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1 AND
					sb.booking_package IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_package_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7 AND
					sb.booking_package IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_package_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14 AND
					sb.booking_package IS NOT NULL, 1,
					NULL)                                                                                             AS bookings_package_14d,

				sb.margin_gbp_domestic,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.margin_gbp_domestic,
					0)                                                                                                AS margin_gbp_domestic_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.margin_gbp_domestic,
					0)                                                                                                AS margin_gbp_domestic_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.margin_gbp_domestic,
					0)                                                                                                AS margin_gbp_domestic_14d,

				sb.margin_gbp_international,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.margin_gbp_international,
					0)                                                                                                AS margin_gbp_international_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.margin_gbp_international,
					0)                                                                                                AS margin_gbp_international_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.margin_gbp_international,
					0)                                                                                                AS margin_gbp_international_14d,

				sb.margin_gbp_hotel,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.margin_gbp_hotel,
					0)                                                                                                AS margin_gbp_hotel_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.margin_gbp_hotel,
					0)                                                                                                AS margin_gbp_hotel_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.margin_gbp_hotel,
					0)                                                                                                AS margin_gbp_hotel_14d,

				sb.margin_gbp_package,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.margin_gbp_package,
					0)                                                                                                AS margin_gbp_package_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.margin_gbp_package,
					0)                                                                                                AS margin_gbp_package_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.margin_gbp_package,
					0)                                                                                                AS margin_gbp_package_14d,

				sb.gross_revenue_gbp,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.gross_revenue_gbp,
					0)                                                                                                AS gross_revenue_gbp_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.gross_revenue_gbp,
					0)                                                                                                AS gross_revenue_gbp_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.gross_revenue_gbp,
					0)                                                                                                AS gross_revenue_gbp_14d,

				sb.gross_revenue_gbp_domestic,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.gross_revenue_gbp_domestic,
					0)                                                                                                AS gross_revenue_gbp_domestic_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.gross_revenue_gbp_domestic,
					0)                                                                                                AS gross_revenue_gbp_domestic_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.gross_revenue_gbp_domestic,
					0)                                                                                                AS gross_revenue_gbp_domestic_14d,

				sb.gross_revenue_gbp_international,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.gross_revenue_gbp_international,
					0)                                                                                                AS gross_revenue_gbp_international_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.gross_revenue_gbp_international,
					0)                                                                                                AS gross_revenue_gbp_international_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.gross_revenue_gbp_international,
					0)                                                                                                AS gross_revenue_gbp_international_14d,

				sb.gross_revenue_gbp_hotel,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.gross_revenue_gbp_hotel,
					0)                                                                                                AS gross_revenue_gbp_hotel_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.gross_revenue_gbp_hotel,
					0)                                                                                                AS gross_revenue_gbp_hotel_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.gross_revenue_gbp_hotel,
					0)                                                                                                AS gross_revenue_gbp_hotel_14d,

				sb.gross_revenue_gbp_package,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 1,
					sb.gross_revenue_gbp_package,
					0)                                                                                                AS gross_revenue_gbp_package_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 7,
					sb.gross_revenue_gbp_package,
					0)                                                                                                AS gross_revenue_gbp_package_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), sb.event_date) <= 14,
					sb.gross_revenue_gbp_package,
					0)                                                                                                AS gross_revenue_gbp_package_14d,
				sb.los,
				sb.los_domestic,
				sb.los_international,
				sb.los_hotel,
				sb.los_package
			FROM stack_bookings sb
				-- with message id
				LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends ags
						  ON sb.message_id = ags.message_id
							  AND sb.campaign_id = ags.campaign_id::VARCHAR
							  AND sb.shiro_user_id = ags.shiro_user_id
							  -- without messsage id
				LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends ags2
						  ON sb.campaign_id = ags2.campaign_id::VARCHAR
							  AND sb.shiro_user_id = ags2.shiro_user_id
							  AND sb.event_date BETWEEN ags2.send_start_date AND ags2.send_end_date
		)
	SELECT
		asd.attribution_model,
		asd.campaign_id,
		asd.message_id,
		asd.message_id_email_hash,
		asd.event_date,
		asd.shiro_user_id,

		COUNT(*)                                     AS bookings,
		SUM(asd.bookings_1d)                         AS bookings_1d,
		SUM(asd.bookings_7d)                         AS bookings_7d,
		SUM(asd.bookings_14d)                        AS bookings_14d,

		SUM(asd.margin_gbp)                          AS margin_gbp,
		SUM(asd.margin_gbp_1d)                       AS margin_gbp_1d,
		SUM(asd.margin_gbp_7d)                       AS margin_gbp_7d,
		SUM(asd.margin_gbp_14d)                      AS margin_gbp_14d,

		SUM(asd.booking_domestic)                    AS bookings_domestic,
		SUM(asd.bookings_domestic_1d)                AS bookings_domestic_1d,
		SUM(asd.bookings_domestic_7d)                AS bookings_domestic_7d,
		SUM(asd.bookings_domestic_14d)               AS bookings_domestic_14d,

		SUM(asd.booking_international)               AS bookings_international,
		SUM(asd.bookings_international_1d)           AS bookings_international_1d,
		SUM(asd.bookings_international_7d)           AS bookings_international_7d,
		SUM(asd.bookings_international_14d)          AS bookings_international_14d,

		SUM(asd.booking_hotel)                       AS bookings_hotel,
		SUM(asd.bookings_hotel_1d)                   AS bookings_hotel_1d,
		SUM(asd.bookings_hotel_7d)                   AS bookings_hotel_7d,
		SUM(asd.bookings_hotel_14d)                  AS bookings_hotel_14d,

		SUM(asd.booking_package)                     AS bookings_package,
		SUM(asd.bookings_package_1d)                 AS bookings_package_1d,
		SUM(asd.bookings_package_7d)                 AS bookings_package_7d,
		SUM(asd.bookings_package_14d)                AS bookings_package_14d,

		SUM(asd.margin_gbp_domestic)                 AS margin_gbp_domestic,
		SUM(asd.margin_gbp_domestic_1d)              AS margin_gbp_domestic_1d,
		SUM(asd.margin_gbp_domestic_7d)              AS margin_gbp_domestic_7d,
		SUM(asd.margin_gbp_domestic_14d)             AS margin_gbp_domestic_14d,

		SUM(asd.margin_gbp_international)            AS margin_gbp_international,
		SUM(asd.margin_gbp_international_1d)         AS margin_gbp_international_1d,
		SUM(asd.margin_gbp_international_7d)         AS margin_gbp_international_7d,
		SUM(asd.margin_gbp_international_14d)        AS margin_gbp_international_14d,

		SUM(asd.margin_gbp_hotel)                    AS margin_gbp_hotel,
		SUM(asd.margin_gbp_hotel_1d)                 AS margin_gbp_hotel_1d,
		SUM(asd.margin_gbp_hotel_7d)                 AS margin_gbp_hotel_7d,
		SUM(asd.margin_gbp_hotel_14d)                AS margin_gbp_hotel_14d,

		SUM(asd.margin_gbp_package)                  AS margin_gbp_package,
		SUM(asd.margin_gbp_package_1d)               AS margin_gbp_package_1d,
		SUM(asd.margin_gbp_package_7d)               AS margin_gbp_package_7d,
		SUM(asd.margin_gbp_package_14d)              AS margin_gbp_package_14d,

		SUM(asd.gross_revenue_gbp)                   AS gross_revenue_gbp,
		SUM(asd.gross_revenue_gbp_1d)                AS gross_revenue_gbp_1d,
		SUM(asd.gross_revenue_gbp_7d)                AS gross_revenue_gbp_7d,
		SUM(asd.gross_revenue_gbp_14d)               AS gross_revenue_gbp_14d,

		SUM(asd.gross_revenue_gbp_domestic)          AS gross_revenue_gbp_domestic,
		SUM(asd.gross_revenue_gbp_domestic_1d)       AS gross_revenue_gbp_domestic_1d,
		SUM(asd.gross_revenue_gbp_domestic_7d)       AS gross_revenue_gbp_domestic_7d,
		SUM(asd.gross_revenue_gbp_domestic_14d)      AS gross_revenue_gbp_domestic_14d,

		SUM(asd.gross_revenue_gbp_international)     AS gross_revenue_gbp_international,
		SUM(asd.gross_revenue_gbp_international_1d)  AS gross_revenue_gbp_international_1d,
		SUM(asd.gross_revenue_gbp_international_7d)  AS gross_revenue_gbp_international_7d,
		SUM(asd.gross_revenue_gbp_international_14d) AS gross_revenue_gbp_international_14d,

		SUM(asd.gross_revenue_gbp_hotel)             AS gross_revenue_gbp_hotel,
		SUM(asd.gross_revenue_gbp_hotel_1d)          AS gross_revenue_gbp_hotel_1d,
		SUM(asd.gross_revenue_gbp_hotel_7d)          AS gross_revenue_gbp_hotel_7d,
		SUM(asd.gross_revenue_gbp_hotel_14d)         AS gross_revenue_gbp_hotel_14d,

		SUM(asd.gross_revenue_gbp_package)           AS gross_revenue_gbp_package,
		SUM(asd.gross_revenue_gbp_package_1d)        AS gross_revenue_gbp_package_1d,
		SUM(asd.gross_revenue_gbp_package_7d)        AS gross_revenue_gbp_package_7d,
		SUM(asd.gross_revenue_gbp_package_14d)       AS gross_revenue_gbp_package_14d,
		AVG(asd.los)                                 AS los,
		AVG(asd.los_domestic)                        AS los_domestic,
		AVG(asd.los_international)                   AS los_international,
		AVG(asd.los_hotel)                           AS los_hotel,
		AVG(asd.los_package)                         AS los_package
	FROM attach_send_data asd
	GROUP BY 1, 2, 3, 4, 5, 6
)
;


[2025-07-08, 05:18:13 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b56-0106-f537-0002-dd0126581553
[2025-07-08, 05:18:13 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 479.2094 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 4.25963949
[2025-07-08, 05:18:13 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:18:13 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_spv_data AS (
	WITH
		stack_spvs AS (
			SELECT
				'last non direct'       AS attribution_model,
				COALESCE(
						tmc.utm_campaign,
						tba2.app_push_open_context:dataFields:campaignId::VARCHAR
				)                       AS campaign_id,
				COALESCE(
						tmc.landing_page_parameters['messageId']::VARCHAR,
						tba.app_push_open_context:dataFields:messageId::VARCHAR
				)                       AS message_id,
				spvs.event_tstamp::DATE AS event_date,
				tba.attributed_user_id  AS shiro_user_id
			FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs AS spvs
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes AS tba
						   ON spvs.touch_id = tba.touch_id
							   AND tba.stitched_identity_type = 'se_user_id'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution attr
						   ON spvs.touch_id = attr.touch_id
							   AND attr.attribution_model = 'last non direct'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel tmc
						   ON attr.attributed_touch_id = tmc.touch_id
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba2
						   ON attr.attributed_touch_id = tba2.touch_id
							   AND tba2.stitched_identity_type = 'se_user_id'
			WHERE (
					  tmc.utm_medium = 'email'
						  AND tmc.utm_campaign IS NOT NULL
						  AND spvs.event_tstamp::DATE >= '2021-11-03'
					  ) OR
				  tba2.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL

			UNION ALL

			SELECT
				'last click'            AS attribution_model,
				COALESCE(
						tmc.utm_campaign,
						tba.app_push_open_context:dataFields:campaignId::VARCHAR
				)                       AS campaign_id,
				COALESCE(
						tmc.landing_page_parameters['messageId']::VARCHAR,
						tba.app_push_open_context:dataFields:messageId::VARCHAR
				)                       AS message_id,
				spvs.event_tstamp::DATE AS event_date,
				tba.attributed_user_id  AS shiro_user_id
			FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs AS spvs
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes AS tba
						   ON spvs.touch_id = tba.touch_id
							   AND tba.stitched_identity_type = 'se_user_id'
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel AS tmc
						   ON spvs.touch_id = tmc.touch_id
			WHERE (
					  tmc.utm_medium = 'email'
						  AND tmc.utm_campaign IS NOT NULL
						  AND spvs.event_tstamp::DATE >= '2021-11-03'
					  ) OR
				  tba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL

			UNION ALL

			SELECT
				'url params'                                                 AS attribution_model,
				PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR AS campaign_id,
				PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR    AS message_id,
				spvs.event_tstamp::DATE                                      AS event_date,
				tba.attributed_user_id                                       AS shiro_user_id
			FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs AS spvs
				INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes AS tba
						   ON spvs.touch_id = tba.touch_id
							   AND tba.stitched_identity_type = 'se_user_id'
			WHERE PARSE_URL(spvs.page_url)['parameters']:utm_medium::VARCHAR = 'email'
			  AND PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR IS NOT NULL
			  AND spvs.event_tstamp::DATE >= '2021-11-03'
		),
		attach_send_data AS (
			SELECT
				ss.attribution_model,
				ss.campaign_id,
				COALESCE(ags.message_id, ags2.message_id)                                                          AS message_id,
				COALESCE(ags.message_id_email_hash, ags2.message_id_email_hash)                                    AS message_id_email_hash,
				ss.event_date,
				ss.shiro_user_id,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), ss.event_date) <= 1, 1,
					0)                                                                                             AS spvs_1d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), ss.event_date) <= 7, 1,
					0)                                                                                             AS spvs_7d,
				IFF(DATEDIFF(DAY, COALESCE(ags.send_start_date, ags2.send_start_date), ss.event_date) <= 14, 1,
					0)                                                                                             AS spvs_14d
			FROM stack_spvs ss
				-- with message id
				LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends ags
						  ON ss.message_id = ags.message_id
							  AND ss.campaign_id = ags.campaign_id::VARCHAR
							  AND ss.shiro_user_id = ags.shiro_user_id
							  -- without messsage id
				LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends ags2
						  ON ss.campaign_id = ags2.campaign_id::VARCHAR
							  AND ss.shiro_user_id = ags2.shiro_user_id
							  AND ss.event_date BETWEEN ags2.send_start_date AND ags2.send_end_date
		)
	SELECT
		asd.attribution_model,
		asd.campaign_id,
		asd.message_id,
		asd.message_id_email_hash,
		asd.event_date,
		asd.shiro_user_id,
		COUNT(*)          AS spvs,
		SUM(asd.spvs_1d)  AS spvs_1d,
		SUM(asd.spvs_7d)  AS spvs_7d,
		SUM(asd.spvs_14d) AS spvs_14d
	FROM attach_send_data asd
	GROUP BY 1, 2, 3, 4, 5, 6
)
;


[2025-07-08, 05:26:24 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b5e-0106-f537-0002-dd0126588727
[2025-07-08, 05:26:24 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 490.5341 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 4.36030324
[2025-07-08, 05:26:24 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:26:24 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_data AS (
	WITH
		bookings AS (
			SELECT
				bk.message_id_email_hash,

				-- last click metrics
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings, 0))                                 AS bookings_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_1d, 0))                              AS bookings_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_7d, 0))                              AS bookings_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_14d, 0))                             AS bookings_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp, 0))                               AS margin_gbp_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_1d, 0))                            AS margin_gbp_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_7d, 0))                            AS margin_gbp_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_14d, 0))                           AS margin_gbp_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_domestic, 0))                        AS bookings_domestic_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_domestic_1d, 0))                     AS bookings_domestic_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_domestic_7d, 0))                     AS bookings_domestic_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_domestic_14d, 0))                    AS bookings_domestic_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_international,
						0))                                                                                   AS bookings_international_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_international_1d,
						0))                                                                                   AS bookings_international_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_international_7d,
						0))                                                                                   AS bookings_international_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_international_14d,
						0))                                                                                   AS bookings_international_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_hotel, 0))                           AS bookings_hotel_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_hotel_1d, 0))                        AS bookings_hotel_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_hotel_7d, 0))                        AS bookings_hotel_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_hotel_14d, 0))                       AS bookings_hotel_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_package, 0))                         AS bookings_package_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_package_1d, 0))                      AS bookings_package_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_package_7d, 0))                      AS bookings_package_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.bookings_package_14d, 0))                     AS bookings_package_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_domestic, 0))                      AS margin_gbp_domestic_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_domestic_1d,
						0))                                                                                   AS margin_gbp_domestic_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_domestic_7d,
						0))                                                                                   AS margin_gbp_domestic_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_domestic_14d,
						0))                                                                                   AS margin_gbp_domestic_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_international,
						0))                                                                                   AS margin_gbp_international_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_international_1d,
						0))                                                                                   AS margin_gbp_international_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_international_7d,
						0))                                                                                   AS margin_gbp_international_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_international_14d,
						0))                                                                                   AS margin_gbp_international_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_hotel, 0))                         AS margin_gbp_hotel_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_hotel_1d, 0))                      AS margin_gbp_hotel_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_hotel_7d, 0))                      AS margin_gbp_hotel_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_hotel_14d, 0))                     AS margin_gbp_hotel_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_package, 0))                       AS margin_gbp_package_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_package_1d, 0))                    AS margin_gbp_package_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_package_7d, 0))                    AS margin_gbp_package_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.margin_gbp_package_14d,
						0))                                                                                   AS margin_gbp_package_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp, 0))                        AS gross_revenue_gbp_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_1d, 0))                     AS gross_revenue_gbp_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_7d, 0))                     AS gross_revenue_gbp_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_14d, 0))                    AS gross_revenue_gbp_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_domestic,
						0))                                                                                   AS gross_revenue_gbp_domestic_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_domestic_1d,
						0))                                                                                   AS gross_revenue_gbp_domestic_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_domestic_7d,
						0))                                                                                   AS gross_revenue_gbp_domestic_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_domestic_14d,
						0))                                                                                   AS gross_revenue_gbp_domestic_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_international,
						0))                                                                                   AS gross_revenue_gbp_international_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_international_1d,
						0))                                                                                   AS gross_revenue_gbp_international_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_international_7d,
						0))                                                                                   AS gross_revenue_gbp_international_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_international_14d,
						0))                                                                                   AS gross_revenue_gbp_international_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_hotel,
						0))                                                                                   AS gross_revenue_gbp_hotel_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_hotel_1d,
						0))                                                                                   AS gross_revenue_gbp_hotel_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_hotel_7d,
						0))                                                                                   AS gross_revenue_gbp_hotel_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_hotel_14d,
						0))                                                                                   AS gross_revenue_gbp_hotel_14d_lc,

				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_package,
						0))                                                                                   AS gross_revenue_gbp_package_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_package_1d,
						0))                                                                                   AS gross_revenue_gbp_package_1d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_package_7d,
						0))                                                                                   AS gross_revenue_gbp_package_7d_lc,
				SUM(IFF(bk.attribution_model = 'last click', bk.gross_revenue_gbp_package_14d,
						0))                                                                                   AS gross_revenue_gbp_package_14d_lc,

				-- last non direct metrics
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings, 0))                            AS bookings_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_1d, 0))                         AS bookings_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_7d, 0))                         AS bookings_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_14d, 0))                        AS bookings_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp, 0))                          AS margin_gbp_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_1d, 0))                       AS margin_gbp_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_7d, 0))                       AS margin_gbp_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_14d, 0))                      AS margin_gbp_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_domestic,
						0))                                                                                   AS bookings_domestic_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_domestic_1d,
						0))                                                                                   AS bookings_domestic_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_domestic_7d,
						0))                                                                                   AS bookings_domestic_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_domestic_14d,
						0))                                                                                   AS bookings_domestic_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_international,
						0))                                                                                   AS bookings_international_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_international_1d,
						0))                                                                                   AS bookings_international_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_international_7d,
						0))                                                                                   AS bookings_international_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_international_14d,
						0))                                                                                   AS bookings_international_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_hotel, 0))                      AS bookings_hotel_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_hotel_1d,
						0))                                                                                   AS bookings_hotel_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_hotel_7d,
						0))                                                                                   AS bookings_hotel_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_hotel_14d,
						0))                                                                                   AS bookings_hotel_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_package, 0))                    AS bookings_package_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_package_1d,
						0))                                                                                   AS bookings_package_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_package_7d,
						0))                                                                                   AS bookings_package_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.bookings_package_14d,
						0))                                                                                   AS bookings_package_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_domestic,
						0))                                                                                   AS margin_gbp_domestic_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_domestic_1d,
						0))                                                                                   AS margin_gbp_domestic_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_domestic_7d,
						0))                                                                                   AS margin_gbp_domestic_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_domestic_14d,
						0))                                                                                   AS margin_gbp_domestic_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_international,
						0))                                                                                   AS margin_gbp_international_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_international_1d,
						0))                                                                                   AS margin_gbp_international_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_international_7d,
						0))                                                                                   AS margin_gbp_international_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_international_14d,
						0))                                                                                   AS margin_gbp_international_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_hotel, 0))                    AS margin_gbp_hotel_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_hotel_1d,
						0))                                                                                   AS margin_gbp_hotel_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_hotel_7d,
						0))                                                                                   AS margin_gbp_hotel_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_hotel_14d,
						0))                                                                                   AS margin_gbp_hotel_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_package,
						0))                                                                                   AS margin_gbp_package_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_package_1d,
						0))                                                                                   AS margin_gbp_package_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_package_7d,
						0))                                                                                   AS margin_gbp_package_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.margin_gbp_package_14d,
						0))                                                                                   AS margin_gbp_package_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp,
						0))                                                                                   AS gross_revenue_gbp_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_1d,
						0))                                                                                   AS gross_revenue_gbp_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_7d,
						0))                                                                                   AS gross_revenue_gbp_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_14d,
						0))                                                                                   AS gross_revenue_gbp_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_domestic,
						0))                                                                                   AS gross_revenue_gbp_domestic_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_domestic_1d,
						0))                                                                                   AS gross_revenue_gbp_domestic_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_domestic_7d,
						0))                                                                                   AS gross_revenue_gbp_domestic_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_domestic_14d,
						0))                                                                                   AS gross_revenue_gbp_domestic_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_international,
						0))                                                                                   AS gross_revenue_gbp_international_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_international_1d,
						0))                                                                                   AS gross_revenue_gbp_international_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_international_7d,
						0))                                                                                   AS gross_revenue_gbp_international_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_international_14d,
						0))                                                                                   AS gross_revenue_gbp_international_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_hotel,
						0))                                                                                   AS gross_revenue_gbp_hotel_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_hotel_1d,
						0))                                                                                   AS gross_revenue_gbp_hotel_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_hotel_7d,
						0))                                                                                   AS gross_revenue_gbp_hotel_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_hotel_14d,
						0))                                                                                   AS gross_revenue_gbp_hotel_14d_lnd,

				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_package,
						0))                                                                                   AS gross_revenue_gbp_package_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_package_1d,
						0))                                                                                   AS gross_revenue_gbp_package_1d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_package_7d,
						0))                                                                                   AS gross_revenue_gbp_package_7d_lnd,
				SUM(IFF(bk.attribution_model = 'last non direct', bk.gross_revenue_gbp_package_14d,
						0))                                                                                   AS gross_revenue_gbp_package_14d_lnd,

				AVG(bk.los)                                                                                   AS los,
				AVG(bk.los_domestic)                                                                          AS los_domestic,
				AVG(bk.los_international)                                                                     AS los_international,
				AVG(bk.los_hotel)                                                                             AS los_hotel,
				AVG(bk.los_package)                                                                           AS los_package

			FROM data_vault_mvp.dwh.iterable_crm_reporting__step10__model_scv_booking_data bk
			GROUP BY 1
		),
		spvs AS (
			SELECT
				spvs.message_id_email_hash,
				SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs, 0))          AS spvs_lc,
				SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_1d, 0))       AS spvs_1d_lc,
				SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_7d, 0))       AS spvs_7d_lc,
				SUM(IFF(spvs.attribution_model = 'last click', spvs.spvs_14d, 0))      AS spvs_14d_lc,

				SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs, 0))     AS spvs_lnd,
				SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_1d, 0))  AS spvs_1d_lnd,
				SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_7d, 0))  AS spvs_7d_lnd,
				SUM(IFF(spvs.attribution_model = 'last non direct', spvs.spvs_14d, 0)) AS spvs_14d_lnd,

				SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs, 0))          AS spvs_url,
				SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_1d, 0))       AS spvs_1d_url,
				SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_7d, 0))       AS spvs_7d_url,
				SUM(IFF(spvs.attribution_model = 'url params', spvs.spvs_14d, 0))      AS spvs_14d_url
			FROM data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_spv_data spvs
			GROUP BY 1
		)

	SELECT
		COALESCE(s.message_id_email_hash, b.message_id_email_hash) AS message_id_email_hash,

		-- last click metics
		b.bookings_lc,
		b.bookings_1d_lc,
		b.bookings_7d_lc,
		b.bookings_14d_lc,

		b.margin_gbp_lc,
		b.margin_gbp_1d_lc,
		b.margin_gbp_7d_lc,
		b.margin_gbp_14d_lc,

		b.bookings_domestic_lc,
		b.bookings_domestic_1d_lc,
		b.bookings_domestic_7d_lc,
		b.bookings_domestic_14d_lc,

		b.bookings_international_lc,
		b.bookings_international_1d_lc,
		b.bookings_international_7d_lc,
		b.bookings_international_14d_lc,

		b.bookings_hotel_lc,
		b.bookings_hotel_1d_lc,
		b.bookings_hotel_7d_lc,
		b.bookings_hotel_14d_lc,

		b.bookings_package_lc,
		b.bookings_package_1d_lc,
		b.bookings_package_7d_lc,
		b.bookings_package_14d_lc,

		b.margin_gbp_domestic_lc,
		b.margin_gbp_domestic_1d_lc,
		b.margin_gbp_domestic_7d_lc,
		b.margin_gbp_domestic_14d_lc,

		b.margin_gbp_international_lc,
		b.margin_gbp_international_1d_lc,
		b.margin_gbp_international_7d_lc,
		b.margin_gbp_international_14d_lc,

		b.margin_gbp_hotel_lc,
		b.margin_gbp_hotel_1d_lc,
		b.margin_gbp_hotel_7d_lc,
		b.margin_gbp_hotel_14d_lc,

		b.margin_gbp_package_lc,
		b.margin_gbp_package_1d_lc,
		b.margin_gbp_package_7d_lc,
		b.margin_gbp_package_14d_lc,

		b.gross_revenue_gbp_lc,
		b.gross_revenue_gbp_1d_lc,
		b.gross_revenue_gbp_7d_lc,
		b.gross_revenue_gbp_14d_lc,

		b.gross_revenue_gbp_domestic_lc,
		b.gross_revenue_gbp_domestic_1d_lc,
		b.gross_revenue_gbp_domestic_7d_lc,
		b.gross_revenue_gbp_domestic_14d_lc,

		b.gross_revenue_gbp_international_lc,
		b.gross_revenue_gbp_international_1d_lc,
		b.gross_revenue_gbp_international_7d_lc,
		b.gross_revenue_gbp_international_14d_lc,

		b.gross_revenue_gbp_hotel_lc,
		b.gross_revenue_gbp_hotel_1d_lc,
		b.gross_revenue_gbp_hotel_7d_lc,
		b.gross_revenue_gbp_hotel_14d_lc,

		b.gross_revenue_gbp_package_lc,
		b.gross_revenue_gbp_package_1d_lc,
		b.gross_revenue_gbp_package_7d_lc,
		b.gross_revenue_gbp_package_14d_lc,

		-- last non direct metrics
		b.bookings_lnd,
		b.bookings_1d_lnd,
		b.bookings_7d_lnd,
		b.bookings_14d_lnd,

		b.margin_gbp_lnd,
		b.margin_gbp_1d_lnd,
		b.margin_gbp_7d_lnd,
		b.margin_gbp_14d_lnd,

		b.bookings_domestic_lnd,
		b.bookings_domestic_1d_lnd,
		b.bookings_domestic_7d_lnd,
		b.bookings_domestic_14d_lnd,

		b.bookings_international_lnd,
		b.bookings_international_1d_lnd,
		b.bookings_international_7d_lnd,
		b.bookings_international_14d_lnd,

		b.bookings_hotel_lnd,
		b.bookings_hotel_1d_lnd,
		b.bookings_hotel_7d_lnd,
		b.bookings_hotel_14d_lnd,

		b.bookings_package_lnd,
		b.bookings_package_1d_lnd,
		b.bookings_package_7d_lnd,
		b.bookings_package_14d_lnd,

		b.margin_gbp_domestic_lnd,
		b.margin_gbp_domestic_1d_lnd,
		b.margin_gbp_domestic_7d_lnd,
		b.margin_gbp_domestic_14d_lnd,

		b.margin_gbp_international_lnd,
		b.margin_gbp_international_1d_lnd,
		b.margin_gbp_international_7d_lnd,
		b.margin_gbp_international_14d_lnd,

		b.margin_gbp_hotel_lnd,
		b.margin_gbp_hotel_1d_lnd,
		b.margin_gbp_hotel_7d_lnd,
		b.margin_gbp_hotel_14d_lnd,

		b.margin_gbp_package_lnd,
		b.margin_gbp_package_1d_lnd,
		b.margin_gbp_package_7d_lnd,
		b.margin_gbp_package_14d_lnd,

		b.gross_revenue_gbp_lnd,
		b.gross_revenue_gbp_1d_lnd,
		b.gross_revenue_gbp_7d_lnd,
		b.gross_revenue_gbp_14d_lnd,

		b.gross_revenue_gbp_domestic_lnd,
		b.gross_revenue_gbp_domestic_1d_lnd,
		b.gross_revenue_gbp_domestic_7d_lnd,
		b.gross_revenue_gbp_domestic_14d_lnd,

		b.gross_revenue_gbp_international_lnd,
		b.gross_revenue_gbp_international_1d_lnd,
		b.gross_revenue_gbp_international_7d_lnd,
		b.gross_revenue_gbp_international_14d_lnd,

		b.gross_revenue_gbp_hotel_lnd,
		b.gross_revenue_gbp_hotel_1d_lnd,
		b.gross_revenue_gbp_hotel_7d_lnd,
		b.gross_revenue_gbp_hotel_14d_lnd,

		b.gross_revenue_gbp_package_lnd,
		b.gross_revenue_gbp_package_1d_lnd,
		b.gross_revenue_gbp_package_7d_lnd,
		b.gross_revenue_gbp_package_14d_lnd,

		s.spvs_lc,
		s.spvs_1d_lc,
		s.spvs_7d_lc,
		s.spvs_14d_lc,

		s.spvs_lnd,
		s.spvs_1d_lnd,
		s.spvs_7d_lnd,
		s.spvs_14d_lnd,

		s.spvs_url,
		s.spvs_1d_url,
		s.spvs_7d_url,
		s.spvs_14d_url,

		b.los,
		b.los_domestic,
		b.los_international,
		b.los_hotel,
		b.los_package

	FROM spvs s
		-- full outer join 'belt and braces' incase a booking occurs
		-- without an spv, eg user has left bfv open.
		FULL OUTER JOIN bookings b ON s.message_id_email_hash = b.message_id_email_hash
)
;


[2025-07-08, 05:26:32 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b66-0106-f537-0002-dd012658cd77
[2025-07-08, 05:26:32 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 7.9680 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.07082705
[2025-07-08, 05:26:32 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:26:32 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__step13__model_rfv AS (
	SELECT
		DATEADD(DAY, 1, rfv.run_date) AS event_date,
		rfv.shiro_user_id,
		ua.signup_tstamp,
		ua.signup_tstamp::DATE        AS signup_date,
		rfv.rfv_segment,
		rfv.lifecycle
	FROM dbt.bi_customer_insight.ci_rfv_segments_historical_weekly rfv
		LEFT JOIN data_vault_mvp.dwh.user_attributes ua ON ua.shiro_user_id = rfv.shiro_user_id
	WHERE rfv.run_date::DATE >= '2021-11-03'
)
;


[2025-07-08, 05:27:33 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b66-0106-f537-0002-dd012658cef7
[2025-07-08, 05:27:33 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 60.7490 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.53999116
[2025-07-08, 05:27:33 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 05:27:33 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting__model_data AS (
	SELECT
		em.message_id_email_hash,
		em.message_id,
		em.campaign_id,
		em.crm_channel_type,
		COALESCE(em.splittable_email_name, em.campaign_name)                   AS combined_email_name,
		IFF(is_automated_campaign, 'Not trading', 'Trading')                   AS trading,
		CASE
			WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amne' THEN 'Newsletter'
			WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amte' THEN 'Trigger'
			WHEN LOWER(SPLIT_PART(em.campaign_name, '_', 1)::VARCHAR) = 'amle' THEN 'Lifecycle'
			ELSE 'Newsletter'
		END                                                                    AS email_type,
		em.splittable_email_name,
		em.mapped_crm_date,
		em.mapped_territory,
		em.current_affiliate_territory,
		COALESCE(em.mapped_objective, SPLIT_PART(combined_email_name, '_', 3)) AS mapped_objective,
		COALESCE(em.mapped_platform, SPLIT_PART(combined_email_name, '_', 4))  AS mapped_platform,
		COALESCE(em.mapped_campaign, SPLIT_PART(combined_email_name, '_', 5))  AS mapped_campaign,
		COALESCE(em.mapped_theme, SPLIT_PART(combined_email_name, '_', 6))     AS mapped_theme,
		COALESCE(em.mapped_segment, SPLIT_PART(combined_email_name, '_', 8))   AS mapped_segment,
		em.is_athena,
		em.is_automated_campaign,
		em.ame_calculated_campaign_name,
		em.email_hash,
		em.shiro_user_id,
		em.campaign_name,
		rfv.rfv_segment,
		rfv.lifecycle                                                          AS rfv_lifecycle,
		CASE
			WHEN rfv.rfv_segment IS NULL AND
				 rfv.signup_date = DATE_TRUNC('week', em.send_event_date)
				THEN 'New Signup in Week'
			ELSE rfv.rfv_segment
		END                                                                    AS rfv_segment_calc,
		CASE
			WHEN rfv.lifecycle IS NULL AND
				 rfv.signup_date = DATE_TRUNC('week', em.send_event_date)
				THEN 'Early Life Active'
			ELSE rfv.lifecycle
		END                                                                    AS rfv_lifecycle_calc,
		em.send_event_date,
		em.send_event_time,
		em.send_start_date,
		em.send_end_date,
		em.email_sends,

		o.email_opens,
		o.email_opens_1d,
		o.email_opens_7d,
		o.email_opens_14d,
		o.unique_email_opens,
		o.unique_email_opens_1d,
		o.unique_email_opens_7d,
		o.unique_email_opens_14d,

		c.email_clicks,
		c.email_clicks_1d,
		c.email_clicks_7d,
		c.email_clicks_14d,
		c.unique_email_clicks,
		c.unique_email_clicks_1d,
		c.unique_email_clicks_7d,
		c.unique_email_clicks_14d,

		in_app_scv.in_app_scv_clicks,
		in_app_scv.in_app_scv_clicks_1d,
		in_app_scv.in_app_scv_clicks_7d,
		in_app_scv.in_app_scv_clicks_14d,
		in_app_scv.unique_in_app_scv_clicks,
		in_app_scv.unique_in_app_scv_clicks_1d,
		in_app_scv.unique_in_app_scv_clicks_7d,
		in_app_scv.unique_in_app_scv_clicks_14d,

		o.first_open_event_date,
		o.first_open_event_time,

		c.first_click_event_date,
		c.first_click_event_time,

		u.unsub_event_date,
		u.unsub_event_time,
		u.email_unsubs,
		u.email_unsubs_1d,
		u.email_unsubs_7d,
		u.email_unsubs_14d,
		u.email_unsubs_complaint,
		u.email_unsubs_complaint_1d,
		u.email_unsubs_complaint_7d,
		u.email_unsubs_complaint_14d,
		u.email_unsubs_email_link,
		u.email_unsubs_email_link_1d,
		u.email_unsubs_email_link_7d,
		u.email_unsubs_email_link_14d,

		COALESCE(scv.bookings_lc, 0)                                           AS bookings_lc,
		COALESCE(scv.bookings_1d_lc, 0)                                        AS bookings_1d_lc,
		COALESCE(scv.bookings_7d_lc, 0)                                        AS bookings_7d_lc,
		COALESCE(scv.bookings_14d_lc, 0)                                       AS bookings_14d_lc,

		COALESCE(scv.margin_gbp_lc, 0)                                         AS margin_gbp_lc,
		COALESCE(scv.margin_gbp_1d_lc, 0)                                      AS margin_gbp_1d_lc,
		COALESCE(scv.margin_gbp_7d_lc, 0)                                      AS margin_gbp_7d_lc,
		COALESCE(scv.margin_gbp_14d_lc, 0)                                     AS margin_gbp_14d_lc,

		COALESCE(scv.bookings_domestic_lc, 0)                                  AS bookings_domestic_lc,
		COALESCE(scv.bookings_domestic_1d_lc, 0)                               AS bookings_domestic_1d_lc,
		COALESCE(scv.bookings_domestic_7d_lc, 0)                               AS bookings_domestic_7d_lc,
		COALESCE(scv.bookings_domestic_14d_lc, 0)                              AS bookings_domestic_14d_lc,

		COALESCE(scv.bookings_international_lc, 0)                             AS bookings_international_lc,
		COALESCE(scv.bookings_international_1d_lc, 0)                          AS bookings_international_1d_lc,
		COALESCE(scv.bookings_international_7d_lc, 0)                          AS bookings_international_7d_lc,
		COALESCE(scv.bookings_international_14d_lc, 0)                         AS bookings_international_14d_lc,

		COALESCE(scv.bookings_hotel_lc, 0)                                     AS bookings_hotel_lc,
		COALESCE(scv.bookings_hotel_1d_lc, 0)                                  AS bookings_hotel_1d_lc,
		COALESCE(scv.bookings_hotel_7d_lc, 0)                                  AS bookings_hotel_7d_lc,
		COALESCE(scv.bookings_hotel_14d_lc, 0)                                 AS bookings_hotel_14d_lc,

		COALESCE(scv.bookings_package_lc, 0)                                   AS bookings_package_lc,
		COALESCE(scv.bookings_package_1d_lc, 0)                                AS bookings_package_1d_lc,
		COALESCE(scv.bookings_package_7d_lc, 0)                                AS bookings_package_7d_lc,
		COALESCE(scv.bookings_package_14d_lc, 0)                               AS bookings_package_14d_lc,

		COALESCE(scv.margin_gbp_domestic_lc, 0)                                AS margin_gbp_domestic_lc,
		COALESCE(scv.margin_gbp_domestic_1d_lc, 0)                             AS margin_gbp_domestic_1d_lc,
		COALESCE(scv.margin_gbp_domestic_7d_lc, 0)                             AS margin_gbp_domestic_7d_lc,
		COALESCE(scv.margin_gbp_domestic_14d_lc, 0)                            AS margin_gbp_domestic_14d_lc,

		COALESCE(scv.margin_gbp_international_lc, 0)                           AS margin_gbp_international_lc,
		COALESCE(scv.margin_gbp_international_1d_lc, 0)                        AS margin_gbp_international_1d_lc,
		COALESCE(scv.margin_gbp_international_7d_lc, 0)                        AS margin_gbp_international_7d_lc,
		COALESCE(scv.margin_gbp_international_14d_lc, 0)                       AS margin_gbp_international_14d_lc,

		COALESCE(scv.margin_gbp_hotel_lc, 0)                                   AS margin_gbp_hotel_lc,
		COALESCE(scv.margin_gbp_hotel_1d_lc, 0)                                AS margin_gbp_hotel_1d_lc,
		COALESCE(scv.margin_gbp_hotel_7d_lc, 0)                                AS margin_gbp_hotel_7d_lc,
		COALESCE(scv.margin_gbp_hotel_14d_lc, 0)                               AS margin_gbp_hotel_14d_lc,

		COALESCE(scv.margin_gbp_package_lc, 0)                                 AS margin_gbp_package_lc,
		COALESCE(scv.margin_gbp_package_1d_lc, 0)                              AS margin_gbp_package_1d_lc,
		COALESCE(scv.margin_gbp_package_7d_lc, 0)                              AS margin_gbp_package_7d_lc,
		COALESCE(scv.margin_gbp_package_14d_lc, 0)                             AS margin_gbp_package_14d_lc,

		COALESCE(scv.gross_revenue_gbp_lc, 0)                                  AS gross_revenue_gbp_lc,
		COALESCE(scv.gross_revenue_gbp_1d_lc, 0)                               AS gross_revenue_gbp_1d_lc,
		COALESCE(scv.gross_revenue_gbp_7d_lc, 0)                               AS gross_revenue_gbp_7d_lc,
		COALESCE(scv.gross_revenue_gbp_14d_lc, 0)                              AS gross_revenue_gbp_14d_lc,

		COALESCE(scv.gross_revenue_gbp_domestic_lc, 0)                         AS gross_revenue_gbp_domestic_lc,
		COALESCE(scv.gross_revenue_gbp_domestic_1d_lc, 0)                      AS gross_revenue_gbp_domestic_1d_lc,
		COALESCE(scv.gross_revenue_gbp_domestic_7d_lc, 0)                      AS gross_revenue_gbp_domestic_7d_lc,
		COALESCE(scv.gross_revenue_gbp_domestic_14d_lc, 0)                     AS gross_revenue_gbp_domestic_14d_lc,

		COALESCE(scv.gross_revenue_gbp_international_lc, 0)                    AS gross_revenue_gbp_international_lc,
		COALESCE(scv.gross_revenue_gbp_international_1d_lc, 0)                 AS gross_revenue_gbp_international_1d_lc,
		COALESCE(scv.gross_revenue_gbp_international_7d_lc, 0)                 AS gross_revenue_gbp_international_7d_lc,
		COALESCE(scv.gross_revenue_gbp_international_14d_lc, 0)                AS gross_revenue_gbp_international_14d_lc,

		COALESCE(scv.gross_revenue_gbp_hotel_lc, 0)                            AS gross_revenue_gbp_hotel_lc,
		COALESCE(scv.gross_revenue_gbp_hotel_1d_lc, 0)                         AS gross_revenue_gbp_hotel_1d_lc,
		COALESCE(scv.gross_revenue_gbp_hotel_7d_lc, 0)                         AS gross_revenue_gbp_hotel_7d_lc,
		COALESCE(scv.gross_revenue_gbp_hotel_14d_lc, 0)                        AS gross_revenue_gbp_hotel_14d_lc,

		COALESCE(scv.gross_revenue_gbp_package_lc, 0)                          AS gross_revenue_gbp_package_lc,
		COALESCE(scv.gross_revenue_gbp_package_1d_lc, 0)                       AS gross_revenue_gbp_package_1d_lc,
		COALESCE(scv.gross_revenue_gbp_package_7d_lc, 0)                       AS gross_revenue_gbp_package_7d_lc,
		COALESCE(scv.gross_revenue_gbp_package_14d_lc, 0)                      AS gross_revenue_gbp_package_14d_lc,

		-- last non direct metrics
		COALESCE(scv.bookings_lnd, 0)                                          AS bookings_lnd,
		COALESCE(scv.bookings_1d_lnd, 0)                                       AS bookings_1d_lnd,
		COALESCE(scv.bookings_7d_lnd, 0)                                       AS bookings_7d_lnd,
		COALESCE(scv.bookings_14d_lnd, 0)                                      AS bookings_14d_lnd,

		COALESCE(scv.margin_gbp_lnd, 0)                                        AS margin_gbp_lnd,
		COALESCE(scv.margin_gbp_1d_lnd, 0)                                     AS margin_gbp_1d_lnd,
		COALESCE(scv.margin_gbp_7d_lnd, 0)                                     AS margin_gbp_7d_lnd,
		COALESCE(scv.margin_gbp_14d_lnd, 0)                                    AS margin_gbp_14d_lnd,

		COALESCE(scv.bookings_domestic_lnd, 0)                                 AS bookings_domestic_lnd,
		COALESCE(scv.bookings_domestic_1d_lnd, 0)                              AS bookings_domestic_1d_lnd,
		COALESCE(scv.bookings_domestic_7d_lnd, 0)                              AS bookings_domestic_7d_lnd,
		COALESCE(scv.bookings_domestic_14d_lnd, 0)                             AS bookings_domestic_14d_lnd,

		COALESCE(scv.bookings_international_lnd, 0)                            AS bookings_international_lnd,
		COALESCE(scv.bookings_international_1d_lnd, 0)                         AS bookings_international_1d_lnd,
		COALESCE(scv.bookings_international_7d_lnd, 0)                         AS bookings_international_7d_lnd,
		COALESCE(scv.bookings_international_14d_lnd, 0)                        AS bookings_international_14d_lnd,

		COALESCE(scv.bookings_hotel_lnd, 0)                                    AS bookings_hotel_lnd,
		COALESCE(scv.bookings_hotel_1d_lnd, 0)                                 AS bookings_hotel_1d_lnd,
		COALESCE(scv.bookings_hotel_7d_lnd, 0)                                 AS bookings_hotel_7d_lnd,
		COALESCE(scv.bookings_hotel_14d_lnd, 0)                                AS bookings_hotel_14d_lnd,

		COALESCE(scv.bookings_package_lnd, 0)                                  AS bookings_package_lnd,
		COALESCE(scv.bookings_package_1d_lnd, 0)                               AS bookings_package_1d_lnd,
		COALESCE(scv.bookings_package_7d_lnd, 0)                               AS bookings_package_7d_lnd,
		COALESCE(scv.bookings_package_14d_lnd, 0)                              AS bookings_package_14d_lnd,

		COALESCE(scv.margin_gbp_domestic_lnd, 0)                               AS margin_gbp_domestic_lnd,
		COALESCE(scv.margin_gbp_domestic_1d_lnd, 0)                            AS margin_gbp_domestic_1d_lnd,
		COALESCE(scv.margin_gbp_domestic_7d_lnd, 0)                            AS margin_gbp_domestic_7d_lnd,
		COALESCE(scv.margin_gbp_domestic_14d_lnd, 0)                           AS margin_gbp_domestic_14d_lnd,

		COALESCE(scv.margin_gbp_international_lnd, 0)                          AS margin_gbp_international_lnd,
		COALESCE(scv.margin_gbp_international_1d_lnd, 0)                       AS margin_gbp_international_1d_lnd,
		COALESCE(scv.margin_gbp_international_7d_lnd, 0)                       AS margin_gbp_international_7d_lnd,
		COALESCE(scv.margin_gbp_international_14d_lnd, 0)                      AS margin_gbp_international_14d_lnd,

		COALESCE(scv.margin_gbp_hotel_lnd, 0)                                  AS margin_gbp_hotel_lnd,
		COALESCE(scv.margin_gbp_hotel_1d_lnd, 0)                               AS margin_gbp_hotel_1d_lnd,
		COALESCE(scv.margin_gbp_hotel_7d_lnd, 0)                               AS margin_gbp_hotel_7d_lnd,
		COALESCE(scv.margin_gbp_hotel_14d_lnd, 0)                              AS margin_gbp_hotel_14d_lnd,

		COALESCE(scv.margin_gbp_package_lnd, 0)                                AS margin_gbp_package_lnd,
		COALESCE(scv.margin_gbp_package_1d_lnd, 0)                             AS margin_gbp_package_1d_lnd,
		COALESCE(scv.margin_gbp_package_7d_lnd, 0)                             AS margin_gbp_package_7d_lnd,
		COALESCE(scv.margin_gbp_package_14d_lnd, 0)                            AS margin_gbp_package_14d_lnd,

		COALESCE(scv.gross_revenue_gbp_lnd, 0)                                 AS gross_revenue_gbp_lnd,
		COALESCE(scv.gross_revenue_gbp_1d_lnd, 0)                              AS gross_revenue_gbp_1d_lnd,
		COALESCE(scv.gross_revenue_gbp_7d_lnd, 0)                              AS gross_revenue_gbp_7d_lnd,
		COALESCE(scv.gross_revenue_gbp_14d_lnd, 0)                             AS gross_revenue_gbp_14d_lnd,

		COALESCE(scv.gross_revenue_gbp_domestic_lnd, 0)                        AS gross_revenue_gbp_domestic_lnd,
		COALESCE(scv.gross_revenue_gbp_domestic_1d_lnd, 0)                     AS gross_revenue_gbp_domestic_1d_lnd,
		COALESCE(scv.gross_revenue_gbp_domestic_7d_lnd, 0)                     AS gross_revenue_gbp_domestic_7d_lnd,
		COALESCE(scv.gross_revenue_gbp_domestic_14d_lnd, 0)                    AS gross_revenue_gbp_domestic_14d_lnd,

		COALESCE(scv.gross_revenue_gbp_international_lnd, 0)                   AS gross_revenue_gbp_international_lnd,
		COALESCE(scv.gross_revenue_gbp_international_1d_lnd, 0)                AS gross_revenue_gbp_international_1d_lnd,
		COALESCE(scv.gross_revenue_gbp_international_7d_lnd, 0)                AS gross_revenue_gbp_international_7d_lnd,
		COALESCE(scv.gross_revenue_gbp_international_14d_lnd, 0)               AS gross_revenue_gbp_international_14d_lnd,

		COALESCE(scv.gross_revenue_gbp_hotel_lnd, 0)                           AS gross_revenue_gbp_hotel_lnd,
		COALESCE(scv.gross_revenue_gbp_hotel_1d_lnd, 0)                        AS gross_revenue_gbp_hotel_1d_lnd,
		COALESCE(scv.gross_revenue_gbp_hotel_7d_lnd, 0)                        AS gross_revenue_gbp_hotel_7d_lnd,
		COALESCE(scv.gross_revenue_gbp_hotel_14d_lnd, 0)                       AS gross_revenue_gbp_hotel_14d_lnd,

		COALESCE(scv.gross_revenue_gbp_package_lnd, 0)                         AS gross_revenue_gbp_package_lnd,
		COALESCE(scv.gross_revenue_gbp_package_1d_lnd, 0)                      AS gross_revenue_gbp_package_1d_lnd,
		COALESCE(scv.gross_revenue_gbp_package_7d_lnd, 0)                      AS gross_revenue_gbp_package_7d_lnd,
		COALESCE(scv.gross_revenue_gbp_package_14d_lnd, 0)                     AS gross_revenue_gbp_package_14d_lnd,

		-- spv metrics
		COALESCE(scv.spvs_lc, 0)                                               AS spvs_lc,
		COALESCE(scv.spvs_1d_lc, 0)                                            AS spvs_1d_lc,
		COALESCE(scv.spvs_7d_lc, 0)                                            AS spvs_7d_lc,
		COALESCE(scv.spvs_14d_lc, 0)                                           AS spvs_14d_lc,

		COALESCE(scv.spvs_lnd, 0)                                              AS spvs_lnd,
		COALESCE(scv.spvs_1d_lnd, 0)                                           AS spvs_1d_lnd,
		COALESCE(scv.spvs_7d_lnd, 0)                                           AS spvs_7d_lnd,
		COALESCE(scv.spvs_14d_lnd, 0)                                          AS spvs_14d_lnd,

		COALESCE(scv.spvs_url, 0)                                              AS spvs_url,
		COALESCE(scv.spvs_1d_url, 0)                                           AS spvs_1d_url,
		COALESCE(scv.spvs_7d_url, 0)                                           AS spvs_7d_url,
		COALESCE(scv.spvs_14d_url, 0)                                          AS spvs_14d_url,
		COALESCE(scv.los, 0)                                                   AS los,
		COALESCE(scv.los_domestic, 0)                                          AS los_domestic,
		COALESCE(scv.los_international, 0)                                     AS los_international,
		COALESCE(scv.los_hotel, 0)                                             AS los_hotel,
		COALESCE(scv.los_package, 0)                                           AS los_package

	FROM data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends em
		-- iterable event data
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step07__aggregate_opens AS o
				  ON o.message_id_email_hash = em.message_id_email_hash
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step08__aggregate_clicks AS c
				  ON c.message_id_email_hash = em.message_id_email_hash
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step08_1__aggregate_clicks_in_app_scv AS in_app_scv
				  ON in_app_scv.message_id_email_hash = em.message_id_email_hash
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step09__aggregate_unsubs AS u
				  ON u.message_id_email_hash = em.message_id_email_hash

					  -- scv event data
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_data scv
				  ON scv.message_id_email_hash = em.message_id_email_hash

					  -- rfv
		LEFT JOIN data_vault_mvp.dwh.iterable_crm_reporting__step13__model_rfv rfv
				  ON rfv.shiro_user_id = em.shiro_user_id
					  AND rfv.event_date = DATE_TRUNC('week', em.send_event_date)
)
;


[2025-07-08, 06:00:39 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b67-0106-f537-0002-dd012658d88b
[2025-07-08, 06:00:39 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 1986.7611 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 17.66009831
[2025-07-08, 06:00:39 UTC] {{assertions.py:207}} INFO - **IO Running self.compute_baseline_query
[2025-07-08, 06:00:39 UTC] {{SQL.py:1909}} INFO - Query:

USE WAREHOUSE pipe_xsmall [2025-07-08, 06:00:40 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b88-0106-f537-0002-dd0126593ea7
[2025-07-08, 06:00:40 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0845 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00075101
[2025-07-08, 06:00:40 UTC] {{SQL.py:1909}} INFO - Query:


SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data

;


[2025-07-08, 06:00:40 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.7345 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00020403
[2025-07-08, 06:00:40 UTC] {{assertions.py:287}} INFO -
        Transform Assertion (kind=warning) :: AssertSomeRows PASSED: 10103685159 > 0 (diff 10103685159). Case detail: We have SOME ROWS.
        baseline_query:

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data
;

-------
baseline: 10103685159
	comparative: 0

[2025-07-08, 06:00:40 UTC] {{assertions.py:207}} INFO - **IO Running self.compute_baseline_query
[2025-07-08, 06:00:40 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 06:00:41 UTC] {{SQL.py:1909}} INFO - Query:

USE WAREHOUSE customer_insight_2xlarge [2025-07-08, 06:00:41 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b88-0106-f537-0002-dd0126593f37
[2025-07-08, 06:00:41 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0865 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002403
[2025-07-08, 06:00:41 UTC] {{SQL.py:1909}} INFO - Query:


SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data

;


[2025-07-08, 06:00:42 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1962 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00174441
[2025-07-08, 06:00:42 UTC] {{assertions.py:455}} INFO - **IO Running self.compute_comparative_query
[2025-07-08, 06:00:42 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 06:00:42 UTC] {{SQL.py:1909}} INFO - Query:


SELECT
	COUNT(*)
FROM (

	SELECT DISTINCT
		message_id_email_hash AS message_id_email_hash
	FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data

)


;


[2025-07-08, 06:02:21 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 99.0107 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.88009515
[2025-07-08, 06:02:21 UTC] {{assertions.py:287}} INFO -
        Transform Assertion (kind=warning) :: AssertNoDuplicates PASSED: 10103685159 = 10103685159 (diff 0). Case detail: We have NO duplicates FOR KEY: '['message_id_email_hash']' IN TABLE 'data_vault_mvp.dwh.iterable_crm_reporting__model_data'
        baseline_query:

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data
;

comparative_query:

SELECT
	COUNT(*)
FROM (
	SELECT DISTINCT
		message_id_email_hash AS message_id_email_hash
	FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data
)
;

-------
baseline: 10103685159
	comparative: 10103685159

[2025-07-08, 06:02:21 UTC] {{SQL.py:1909}} INFO - Query:

USE WAREHOUSE pipe_xsmall [2025-07-08, 06:02:21 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b8a-0106-f537-0002-dd012659697f
[2025-07-08, 06:02:21 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0748 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00066494
[2025-07-08, 06:02:21 UTC] {{SQL.py:1909}} INFO - Query:


CREATE SCHEMA IF NOT EXISTS data_vault_mvp.dwh
;


[2025-07-08, 06:02:21 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b8a-0106-f537-0002-dd012659698f
[2025-07-08, 06:02:21 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0481 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00001336
[2025-07-08, 06:02:21 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 06:02:21 UTC] {{SQL.py:1909}} INFO - Query:

USE WAREHOUSE customer_insight_2xlarge [2025-07-08, 06:02:21 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b8a-0106-f537-0002-dd01265969b3
[2025-07-08, 06:02:21 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0578 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00001607
[2025-07-08, 06:02:21 UTC] {{SQL.py:1909}} INFO - Query:


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable_crm_reporting COPY GRANTS
(

	-- (lineage) metadata for the current job
	schedule_tstamp                         TIMESTAMP,
	run_tstamp                              TIMESTAMP,
	operation_id                            VARCHAR,
	created_at                              TIMESTAMP,
	updated_at                              TIMESTAMP,

	message_id_email_hash                   VARCHAR NOT NULL PRIMARY KEY,
	message_id                              VARCHAR,
	campaign_id                             NUMBER,
	crm_channel_type                        VARCHAR,
	combined_email_name                     VARCHAR,
	trading                                 VARCHAR,
	email_type                              VARCHAR,
	splittable_email_name                   VARCHAR,
	mapped_crm_date                         VARCHAR,
	mapped_territory                        VARCHAR,
	current_affiliate_territory             VARCHAR,
	mapped_objective                        VARCHAR,
	mapped_platform                         VARCHAR,
	mapped_campaign                         VARCHAR,
	mapped_theme                            VARCHAR,
	mapped_segment                          VARCHAR,
	is_athena                               BOOLEAN,
	is_automated_campaign                   BOOLEAN,
	ame_calculated_campaign_name            VARCHAR,
	email_hash                              VARCHAR,
	shiro_user_id                           NUMBER,
	campaign_name                           VARCHAR,
	rfv_segment                             VARCHAR,
	rfv_lifecycle                           VARCHAR,
	rfv_segment_calc                        VARCHAR,
	rfv_lifecycle_calc                      VARCHAR,
	send_event_date                         DATE,
	send_event_time                         TIMESTAMP,
	send_start_date                         DATE,
	send_end_date                           DATE,
	email_sends                             NUMBER,
	email_opens                             NUMBER,
	email_opens_1d                          NUMBER,
	email_opens_7d                          NUMBER,
	email_opens_14d                         NUMBER,
	unique_email_opens                      NUMBER,
	unique_email_opens_1d                   NUMBER,
	unique_email_opens_7d                   NUMBER,
	unique_email_opens_14d                  NUMBER,
	email_clicks                            NUMBER,
	email_clicks_1d                         NUMBER,
	email_clicks_7d                         NUMBER,
	email_clicks_14d                        NUMBER,
	unique_email_clicks                     NUMBER,
	unique_email_clicks_1d                  NUMBER,
	unique_email_clicks_7d                  NUMBER,
	unique_email_clicks_14d                 NUMBER,
	in_app_scv_clicks                       NUMBER,
	in_app_scv_clicks_1d                    NUMBER,
	in_app_scv_clicks_7d                    NUMBER,
	in_app_scv_clicks_14d                   NUMBER,
	unique_in_app_scv_clicks                NUMBER,
	unique_in_app_scv_clicks_1d             NUMBER,
	unique_in_app_scv_clicks_7d             NUMBER,
	unique_in_app_scv_clicks_14d            NUMBER,
	first_open_event_date                   DATE,
	first_open_event_time                   TIMESTAMP,
	first_click_event_date                  DATE,
	first_click_event_time                  TIMESTAMP,
	unsub_event_date                        DATE,
	unsub_event_time                        TIMESTAMP,
	email_unsubs                            NUMBER,
	email_unsubs_1d                         NUMBER,
	email_unsubs_7d                         NUMBER,
	email_unsubs_14d                        NUMBER,
	email_unsubs_complaint                  NUMBER,
	email_unsubs_complaint_1d               NUMBER,
	email_unsubs_complaint_7d               NUMBER,
	email_unsubs_complaint_14d              NUMBER,
	email_unsubs_email_link                 NUMBER,
	email_unsubs_email_link_1d              NUMBER,
	email_unsubs_email_link_7d              NUMBER,
	email_unsubs_email_link_14d             NUMBER,
	bookings_lc                             NUMBER,
	bookings_1d_lc                          NUMBER,
	bookings_7d_lc                          NUMBER,
	bookings_14d_lc                         NUMBER,
	margin_gbp_lc                           NUMBER,
	margin_gbp_1d_lc                        NUMBER,
	margin_gbp_7d_lc                        NUMBER,
	margin_gbp_14d_lc                       NUMBER,
	bookings_domestic_lc                    NUMBER,
	bookings_domestic_1d_lc                 NUMBER,
	bookings_domestic_7d_lc                 NUMBER,
	bookings_domestic_14d_lc                NUMBER,
	bookings_international_lc               NUMBER,
	bookings_international_1d_lc            NUMBER,
	bookings_international_7d_lc            NUMBER,
	bookings_international_14d_lc           NUMBER,
	bookings_hotel_lc                       NUMBER,
	bookings_hotel_1d_lc                    NUMBER,
	bookings_hotel_7d_lc                    NUMBER,
	bookings_hotel_14d_lc                   NUMBER,
	bookings_package_lc                     NUMBER,
	bookings_package_1d_lc                  NUMBER,
	bookings_package_7d_lc                  NUMBER,
	bookings_package_14d_lc                 NUMBER,
	margin_gbp_domestic_lc                  NUMBER,
	margin_gbp_domestic_1d_lc               NUMBER,
	margin_gbp_domestic_7d_lc               NUMBER,
	margin_gbp_domestic_14d_lc              NUMBER,
	margin_gbp_international_lc             NUMBER,
	margin_gbp_international_1d_lc          NUMBER,
	margin_gbp_international_7d_lc          NUMBER,
	margin_gbp_international_14d_lc         NUMBER,
	margin_gbp_hotel_lc                     NUMBER,
	margin_gbp_hotel_1d_lc                  NUMBER,
	margin_gbp_hotel_7d_lc                  NUMBER,
	margin_gbp_hotel_14d_lc                 NUMBER,
	margin_gbp_package_lc                   NUMBER,
	margin_gbp_package_1d_lc                NUMBER,
	margin_gbp_package_7d_lc                NUMBER,
	margin_gbp_package_14d_lc               NUMBER,
	gross_revenue_gbp_lc                    NUMBER,
	gross_revenue_gbp_1d_lc                 NUMBER,
	gross_revenue_gbp_7d_lc                 NUMBER,
	gross_revenue_gbp_14d_lc                NUMBER,
	gross_revenue_gbp_domestic_lc           NUMBER,
	gross_revenue_gbp_domestic_1d_lc        NUMBER,
	gross_revenue_gbp_domestic_7d_lc        NUMBER,
	gross_revenue_gbp_domestic_14d_lc       NUMBER,
	gross_revenue_gbp_international_lc      NUMBER,
	gross_revenue_gbp_international_1d_lc   NUMBER,
	gross_revenue_gbp_international_7d_lc   NUMBER,
	gross_revenue_gbp_international_14d_lc  NUMBER,
	gross_revenue_gbp_hotel_lc              NUMBER,
	gross_revenue_gbp_hotel_1d_lc           NUMBER,
	gross_revenue_gbp_hotel_7d_lc           NUMBER,
	gross_revenue_gbp_hotel_14d_lc          NUMBER,
	gross_revenue_gbp_package_lc            NUMBER,
	gross_revenue_gbp_package_1d_lc         NUMBER,
	gross_revenue_gbp_package_7d_lc         NUMBER,
	gross_revenue_gbp_package_14d_lc        NUMBER,
	bookings_lnd                            NUMBER,
	bookings_1d_lnd                         NUMBER,
	bookings_7d_lnd                         NUMBER,
	bookings_14d_lnd                        NUMBER,
	margin_gbp_lnd                          NUMBER,
	margin_gbp_1d_lnd                       NUMBER,
	margin_gbp_7d_lnd                       NUMBER,
	margin_gbp_14d_lnd                      NUMBER,
	bookings_domestic_lnd                   NUMBER,
	bookings_domestic_1d_lnd                NUMBER,
	bookings_domestic_7d_lnd                NUMBER,
	bookings_domestic_14d_lnd               NUMBER,
	bookings_international_lnd              NUMBER,
	bookings_international_1d_lnd           NUMBER,
	bookings_international_7d_lnd           NUMBER,
	bookings_international_14d_lnd          NUMBER,
	bookings_hotel_lnd                      NUMBER,
	bookings_hotel_1d_lnd                   NUMBER,
	bookings_hotel_7d_lnd                   NUMBER,
	bookings_hotel_14d_lnd                  NUMBER,
	bookings_package_lnd                    NUMBER,
	bookings_package_1d_lnd                 NUMBER,
	bookings_package_7d_lnd                 NUMBER,
	bookings_package_14d_lnd                NUMBER,
	margin_gbp_domestic_lnd                 NUMBER,
	margin_gbp_domestic_1d_lnd              NUMBER,
	margin_gbp_domestic_7d_lnd              NUMBER,
	margin_gbp_domestic_14d_lnd             NUMBER,
	margin_gbp_international_lnd            NUMBER,
	margin_gbp_international_1d_lnd         NUMBER,
	margin_gbp_international_7d_lnd         NUMBER,
	margin_gbp_international_14d_lnd        NUMBER,
	margin_gbp_hotel_lnd                    NUMBER,
	margin_gbp_hotel_1d_lnd                 NUMBER,
	margin_gbp_hotel_7d_lnd                 NUMBER,
	margin_gbp_hotel_14d_lnd                NUMBER,
	margin_gbp_package_lnd                  NUMBER,
	margin_gbp_package_1d_lnd               NUMBER,
	margin_gbp_package_7d_lnd               NUMBER,
	margin_gbp_package_14d_lnd              NUMBER,
	gross_revenue_gbp_lnd                   NUMBER,
	gross_revenue_gbp_1d_lnd                NUMBER,
	gross_revenue_gbp_7d_lnd                NUMBER,
	gross_revenue_gbp_14d_lnd               NUMBER,
	gross_revenue_gbp_domestic_lnd          NUMBER,
	gross_revenue_gbp_domestic_1d_lnd       NUMBER,
	gross_revenue_gbp_domestic_7d_lnd       NUMBER,
	gross_revenue_gbp_domestic_14d_lnd      NUMBER,
	gross_revenue_gbp_international_lnd     NUMBER,
	gross_revenue_gbp_international_1d_lnd  NUMBER,
	gross_revenue_gbp_international_7d_lnd  NUMBER,
	gross_revenue_gbp_international_14d_lnd NUMBER,
	gross_revenue_gbp_hotel_lnd             NUMBER,
	gross_revenue_gbp_hotel_1d_lnd          NUMBER,
	gross_revenue_gbp_hotel_7d_lnd          NUMBER,
	gross_revenue_gbp_hotel_14d_lnd         NUMBER,
	gross_revenue_gbp_package_lnd           NUMBER,
	gross_revenue_gbp_package_1d_lnd        NUMBER,
	gross_revenue_gbp_package_7d_lnd        NUMBER,
	gross_revenue_gbp_package_14d_lnd       NUMBER,
	spvs_lc                                 NUMBER,
	spvs_1d_lc                              NUMBER,
	spvs_7d_lc                              NUMBER,
	spvs_14d_lc                             NUMBER,
	spvs_lnd                                NUMBER,
	spvs_1d_lnd                             NUMBER,
	spvs_7d_lnd                             NUMBER,
	spvs_14d_lnd                            NUMBER,
	spvs_url                                NUMBER,
	spvs_1d_url                             NUMBER,
	spvs_7d_url                             NUMBER,
	spvs_14d_url                            NUMBER,
	los                                     NUMBER,
	los_domestic                            NUMBER,
	los_international                       NUMBER,
	los_hotel                               NUMBER,
	los_package                             NUMBER

)
	CLUSTER BY (campaign_id, send_event_date::DATE)
;
;


[2025-07-08, 06:02:22 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b8a-0106-f537-0002-dd01265969c7
[2025-07-08, 06:02:22 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.4404 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00391509
[2025-07-08, 06:02:22 UTC] {{SQL.py:1889}} INFO - USING CUSTOM WAREHOUSE: 'customer_insight_2xlarge'
[2025-07-08, 06:02:22 UTC] {{SQL.py:1909}} INFO - Query:


INSERT INTO data_vault_mvp.dwh.iterable_crm_reporting
SELECT
	'2025-07-07 04:30:00',
	'2025-07-08 04:32:05',
	'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting.py__20250707T043000__daily_at_04h30',
	CURRENT_TIMESTAMP()::TIMESTAMP,
	CURRENT_TIMESTAMP()::TIMESTAMP,

	batch.message_id_email_hash,
	batch.message_id,
	batch.campaign_id,
	batch.crm_channel_type,
	batch.combined_email_name,
	batch.trading,
	batch.email_type,
	batch.splittable_email_name,
	batch.mapped_crm_date,
	batch.mapped_territory,
	batch.current_affiliate_territory,
	batch.mapped_objective,
	batch.mapped_platform,
	batch.mapped_campaign,
	batch.mapped_theme,
	batch.mapped_segment,
	batch.is_athena,
	batch.is_automated_campaign,
	batch.ame_calculated_campaign_name,
	batch.email_hash,
	batch.shiro_user_id,
	batch.campaign_name,
	batch.rfv_segment,
	batch.rfv_lifecycle,
	batch.rfv_segment_calc,
	batch.rfv_lifecycle_calc,
	batch.send_event_date,
	batch.send_event_time,
	batch.send_start_date,
	batch.send_end_date,
	batch.email_sends,
	batch.email_opens,
	batch.email_opens_1d,
	batch.email_opens_7d,
	batch.email_opens_14d,
	batch.unique_email_opens,
	batch.unique_email_opens_1d,
	batch.unique_email_opens_7d,
	batch.unique_email_opens_14d,
	batch.email_clicks,
	batch.email_clicks_1d,
	batch.email_clicks_7d,
	batch.email_clicks_14d,
	batch.unique_email_clicks,
	batch.unique_email_clicks_1d,
	batch.unique_email_clicks_7d,
	batch.unique_email_clicks_14d,
	batch.in_app_scv_clicks,
	batch.in_app_scv_clicks_1d,
	batch.in_app_scv_clicks_7d,
	batch.in_app_scv_clicks_14d,
	batch.unique_in_app_scv_clicks,
	batch.unique_in_app_scv_clicks_1d,
	batch.unique_in_app_scv_clicks_7d,
	batch.unique_in_app_scv_clicks_14d,
	batch.first_open_event_date,
	batch.first_open_event_time,
	batch.first_click_event_date,
	batch.first_click_event_time,
	batch.unsub_event_date,
	batch.unsub_event_time,
	batch.email_unsubs,
	batch.email_unsubs_1d,
	batch.email_unsubs_7d,
	batch.email_unsubs_14d,
	batch.email_unsubs_complaint,
	batch.email_unsubs_complaint_1d,
	batch.email_unsubs_complaint_7d,
	batch.email_unsubs_complaint_14d,
	batch.email_unsubs_email_link,
	batch.email_unsubs_email_link_1d,
	batch.email_unsubs_email_link_7d,
	batch.email_unsubs_email_link_14d,
	batch.bookings_lc,
	batch.bookings_1d_lc,
	batch.bookings_7d_lc,
	batch.bookings_14d_lc,
	batch.margin_gbp_lc,
	batch.margin_gbp_1d_lc,
	batch.margin_gbp_7d_lc,
	batch.margin_gbp_14d_lc,
	batch.bookings_domestic_lc,
	batch.bookings_domestic_1d_lc,
	batch.bookings_domestic_7d_lc,
	batch.bookings_domestic_14d_lc,
	batch.bookings_international_lc,
	batch.bookings_international_1d_lc,
	batch.bookings_international_7d_lc,
	batch.bookings_international_14d_lc,
	batch.bookings_hotel_lc,
	batch.bookings_hotel_1d_lc,
	batch.bookings_hotel_7d_lc,
	batch.bookings_hotel_14d_lc,
	batch.bookings_package_lc,
	batch.bookings_package_1d_lc,
	batch.bookings_package_7d_lc,
	batch.bookings_package_14d_lc,
	batch.margin_gbp_domestic_lc,
	batch.margin_gbp_domestic_1d_lc,
	batch.margin_gbp_domestic_7d_lc,
	batch.margin_gbp_domestic_14d_lc,
	batch.margin_gbp_international_lc,
	batch.margin_gbp_international_1d_lc,
	batch.margin_gbp_international_7d_lc,
	batch.margin_gbp_international_14d_lc,
	batch.margin_gbp_hotel_lc,
	batch.margin_gbp_hotel_1d_lc,
	batch.margin_gbp_hotel_7d_lc,
	batch.margin_gbp_hotel_14d_lc,
	batch.margin_gbp_package_lc,
	batch.margin_gbp_package_1d_lc,
	batch.margin_gbp_package_7d_lc,
	batch.margin_gbp_package_14d_lc,
	batch.gross_revenue_gbp_lc,
	batch.gross_revenue_gbp_1d_lc,
	batch.gross_revenue_gbp_7d_lc,
	batch.gross_revenue_gbp_14d_lc,
	batch.gross_revenue_gbp_domestic_lc,
	batch.gross_revenue_gbp_domestic_1d_lc,
	batch.gross_revenue_gbp_domestic_7d_lc,
	batch.gross_revenue_gbp_domestic_14d_lc,
	batch.gross_revenue_gbp_international_lc,
	batch.gross_revenue_gbp_international_1d_lc,
	batch.gross_revenue_gbp_international_7d_lc,
	batch.gross_revenue_gbp_international_14d_lc,
	batch.gross_revenue_gbp_hotel_lc,
	batch.gross_revenue_gbp_hotel_1d_lc,
	batch.gross_revenue_gbp_hotel_7d_lc,
	batch.gross_revenue_gbp_hotel_14d_lc,
	batch.gross_revenue_gbp_package_lc,
	batch.gross_revenue_gbp_package_1d_lc,
	batch.gross_revenue_gbp_package_7d_lc,
	batch.gross_revenue_gbp_package_14d_lc,
	batch.bookings_lnd,
	batch.bookings_1d_lnd,
	batch.bookings_7d_lnd,
	batch.bookings_14d_lnd,
	batch.margin_gbp_lnd,
	batch.margin_gbp_1d_lnd,
	batch.margin_gbp_7d_lnd,
	batch.margin_gbp_14d_lnd,
	batch.bookings_domestic_lnd,
	batch.bookings_domestic_1d_lnd,
	batch.bookings_domestic_7d_lnd,
	batch.bookings_domestic_14d_lnd,
	batch.bookings_international_lnd,
	batch.bookings_international_1d_lnd,
	batch.bookings_international_7d_lnd,
	batch.bookings_international_14d_lnd,
	batch.bookings_hotel_lnd,
	batch.bookings_hotel_1d_lnd,
	batch.bookings_hotel_7d_lnd,
	batch.bookings_hotel_14d_lnd,
	batch.bookings_package_lnd,
	batch.bookings_package_1d_lnd,
	batch.bookings_package_7d_lnd,
	batch.bookings_package_14d_lnd,
	batch.margin_gbp_domestic_lnd,
	batch.margin_gbp_domestic_1d_lnd,
	batch.margin_gbp_domestic_7d_lnd,
	batch.margin_gbp_domestic_14d_lnd,
	batch.margin_gbp_international_lnd,
	batch.margin_gbp_international_1d_lnd,
	batch.margin_gbp_international_7d_lnd,
	batch.margin_gbp_international_14d_lnd,
	batch.margin_gbp_hotel_lnd,
	batch.margin_gbp_hotel_1d_lnd,
	batch.margin_gbp_hotel_7d_lnd,
	batch.margin_gbp_hotel_14d_lnd,
	batch.margin_gbp_package_lnd,
	batch.margin_gbp_package_1d_lnd,
	batch.margin_gbp_package_7d_lnd,
	batch.margin_gbp_package_14d_lnd,
	batch.gross_revenue_gbp_lnd,
	batch.gross_revenue_gbp_1d_lnd,
	batch.gross_revenue_gbp_7d_lnd,
	batch.gross_revenue_gbp_14d_lnd,
	batch.gross_revenue_gbp_domestic_lnd,
	batch.gross_revenue_gbp_domestic_1d_lnd,
	batch.gross_revenue_gbp_domestic_7d_lnd,
	batch.gross_revenue_gbp_domestic_14d_lnd,
	batch.gross_revenue_gbp_international_lnd,
	batch.gross_revenue_gbp_international_1d_lnd,
	batch.gross_revenue_gbp_international_7d_lnd,
	batch.gross_revenue_gbp_international_14d_lnd,
	batch.gross_revenue_gbp_hotel_lnd,
	batch.gross_revenue_gbp_hotel_1d_lnd,
	batch.gross_revenue_gbp_hotel_7d_lnd,
	batch.gross_revenue_gbp_hotel_14d_lnd,
	batch.gross_revenue_gbp_package_lnd,
	batch.gross_revenue_gbp_package_1d_lnd,
	batch.gross_revenue_gbp_package_7d_lnd,
	batch.gross_revenue_gbp_package_14d_lnd,
	batch.spvs_lc,
	batch.spvs_1d_lc,
	batch.spvs_7d_lc,
	batch.spvs_14d_lc,
	batch.spvs_lnd,
	batch.spvs_1d_lnd,
	batch.spvs_7d_lnd,
	batch.spvs_14d_lnd,
	batch.spvs_url,
	batch.spvs_1d_url,
	batch.spvs_7d_url,
	batch.spvs_14d_url,
	batch.los,
	batch.los_domestic,
	batch.los_international,
	batch.los_hotel,
	batch.los_package
FROM data_vault_mvp.dwh.iterable_crm_reporting__model_data batch
;


[2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b8a-0106-f537-0002-dd0126596a3b
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 522.8204 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 4.64729258
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

USE WAREHOUSE pipe_xsmall [2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e32b
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0839 seconds, Snowflake WAREHOUSE size = customer_insight_2xlarge, Snowflake credits used (estimate) = 0.00074593
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step01__campaign_enrichment
;

[2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e333
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1641 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00004559
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step02__model_sends
;

[2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e33f
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1458 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00004051
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step03__model_opens
;

[2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e34f
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1212 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00003367
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step04__model_clicks
;

[2025-07-08, 06:11:05 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e357
[2025-07-08, 06:11:05 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0965 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002682
[2025-07-08, 06:11:05 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step04_1__model_in_app_scv_clicks
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e35b
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1248 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00003466
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step05__model_unsubs
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e363
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1030 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002862
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step06__aggregate_sends
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e367
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1039 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002885
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step07__aggregate_opens
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e36b
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1238 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00003440
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step08__aggregate_clicks
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e36f
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0973 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002702
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step08_1__aggregate_clicks_in_app_scv
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e373
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1074 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002982
[2025-07-08, 06:11:06 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step09__aggregate_unsubs
;

[2025-07-08, 06:11:06 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e377
[2025-07-08, 06:11:06 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.0857 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002381
[2025-07-08, 06:11:07 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step10__model_scv_booking_data
;

[2025-07-08, 06:11:07 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e37f
[2025-07-08, 06:11:07 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1050 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002917
[2025-07-08, 06:11:07 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step11__model_scv_spv_data
;

[2025-07-08, 06:11:07 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e387
[2025-07-08, 06:11:07 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1049 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00002914
[2025-07-08, 06:11:07 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step12__model_scv_data
;

[2025-07-08, 06:11:07 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e397
[2025-07-08, 06:11:07 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1366 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00003795
[2025-07-08, 06:11:07 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__step13__model_rfv
;

[2025-07-08, 06:11:07 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e3ab
[2025-07-08, 06:11:07 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1822 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00005060
[2025-07-08, 06:11:07 UTC] {{SQL.py:1909}} INFO - Query:

DROP TABLE data_vault_mvp.dwh.iterable_crm_reporting__model_data
;

[2025-07-08, 06:11:07 UTC] {{SQL.py:1917}} INFO - Snowflake query ID = 01bd8b93-0106-f537-0002-dd012659e3bf
[2025-07-08, 06:11:07 UTC] {{smart_timer.py:58}} INFO - Query elapsed TIME = 0.1158 seconds, Snowflake WAREHOUSE size = pipe_xsmall, Snowflake credits used (estimate) = 0.00003216
[2025-07-08, 06:11:08 UTC] {{taskinstance.py:1318}} INFO - Marking TASK AS SUCCESS. dag_id=dwh__iterable_crm_reporting__daily_at_04h30, task_id=SelfDescribingOperation__dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting.py, execution_date=20250707T043000, start_date=20250708T043204, end_date=20250708T061108
[2025-07-08, 06:11:08 UTC] {{local_task_job.py:208}} INFO - TASK exited WITH RETURN code 0
[2025-07-08, 06:11:09 UTC] {{taskinstance.py:2578}} INFO - 0 DOWNSTREAM TASKS scheduled FROM follow-ON SCHEDULE CHECK;;

SELECT *
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.email_sends > 1
