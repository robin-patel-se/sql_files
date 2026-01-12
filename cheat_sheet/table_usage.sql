-- TABLES NOT being queried IN LAST X DAYS
WITH
	tablesrecent AS
		(
			SELECT
				f1.value:"objectName"::string AS tn
			FROM snowflake.account_usage.access_history
					,
				 LATERAL FLATTEN(base_objects_accessed) f1
			WHERE f1.value:"objectDomain"::string = 'Table'
			  AND f1.value:"objectId" IS NOT NULL
			  AND query_start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
			GROUP BY 1
		),

	tablesall AS
		(
			SELECT
				table_id::integer                                         AS tid,
				table_catalog || '.' || table_schema || '.' || table_name AS tn1
			FROM snowflake.account_usage.tables
			WHERE deleted IS NULL
		)

SELECT *
FROM tablesall
WHERE tn1 NOT IN (
	SELECT
		tn
	FROM tablesrecent
)
;

USE ROLE pipelinerunner
;

SELECT *
FROM snowflake.account_usage.access_history
;

------------------------------------------------------------------------------------------------------------------------


USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.bi.chrt_fact_cohort_metrics, se.bi.chrt_fact_cohort_metrics')
;

SELECT *
FROM scratch.robinpatel.table_usage
;


------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.dwh.sale_active, se.data.sale_active')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view',
												'data_vault_mvp.dwh.sale_active, se.data.sale_active',
												'collab, data_vault_mvp, se, data_science')
;

SELECT *
FROM scratch.robinpatel.table_reference_in_view
;
