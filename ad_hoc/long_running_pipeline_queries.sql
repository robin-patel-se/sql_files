SELECT *
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE - 5
;


SELECT
	qh.pipeline_filename,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE - 5
GROUP BY 1
ORDER BY 2 DESC
;

SELECT
	qh.pipeline_script_path,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE
GROUP BY 1
ORDER BY 2 DESC
;



SELECT
-- 	qh.pipeline_script_path,
	SUM(qh.cost__query_duration)
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.user_name = 'PIPELINERUNNER'
  AND qh.start_time >= CURRENT_DATE
ANd qh.pipeline_script_path LIKE '/usr/local/airflow/dags/biapp/manifests/incoming/ari%'
-- GROUP BY 1
-- ORDER BY 2 DESC
;