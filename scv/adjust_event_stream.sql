SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %';

SELECT user_name,
       MIN(qh.start_time) AS min_start_time,
       MAX(qh.start_time) AS max_start_time,
       COUNT(*)           AS queries
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %'
GROUP BY 1;

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %'
  AND qh.user_name IS DISTINCT FROM 'PIPELINERUNNER'
  AND qh.execution_status = 'SUCCESS';

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %'
  AND qh.user_name = 'DATASCIENCERUNNER'
  AND qh.execution_status = 'SUCCESS';

SELECT qh.start_time::DATE,
       AVG(DATEDIFF(SECOND, qh.start_time, qh.end_time)) / 60 AS duration,
       COUNT(*)
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %'
  AND qh.user_name = 'DATASCIENCERUNNER'
  AND qh.execution_status = 'SUCCESS'
GROUP BY 1;



SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '% hygiene_vault_mvp.snowplow.event_stream %'
  AND qh.user_name = 'DATASCIENCERUNNER'
  AND qh.execution_status = 'SUCCESS'
  AND qh.start_time >= CURRENT_DATE - 1;

SELECT *
FROM scratch.information_schema.views v
WHERE LOWER(v.view_definition) LIKE '% hygiene_vault_mvp.snowplow.event_stream %';

SELECT *
FROM collab.information_schema.views v
WHERE LOWER(v.view_definition) LIKE '% hygiene_vault_mvp.snowplow.event_stream %';

