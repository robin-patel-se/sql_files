SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1
  AND qh.user_name NOT IN ('PIPELINERUNNER', 'SNOWPLOW', 'TABLEAU', 'DATASCIENCEAPI', 'DATASCIENCERUNNER')
  AND qh.query_type NOT IN ('DESCRIBE',
                            'SHOW',
                            'USE',
                            'GET_FILES',
                            'CREATE_TABLE_AS_SELECT',
                            'ROLLBACK',
                            'PUT_FILES',
                            'SET',
                            'UNKNOWN',
                            'DROP',
                            'REMOVE_FILES'
    )
AND qh.warehouse_id IS NOT NULL
;


SELECT DATE_TRUNC(MINUTE, qh.start_time) AS query_minute,
       qh.warehouse_name,
       COUNT(*)                          AS queries
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1
  AND qh.user_name NOT IN ('PIPELINERUNNER', 'SNOWPLOW', 'TABLEAU', 'DATASCIENCEAPI', 'DATASCIENCERUNNER')
  AND qh.query_type NOT IN ('DESCRIBE',
                            'SHOW',
                            'USE',
                            'GET_FILES',
                            'CREATE_TABLE_AS_SELECT',
                            'ROLLBACK',
                            'PUT_FILES',
                            'SET',
                            'UNKNOWN',
                            'DROP',
                            'REMOVE_FILES'
    )
GROUP BY 1, 2
;


SELECT qh.query_type, COUNT(*)
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1
  AND qh.user_name NOT IN ('PIPELINERUNNER', 'SNOWPLOW', 'TABLEAU', 'DATASCIENCEAPI', 'DATASCIENCERUNNER')
  AND qh.query_type NOT IN ('DESCRIBE',
                            'SHOW',
                            'USE',
                            'GET_FILES',
                            'CREATE_TABLE_AS_SELECT',
                            'ROLLBACK',
                            'PUT_FILES',
                            'SET',
                            'UNKNOWN'
    )
GROUP BY 1