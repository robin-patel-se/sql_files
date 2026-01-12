SELECT dr.id,
       dr.dag_id,
       dr.execution_date,
       dr.state                    AS dag_state,
       dr.run_id,
       dr.start_date               AS dag_start_date,
       dr.end_date                 AS dag_end_date,
       dr.end_date - dr.start_date AS dag_duration,
       ti.task_id,
       ti.state                    AS task_state,
       ti.duration,
       ti.start_date               AS task_start_date,
       ti.end_date                 AS task_end_date,
       ti.end_date - ti.start_date AS task_duration
FROM dag_run dr
         LEFT JOIN task_instance ti ON dr.dag_id = ti.dag_id AND dr.execution_date = ti.execution_date
WHERE dr.dag_id = 'single_customer_view__daily'
  AND dr.start_date::DATE = '2020-04-14'
  AND ti.task_id NOT LIKE 'wait%'
  AND ti.duration IS NOT NULL
ORDER BY ti.duration DESC;
