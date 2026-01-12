SELECT *,
       dr.end_date - dr.start_date as diff
FROM dag_run dr
WHERE dr.execution_date::DATE = current_date - 2
ORDER BY diff DESC

;
-- WHERE dr.dag_id = 'single_customer_view__daily_at_03h00'



