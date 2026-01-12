-- biapp/task_catalogue/dv/bi/scv/customer_yearly_first_session.py
-- biapp/task_catalogue/se/bi/scv/customer_yearly_first_session.py

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.bi.customer_yearly_first_session, se.bi.customer_yearly_first_session')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

-- 0 rows

-- No pipeline dependencies found
-- Query elapsed time = 187.3001 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.20811119
-- Query elapsed time = 10.1063 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.01122920

-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/bi__customer_yearly_first_session__daily_at_04h30/grid?dag_run_id=scheduled__2025-06-18T04%3A30%3A00%2B00%3A00&task_id=SelfDescribingOperation__dv.bi.scv.customer_yearly_first_session.py
------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/bi/booking/customer_yearly_booking.py
-- biapp/task_catalogue/se/bi/booking/customer_yearly_booking.py


CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.bi.customer_yearly_booking, se.bi.customer_yearly_booking')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

-- 0 rows

-- No pipeline dependencies found
-- Query elapsed time = 3.8530 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.00428107
-- Query elapsed time = 55.6242 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.06180469
-- Query elapsed time = 7.2567 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.00806304

-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/bi__customer_yearly_booking__daily_at_04h30/grid?dag_run_id=scheduled__2025-06-18T04%3A30%3A00%2B00%3A00&task_id=SelfDescribingOperation__dv.bi.booking.customer_yearly_booking.py
------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/bi/email/daily_customer_email_activity.py
-- biapp/task_catalogue/se/bi/email/daily_customer_email_activity.py

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.bi.daily_customer_email_activity, se.bi.daily_customer_email_activity')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

-- 0 rows

-- Only ONE dependency monthly_email_metrics_by_segment -- which is an aggregation (and segmentation) of this dataset
-- Query elapsed time = 15.5660 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.01729559
-- Query elapsed time = 18.8147 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.02090522

-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/bi__daily_customer_email_activity__daily_at_05h30/grid?dag_run_id=scheduled__2025-06-18T05%3A30%3A00%2B00%3A00&task_id=SelfDescribingOperation__dv.bi.email.daily_customer_email_activity.py

------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/bi/email/monthly_email_metrics_by_segment.py
-- biapp/task_catalogue/se/bi/email/monthly_email_metrics_by_segment.py

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.bi.monthly_email_metrics_by_segment, se.bi.monthly_email_metrics_by_segment')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

-- 0 rows

-- No pipeline dependencies found
-- Query elapsed time = 614.0546 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.68228288
-- Query elapsed time = 2.9910 seconds, Snowflake warehouse size = pipe_medium, Snowflake credits used (estimate) = 0.00332338

-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/bi__monthly_email_metrics_by_segment__daily_at_05h30/grid?dag_run_id=scheduled__2025-06-18T05%3A30%3A00%2B00%3A00&task_id=SelfDescribingOperation__dv.bi.email.monthly_email_metrics_by_segment.py


SELECT *
FROM data_vault_mvp.bi.sale_date_spvs
;

SELECT
	MAX(bpe.event_tstamp)
FROM data_vault_mvp.bi.branch_purchase_events bpe
;

SELECT *
FROM se.data.scv_touched_app_installs stai
;

------------------------------------------------------------------------------------------------------------------------

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.dwh.iterable_crm_reporting_insertions')
;

SELECT *
FROM scratch.robinpatel.table_usage
; -- 0 rows


CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'data_vault_mvp.dwh.email_reporting_iterable, data_vault_mvp.dwh.email_reporting, se.data.email_reporting')
;

SELECT *
FROM scratch.robinpatel.table_usage
; -- 0 rows


CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'se.data.sale_territory_active_snapshot')
;

SELECT *
FROM scratch.robinpatel.table_usage
; -- 0 rows


SELECT DISTINCT
	name_tracker
FROM snowplow.atomic.events
;

SELECT COALESCE(NULL, NULLIF(NULL, ''), 'empty-tracker-name') NOT LIKE 'test-%'
;

SELECT NULLIF('test', 'test')
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	name_tracker,
	COUNT(*)
FROM snowplow.atomic.events
GROUP BY 1
;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

SELECT *
FROM snowflake.account_usage.query_history qh
-- WHERE qh.query_id = '01bd3d84-0206-d357-0002-dd01246817a3'
	WHERE qh.query_text LIKE 'MERGE INTO hygiene_vault_mvp.snowplow.event_stream %'
  AND qh.start_time >= CURRENT_DATE
;

USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'se.data.user_email_clicks, data_vault_mvp.dwh.user_email_clicks')
;
SELECT *
FROM scratch.robinpatel.table_usage
; -- 0 rows