/*
00_event_stream_modelling	03_touchification		06_touch_channelling		trimmed_event_stream.py
01_url_manipulation		04_events_of_interest		07_touch_attribution
02_identity_stitching		05_touch_basic_attributes	single_customer_view.py


biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py

biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling:
	01_artificial_transaction_insert.py
	02_page_screen_enrichment.py
	03_app_push_enhancement.py

biapp/task_catalogue/dv/dwh/scv/01_url_manipulation:
	01_module_unique_urls.py
	02_01_module_url_hostname.py
	02_02_module_url_params.py
	03_module_extracted_params.py

biapp/task_catalogue/dv/dwh/scv/02_identity_stitching:
	01_module_identity_associations.py
	02_module_identity_stitching.py

biapp/task_catalogue/dv/dwh/scv/03_touchification:
	01_touchifiable_events.py
	02_02_time_diff_marker.py
	02_01_utm_or_referrer_hostname_marker.py
	03_touchification.py

biapp/task_catalogue/dv/dwh/scv/04_events_of_interest:
	01_module_touched_spvs.py
	02_module_touched_transactions.py
	03_module_touched_searches.py
	04_module_touched_app_installs.py
	05_module_touched_feature_flags.py
	06_module_touched_authorisation_events.py
	07_module_touched_booking_form_views.py
	08_module_touched_in_app_notification_events.py
	09_module_touched_pay_button_clicks.py
	10_module_events_of_interest.py

biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes:
	00_anomalous_user_dates.py
	01_module_touch_basic_attributes.py

biapp/task_catalogue/dv/dwh/scv/06_touch_channelling:
	01_module_touch_utm_referrer.py
	02_module_touch_marketing_channel.py

biapp/task_catalogue/dv/dwh/scv/07_touch_attribution:
	01_module_touch_attribution.py

single_customer_view.py
trimmed_event_stream.py
 */


self_describing_task
\
    --include 'biapp.task_catalogue.se.data.scv.scv_touch_attribution.py' \
    --method 'run' \
    --start '2025-06-17 00:00:00' \
    --end '2025-06-17 00:00:00'

CREATE SCHEMA hygiene_vault_mvp_dev_robin.snowplow
;

CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg CLONE data_vault_mvp.single_customer_view_stg
;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream
;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= CURRENT_DATE - 7
LIMIT 100
;



------------------------------------------------------------------------------------------------------------------------
-- checking clustered tables
SELECT
	TO_DATE(start_time) AS date,
	database_name,
	schema_name,
	table_name,
	SUM(credits_used)   AS credits_used
FROM snowflake.account_usage.automatic_clustering_history
WHERE start_time >= CURRENT_DATE
GROUP BY 1, 2, 3, 4
;


SELECT
	CURRENT_DATE,
	DATE_TRUNC(MONTH, DATEADD(YEAR, -3, CURRENT_DATE() - 1)) -- replace current date with scheduled tstamp
;

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
;



SELECT *
FROM snowflake.account_usage.query_history
WHERE warehouse_size = '6X-Large'
  AND start_time::DATE >= CURRENT_DATE() - 180
ORDER BY start_time
	DESC
;

SELECT GET_DDL('table', 'collab.muse.snowflake_query_history_v2')
;

SELECT DISTINCT
	qh.warehouse_name
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1
;


SELECT *
FROM collab.muse.snowflake_query_history_v2 qh
WHERE qh.warehouse_name LIKE 'SCV%'
  AND qh.start_time >= CURRENT_DATE - 1
ORDER BY cost__query_duration DESC

------------------------------------------------------------------------------------------------------------------------

SELECT MIN(session_date) FROM data_vault_mvp.bi.site_funnels sf;


SELECT MIN(date_) FROM data_vault_mvp.dwh.iterable_crm_reporting_insertions icri;