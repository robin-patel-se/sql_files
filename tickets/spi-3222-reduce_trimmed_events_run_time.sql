CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar
;

-- summarisation of cost of this step:
SELECT
	qh.start_time::DATE                              AS date,
	AVG(total_elapsed_time / 1000 * CASE warehouse_size
										WHEN 'X-Small' THEN 1 / 60 / 60
										WHEN 'Small' THEN 2 / 60 / 60
										WHEN 'Medium' THEN 4 / 60 / 60
										WHEN 'Large' THEN 8 / 60 / 60
										WHEN 'X-Large' THEN 16 / 60 / 60
										WHEN '2X-Large' THEN 32 / 60 / 60
										WHEN '3X-Large' THEN 64 / 60 / 60
										WHEN '4X-Large' THEN 128 / 60 / 60
										ELSE 0
									END)             AS avg_estimated_credits,
	MAX(total_elapsed_time / 1000 * CASE warehouse_size
										WHEN 'X-Small' THEN 1 / 60 / 60
										WHEN 'Small' THEN 2 / 60 / 60
										WHEN 'Medium' THEN 4 / 60 / 60
										WHEN 'Large' THEN 8 / 60 / 60
										WHEN 'X-Large' THEN 16 / 60 / 60
										WHEN '2X-Large' THEN 32 / 60 / 60
										WHEN '3X-Large' THEN 64 / 60 / 60
										WHEN '4X-Large' THEN 128 / 60 / 60
										ELSE 0
									END)             AS max_estimated_credits,
	AVG(TIMEDIFF('min', qh.start_time, qh.end_time)) AS avg_total_elapsed_time_minutes,
	COUNT(*)                                         AS number_of_queries
FROM snowflake.account_usage.query_history qh
WHERE qh.query_text LIKE 'CREATE OR REPLACE TABLE data_vault_mvp.dwh.trimmed_event_stream__step01__get_source_batch%'
  AND qh.user_name IN ('PIPELINERUNNER', 'ROBINPATEL')
  AND qh.start_time >= CURRENT_DATE - 2
GROUP BY 1
;


-- test when removing the se calendar join: 01a85484-3202-0639-0000-02ddd05acdfe

-- check versions of this query over the last day.
SELECT
			total_elapsed_time / 1000 *
			CASE warehouse_size
				WHEN 'X-Small' THEN 1 / 60 / 60
				WHEN 'Small' THEN 2 / 60 / 60
				WHEN 'Medium' THEN 4 / 60 / 60
				WHEN 'Large' THEN 8 / 60 / 60
				WHEN 'X-Large' THEN 16 / 60 / 60
				WHEN '2X-Large' THEN 32 / 60 / 60
				WHEN '3X-Large' THEN 64 / 60 / 60
				WHEN '4X-Large' THEN 128 / 60 / 60
				ELSE 0
			END                                         AS estimated_credits,
			TIMEDIFF('min', qh.start_time, qh.end_time) AS total_elapsed_time_minutes,
			qh.user_name,
			qh.query_text,
			qh.start_time,
			qh.end_time,
			qh.execution_status,
			qh.warehouse_name,
			qh.percentage_scanned_from_cache
FROM snowflake.account_usage.query_history qh
WHERE qh.query_text LIKE ANY
	  ('CREATE OR REPLACE TABLE data_vault_mvp.dwh.trimmed_event_stream__step01__get_source_batch%',
	   'CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.trimmed_event_stream__step01__get_source_batch%')
  AND qh.user_name IN ('PIPELINERUNNER', 'ROBINPATEL')
  AND qh.start_time >= CURRENT_DATE - 3
;


------------------------------------------------------------------------------------------------------------------------
-- filtering to explicit events doesn't change run time.

------------------------------------------------------------------------------------------------------------------------
-- check if reducing columns reduces run time

-- searches  by checkin
SELECT
	TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) AS check_in_date,
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
		))                                                                                  AS territory,
	COUNT(*)                                                                                AS searches
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE e.event_tstamp::DATE = CURRENT_DATE
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND e.contexts_com_secretescapes_search_context_1[0]['had_results']::BOOLEAN = TRUE
  AND TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) >= CURRENT_DATE
  AND TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) BETWEEN CURRENT_DATE AND DATEADD(MONTH, 6, CURRENT_DATE)
GROUP BY 1, 2

-- spvs

SELECT
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
		))                            AS territory,
	DATE_TRUNC('hour', event_tstamp)  AS hour,
	event_tstamp::DATE = CURRENT_DATE AS today,
	e.event_tstamp_yesterday,
	e.event_tstamp_today_last_week,
	e.event_tstamp_today_ly,
	e.event_tstamp_today_lly,
	e.event_tstamp_today_2019,
	COUNT(*)                          AS spvs
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE (
		( -- old world native app event data
					e.collector_tstamp < '2020-02-28 00:00:00'
				AND
					e.se_sale_id IS NOT NULL
			)
		OR
		( -- new world native app event data
					e.collector_tstamp >= '2020-02-28 00:00:00'
				AND
					e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
			)
	)
  AND DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

-- top 10 spvs
WITH
	spv_counts AS (
		SELECT
			e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
			e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
			se.data.posa_category_from_territory(COALESCE(
					se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
					REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
							'UK'),
					REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
				))                                                                       AS territory,
			COUNT(*)                                                                     AS spvs
		FROM data_vault_mvp.dwh.trimmed_event_stream e
		WHERE e.event_tstamp::DATE = CURRENT_DATE
		  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
		GROUP BY 1, 2, 3
	),
	top_ten_sales_by_territory AS (
		SELECT
			sc.sale_name,
			sc.se_sale_id,
			sc.territory,
			sc.spvs
		FROM spv_counts sc
		QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.spvs DESC) <= 10
	),
	lifetime_spvs AS (
		SELECT
			sts.se_sale_id,
			se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS territory,
			COUNT(*)                                                             AS lifetime_spvs
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
			INNER JOIN top_ten_sales_by_territory tts ON sts.se_sale_id = tts.se_sale_id AND
														 se.data.posa_category_from_territory(stmc.touch_affiliate_territory) =
														 tts.territory
		GROUP BY 1, 2
	)
SELECT
	tts.sale_name,
	tts.se_sale_id,
	ds.sale_start_date,
	ssa.company_name,
	tts.territory,
	tts.spvs,
	ls.lifetime_spvs
FROM top_ten_sales_by_territory tts
	LEFT JOIN lifetime_spvs ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
	LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
	LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id

-- search by terms

WITH
	agg_search_term AS (
		SELECT
			e.contexts_com_secretescapes_search_context_1[0]['location']::VARCHAR AS search_term,
			se.data.posa_category_from_territory(COALESCE(
					se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
					REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
							'UK'),
					REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
				))                                                                AS territory,
			COUNT(*)                                                              AS fulfilled_searches
		FROM data_vault_mvp.dwh.trimmed_event_stream e
		WHERE e.event_tstamp::DATE = CURRENT_DATE
		  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
		  AND e.contexts_com_secretescapes_search_context_1[0]['had_results']::BOOLEAN = TRUE
		  AND e.contexts_com_secretescapes_search_context_1[0]['location']::VARCHAR IS DISTINCT FROM ''
		  AND e.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR IS NOT NULL
		GROUP BY 1, 2
	)
SELECT *
FROM agg_search_term sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.fulfilled_searches DESC) <= 10
;


SELECT *
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.user_name) LIKE '%dbt%'
  AND qh.start_time::DATE = CURRENT_DATE
-- AND qh.query_type LIKE 'CREATE%';

/*
Production env, which department
Development env, which person
*/

SELECT *
FROM data_vault_mvp.dwh.trimmed_event_stream tes
;


USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage', 'se.data_pii.trimmed_event_stream')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

SELECT *
FROM snowflake.account_usage.query_history
WHERE query_history.role_name = 'PERSONAL_ROLE__TABLEAU'
  AND LOWER(query_history.query_text) LIKE '%trimmed_event_stream%'



SELECT
	se.data.posa_category_from_territory(COALESCE(
			se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
			REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
			REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
		))                            AS territory,
	DATE_TRUNC('hour', event_tstamp)  AS hour,
	event_tstamp::DATE = CURRENT_DATE AS today,
	e.event_tstamp_yesterday,
	e.event_tstamp_today_last_week,
	e.event_tstamp_today_ly,
	e.event_tstamp_today_lly,
	e.event_tstamp_today_2019,
	COUNT(*)                          AS spvs
FROM data_vault_mvp_dev_robin.dwh.trimmed_event_stream e
WHERE (--app spvs
		( -- old world native app event data
					e.collector_tstamp < '2020-02-28 00:00:00'
				AND
					e.contexts_com_secretescapes_sale_page_context_1 IS NOT NULL
			)
		OR
		( -- new world native app event data
					e.collector_tstamp >= '2020-02-28 00:00:00'
				AND
					(
								e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
							OR
								e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
						)
			)
	)
   OR (--web spvs
		(--client side tracking, prior implementation/validation
					e.collector_tstamp < '2020-02-28 00:00:00'
				AND (
								e.page_urlpath LIKE '%/sale'
							OR
								e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
						)
				AND e.is_server_side_event = FALSE -- exclude non validated ss events
			)
		OR
		(--server side tracking, post implementation/validation
					e.collector_tstamp >= '2020-02-28 00:00:00'
				AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
				AND PARSE_URL(e.page_url, 1)['path']::VARCHAR NOT LIKE
					'%/sale-offers' -- remove issue where spv events were firing on offer pages
				AND e.is_server_side_event = TRUE
			)
	)
   OR --wrd spvs
			e.se_category = 'web redirect click'
		AND DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
;


WITH
	spv_counts AS (
		SELECT
			e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
			e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
			se.data.posa_category_from_territory(COALESCE(
					se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
					REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
							'UK'),
					REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
				))                                                                       AS territory,
			COUNT(*)                                                                     AS spvs
		FROM data_vault_mvp_dev_robin.dwh.trimmed_event_stream e
		WHERE e.event_tstamp::DATE = CURRENT_DATE
		  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
		GROUP BY 1, 2, 3
	),
	top_ten_sales_by_territory AS (
		SELECT
			sc.sale_name,
			sc.se_sale_id,
			sc.territory,
			sc.spvs
		FROM spv_counts sc
		QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.spvs DESC) <= 10
	),
	lifetime_spvs AS (
		SELECT
			sts.se_sale_id,
			se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS territory,
			COUNT(*)                                                             AS lifetime_spvs
		FROM se.data.scv_touched_spvs sts
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
			INNER JOIN top_ten_sales_by_territory tts ON sts.se_sale_id = tts.se_sale_id AND
														 se.data.posa_category_from_territory(stmc.touch_affiliate_territory) =
														 tts.territory
		GROUP BY 1, 2
	)
SELECT
	tts.sale_name,
	tts.se_sale_id,
	ds.sale_start_date,
	ssa.company_name,
	tts.territory,
	tts.spvs,
	ls.lifetime_spvs
FROM top_ten_sales_by_territory tts
	LEFT JOIN lifetime_spvs ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
	LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
	LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id
;

------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE data_vault_mvp.dwh.trimmed_event_stream__step01__get_source_batch AS
WITH
	distinct_dates AS (
		SELECT
			c.date_value
		FROM data_vault_mvp.dwh.se_calendar c
		WHERE c.today
		   OR c.yesterday
		   OR c.today_last_week
		   OR c.today_ly
		   OR c.today_lly
		   OR c.today_2019

	)
SELECT
	e.event_hash,
	e.is_robot_spider_event,
	e.is_internal_ip_address_event,
	e.is_server_side_event,

	e.event_tstamp,
	c.yesterday       AS event_tstamp_yesterday,
	c.today_last_week AS event_tstamp_today_last_week,
	c.today_ly        AS event_tstamp_today_ly,
	c.today_lly       AS event_tstamp_today_lly,
	c.today_2019      AS event_tstamp_today_2019,
	e.se_sale_id,

	e.app_id,
	e.collector_tstamp,
	e.page_url,
	e.page_urlpath,
	e.se_category,
	e.contexts_com_secretescapes_sale_page_context_1,
	e.contexts_com_secretescapes_screen_context_1,
	e.contexts_com_secretescapes_content_context_1,
	e.contexts_com_secretescapes_secret_escapes_sale_context_1,
	e.contexts_com_secretescapes_user_context_1,
	e.contexts_com_secretescapes_product_display_context_1,
	e.contexts_com_secretescapes_search_context_1

FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE
  -- explicit list of events we care about
	(
				e.event_name IN ('page_view', 'screen_view')
			OR e.se_category = 'web redirect click' -- for web redirect spvs
			OR e.contexts_com_secretescapes_search_context_1 IS NOT NULL -- search events
		)
  -- remove robots
  AND e.is_robot_spider_event = FALSE
  -- remove se office ip addresses
  AND e.is_internal_ip_address_event = FALSE
  -- trim early events
  AND e.event_tstamp >= '2019-01-01'
  -- calendar dates we need to include for comparative numbers
  AND e.event_tstamp::DATE IN (
	SELECT distinct_dates.date_value
	FROM distinct_dates
)
;


SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es

-- CURRENT extract from logs
-- [2023-09-05, 10:46:30 UTC] {{biapp.core.sql}} INFO - Snowflake query ID = 01aec88e-0202-c9f9-0000-02dde78ae64e
-- [2023-09-05, 10:46:30 UTC] {{biapp.core.sql}} INFO - Elapsed time = 1413.3176 seconds
-- [2023-09-05, 10:46:30 UTC] {{biapp.core.sql}} INFO - Snowflake warehouse size = pipe_large
-- [2023-09-05, 10:46:30 UTC] {{biapp.core.sql}} INFO - Snowflake credits used (estimate) = 3.14070574
-- [2023-09-05, 10:46:30 UTC] {{biapp.core.sql}} INFO - Query:

-- first just trying up warehouse

-- [2023-09-05 11:28:33,367] {sql.py:1948} INFO - Snowflake query ID = 01aec8c6-0202-c9fa-0000-02dde78d809e
-- [2023-09-05 11:28:33,378] {timer.py:116} INFO - Elapsed time = 616.4259 seconds
-- [2023-09-05 11:28:33,380] {timer.py:116} INFO - Snowflake warehouse size = pipe_xlarge
-- [2023-09-05 11:28:33,382] {timer.py:116} INFO - Snowflake credits used (estimate) = 2.73967069

-- saving of 0.41 credits, good but we need to do better

-- then try nesting calendar dates into a cte and joining

	[2023-09-05 11:47:20,021] {SQL.py:1948} INFO - Snowflake query ID = 01aec8d7-0202-c9f9-0000-02dde78ea322
[2023-09-05 11:47:20,027] {timer.py:116} INFO - Elapsed TIME = 698.8585 SECONDS
[2023-09-05 11:47:20,028] {timer.py:116} INFO - Snowflake WAREHOUSE size = pipe_xlarge
[2023-09-05 11:47:20,030] {timer.py:116} INFO - Snowflake credits used (estimate) = 3.10603759

-- filter events we care about before the calendar join
-- split into 3 queries


-- then try nesting calendar dates into a cte and sub query filter
-- try creating a temp step table with calendar dates of interest


-- testing 2xl
-- [2023-09-05 14:11:10,089] {sql.py:1948} INFO - Snowflake query ID = 01aec96d-0202-c9fa-0000-02dde79334f6
-- [2023-09-05 14:11:10,092] {timer.py:116} INFO - Elapsed time = 339.9260 seconds
-- [2023-09-05 14:11:10,093] {timer.py:116} INFO - Snowflake warehouse size = pipe_2xlarge
-- [2023-09-05 14:11:10,094] {timer.py:116} INFO - Snowflake credits used (estimate) = 3.02156406

-- testing 4xl
-- [2023-09-05 14:24:54,471] {sql.py:1948} INFO - Snowflake query ID = 01aec97f-0202-c9f9-0000-02dde7938ab2
-- [2023-09-05 14:24:54,475] {timer.py:116} INFO - Elapsed time = 90.1563 seconds
-- [2023-09-05 14:24:54,476] {timer.py:116} INFO - Snowflake warehouse size = pipe_4xlarge
-- [2023-09-05 14:24:54,477] {timer.py:116} INFO - Snowflake credits used (estimate) = 3.20555737

-- checking the query execution path to see if we can understand how to reduce costs:

USE WAREHOUSE pipe_xlarge
;

-- current code
SELECT
	e.event_hash,
	e.is_robot_spider_event,
	e.is_internal_ip_address_event,
	e.is_server_side_event,

	e.event_tstamp,
	c.yesterday       AS event_tstamp_yesterday,
	c.today_last_week AS event_tstamp_today_last_week,
	c.today_ly        AS event_tstamp_today_ly,
	c.today_lly       AS event_tstamp_today_lly,
	c.today_2019      AS event_tstamp_today_2019,
	e.se_sale_id,

	e.app_id,
	e.collector_tstamp,
	e.page_url,
	e.page_urlpath,
	e.se_category,
	e.contexts_com_secretescapes_sale_page_context_1,
	e.contexts_com_secretescapes_screen_context_1,
	e.contexts_com_secretescapes_content_context_1,
	e.contexts_com_secretescapes_secret_escapes_sale_context_1,
	e.contexts_com_secretescapes_user_context_1,
	e.contexts_com_secretescapes_product_display_context_1,
	e.contexts_com_secretescapes_search_context_1

FROM hygiene_vault_mvp.snowplow.event_stream e
	INNER JOIN data_vault_mvp_dev_robin.dwh.se_calendar_temp c ON e.event_tstamp::DATE = c.date_value
WHERE
  -- explicit list of events we care about
	(
				e.event_name IN ('page_view', 'screen_view')
			OR e.se_category = 'web redirect click' -- for web redirect spvs
			OR e.contexts_com_secretescapes_search_context_1 IS NOT NULL -- search events
		)
  -- remove robots
  AND e.is_robot_spider_event = FALSE
  -- remove se office ip addresses
  AND e.is_internal_ip_address_event = FALSE
  -- trim early events
  AND e.event_tstamp >= '2019-01-01'
  -- calendar dates we need to include for comparative numbers
  AND (
		c.today
		OR c.yesterday
		OR c.today_last_week
		OR c.today_ly
		OR c.today_lly
		OR c.today_2019
	);

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar_temp AS SELECT * FROM data_vault_mvp.dwh.se_calendar sc;