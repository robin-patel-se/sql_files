SELECT *
FROM se.data.fact_booking fb
WHERE fb.booking_id = 'A15102062'
;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification AS
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	'page views'        AS event_category,
	'booking_form_view' AS event_subcategory
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at >= TIMESTAMPADD('day', -2, '2023-08-08 03:00:00'::TIMESTAMP)
WHERE es.updated_at >= TIMESTAMPADD('day', -2, '2023-08-08 03:00:00'::TIMESTAMP)
  AND es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;


USE WAREHOUSE pipe_xlarge
;

SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	*
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
WHERE mt.updated_at >= TIMESTAMPADD('day', -2, '2023-08-08 03:00:00'::TIMESTAMP)
  AND es.updated_at >= TIMESTAMPADD('day', -2, '2023-08-08 03:00:00'::TIMESTAMP)
  AND es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;


------------------------------------------------------------------------------------------------------------------------

SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.se_sale_id,
	es.page_url,
	es.page_urlpath,
	'TRAVELBIRD'        AS tech_platform,
	'page views'        AS event_category,
	'booking_form_view' AS event_subcategory
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at::DATE >= CURRENT_DATE - 2
WHERE es.updated_at::DATE >= CURRENT_DATE - 2       -- TODO adjust
  AND es.page_urlhost LIKE '%.sales.%'              -- tracy domain filter
  AND es.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
  AND SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;

-- client side 372 and has no se_sale_id
-- server side 642

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
WHERE module_touched_booking_form_views.tech_platform = 'TRAVELBIRD'

;


SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	es.page_urlpath,
	es.*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
WHERE es.updated_at >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
  AND es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
  AND es.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR = 'Travelbird Platform'
  AND SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
;

SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	es.page_urlpath
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
WHERE es.updated_at >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
  AND es.page_urlhost LIKE '%.sales.%'               -- tracy domain filter
  AND es.page_urlpath REGEXP '/booking/\\d(6, 7)/.*' -- booking flows
  AND SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;

SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	es.page_urlpath
FROM hygiene_vault_mvp.snowplow.event_stream es
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at >= CURRENT_DATE - 2
WHERE es.updated_at >= CURRENT_DATE - 2
  AND es.page_urlhost LIKE '%.sales.%'              -- tracy domain filter
  AND es.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
  AND SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views__step02__model_tracy_data
;



SELECT
	es.event_hash,
	mt.touch_id,
	es.event_tstamp,
	es.booking_id,
	es.se_sale_id,
	es.page_url,
	es.page_urlpath,
	SPLIT_PART(es.page_urlpath, '/', 5)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
			   ON es.event_hash = mt.event_hash
				   AND mt.updated_at >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
WHERE es.updated_at::DATE >= TIMESTAMPADD('day', -1, '2023-08-09 03:00:00'::TIMESTAMP)
  AND es.page_urlhost LIKE '%.sales.%' -- tracy domain filter
  AND es.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR IS NOT DISTINCT FROM 'Travelbird Platform'
  AND SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
  AND es.event_name = 'page_view'
  AND es.is_server_side_event
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderproperty CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty
;

SELECT
	SPLIT_PART(page_urlpath, '/', 5),
	*
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views__step02__model_tracy_data
;


SELECT
	oo.order_id,
	oo.value
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty oo
WHERE name = 'form_session_id'
QUALIFY COUNT(*) OVER (PARTITION BY oo.order_id)

SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty oo
WHERE name = 'form_session_id'
;

SELECT
	event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
GROUP BY 1
;

SELECT
	sts.event_tstamp::DATE AS date,
	COUNT(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
GROUP BY 1
;

SELECT
	stt.event_tstamp::DATE AS date,
	COUNT(*)
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp::DATE >= CURRENT_DATE - 5
GROUP BY 1
;

SELECT
	ude.evt_date,
	ude.evt_name,
	COUNT(*)
FROM data_science.predictive_modeling.user_deal_events ude
WHERE ude.evt_date = CURRENT_DATE - 5
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/07_events_of_interest/07_module_touched_booking_form_views.py'  --method 'run' --start '2023-08-13 00:00:00' --end '2023-08-13 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data/scv/scv_touched_booking_form_views.py'  --method 'run' --start '2023-08-13 00:00:00' --end '2023-08-13 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- updating ds dgp_all_sessions

SELECT
	se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
	stbfv.se_sale_id                                                         AS deal_id,
	stba.attributed_user_id                                                  AS user_id,
	'book-form'                                                              AS evt_name,
	stbfv.event_tstamp::DATE                                                 AS evt_date,
	stbfv.event_tstamp                                                       AS event_ts
FROM se.data.scv_touched_booking_form_views stbfv
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba
			   ON sts.touch_id = stba.touch_id
				   AND stba.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	INNER JOIN se.data.se_user_attributes ua ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
	-- filter to look at last 5 days minus today
WHERE stbfv.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE


-- -- historic spvs computed by scv
-- SELECT
-- 	se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
-- 	sts.se_sale_id                                                           AS deal_id,
-- 	stba.attributed_user_id::INT                                             AS user_id,
-- 	'deal-view'                                                              AS evt_name,
-- 	sts.event_tstamp::DATE                                                   AS evt_date,
-- 	sts.event_tstamp                                                         AS event_ts
-- FROM se.data.scv_touched_spvs sts
-- 	INNER JOIN se.data_pii.scv_touch_basic_attributes stba
-- 			   ON sts.touch_id = stba.touch_id
-- 				   AND stba.stitched_identity_type = 'se_user_id'
-- 	INNER JOIN se.data.scv_touch_marketing_channel stmc
-- 			   ON sts.touch_id = stmc.touch_id
-- 	INNER JOIN se.data.se_user_attributes ua
-- 			   ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
-- 	-- filter to look at last 5 days minus today
-- WHERE sts.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE

	airflow dags backfill --start-date '2020-02-28 00:00:00' --end-date '2020-02-28 12:00:00' single_customer_view__daily_at_03h00

USE ROLE pipelinerunner
;

CREATE SCHEMA data_vault_mvp.single_customer_view_stg_backup_20230822 CLONE data_vault_mvp.single_customer_view_stg
;

GRANT USAGE ON SCHEMA data_vault_mvp.single_customer_view_stg_backup_20230822 TO ROLE data_team_basic
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
WHERE tech_platform = 'SECRET_ESCAPES'
;

GRANT SELECT ON TABLE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views TO ROLE data_team_basic
;

GRANT SELECT ON TABLE se.data.scv_touched_booking_form_views TO ROLE se_basic
;

SELECT
	bfvs.event_tstamp::DATE AS date,
	COUNT(*)
FROM se.data.scv_touched_booking_form_views bfvs
GROUP BY 1
;


SELECT
	stt.event_tstamp::DATE AS date,
	COUNT(*)
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp >= '2020-02-28'
GROUP BY 1
;


USE ROLE pipelinerunner
;

DROP SCHEMA data_vault_mvp.single_customer_view_stg_backup_20230822
;


WITH
	booking_form_before_today AS (
		SELECT
			se.data.territory_id_from_territory_name(stmc.touch_affiliate_territory) AS territory_id,
			stbfv.se_sale_id                                                         AS deal_id,
			stba.attributed_user_id                                                  AS user_id,
			'book-form'                                                              AS evt_name,
			stbfv.event_tstamp::DATE                                                 AS evt_date,
			stbfv.event_tstamp                                                       AS event_ts
		FROM se.data.scv_touched_booking_form_views stbfv
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba
					   ON stbfv.touch_id = stba.touch_id
						   AND stba.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			INNER JOIN se.data.se_user_attributes ua ON TRY_TO_NUMBER(stba.attributed_user_id) = ua.shiro_user_id
			-- filter to look at last 5 days minus today
		WHERE stbfv.event_tstamp BETWEEN CURRENT_DATE - 5 AND CURRENT_DATE
	),
	booking_form_today AS (
		SELECT
			COALESCE(
					se.data.territory_id_from_territory_name(es.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR),
					ds.posa_territory_id, -- if the spv is new data model sale, take it from sale data
					ua.current_affiliate_territory_id -- if these user joined before today
				)::INT                                                          AS territory_id,
			es.se_sale_id                                                       AS deal_id,
			COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT AS user_id,
			'book-form'                                                         AS evt_name,
			es.event_tstamp::DATE                                               AS evt_date,
			es.event_tstamp                                                     AS event_ts
		FROM se.data_pii.scv_event_stream es
			LEFT JOIN data_vault_mvp.single_customer_view_stg.module_identity_stitching mis
					  ON COALESCE(es.unique_browser_id, es.cookie_id, es.idfv, es.session_userid) =
						 COALESCE(mis.unique_browser_id, mis.cookie_id, mis.idfv, mis.session_userid)
			LEFT JOIN se.data.se_user_attributes ua
					  ON COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT = ua.shiro_user_id
			LEFT JOIN se.data.dim_sale ds
					  ON es.se_sale_id = ds.se_sale_id AND ds.data_model = 'New Data Model'
		WHERE es.event_tstamp >= CURRENT_DATE
		  AND es.event_name = 'page_view'
		  -- remove booking form views where we cannot identify the user at all
		  AND COALESCE(TRY_TO_NUMBER(mis.attributed_user_id), es.se_user_id)::INT IS NOT NULL
		  AND (
				(-- camilla bfvs
							es.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
						AND es.is_server_side_event
						-- filter to exclude tracy bfvs
						AND
							es.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR IS DISTINCT FROM 'Travelbird Platform'
						AND es.booking_id NOT LIKE 'v3%'
					)
				OR
				(-- tracy bfvs
							SPLIT_PART(es.page_urlpath, '/', 6) = 'checkout'
						AND es.is_server_side_event
						-- filter explicitly for tracy powered bfvs
						AND
							es.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR IS NOT DISTINCT FROM 'Travelbird Platform'
					)
			)

	),
	stack AS (
		SELECT
			bfbt.territory_id,
			bfbt.deal_id,
			bfbt.user_id,
			bfbt.evt_name,
			bfbt.evt_date,
			bfbt.event_ts
		FROM booking_form_before_today bfbt
		UNION ALL
		SELECT
			bft.territory_id,
			bft.deal_id,
			bft.user_id,
			bft.evt_name,
			bft.evt_date,
			bft.event_ts
		FROM booking_form_today bft
	)
SELECT
	s.territory_id,
	s.deal_id,
	s.user_id,
	s.evt_name,
	s.evt_date,
	MIN(s.event_ts) AS max_event_ts
FROM stack s
GROUP BY 1, 2, 3, 4, 5
;