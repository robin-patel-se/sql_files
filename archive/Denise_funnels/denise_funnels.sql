SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE --TODO adjust
		AND
	  (contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
		  OR page_urlpath LIKE '%current-sales%'
		  OR page_urlpath LIKE '%aktuelle-angebote%'
		  OR page_urlpath LIKE '%currentSales'
		  OR page_urlpath LIKE '%aanbedingen%' -- NL
		  OR page_urlpath LIKE '%offerte-in-corso%' -- IT
		  OR page_urlpath LIKE '%nuvaerende-salg%'
		  OR page_urlpath LIKE '%aktuella-kampanjer%'
		  OR page_urlpath = '/'
		  )
;


SELECT
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.event_tstamp::DATE = CURRENT_DATE --TODO adjust
GROUP BY 1


------------------------------------------------------------------------------------------------------------------------
-- camilla -- need to remove native app conversions using device platorm
-- desktop
-- mobile


-- camilla - web
SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1--TODO adjust
  AND cespc.page_classification IN (
									'sale',
									'booking_form',
									'booking_confirmation'
	)
  AND cespc.device_platform = 'web'
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost NOT LIKE '%.sales.%'
GROUP BY 1, 2
;

-- checking against scv platform
SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
	INNER JOIN se.data_pii.scv_session_events_link ssel ON cespc.event_hash = ssel.event_hash
	INNER JOIN se.data.scv_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1--TODO adjust
  AND cespc.page_classification IN (
									'sale',
									'booking_form',
									'booking_confirmation'
	)
  AND stba.platform = 'Web'
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost NOT LIKE '%.sales.%'
GROUP BY 1, 2
;

SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
	INNER JOIN se.data_pii.scv_session_events_link ssel ON cespc.event_hash = ssel.event_hash
	INNER JOIN se.data.scv_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1--TODO adjust
  AND cespc.page_classification IN (
									'sale',
									'booking_form',
									'booking_confirmation'
	)
  AND stba.platform IN ('Mobile Web', 'Tablet Web')
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost NOT LIKE '%.sales.%'
GROUP BY 1, 2
;


-- camilla - mweb
SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1--TODO adjust
  AND cespc.page_classification IN (
									'sale',
									'offer',
									'booking_form',
									'booking_confirmation'
	)
  AND cespc.device_platform IN ('mobile web', 'tablet web')
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost NOT LIKE '%.sales.%'
GROUP BY 1, 2


------------------------------------------------------------------------------------------------------------------------


-- tracy dates selection logic does capture the first stage of booking flow (dates).
--tracy web
SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
  AND cespc.page_classification IN
	  ('booking_flow_accommodation_selection', 'booking_flow_cars_selection', 'booking_flow_extras_selection',
	   'booking_flow_flight_selection', 'booking_flow_room_selection', 'booking_flow_roundtrip_selection',
	   'booking_flow_tickets_selection', 'booking_form', 'sale')

  AND cespc.device_platform = 'web'
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost LIKE '%.sales.%'
GROUP BY 1, 2
;

--tracy mweb
SELECT
	cespc.event_tstamp::DATE AS event_date,
	cespc.page_classification,
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
  AND cespc.page_classification IN
	  ('booking_flow_accommodation_selection', 'booking_flow_cars_selection', 'booking_flow_extras_selection',
	   'booking_flow_flight_selection', 'booking_flow_room_selection', 'booking_flow_roundtrip_selection',
	   'booking_flow_tickets_selection', 'booking_form', 'sale')

  AND cespc.device_platform IN ('mobile web', 'tablet web')
  AND cespc.is_server_side_event = FALSE
  AND cespc.is_robot_spider_event = FALSE
  AND cespc.page_urlhost LIKE '%.sales.%'
GROUP BY 1, 2
;


SELECT
	stba.platform,
	COUNT(*)
FROM se.data.scv_touched_spvs spv
	INNER JOIN se.data.scv_touch_basic_attributes stba ON stba.touch_id = spv.touch_id
WHERE event_tstamp::DATE = CURRENT_DATE - 1
GROUP BY 1
;


SELECT *
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_classification = 'sale'
  AND cespc.page_urlhost LIKE '%.sales.%'
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE
  AND cespc.is_server_side_event = FALSE
;


SELECT *
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_urlpath LIKE '%/v3-%'
  AND cespc.page_urlhost LIKE '%.sales.%'
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE
  AND cespc.is_server_side_event = FALSE
;


SELECT *
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_urlpath REGEXP '/booking/\\d{6,7}/.*/.*/'
  AND cespc.page_urlhost LIKE '%.sales.%'
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE
;


SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A114177'
;

SELECT *
FROM se.data.tb_offer t
WHERE t.tb_offer_id = 114177


-- /booking/114177/zauberhaftes-marrakesch-im-riad-de/v3-05f71d10-786f-4a9d-9787-52caf3eb3f58-1687407328657-H2UEXN5Y
-- /booking/<offer_id>/sale_url_slug/<date_page_hash???>

-- condense all url paths per offer id to get a configuration for each offer
-- aggregate page paths up to offer id
-- attach to offer id configuration


SELECT
	cespc.page_urlpath,
	REGEXP_SUBSTR(cespc.page_urlpath, '/booking', 1, 1, 'e'),
	COUNT(*)
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_urlpath LIKE '/booking/118899/%'
  AND cespc.page_urlhost LIKE '%.sales.%'
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE - 1
  AND cespc.is_server_side_event = FALSE
GROUP BY 1
;


SELECT
	cespc.page_urlpath,
-- 	REGEXP_SUBSTR(cespc.page_urlpath, '/booking/\\d{6,7}/[\\w|-]*/v3', 1, 1, 'e')
	SPLIT_PART(cespc.page_urlpath, '/', 3)                                                            AS offer_id,
	SPLIT_PART(cespc.page_urlpath, '/', 4)                                                            AS offer_url_slug,
	SPLIT_PART(cespc.page_urlpath, '/', 5)                                                            AS session_id,
	IFF(SPLIT_PART(cespc.page_urlpath, '/', 6) = '', 'dates', SPLIT_PART(cespc.page_urlpath, '/', 6)) AS component
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_urlpath LIKE '/booking/118899/%' --TODO temporary filter
-- WHERE cespc.page_urlpath REGEXP '/booking/\\d{6,7}/.*'
  AND cespc.page_urlhost LIKE '%.sales.%'         -- tracy domain filter
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE - 1
  AND cespc.is_server_side_event = FALSE
;


------------------------------------------------------------------------------------------------------------------------
-- 4 big package groups
-- 3pp (calendar and booking)
--


-- no front end roadmap
-- date
-- pillar 1 and 3 (front end improvements)
-- need to know the biggest drop offs
-- where are they dropping off, so that they can ascertain where to prioritise features

-- nice to have:
-- user side
-- territory
-- device (mobile)
-- channel ??
-- referrer to sale page (search, direct)

-- https://docs.google.com/presentation/d/1XR_ZxOxDUpd9JkmryEAjYt1O3waw35yvIMxeZn5cFbc/edit#slide=id.g21a5ed563ad_0_0


SELECT DISTINCT
	SPLIT_PART(cespc.page_urlpath, '/', 3)                                                            AS offer_id,
	SPLIT_PART(cespc.page_urlpath, '/', 4)                                                            AS offer_url_slug,
	IFF(SPLIT_PART(cespc.page_urlpath, '/', 6) = '', 'dates', SPLIT_PART(cespc.page_urlpath, '/', 6)) AS component
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
WHERE cespc.page_urlpath REGEXP '/booking/\\d{6,7}/.*'
  AND cespc.page_urlhost LIKE '%.sales.%'         -- tracy domain filter
  AND cespc.event_name = 'page_view'
  AND cespc.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO adjust
  AND cespc.is_server_side_event = FALSE
ORDER BY 1
;

WITH
	model_data AS (
		SELECT
			cespc.event_tstamp,
			cespc.refr_urlpath,
			cespc.posa_territory, --TODO replace with posa logic
			cespc.device_platform,
			SPLIT_PART(cespc.page_urlpath, '/', 3)      AS offer_id,
			SPLIT_PART(cespc.page_urlpath, '/', 4)      AS offer_url_slug,
			IFF(SPLIT_PART(cespc.page_urlpath, '/', 6) = '', 'dates',
				SPLIT_PART(cespc.page_urlpath, '/', 6)) AS component
		FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification cespc
		WHERE cespc.page_urlpath REGEXP '/booking/\\d{6,7}/.*'
		  AND cespc.page_urlhost LIKE '%.sales.%'          -- tracy domain filter
		  AND cespc.event_name = 'page_view'
		  AND cespc.event_tstamp::DATE = CURRENT_DATE - 10 -- TODO adjust
		  AND cespc.is_server_side_event = FALSE
	)

SELECT
	md.offer_id,
	md.event_tstamp::DATE,
	SUM(IFF(md.component = 'dates', 1, 0))         AS dates_page_views,
	SUM(IFF(md.component = 'roundtrip', 1, 0))     AS roundtrip_page_views,
	SUM(IFF(md.component = 'accommodation', 1, 0)) AS accommodation_page_views,
	SUM(IFF(md.component = 'rooms', 1, 0))         AS rooms_page_views,
	SUM(IFF(md.component = 'extras', 1, 0))        AS extras_page_views,
	SUM(IFF(md.component = 'tickets', 1, 0))       AS tickets_page_views,
	SUM(IFF(md.component = 'flights', 1, 0))       AS flights_page_views,
	SUM(IFF(md.component = 'cars', 1, 0))          AS cars_page_views,
	SUM(IFF(md.component = 'checkout', 1, 0))      AS checkout_page_views
FROM model_data md
GROUP BY 1, 2


;


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

SET lookback = 90
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.tracy_funnels AS (
	WITH
		model_data AS (
			-- model input data to compute on
			SELECT
				ses.event_tstamp,
				ses.page_urlpath,
				ses.refr_urlpath,
				ses.posa_territory, --TODO replace with posa logic
				ses.device_platform,
-- 			IFF(TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)) IS NOT NULL,
-- 				SPLIT_PART(ses.page_urlpath, '/', 3),
-- 				SPLIT_PART(ses.page_urlpath, '/', 4))
-- 				  AS offer_url_slug,
				COALESCE(
						TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)),
						TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 3))
					) AS offer_id,
				CASE
					WHEN ses.page_urlpath REGEXP '/\\d{6,7}/.*' THEN 'sale_page'
					WHEN SPLIT_PART(ses.page_urlpath, '/', 6) = '' THEN 'dates'
					ELSE SPLIT_PART(ses.page_urlpath, '/', 6)
				END   AS component
			FROM se.data_pii.scv_event_stream ses
			WHERE ses.page_urlhost LIKE '%.sales.%'                  -- tracy domain filter
			  AND ses.event_name = 'page_view'
			  AND ses.event_tstamp::DATE >= CURRENT_DATE - $lookback -- TODO adjust
			  AND ses.is_server_side_event = FALSE
			  AND (ses.page_urlpath REGEXP '/\\d{6,7}/.*' -- sale page
				OR
				   ses.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
				)
		),
		offer_paths AS (
			-- create distinct list of components based on observed page views
			SELECT DISTINCT
				md.offer_id,
				md.component
			FROM model_data md
		),
		offer_configuration AS (
			-- order components based on static hierarchy
			SELECT
				op.offer_id,
				LISTAGG(
						op.component, ' > ') WITHIN
					GROUP ( ORDER BY CASE op.component
										 WHEN 'sale_page' THEN 1
										 WHEN 'dates' THEN 2
										 WHEN 'roundtrip' THEN 3
										 WHEN 'accommodation' THEN 4
										 WHEN 'rooms' THEN 5
										 WHEN 'extras' THEN 6
										 WHEN 'tickets' THEN 7
										 WHEN 'flights' THEN 8
										 WHEN 'cars' THEN 9
										 WHEN 'checkout' THEN 10
									 END) AS funnel_configuration
			FROM offer_paths op
			GROUP BY 1
		),
		model_page_views AS (
			-- model page view metrics to defined granularity, can expand based on request
			SELECT
				md.offer_id,
				md.event_tstamp::DATE                          AS event_date,
				SUM(IFF(md.component = 'sale_page', 1, 0))     AS sale_page_views,
				SUM(IFF(md.component = 'dates', 1, 0))         AS dates_page_views,
				SUM(IFF(md.component = 'roundtrip', 1, 0))     AS roundtrip_page_views,
				SUM(IFF(md.component = 'accommodation', 1, 0)) AS accommodation_page_views,
				SUM(IFF(md.component = 'rooms', 1, 0))         AS rooms_page_views,
				SUM(IFF(md.component = 'extras', 1, 0))        AS extras_page_views,
				SUM(IFF(md.component = 'tickets', 1, 0))       AS tickets_page_views,
				SUM(IFF(md.component = 'flights', 1, 0))       AS flights_page_views,
				SUM(IFF(md.component = 'cars', 1, 0))          AS cars_page_views,
				SUM(IFF(md.component = 'checkout', 1, 0))      AS checkout_page_views
			FROM model_data md
			GROUP BY 1, 2
		),
		offer_metrics AS (
			SELECT
				t.se_sale_id,
				oc.offer_id,
				oc.funnel_configuration,
				mpv.event_date,
				sale_page_views,
				mpv.dates_page_views,
				mpv.roundtrip_page_views,
				mpv.accommodation_page_views,
				mpv.rooms_page_views,
				mpv.extras_page_views,
				mpv.tickets_page_views,
				mpv.flights_page_views,
				mpv.cars_page_views,
				mpv.checkout_page_views
			FROM offer_configuration oc
				LEFT JOIN se.data.tb_offer t ON oc.offer_id = t.tb_offer_id
				LEFT JOIN model_page_views mpv ON oc.offer_id = mpv.offer_id
		),
		aggregated AS (
			SELECT
				SUM(om.sale_page_views)                               AS sale_page_views,
				SUM(om.dates_page_views)                              AS dates_page_views,
				SUM(om.dates_page_views) / SUM(om.sale_page_views)    AS dates_ctr,
				SUM(om.checkout_page_views)                           AS checkout_page_views,
				SUM(om.checkout_page_views) / SUM(om.sale_page_views) AS funnel_cvr
			FROM offer_metrics om
		)
	SELECT *
	FROM aggregated
)-- WHERE om.offer_id = 118833

;


SELECT *
FROM scratch.robinpatel.tracy_funnels
;

-- large proportion of sales that don't get past sale page or dates page
SELECT
	tf.funnel_configuration,
	COUNT(DISTINCT tf.offer_id) AS offers
FROM scratch.robinpatel.tracy_funnels tf
GROUP BY ALL
;


-- What is undefined in url path? eg. /booking/119450/uk-3pp-core-zd-rural-the-atlantic-hotel-jersey/v3-4ee30e5d-605b-4a71-8daf-2ba262bfaac7-1683734492986-U4WVONWU/undefined
SELECT
	ses.event_tstamp,
	ses.page_url,
	ses.page_urlpath,
	ses.refr_urlpath,
	ses.posa_territory, --TODO replace with posa logic
	ses.device_platform,
	COALESCE(
			TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)),
			TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 3))
		) AS offer_id,
	CASE
		WHEN ses.page_urlpath REGEXP '/\\d{6,7}/.*' THEN 'sale_page'
		WHEN SPLIT_PART(ses.page_urlpath, '/', 6) = '' THEN 'dates'
		ELSE SPLIT_PART(ses.page_urlpath, '/', 6)
	END   AS component
FROM se.data_pii.scv_event_stream ses
WHERE ses.page_urlhost LIKE '%.sales.%'                  -- tracy domain filter
  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE >= CURRENT_DATE - $lookback -- TODO adjust
  AND ses.is_server_side_event = FALSE
  AND (ses.page_urlpath REGEXP '/\\d{6,7}/.*' -- sale page
	OR
	   ses.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
	)
  AND component = 'undefined'
;

-- are there flows that change dynamically? eg, you only see extras if you choose something on the step before

-- pub date
-- concept name
-- product config
-- unique user count
-- session count

-- device type

USE WAREHOUSE pipe_xlarge
;

WITH
	model_data AS (
		-- model input data to compute on
		SELECT
			ses.event_tstamp,
			ses.page_urlpath,
			ses.refr_urlpath,
			ses.posa_territory, --TODO replace with posa logic
			ses.device_platform,
			ses.unique_browser_id,
			COALESCE(
					TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)),
					TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 3))
				) AS offer_id,
			CASE
				WHEN ses.page_urlpath REGEXP '/\\d{6,7}/.*' THEN 'sale_page'
				WHEN SPLIT_PART(ses.page_urlpath, '/', 6) = '' THEN 'dates'
				ELSE SPLIT_PART(ses.page_urlpath, '/', 6)
			END   AS component
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.page_urlhost LIKE '%.sales.%' -- tracy domain filter
		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE BETWEEN '2023-01-01' AND CURRENT_DATE - 1
		  AND ses.is_server_side_event = FALSE
		  AND (ses.page_urlpath REGEXP '/\\d{6,7}/.*' -- sale page
			OR
			   ses.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
			)
	),
	offer_paths AS (
		-- create distinct list of components based on observed page views
		SELECT DISTINCT
			md.offer_id,
			md.component
		FROM model_data md
	),
	offer_configuration AS (
		-- order components based on static hierarchy
		SELECT
			op.offer_id,
			LISTAGG(
					op.component, ' > ') WITHIN
				GROUP ( ORDER BY CASE op.component
									 WHEN 'sale_page' THEN 1
									 WHEN 'dates' THEN 2
									 WHEN 'roundtrip' THEN 3
									 WHEN 'accommodation' THEN 4
									 WHEN 'rooms' THEN 5
									 WHEN 'extras' THEN 6
									 WHEN 'tickets' THEN 7
									 WHEN 'flights' THEN 8
									 WHEN 'cars' THEN 9
									 WHEN 'checkout' THEN 10
								 END) AS funnel_configuration
		FROM offer_paths op
		GROUP BY 1
	),
	model_page_views AS (
		-- model page view metrics to defined granularity, can expand based on request
		SELECT
			md.offer_id,
			md.event_tstamp::DATE                                                           AS event_date,
			SUM(IFF(md.component = 'sale_page', 1, 0))                                      AS sale_page_views,
			COUNT(DISTINCT IFF(md.component = 'sale_page', md.unique_browser_id, NULL))     AS sale_page_users,
			SUM(IFF(md.component = 'dates', 1, 0))                                          AS dates_page_views,
			COUNT(DISTINCT IFF(md.component = 'dates', md.unique_browser_id, NULL))         AS dates_page_users,
			SUM(IFF(md.component = 'roundtrip', 1, 0))                                      AS roundtrip_page_views,
			COUNT(DISTINCT IFF(md.component = 'roundtrip', md.unique_browser_id, NULL))     AS roundtrip_page_users,
			SUM(IFF(md.component = 'accommodation', 1, 0))                                  AS accommodation_page_views,
			COUNT(DISTINCT IFF(md.component = 'accommodation', md.unique_browser_id, NULL)) AS accommodation_page_users,
			SUM(IFF(md.component = 'rooms', 1, 0))                                          AS rooms_page_views,
			COUNT(DISTINCT IFF(md.component = 'rooms', md.unique_browser_id, NULL))         AS rooms_page_users,
			SUM(IFF(md.component = 'extras', 1, 0))                                         AS extras_page_views,
			COUNT(DISTINCT IFF(md.component = 'extras', md.unique_browser_id, NULL))        AS extras_page_users,
			SUM(IFF(md.component = 'tickets', 1, 0))                                        AS tickets_page_views,
			COUNT(DISTINCT IFF(md.component = 'tickets', md.unique_browser_id, NULL))       AS tickets_page_users,
			SUM(IFF(md.component = 'flights', 1, 0))                                        AS flights_page_views,
			COUNT(DISTINCT IFF(md.component = 'flights', md.unique_browser_id, NULL))       AS flights_page_users,
			SUM(IFF(md.component = 'cars', 1, 0))                                           AS cars_page_views,
			COUNT(DISTINCT IFF(md.component = 'cars', md.unique_browser_id, NULL))          AS cars_page_users,
			SUM(IFF(md.component = 'checkout', 1, 0))                                       AS checkout_page_views,
			COUNT(DISTINCT IFF(md.component = 'checkout', md.unique_browser_id, NULL))      AS checkout_page_users
		FROM model_data md
		GROUP BY 1, 2
	)
SELECT
	t.se_sale_id,
	oc.offer_id,
	oc.funnel_configuration,
	t.pub_date,
	t.concept_name,
	t.product_configuration,

	mpv.event_date,
	mpv.sale_page_views,
	mpv.sale_page_users,
	mpv.dates_page_views,
	mpv.dates_page_users,
	mpv.roundtrip_page_views,
	mpv.roundtrip_page_users,
	mpv.accommodation_page_views,
	mpv.accommodation_page_users,
	mpv.rooms_page_views,
	mpv.rooms_page_users,
	mpv.extras_page_views,
	mpv.extras_page_users,
	mpv.tickets_page_views,
	mpv.tickets_page_users,
	mpv.flights_page_views,
	mpv.flights_page_users,
	mpv.cars_page_views,
	mpv.cars_page_users,
	mpv.checkout_page_views,
	mpv.checkout_page_users
FROM offer_configuration oc
	LEFT JOIN se.data.tb_offer t ON oc.offer_id = t.tb_offer_id
	LEFT JOIN model_page_views mpv ON oc.offer_id = mpv.offer_id
;


SELECT *
FROM latest_vault.cms_mysql.exchange_rate er
;

SELECT *
FROM se.data.fx_rates fr
;


------------------------------------------------------------------------------------------------------------------------


SET lookback = 1
;

WITH
	model_data AS (
		-- model input data to compute on
		SELECT
			ses.event_tstamp,
			ses.page_urlpath,
			ses.refr_urlpath,
			ses.posa_territory, --TODO replace with posa logic
			ses.device_platform,
-- 			IFF(TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)) IS NOT NULL,
-- 				SPLIT_PART(ses.page_urlpath, '/', 3),
-- 				SPLIT_PART(ses.page_urlpath, '/', 4))
-- 				  AS offer_url_slug,
			COALESCE(
					TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 2)),
					TRY_TO_NUMBER(SPLIT_PART(ses.page_urlpath, '/', 3))
				) AS offer_id,
			CASE
				WHEN ses.page_urlpath REGEXP '/\\d{6,7}/.*' THEN 'sale_page'
				WHEN SPLIT_PART(ses.page_urlpath, '/', 6) = '' THEN 'dates'
				ELSE SPLIT_PART(ses.page_urlpath, '/', 6)
			END   AS component
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.page_urlhost LIKE '%.sales.%'                  -- tracy domain filter
		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE >= CURRENT_DATE - $lookback -- TODO adjust
		  AND ses.is_server_side_event = FALSE
		  AND (ses.page_urlpath REGEXP '/\\d{6,7}/.*' -- sale page
			OR
			   ses.page_urlpath REGEXP '/booking/\\d{6,7}/.*' -- booking flows
			)
	),
	offer_paths AS (
		-- create distinct list of components based on observed page views
		SELECT DISTINCT
			md.offer_id,
			md.component
		FROM model_data md
	),
	offer_configuration AS (
		-- order components based on static hierarchy
		SELECT
			op.offer_id,
			LISTAGG(
					op.component, ' > ') WITHIN
				GROUP ( ORDER BY CASE op.component
									 WHEN 'sale_page' THEN 1
									 WHEN 'dates' THEN 2
									 WHEN 'roundtrip' THEN 3
									 WHEN 'accommodation' THEN 4
									 WHEN 'rooms' THEN 5
									 WHEN 'extras' THEN 6
									 WHEN 'tickets' THEN 7
									 WHEN 'flights' THEN 8
									 WHEN 'cars' THEN 9
									 WHEN 'checkout' THEN 10
								 END) AS funnel_configuration
		FROM offer_paths op
		GROUP BY 1
	),
	model_page_views AS (
		-- model page view metrics to defined granularity, can expand based on request
		SELECT
			md.offer_id,
			md.event_tstamp::DATE                          AS event_date,
			SUM(IFF(md.component = 'sale_page', 1, 0))     AS sale_page_views,
			SUM(IFF(md.component = 'dates', 1, 0))         AS dates_page_views,
			SUM(IFF(md.component = 'roundtrip', 1, 0))     AS roundtrip_page_views,
			SUM(IFF(md.component = 'accommodation', 1, 0)) AS accommodation_page_views,
			SUM(IFF(md.component = 'rooms', 1, 0))         AS rooms_page_views,
			SUM(IFF(md.component = 'extras', 1, 0))        AS extras_page_views,
			SUM(IFF(md.component = 'tickets', 1, 0))       AS tickets_page_views,
			SUM(IFF(md.component = 'flights', 1, 0))       AS flights_page_views,
			SUM(IFF(md.component = 'cars', 1, 0))          AS cars_page_views,
			SUM(IFF(md.component = 'checkout', 1, 0))      AS checkout_page_views
		FROM model_data md
		GROUP BY 1, 2
	),
	offer_metrics AS (
		SELECT
			t.se_sale_id,
			oc.offer_id,
			oc.funnel_configuration,
			mpv.event_date,
			sale_page_views,
			mpv.dates_page_views,
			mpv.roundtrip_page_views,
			mpv.accommodation_page_views,
			mpv.rooms_page_views,
			mpv.extras_page_views,
			mpv.tickets_page_views,
			mpv.flights_page_views,
			mpv.cars_page_views,
			mpv.checkout_page_views
		FROM offer_configuration oc
			LEFT JOIN se.data.tb_offer t ON oc.offer_id = t.tb_offer_id
			LEFT JOIN model_page_views mpv ON oc.offer_id = mpv.offer_id
	),
	aggregated AS (
		SELECT
			SUM(om.sale_page_views)                               AS sale_page_views,
			SUM(om.dates_page_views)                              AS dates_page_views,
			SUM(om.dates_page_views) / SUM(om.sale_page_views)    AS dates_ctr,
			SUM(om.checkout_page_views)                           AS checkout_page_views,
			SUM(om.checkout_page_views) / SUM(om.sale_page_views) AS funnel_cvr
		FROM offer_metrics om
	)
SELECT *
FROM aggregated
-- WHERE om.offer_id = 118833
;