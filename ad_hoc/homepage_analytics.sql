WITH
	content_interactions AS (
		SELECT
			pse.event_hash,
			pse.event_tstamp,
			pse.content_interaction_array,
			i.value:element_category::VARCHAR     AS element_category,
			i.value:element_sub_category::VARCHAR AS element_sub_category,
			i.value:interaction_type::VARCHAR     AS interaction_type
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment pse,
			 LATERAL FLATTEN(INPUT => pse.content_interaction_array, OUTER => TRUE) i
		WHERE pse.event_name = 'page_view'
		  AND pse.content_interaction_array IS NOT NULL
		  AND pse.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
	)
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
	LEFT JOIN content_interactions ci ON es.event_hash = ci.event_hash
WHERE es.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
  AND es.event_name = 'page_view'
  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
			es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
		OR es.page_urlpath LIKE '%current-sales%'
		OR es.page_urlpath LIKE '%aktuelle-angebote%'
		OR es.page_urlpath LIKE '%currentSales'
		OR es.page_urlpath LIKE '%aanbedingen%' -- NL
		OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
		OR es.page_urlpath LIKE '%nuvaerende-salg%'
		OR es.page_urlpath LIKE '%aktuella-kampanjer%'
		OR es.page_urlpath = '/'
	)
;


SELECT
	spse.event_hash,
	spse.event_tstamp,
	i.value['element_category']::VARCHAR     AS element_category,
	i.value['element_sub_category']::VARCHAR AS element_sub_category,
	i.value['interaction_type']::VARCHAR     AS interaction_type
FROM se.data_pii.scv_page_screen_enrichment spse,
	 LATERAL FLATTEN(INPUT => spse.content_interaction_array, OUTER => TRUE) i
WHERE spse.event_name = 'page_view'
  AND spse.content_interaction_array IS NOT NULL
  AND spse.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
;

-- SELECT es.unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::VARCHAR as target_url
-- FROM hygiene_vault_mvp.snowplow.event_stream es
-- WHERE es.event_tstamp::DATE = CURRENT_DATE - 1
--   AND es.event_name = 'link_click'
--   AND ( -- homepage filter logic lifted from
-- -- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
-- 			es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
-- 		OR es.page_urlpath LIKE '%current-sales%'
-- 		OR es.page_urlpath LIKE '%aktuelle-angebote%'
-- 		OR es.page_urlpath LIKE '%currentSales'
-- 		OR es.page_urlpath LIKE '%aanbedingen%' -- NL
-- 		OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
-- 		OR es.page_urlpath LIKE '%nuvaerende-salg%'
-- 		OR es.page_urlpath LIKE '%aktuella-kampanjer%'
-- 		OR es.page_urlpath = '/'
-- 	)

SELECT
	es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,

	*
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR = 'user'
  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
			es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
		OR es.page_urlpath LIKE '%current-sales%'
		OR es.page_urlpath LIKE '%aktuelle-angebote%'
		OR es.page_urlpath LIKE '%currentSales'
		OR es.page_urlpath LIKE '%aanbedingen%' -- NL
		OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
		OR es.page_urlpath LIKE '%nuvaerende-salg%'
		OR es.page_urlpath LIKE '%aktuella-kampanjer%'
		OR es.page_urlpath = '/'
	)
;

WITH
	input_data AS (
		SELECT
			pse.event_hash,
			pse.event_tstamp,
			i.value:element_category::VARCHAR     AS element_category,
			i.value:element_sub_category::VARCHAR AS element_sub_category,
			i.value:interaction_type::VARCHAR     AS interaction_type
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment pse,
			 LATERAL FLATTEN(INPUT => pse.content_interaction_array, OUTER => TRUE) i
		WHERE pse.event_name = 'page_view'
		  AND pse.content_interaction_array IS NOT NULL
		  AND pse.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
		  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					pse.page_url LIKE '%current-sales%'
				OR pse.page_url LIKE '%aktuelle-angebote%'
				OR pse.page_url LIKE '%currentSales'
				OR pse.page_url LIKE '%aanbedingen%' -- NL
				OR pse.page_url LIKE '%offerte-in-corso%' -- IT
				OR pse.page_url LIKE '%nuvaerende-salg%'
				OR pse.page_url LIKE '%aktuella-kampanjer%'
				OR pse.page_url = '/'
			)
	)
SELECT
	d.element_category,
	d.element_sub_category,
	d.interaction_type,
	COUNT(*)
FROM input_data d
GROUP BY 1, 2, 3
;

;



WITH
	content_interactions AS (
		SELECT
			pse.event_hash,
			pse.event_tstamp,
			pse.page_url,
			pse.content_interaction_array,
			COALESCE(i.value:element_category::VARCHAR, 'not set')     AS element_category,
			COALESCE(i.value:element_sub_category::VARCHAR, 'not set') AS element_sub_category,
			COALESCE(i.value:interaction_type::VARCHAR, 'not set')     AS interaction_type
		FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment pse,
			 LATERAL FLATTEN(INPUT => pse.content_interaction_array, OUTER => TRUE) i
		WHERE pse.event_name = 'page_view'
		  AND pse.content_interaction_array IS NOT NULL
		  AND pse.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
	),
	homepage_content_interactions AS (
		SELECT
			mtmc.touch_affiliate_territory,
			es.event_tstamp,
			es.event_hash,
			es.page_url,
			es.page_urlhost,
			es.page_urlpath,
			es.device_platform,
			es.contexts_com_secretescapes_content_context_1,
			mt.touch_id,
			mt.attributed_user_id,
			mt.stitched_identity_type,
			ci.content_interaction_array,
			ci.element_category,
			ci.element_sub_category,
			ci.interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
					   ON es.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
			INNER JOIN content_interactions ci ON es.event_hash = ci.event_hash
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
					   ON mt.touch_id = mtmc.touch_id
		WHERE es.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
		  AND es.event_name = 'page_view'
		  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
				OR es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)
	)
SELECT
	hci.touch_affiliate_territory,
	hci.element_category,
	COUNT(*) AS interactions
FROM homepage_content_interactions hci
GROUP BY 1, 2
--
-- SELECT
-- 	hci.element_category,
-- 	hci.element_sub_category,
-- 	hci.interaction_type,
-- 	COUNT(*)
-- FROM homepage_content_interactions hci
-- GROUP BY 1, 2, 3

-- SELECT * FROM homepage_content_interactions hci
-- WHERE hci.element_category = 'search results'
;


SELECT
	e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR,
	*
FROM snowplow.atomic.events e
WHERE e.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND e.collector_tstamp >= CURRENT_DATE - 5
  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
			e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
		OR e.page_urlpath LIKE '%current-sales%'
		OR e.page_urlpath LIKE '%aktuelle-angebote%'
		OR e.page_urlpath LIKE '%currentSales'
		OR e.page_urlpath LIKE '%aanbedingen%' -- NL
		OR e.page_urlpath LIKE '%offerte-in-corso%' -- IT
		OR e.page_urlpath LIKE '%nuvaerende-salg%'
		OR e.page_urlpath LIKE '%aktuella-kampanjer%'
		OR e.page_urlpath = '/'
	)
--   AND e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR =
-- 	  'search results';


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_hash = '9ddddde86d5b2bb7420c496232d77ef5e451652049247cf888cea2cb5cd7686f' AND
	  es.event_tstamp > CURRENT_DATE - 1
;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = '44f46af8-f704-4b22-8cc8-11965b66c65b'
  AND es.event_tstamp::DATE = '2023-05-24'
;

-- found examples of different page urls with the same web page id, so we cannot use the web page id as a unique key to join on.


------------------------------------------------------------------------------------------------------------------------

WITH
	content_interactions AS (
		-- note that we've witnessed a web page id persisting on multiple pages, therefore downstream use of this
		-- data should always join on url too.
		SELECT
			es.event_hash,
			es.page_url,
			es.event_tstamp,
			es.page_urlpath,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                 AS web_page_id,
			es.contexts_com_secretescapes_content_element_interaction_context_1,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND es.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
	),
	homepage_content_interactions AS (
		SELECT
			mtmc.touch_affiliate_territory,
			es.event_tstamp,
			es.event_hash,
			es.page_url,
			es.page_urlhost,
			es.page_urlpath,
			es.device_platform,
			es.contexts_com_secretescapes_content_context_1,
			mt.touch_id,
			mt.attributed_user_id,
			mt.stitched_identity_type,
			ci.contexts_com_secretescapes_content_element_interaction_context_1,
			ci.element_category,
			ci.element_sub_category,
			ci.interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
					   ON es.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
			INNER JOIN content_interactions ci
					   ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = ci.web_page_id
						   AND es.page_url = ci.page_url
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
					   ON mt.touch_id = mtmc.touch_id
		WHERE es.event_tstamp::DATE = CURRENT_DATE - 1 --TODO adjust
		  AND es.event_name = 'page_view'
		  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
				OR es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)
	)
-- SELECT
-- 	hci.touch_affiliate_territory,
-- 	hci.element_category,
-- 	COUNT(*) AS interactions
-- FROM homepage_content_interactions hci
-- GROUP BY 1, 2
--
SELECT
	hci.element_category,
	hci.element_sub_category,
	hci.interaction_type,
	COUNT(*)
FROM homepage_content_interactions hci
GROUP BY 1, 2, 3

-- SELECT * FROM homepage_content_interactions hci
-- WHERE hci.element_category = 'search results'
;

SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1 -- TODO remove
  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
			es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
		OR es.page_urlpath LIKE '%current-sales%'
		OR es.page_urlpath LIKE '%aktuelle-angebote%'
		OR es.page_urlpath LIKE '%currentSales'
		OR es.page_urlpath LIKE '%aanbedingen%' -- NL
		OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
		OR es.page_urlpath LIKE '%nuvaerende-salg%'
		OR es.page_urlpath LIKE '%aktuella-kampanjer%'
		OR es.page_urlpath = '/'
	)
;

-- 43652
-- prototype query
WITH
	session_bookings AS (
		-- to handle multiple bookings per session
		SELECT
			mtt.touch_id,
			LISTAGG(mtt.booking_id, ', ')                       AS booking_id_list,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gross_of_toms_gbp_constant_currency
		FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
			INNER JOIN se.data.fact_complete_booking fcb ON mtt.booking_id = fcb.booking_id
		WHERE mtt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		GROUP BY 1
	),
	homepage_views AS (
		SELECT
			mtmc.touch_affiliate_territory,
			es.event_tstamp,
			es.event_hash,
			es.page_url,
			es.page_urlhost,
			es.page_urlpath,
			es.device_platform,
			es.contexts_com_secretescapes_content_context_1,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			mt.touch_id,
			mt.attributed_user_id,
			SHA2(mt.attributed_user_id)                                             AS attributed_user_id_hash,
			mt.stitched_identity_type,
			sb.booking_id_list,
			-- calcs to handle multiple homepage view per session
			sb.bookings / COUNT(*) OVER (PARTITION BY mt.touch_id)                  AS bookings_per_homepage,
			sb.margin_gross_of_toms_gbp_constant_currency /
			COUNT(*) OVER (PARTITION BY mt.touch_id)                                AS margin_gross_of_toms_gbp_constant_currency_per_homepage
		FROM hygiene_vault_mvp.snowplow.event_stream es
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
					   ON es.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
					   ON mt.touch_id = mtmc.touch_id
			LEFT JOIN  session_bookings sb ON mt.touch_id = sb.touch_id
		WHERE es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  AND es.event_name = 'page_view'
		  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
				OR es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)
	),
	homepage_interactions AS (
		SELECT
			es.event_hash,
			es.page_url,
			es.event_tstamp,
			es.page_urlpath,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                 AS web_page_id,
			es.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  AND ( -- homepage referrer filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)

		UNION ALL

		SELECT
			es.event_hash,
			es.page_referrer,
			es.event_tstamp,
			PARSE_URL(es.page_referrer, 1)['path']::VARCHAR                         AS page_urlpath,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			es.contexts_com_secretescapes_search_context_1                          AS context,
			'search'                                                                AS element_category,
			'homepage panel'                                                        AS element_sub_category,
			'click'                                                                 AS interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
		  AND es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  -- only want user based searches
		  AND es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR = 'user'
		  AND ( -- homepage referrer filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%current-sales%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aktuelle-angebote%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%currentSales'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aanbedingen%' -- NL
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%offerte-in-corso%' -- IT
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%nuvaerende-salg%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aktuella-kampanjer%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR = '/'
			)
-- found that the search events that are triggered from the homepage carry the web page id but is persisted throughout
-- subsequent searches so need to remove all but first
		QUALIFY ROW_NUMBER() OVER (PARTITION BY web_page_id ORDER BY es.event_hash) = 1
	)
SELECT
	hpv.touch_affiliate_territory,
	hpv.event_tstamp,
	hpv.event_hash,
	hpv.page_url,
	hpv.page_urlhost,
	hpv.page_urlpath,
	hpv.device_platform,
	hpv.contexts_com_secretescapes_content_context_1,
	hpv.web_page_id,
	hpv.touch_id,
	hpv.attributed_user_id,
	hpv.attributed_user_id_hash,
	hpv.stitched_identity_type,
	hpv.booking_id_list,
	hpv.bookings_per_homepage,
	hpv.margin_gross_of_toms_gbp_constant_currency_per_homepage,
	hpi.page_url,
	hpi.event_tstamp,
	hpi.page_urlpath,
	hpi.web_page_id,
	hpi.context,
	hpi.element_category,
	hpi.element_sub_category,
	hpi.interaction_type,
	hpv.bookings_per_homepage /
	COUNT(*) OVER (PARTITION BY hpv.web_page_id) AS bookings_per_interaction,
	hpv.margin_gross_of_toms_gbp_constant_currency_per_homepage /
	COUNT(*) OVER (PARTITION BY hpv.web_page_id) AS margin_gross_of_toms_gbp_constant_currency_per_interaction
FROM homepage_views hpv
	LEFT JOIN homepage_interactions hpi ON hpv.web_page_id = hpi.web_page_id AND hpv.page_url = hpi.page_url
;

-- adjusting prototype to just interactions
WITH
	session_bookings AS (
		-- to handle multiple bookings per session
		SELECT
			mtt.touch_id,
			LISTAGG(mtt.booking_id, ', ')                       AS booking_id_list,
			COUNT(DISTINCT fcb.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gross_of_toms_gbp_constant_currency
		FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions mtt
			INNER JOIN se.data.fact_complete_booking fcb ON mtt.booking_id = fcb.booking_id
		WHERE mtt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		GROUP BY 1
	),
	homepage_views AS (
		SELECT
			es.page_url,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			mt.touch_id,
			sb.booking_id_list,
			-- calcs to handle multiple homepage view per session
			sb.bookings / COUNT(*) OVER (PARTITION BY mt.touch_id)                  AS bookings_per_homepage,
			sb.margin_gross_of_toms_gbp_constant_currency /
			COUNT(*) OVER (PARTITION BY mt.touch_id)                                AS margin_gross_of_toms_gbp_constant_currency_per_homepage
		FROM hygiene_vault_mvp.snowplow.event_stream es
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
					   ON es.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
			INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
					   ON mt.touch_id = mtmc.touch_id
			LEFT JOIN  session_bookings sb ON mt.touch_id = sb.touch_id
		WHERE es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  AND es.event_name = 'page_view'
		  AND ( -- homepage filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
				OR es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)
	),
	homepage_interactions AS (
		SELECT
			es.event_hash,
			es.page_url,
			es.event_tstamp,
			es.page_urlpath,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                 AS web_page_id,
			es.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			es.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  AND ( -- homepage referrer filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					es.page_urlpath LIKE '%current-sales%'
				OR es.page_urlpath LIKE '%aktuelle-angebote%'
				OR es.page_urlpath LIKE '%currentSales'
				OR es.page_urlpath LIKE '%aanbedingen%' -- NL
				OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR es.page_urlpath LIKE '%nuvaerende-salg%'
				OR es.page_urlpath LIKE '%aktuella-kampanjer%'
				OR es.page_urlpath = '/'
			)

		UNION ALL

		SELECT
			es.event_hash,
			es.page_referrer,
			es.event_tstamp,
			PARSE_URL(es.page_referrer, 1)['path']::VARCHAR                         AS page_urlpath,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			es.contexts_com_secretescapes_search_context_1                          AS context,
			'search'                                                                AS element_category,
			'homepage panel'                                                        AS element_sub_category,
			'click'                                                                 AS interaction_type
		FROM hygiene_vault_mvp.snowplow.event_stream es
		WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
		  AND es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  -- only want user based searches
		  AND es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR = 'user'
		  AND ( -- homepage referrer filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
					PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%current-sales%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aktuelle-angebote%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%currentSales'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aanbedingen%' -- NL
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%offerte-in-corso%' -- IT
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%nuvaerende-salg%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR LIKE '%aktuella-kampanjer%'
				OR PARSE_URL(es.page_referrer, 1)['path']::VARCHAR = '/'
			)
-- found that the search events that are triggered from the homepage carry the web page id but is persisted throughout
-- subsequent searches so need to remove all but first
		QUALIFY ROW_NUMBER() OVER (PARTITION BY web_page_id ORDER BY es.event_hash) = 1
	)
SELECT
	hpi.event_hash,
	hpi.page_url,
	hpi.event_tstamp,
	hpi.page_urlpath,
	hpi.web_page_id,
	hpi.context,
	hpi.element_category,
	hpi.element_sub_category,
	hpi.interaction_type,
	hpv.booking_id_list,
	hpv.bookings_per_homepage /
	COUNT(*) OVER (PARTITION BY hpv.web_page_id) AS bookings_per_interaction,
	hpv.margin_gross_of_toms_gbp_constant_currency_per_homepage /
	COUNT(*) OVER (PARTITION BY hpv.web_page_id) AS margin_gross_of_toms_gbp_constant_currency_per_interaction
FROM homepage_views hpv
	INNER JOIN homepage_interactions hpi ON hpv.web_page_id = hpi.web_page_id AND hpv.page_url = hpi.page_url
;

------------------------------------------------------------------------------------------------------------------------

-- investigating search interactions
SELECT

	es.event_hash,
	es.page_referrer,
	es.event_tstamp,
	es.page_urlpath,
	es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
	'search'                                                                AS element_category,
	'homepage panel'                                                        AS element_sub_category,
	'click'                                                                 AS interaction_type,
-- 	es.contexts_com_secretescapes_content_element_interaction_context_1,
-- 	es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
-- 	es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
-- 	es.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
	es.contexts_com_secretescapes_search_context_1,
	es.*
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
  AND es.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR = 'user'
  AND ( -- homepage referrer filter logic lifted from
-- https://github.com/secretescapes/dbt/blob/34dd2d3b15e020b2064d3be1a09b477c535e0ac8/models/data_intermediate/customer_insight/event-stream-page-classification/ci_event_stream_page_classification.sql#L115-L125
			es.page_referrer LIKE '%current-sales%'
		OR es.page_referrer LIKE '%aktuelle-angebote%'
		OR es.page_referrer LIKE '%currentSales'
		OR es.page_referrer LIKE '%aanbedingen%' -- NL
		OR es.page_referrer LIKE '%offerte-in-corso%' -- IT
		OR es.page_referrer LIKE '%nuvaerende-salg%'
		OR es.page_referrer LIKE '%aktuella-kampanjer%'
		OR es.page_referrer = '/'
	)
-- found that the search events that are triggered from the homepage carry the web page id but is persisted throughout
-- subsequent searches so need to remove all but first
QUALIFY ROW_NUMBER() OVER (PARTITION BY web_page_id ORDER BY es.event_hash) = 1
;
