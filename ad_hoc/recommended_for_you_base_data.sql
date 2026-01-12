/*
Metrics Needed
Homepage views
Clicks on Recommended For you (Clicks RFY)
CTR RFY (Clicks RFY / Homepage Views)
Session Bookings
Session SPVs
Session Margin
Session Bookings with RFY
Session Bookings without RFY
Session SPVs with RFY
Session SPVs without RFY
Session Margin with RFY
Session Margin without RFY
Session Time
Booking Page Views

Grain
Attributes Needed
User id
Date
Device
AB Test Segment -- data_science.operational_output.model_gateway_data_ab do it based on user id and date
Territory
RFV Segment of user
Logged in State

Conditions
UK and DE only
All platforms
Logged in Users Only
*/

USE WAREHOUSE pipe_xlarge
;

WITH
	web_homepage_views AS (
		SELECT
			es.page_url,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			mt.touch_id,
			mt.attributed_user_id,
			stba.touch_start_tstamp,
			stmc.touch_affiliate_territory
		FROM se.data_pii.scv_event_stream es
			INNER JOIN se.data_pii.scv_session_events_link mt
					   ON es.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
						   AND mt.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON mt.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON mt.touch_id = stmc.touch_id
		WHERE es.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
		  AND es.event_name = 'page_view'
		  AND (
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
	agg_web_homepage_views AS (
		SELECT
			hv.touch_id,
			hv.attributed_user_id,
			hv.touch_start_tstamp,
			hv.touch_affiliate_territory,
			COUNT(*) AS homepage_views
		FROM web_homepage_views hv
		GROUP BY 1, 2, 3, 4
	)
		,
	web_rfy_interactions_on_homepage AS (
-- 		filter and structure data from homepage interactions
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
		  AND es.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IS NOT DISTINCT FROM 'recommended for you'
	)
		,
	web_rfy_homepage_interactions_session AS (
		-- model homepage interactions back to the page view event that occurred so session level information can be included
		SELECT
			hv.touch_id
-- 			hv.attributed_user_id,
-- 			hv.touch_start_tstamp,
-- 			hv.touch_affiliate_territory,
-- 			rioh.event_hash,
-- 			rioh.page_url,
-- 			rioh.event_tstamp,
-- 			rioh.page_urlpath,
-- 			rioh.web_page_id,
-- 			rioh.context,
-- 			rioh.element_category,
-- 			rioh.element_sub_category,
-- 			rioh.interaction_type
		FROM web_rfy_interactions_on_homepage rioh
			INNER JOIN web_homepage_views hv ON rioh.web_page_id = hv.web_page_id
	),
	agg_web_rfy_homepage_interaction AS (
		SELECT
			his.touch_id,
			COUNT(*) AS homepage_rfy_clicks
		FROM web_rfy_homepage_interactions_session his
		GROUP BY 1
	),
	model_data AS (
		SELECT
			ahv.touch_id,
			ahv.attributed_user_id,
			ahv.touch_start_tstamp,
			ahv.touch_affiliate_territory,
			ahv.homepage_views,
			arhi.homepage_rfy_clicks
		FROM agg_web_homepage_views ahv
			LEFT JOIN agg_web_rfy_homepage_interaction arhi ON ahv.touch_id = arhi.touch_id
	)
SELECT
	model_data.touch_start_tstamp::DATE              AS date,
	model_data.touch_affiliate_territory,
	SUM(model_data.homepage_views)                   AS total_homepage_views,
	SUM(model_data.homepage_rfy_clicks)              AS total_homepage_rfy_clicks,
	total_homepage_rfy_clicks / total_homepage_views AS ctr
FROM model_data
GROUP BY 1, 2
;
------------------------------------------------------------------------------------------------------------------------
SELECT
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 7 --TODO adjust
  AND ses.event_name = 'screen_view'
;


SELECT
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 7 --TODO adjust
  AND ses.event_name = 'screen_view'
GROUP BY 1
ORDER BY 2 DESC

------------------------------------------------------------------------------------------------------------------------
WITH
	app_homepage_views AS (
		-- app homepage screen views
		SELECT
			ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR            AS screen_name,
			ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR AS snowplow_session_id,
			mt.touch_id,
			mt.attributed_user_id,
			stba.touch_start_tstamp,
			stmc.touch_affiliate_territory
		FROM se.data_pii.scv_event_stream ses
			INNER JOIN se.data_pii.scv_session_events_link mt
					   ON ses.event_hash = mt.event_hash
						   AND mt.event_tstamp::DATE >= CURRENT_DATE - 7 --TODO adjust
						   AND mt.stitched_identity_type = 'se_user_id'
			INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON mt.touch_id = stba.touch_id
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON mt.touch_id = stmc.touch_id
		WHERE ses.event_tstamp >= CURRENT_DATE - 7 --TODO adjust
		  AND ses.event_name = 'screen_view'
		  AND ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR IN
			  ('homepage', 'homepage collection')
	),
	agg_app_homepage_views AS (
		SELECT
			hv.touch_id,
			hv.attributed_user_id,
			hv.touch_start_tstamp,
			hv.touch_affiliate_territory,
			COUNT(*) AS homepage_views
		FROM app_homepage_views hv
		GROUP BY 1, 2, 3, 4
	),
	app_homepage_rfv_interactions AS (
		-- app homepage interactions
		SELECT
			ses.event_hash,
			ses.page_url,
			ses.event_tstamp,
			ses.page_urlpath,
			ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
			ses.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.event_tstamp >= CURRENT_DATE - 7 --TODO adjust
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND ses.device_platform LIKE 'native app%'
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR =
			  'recommended for you'
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IN
			  ('homepage panel', 'homepage collection')
	),

	touch_id_to_snowplow_session_id AS (
		-- create a simplified list that associates a snowplow session id to a scv touch id
		SELECT DISTINCT
			ahv.touch_id,
			ahv.snowplow_session_id
		FROM app_homepage_views ahv
	),
	app_rfy_homepage_interactions_session AS (
		-- model homepage interactions back to a homepage screen view event that occured so session level information can be included
		SELECT
			ahv.touch_id
-- 			ahri.event_hash,
-- 			ahri.page_url,
-- 			ahri.event_tstamp,
-- 			ahri.page_urlpath,
-- 			ahri.context,
-- 			ahri.element_category,
-- 			ahri.element_sub_category,
-- 			ahri.interaction_type
		FROM app_homepage_rfv_interactions ahri
			INNER JOIN touch_id_to_snowplow_session_id ahv ON ahri.snowplow_session_id = ahv.snowplow_session_id
	),
	agg_app_rfy_homepage_interaction AS (
		SELECT
			arhis.touch_id,
			COUNT(*) AS homepage_rfy_clicks
		FROM app_rfy_homepage_interactions_session arhis
		GROUP BY 1
	),
	model_data AS (
		SELECT
			aahv.touch_id,
			aahv.attributed_user_id,
			aahv.touch_start_tstamp,
			aahv.touch_affiliate_territory,
			aahv.homepage_views,
			aarhi.homepage_rfy_clicks
		FROM agg_app_homepage_views aahv
			LEFT JOIN agg_app_rfy_homepage_interaction aarhi ON aahv.touch_id = aarhi.touch_id
	)
SELECT
	model_data.touch_start_tstamp::DATE              AS date,
	model_data.touch_affiliate_territory,
	SUM(model_data.homepage_views)                   AS total_homepage_views,
	SUM(model_data.homepage_rfy_clicks)              AS total_homepage_rfy_clicks,
	total_homepage_rfy_clicks / total_homepage_views AS ctr
FROM model_data
GROUP BY 1, 2
;

SELECT *
FROM model_data
;

------------------------------------------------------------------------------------------------------------------------

-- session gianni clicking rfy
SELECT
	ses.event_id,
	ses.event_name,
	ses.v_tracker,
	ses.device_platform,
	ses.event_tstamp,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.event_vendor,
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR                               AS screen_name,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['sale_id']::VARCHAR              AS se_sale_id,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
	ses.contexts_com_secretescapes_content_element_interaction_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1
FROM se.data_pii.scv_event_stream ses
-- used to find the session touch_id 'd0efd2d373135ca6f47f10ec453587a59587e1f265567fd256d84a524c84a41d'
-- 	INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
WHERE ses.user_id = '72868430'
  AND ses.collector_tstamp::DATE = '2023-07-24'
  AND ses.device_platform = 'native app ios'
ORDER BY ses.event_tstamp
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	model_data.touch_start_tstamp::DATE              AS date,
	model_data.touch_affiliate_territory,
	SUM(model_data.homepage_views)                   AS total_homepage_views,
	SUM(model_data.homepage_rfy_clicks)              AS total_homepage_rfy_clicks,
	total_homepage_rfy_clicks / total_homepage_views AS ctr
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_web_recommended_for_you_ctr model_data
WHERE model_data.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2
;



SELECT
	model_data.touch_start_tstamp::DATE              AS date,
	model_data.touch_affiliate_territory,
	SUM(model_data.homepage_views)                   AS total_homepage_views,
	SUM(model_data.homepage_rfy_clicks)              AS total_homepage_rfy_clicks,
	total_homepage_rfy_clicks / total_homepage_views AS ctr
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_app_recommended_for_you_ctr model_data
WHERE model_data.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2
;

WITH
	stack AS
		(
			SELECT
				darfyc.touch_id,
				darfyc.attributed_user_id,
				darfyc.touch_start_tstamp,
				darfyc.touch_affiliate_territory,
				darfyc.homepage_views,
				darfyc.homepage_rfy_clicks
			FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_app_recommended_for_you_ctr darfyc
			UNION ALL
			SELECT
				dwrfyc.touch_id,
				dwrfyc.attributed_user_id,
				dwrfyc.touch_start_tstamp,
				dwrfyc.touch_affiliate_territory,
				dwrfyc.homepage_views,
				dwrfyc.homepage_rfy_clicks
			FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_web_recommended_for_you_ctr dwrfyc
		)
SELECT
	s.touch_id,
	stba.touch_logged_in,
	stba.touch_experience,
	s.attributed_user_id,
	ab.territory_id,
	ab.apollo_recommended_model,
	ab.ab_test_segment,
	ab.recommended_deal_flag,
	s.touch_start_tstamp,
	s.touch_affiliate_territory,
	s.homepage_views,
	s.homepage_rfy_clicks
FROM stack s
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON s.touch_id = stba.touch_id
	LEFT JOIN  data_science.operational_output.model_gateway_data_ab ab
			   ON s.attributed_user_id = ab.user_id AND s.touch_start_tstamp::DATE = ab.upload_ts::DATE
;


SELECT
	ab.territory_id,
	ab.user_id,
	ab.apollo_recommended_model,
	ab.ab_test_segment,
	ab.recommended_deal_flag,
	ab.upload_ts
FROM data_science.operational_output.model_gateway_data_ab ab
;


SELECT *
FROM dbt.bi_data_platform__intermediate.dp_03_recommended_for_you_ctr
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_02_app_recommended_for_you_ctr
WHERE homepage_rfy_clicks = 0
;

SELECT *
FROM latest_vault.cms_mysql.booking_cancellation bc
;

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE
WHERE ses.event_tstamp >= CURRENT_DATE
  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
;


SELECT
	ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR AS sub_category,
	*
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE
WHERE ses.event_tstamp >= CURRENT_DATE
  AND ssel.touch_id = 'f1c2a1cac17de7d72bfb56858902e510e18f17a85bc3c2bb65f787748f89c787'
;


SELECT
	ssel.touch_id,
	COUNT(*) AS booking_form_views
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON ses.event_hash = ssel.event_hash
				   AND ssel.stitched_identity_type = 'se_user_id'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
WHERE ses.event_name = 'page_view'
  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
GROUP BY 1
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_05_recommended_for_you_ctr
;

USE ROLE pipelinerunner
;

SELECT *
FROM dbt.bi_data_platform.dp_recommended_for_you_data_model
;

GRANT SELECT ON TABLE dbt.bi_data_platform.dp_recommended_for_you_data_model TO ROLE data_team_basic
;

------------------------------------------------------------------------------------------------------------------------
-- analysing rfy ab test data

SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	rfy.touch_experience,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt.bi_data_platform.dp_recommended_for_you_data_model rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2, 3
;

SELECT *
FROM dbt.bi_data_platform.dp_recommended_for_you_data_model rfy
WHERE rfy.touch_affiliate_territory = 'UK'
  AND rfy.touch_start_tstamp::DATE BETWEEN '2023-07-26' AND '2023-07-27'
;

SELECT
	stba.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)           AS day_of_week,
	COUNT(*)                      AS sessions
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stmc.touch_affiliate_territory = 'UK'
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1, 2
;


SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt.bi_data_platform__intermediate.dp_02_app_recommended_for_you_ctr rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2
;


SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt.bi_data_platform__intermediate.dp_01_web_recommended_for_you_ctr rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2
;

SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt.bi_data_platform__intermediate.dp_05_recommended_for_you_ctr rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2
;


SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt.bi_data_platform__intermediate.dp_05_recommended_for_you_ctr rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2
;



SELECT
	rfy.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)          AS day_of_week,
	COUNT(*)                     AS sessions
FROM dbt_dev.dbt_robinpatel_data_platform__intermediate.dp_01_web_recommended_for_you_ctr rfy
WHERE rfy.touch_affiliate_territory = 'UK'
GROUP BY 1, 2
;


SELECT
	stba.touch_start_tstamp::DATE AS touch_date,
	DAYNAME(touch_date)           AS day_of_week,
	COUNT(*)                      AS sessions
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stmc.touch_affiliate_territory = 'UK'
  AND stba.touch_start_tstamp >= '2023-01-01'
GROUP BY 1, 2
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	es.event_tstamp::DATE       AS date,
	COUNT(*)                    AS homempage_views,
	COUNT(DISTINCT mt.touch_id) AS sessions
FROM se.data_pii.scv_event_stream AS es
	INNER JOIN se.data_pii.scv_session_events_link AS mt
			   ON
						   es.event_hash = mt.event_hash
					   AND mt.event_tstamp BETWEEN '2023-01-01' AND CURRENT_DATE
					   AND mt.stitched_identity_type = 'se_user_id'
	INNER JOIN
			   se.data_pii.scv_touch_basic_attributes AS stba
			   ON mt.touch_id = stba.touch_id
	INNER JOIN
			   se.data.scv_touch_marketing_channel AS stmc
			   ON mt.touch_id = stmc.touch_id
WHERE es.event_tstamp BETWEEN '2023-07-01' AND CURRENT_DATE
  AND es.event_name = 'page_view'
  AND (
			es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR
			= 'homepage'
		OR es.page_urlpath LIKE '%current-sales%'
		OR es.page_urlpath LIKE '%aktuelle-angebote%'
		OR es.page_urlpath LIKE '%currentSales'
		OR es.page_urlpath LIKE '%aanbedingen%' -- NL
		OR es.page_urlpath LIKE '%offerte-in-corso%' -- IT
		OR es.page_urlpath LIKE '%nuvaerende-salg%'
		OR es.page_urlpath LIKE '%aktuella-kampanjer%'
		OR es.page_urlpath = '/'
	)
  AND stmc.touch_affiliate_territory = 'UK'
GROUP BY 1
;



SELECT *
FROM latest_vault.cms_mysql.tag_links tl
WHERE tl.tag_ref = 56789
;


SELECT
	drfydm.touch_start_tstamp::DATE AS date,
	drfydm.touch_experience,
	COUNT(*)
FROM dbt.bi_data_platform.dp_recommended_for_you_data_model drfydm
GROUP BY 1, 2
;



WITH
	agg_data AS (
		SELECT
			rfy.touch_experience,
			rfy.ab_test_segment,
			rfy.touch_affiliate_territory,
			COUNT(*)                                                    AS sessions,
			COUNT(IFF(rfy.homepage_views > 0, rfy.touch_id, NULL))      AS sessions_homepage,
			COUNT(IFF(rfy.homepage_rfy_clicks > 0, rfy.touch_id, NULL)) AS sessions_rfy,
			COUNT(IFF(rfy.bookings > 0, rfy.touch_id, NULL))            AS sessions_booking,
			SUM(rfy.homepage_views)                                     AS homepage_views,
			SUM(rfy.homepage_rfy_clicks)                                AS homepage_rfy_clicks,
			SUM(rfy.bookings)                                           AS bookings,
			SUM(rfy.margin_gbp)                                         AS margin_gbp
		FROM dbt.bi_data_platform.dp_recommended_for_you_data_model rfy
			INNER JOIN se.data_pii.scv_touch_basic_attributes tba ON tba.touch_id = rfy.touch_id
		WHERE rfy.ab_test_segment IS NOT NULL
		  AND rfy.touch_start_tstamp::DATE >= '2023-07-29' --start date iOS go live
		  AND rfy.touch_start_tstamp::DATE < CURRENT_DATE
		  AND rfy.touch_affiliate_territory IN ('UK', 'DE')

		GROUP BY 1, 2, 3
	)
SELECT
	ad.touch_affiliate_territory,
	ad.touch_experience,
	ad.ab_test_segment,
	ad.sessions,
	ad.homepage_views,
	ad.sessions_homepage / ad.sessions     AS homepage_sessions_share,
	ad.homepage_rfy_clicks,
	ad.sessions_rfy / ad.sessions_homepage AS homepage_rfy_share,
	ad.bookings,
	ad.sessions_booking / ad.sessions      AS booking_session_conversion,
	ad.margin_gbp,
	ad.margin_gbp / ad.sessions            AS margin_per_session
FROM agg_data ad
WHERE ad.touch_experience IN ('native app ios', 'mobile web')
;

------------------------------------------------------------------------------------------------------------------------
