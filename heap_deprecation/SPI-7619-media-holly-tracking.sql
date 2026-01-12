-- investigating microsite tracking
/*
A microsite is tracking on the mp. subdomain
*/

USE WAREHOUSE pipe_xlarge
;

WITH
	event_data AS (
		SELECT
			ses.event_tstamp,
			ses.page_url,
			ses.page_urlpath,
			ses.device_platform                  AS touch_experience,
			SPLIT_PART(ses.page_urlpath, '/', 2) AS microsite_territory,
			SPLIT_PART(ses.page_urlpath, '/', 3) AS microsite_time_period,
			SPLIT_PART(ses.page_urlpath, '/', 4) AS microsite_campaign,
			ses.page_title,
			ssel.attributed_user_id,
			spse.page_duration_seconds
		FROM se.data_pii.scv_event_stream ses
		INNER JOIN se.data_pii.scv_page_screen_enrichment spse
			ON ses.event_hash = spse.event_hash
			AND spse.event_tstamp >= '2025-01-01'
		INNER JOIN se.data_pii.scv_session_events_link ssel
			ON ssel.event_hash = ses.event_hash
			AND ssel.event_tstamp >= '2025-01-01'
		INNER JOIN se.data.scv_touch_basic_attributes stba
			ON ssel.touch_id = stba.touch_id
			AND stba.touch_start_tstamp >= '2025-01-01'
		WHERE ses.event_name = 'page_view'
		  AND ses.page_urlhost LIKE 'mp%' -- microsites
-- 		  AND ses.page_urlpath LIKE '/uk/2025/morocco/%'
	)
SELECT
	event_data.event_tstamp::DATE                 AS date,
	event_data.page_title,
	event_data.touch_experience,
	event_data.microsite_territory,
	event_data.microsite_time_period,
	event_data.microsite_campaign,
	COUNT(DISTINCT event_data.attributed_user_id) AS unique_users,
	COUNT(*)                                      AS page_views,
	-- compromised at the moment as there's no page pings on microsites
	AVG(event_data.page_duration_seconds)         AS avg_dwell_time
FROM event_data
GROUP BY ALL


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '67970160' AND stba.touch_start_tstamp::DATE = '2025-07-30'



SELECT *
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.touch_start_tstamp >= CURRENT_DATE - 1
  AND stmc.utm_medium = 'SE_media'
  AND stmc.utm_campaign = 'Morocco_Summer_UK_2025'
;



SELECT
	column1,
	SPLIT_PART(column1, '/', 1),
	SPLIT_PART(column1, '/', 2),
	SPLIT_PART(column1, '/', 3),
	SPLIT_PART(column1, '/', 4),
FROM
VALUES ('/uk/2025/morocco/competition-entered/')
;


------------------------------------------------------------------------------------------------------------------------
WITH
	microsite_generated_sessions AS (
		-- shortlist of sessions that started as a result of a microsite referral
		SELECT
			session_attributes.touch_id,
			session_attributes.touch_experience,
			session_attributes.touch_start_tstamp,
			session_attributes.touch_landing_page,
			session_attributes.touch_landing_page_categorisation,
			session_attributes.attributed_user_id_hash,
			session_attributes.touch_duration_seconds_enhanced,
			session_attributes.touch_referrer_url,
			session_attributes.num_trxs,
			session_attributes.num_spvs
		FROM se.data.scv_touch_basic_attributes session_attributes
		-- sessions that started as a result of a referral from a microsite
		WHERE session_attributes.touch_referrer_url LIKE 'https://mp.secretescapes.%'
		  AND session_attributes.touch_start_tstamp >= '2025-01-01'
	),
	bookings AS (
		SELECT
			touched_transactions.touch_id,
			COUNT(DISTINCT touched_transactions.booking_id)              AS bookings,
			SUM(fact_booking.gross_revenue_gbp_constant_currency)        AS gross_revenue_gbp,
			SUM(fact_booking.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp,
		FROM se.data.scv_touched_transactions touched_transactions
		INNER JOIN microsite_generated_sessions sessions
			ON touched_transactions.touch_id = sessions.touch_id
		INNER JOIN se.data.fact_booking fact_booking
			ON touched_transactions.booking_id = fact_booking.booking_id
			AND fact_booking.booking_status_type IN ('live', 'cancelled')
		WHERE touched_transactions.event_tstamp >= '2025-01-01'
		GROUP BY touched_transactions.touch_id
	)
SELECT
	sessions.touch_id,
	sessions.touch_experience,
	sessions.touch_start_tstamp,
	sessions.touch_landing_page,
	sessions.touch_landing_page_categorisation,
	sessions.attributed_user_id_hash,
	sessions.touch_duration_seconds_enhanced,
	sessions.touch_referrer_url,
	channel.touch_affiliate_territory AS territory,
	channel.touch_mkt_channel,
	channel.utm_source,
	channel.utm_medium,
	channel.utm_campaign,
	sessions.num_trxs,
	sessions.num_spvs,
	bookings.bookings,
	bookings.gross_revenue_gbp,
	bookings.margin_gbp
FROM microsite_generated_sessions AS sessions
INNER JOIN se.data.scv_touch_marketing_channel channel
	ON sessions.touch_id = channel.touch_id
	AND channel.touch_start_tstamp >= '2025-01-01'
LEFT JOIN bookings
	ON sessions.touch_id = bookings.touch_id

-----------------------------------------------------------------------------------------------------------------------
-- checking top banner

SELECT *
FROM se.data_pii.scv_page_screen_enrichment spse
WHERE se.data.page_url_categorisation(spse.page_url) = 'home page'
  AND spse.event_tstamp >= CURRENT_DATE - 10
;

-- It appears that content viewed events fire for media banners, these can be used to determine impressions
-- One thing to note is it looks like;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2025-07-30'
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
--   AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.user_id = 67970160
;


SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE email = 'robin.patel@secretescapes.com'
;


-- currently don't have content interaction or click events for banners

SET start_date = '2025-06-01'
;

USE WAREHOUSE pipe_xlarge
;

WITH
	impressions AS (
		-- explode out content viewed array to get banner impressions
		SELECT
			pages.*,
			viewed_items.value['elements'][0]::VARCHAR AS element
		FROM se.data_pii.scv_page_screen_enrichment pages,
			 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
		WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
		  AND pages.event_tstamp BETWEEN $start_date AND CURRENT_DATE
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  (
			   'top media banner',
			   'middle media banner',
			   'footer media banner',
			   'two column slot media banner'
				  )
	),
	banner_views AS (
		SELECT
			-- aggregate up to common grain, note that impressions are unique per page view, if someone
			-- scrolls a banner in and out of the view port it will fire multiple impressions, however we
			-- only count this once.
			impressions.event_tstamp::DATE         AS date,
			impressions.element,
			COUNT(DISTINCT impressions.event_hash) AS banner_impressions
		FROM impressions
		GROUP BY impressions.event_tstamp::DATE,
				 impressions.element
	),
	top_banner_clicks AS (
		-- inferring clicks based on a page view with utm params set on the banner link
		-- note that this measure is only accurate if the hyperlinks on the banner are set consistently
		SELECT
			ses.event_tstamp::DATE         AS date,
			'top media banner'             AS element,
			COUNT(DISTINCT ses.event_hash) AS clicks
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.event_name = 'page_view'
		  AND ses.event_tstamp BETWEEN $start_date AND CURRENT_DATE
		  AND LOWER(ses.mkt_source) LIKE ANY ('topbanner', 'herobanner', 'top_hero_banner%',
											  'thb') -- as observed from butterbox campaign on the 7th August 2025
		  AND ses.page_urlhost = 'mp.secretescapes.com'
		GROUP BY ses.event_tstamp::DATE
	)
SELECT
	banner_views.date,
	banner_views.element,
	banner_impressions,
	top_banner_clicks.clicks
FROM banner_views
LEFT JOIN top_banner_clicks
	ON banner_views.date = top_banner_clicks.date
	AND banner_views.element = top_banner_clicks.element
;

-- Example page url when clicking on top banner of homepage to a microsite: https://mp.secretescapes.com/uk/2025/pet-friendly-escapes/?utm_source=topbanner&utm_medium=SE_media&utm_campaign=Butternut_UK_2025


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_name = 'page_view'
  AND ses.event_tstamp BETWEEN $start_date AND CURRENT_DATE
  AND LOWER(ses.mkt_source) LIKE ANY ('topbanner', 'herobanner', 'top_hero_banner%',
									  'thb') -- as observed from butterbox campaign on the 7th August 2025
  AND ses.page_urlhost = 'mp.secretescapes.com'


SET start_date = '2025-08-01'
;

WITH
	flatten_views AS (
		-- explode out content viewed array to get banner impressions
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			sessions.touch_experience,
			channel.touch_affiliate_territory          AS territory,
			viewed_items.value['elements'][0]::VARCHAR AS element
		FROM se.data_pii.scv_page_screen_enrichment pages
			 INNER JOIN se.data_pii.scv_session_events_link sessionisation
			ON pages.event_hash = sessionisation.event_hash
			AND sessionisation.event_tstamp >= ' 2025-09-08'
			 INNER JOIN se.data.scv_touch_basic_attributes sessions
			ON sessionisation.touch_id = sessions.touch_id
			AND sessions.touch_start_tstamp >= '2025-09-08'
			 INNER JOIN se.data.scv_touch_marketing_channel channel
			ON sessionisation.touch_id = channel.touch_id
			AND channel.touch_start_tstamp >= '2025-09-08',
			 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
		WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
		  AND pages.content_viewed_array IS NOT NULL
		  -- 8th september is when impressions and interaction events were corrected from
		  AND pages.event_tstamp BETWEEN '2025-09-08' AND CURRENT_DATE
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  (
			   'promotional strapline header banner',
			   'top media banner',
			   'middle media banner',
			   'footer media banner',
			   'two column slot media banner'
				  )
	),
	impressions AS (
		SELECT
			-- aggregate up to common grain, note that impressions are unique per page view, if someone
			-- scrolls a banner in and out of the view port it will fire multiple impressions, however we
			-- only count this once.
			flatten_views.event_tstamp::DATE         AS date,
			flatten_views.element,
			flatten_views.touch_experience,
			flatten_views.territory,
			COUNT(DISTINCT flatten_views.event_hash) AS banner_impressions
		FROM flatten_views
		GROUP BY flatten_views.event_tstamp::DATE,
				 flatten_views.element,
				 flatten_views.touch_experience,
				 flatten_views.territory
	),
	flatten_clicks AS (
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			sessions.touch_experience,
			channel.touch_affiliate_territory                    AS territory,
			interaction_items.value['element_category']::VARCHAR AS element
		FROM se.data_pii.scv_page_screen_enrichment pages
			 INNER JOIN se.data_pii.scv_session_events_link sessionisation
			ON pages.event_hash = sessionisation.event_hash
			AND sessionisation.event_tstamp >= ' 2025-09-08'
			 INNER JOIN se.data.scv_touch_basic_attributes sessions
			ON sessionisation.touch_id = sessions.touch_id
			AND sessions.touch_start_tstamp >= '2025-09-08'
			 INNER JOIN se.data.scv_touch_marketing_channel channel
			ON sessionisation.touch_id = channel.touch_id
			AND channel.touch_start_tstamp >= '2025-09-08',
			 LATERAL FLATTEN(INPUT => pages.content_interaction_array, OUTER => TRUE) interaction_items
-- limit to home page only
		WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
		  -- 8th september is when impressions and interaction events were corrected from
		  AND pages.event_tstamp BETWEEN '2025-09-08' AND CURRENT_DATE
		  AND pages.content_interaction_array IS NOT NULL
		  AND interaction_items.value['element_category']::VARCHAR IN
			  (
			   'promotional strapline header banner',
			   'top media banner',
			   'middle media banner',
			   'footer media banner',
			   'two column slot media banner'
				  )
	),
	clicks AS (
		SELECT
			-- aggregate up to common grain, note that impressions are unique per page view, if someone
			-- scrolls a banner in and out of the view port it will fire multiple impressions, however we
			-- only count this once.
			flatten_clicks.event_tstamp::DATE         AS date,
			flatten_clicks.element,
			flatten_clicks.touch_experience,
			flatten_clicks.territory,
			COUNT(DISTINCT flatten_clicks.event_hash) AS banner_clicks
		FROM flatten_clicks
		GROUP BY flatten_clicks.event_tstamp::DATE,
				 flatten_clicks.element,
				 flatten_clicks.touch_experience,
				 flatten_clicks.territory
	)
SELECT
	impressions.date,
	impressions.element,
	impressions.banner_impressions,
	impressions.touch_experience,
	impressions.territory,
	clicks.banner_clicks
FROM impressions
LEFT JOIN clicks
	ON impressions.date = clicks.date
	AND impressions.element = clicks.element
	AND impressions.touch_experience = clicks.touch_experience
	AND impressions.territory = clicks.territory
;

------------------------------------------------------------------------------------------------------------------------
-- investigating content viewed drop off

SELECT
	event_tstamp::DATE AS date,
	COUNT(*)           AS content_viewed_events
FROM se.data_pii.scv_event_stream
WHERE se.data.page_url_categorisation(page_url) = 'home page'
  AND event_tstamp >= CURRENT_DATE - 5
  AND contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'][0] IS NOT NULL
  AND se_brand = 'SE Brand'
GROUP BY ALL
;


SELECT
	event_tstamp::DATE AS date,
	COUNT(*)           AS content_interaction_events
FROM se.data_pii.scv_event_stream
WHERE se.data.page_url_categorisation(page_url) = 'home page'
  AND event_tstamp >= CURRENT_DATE - 5
  AND contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IN
	  (
	   'promotional strapline header banner',
	   'top media banner',
	   'middle media banner',
	   'footer media banner',
	   'two column slot media banner'
		  )
  AND se_brand = 'SE Brand'
GROUP BY ALL
;


-- explode out content viewed array to get banner impressions
SELECT
	pages.event_tstamp,
	pages.event_hash,
	viewed_items.value['elements'][0]::VARCHAR AS element
FROM se.data_pii.scv_page_screen_enrichment pages
	 INNER JOIN se.data_pii.scv_session_events_link sessionisation
	ON pages.event_hash = sessionisation.event_hash
	AND sessionisation.event_tstamp >= ' 2025-09-08',
	 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
  AND pages.content_viewed_array IS NOT NULL
  -- 8th september is when impressions and interaction events were corrected from
  AND pages.event_tstamp BETWEEN '2025-09-08' AND CURRENT_DATE
  AND viewed_items.value['elements'][0]::VARCHAR IN
	  (
	   'promotional strapline header banner',
	   'top media banner',
	   'middle media banner',
	   'footer media banner',
	   'two column slot media banner'
		  )

-- check number of homepage views
SELECT
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_brand = 'SE Brand'
  AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-04'
  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.is_server_side_event = FALSE
;

-- circa 64.5K on November 4th

-- check number of content viewed events for top media banner on the 4th of November
SELECT
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'],
	ses.page_url,
	ses.event_name,
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR
FROM se.data_pii.scv_event_stream ses,
	 LATERAL FLATTEN(INPUT => ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'], OUTER =>
					 TRUE) viewed_elements
WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-04'
  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND viewed_elements.value = 'top media banner'
-- circa 111.6K impressions

;

-- check unique page views that had a top banner impression
SELECT DISTINCT
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR
FROM se.data_pii.scv_event_stream ses,
	 LATERAL FLATTEN(INPUT => ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'], OUTER =>
					 TRUE) viewed_elements
WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-04'
  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND viewed_elements.value = 'top media banner'
-- 45.5K


-- check how many are associated to a page view
WITH
	content_viewed_top_banner AS (
		SELECT
			ses.contexts_com_secretescapes_content_element_viewed_context_1,
			ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'],
			ses.page_url,
			ses.event_name,
			ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'],
							 OUTER =>
							 TRUE) viewed_elements
		WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE = '2025-11-04'
		  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
		  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
		  AND viewed_elements.value = 'top media banner'
	),
	distinct_content_viewed AS (
		SELECT DISTINCT
			web_page_id
		FROM content_viewed_top_banner
	),
	modelling AS (
		SELECT
			es.event_name,
			es.event_tstamp,
			es.event_hash,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id
		FROM se.data_pii.scv_event_stream es
		INNER JOIN distinct_content_viewed dcv
			ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = dcv.web_page_id
		WHERE es.event_tstamp::DATE = '2025-11-04'
		  AND es.event_name = 'page_view'
		  AND es.se_brand = 'SE Brand'
	)
SELECT
	COUNT(DISTINCT event_hash)
FROM modelling
;

-- 46.1K homepage view events have a top banner impression

-- check footer banner impressions
WITH
	impression_elements AS (
		SELECT
			ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
		FROM se.data_pii.scv_event_stream ses,
			 LATERAL FLATTEN(INPUT => ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'],
							 OUTER
							 =>
							 TRUE) viewed_elements
		WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE = '2025-11-04'
		  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
		  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
		  AND viewed_elements.value = 'footer media banner'
	),
	distinct_content_viewed AS (
		SELECT DISTINCT
			web_page_id
		FROM impression_elements
	),
	modelling AS (
		SELECT
			es.event_name,
			es.event_tstamp,
			es.event_hash,
			es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id
		FROM se.data_pii.scv_event_stream es
		INNER JOIN distinct_content_viewed dcv
			ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = dcv.web_page_id
		WHERE es.event_tstamp::DATE = '2025-11-04'
		  AND es.event_name = 'page_view'
		  AND es.se_brand = 'SE Brand'
	)
SELECT *
FROM modelling
;

-- the tableau report is currently showing circa 6-8K per day
-- however this query shows circa 1K which is much more inline with what we expect

-- code from homepage tableau worksheet
SELECT
	pages.event_tstamp,
	pages.event_hash,
	sessions.touch_experience,
	channel.touch_affiliate_territory          AS territory,
	viewed_items.value['elements'][0]::VARCHAR AS element
FROM se.data_pii.scv_page_screen_enrichment pages
	 INNER JOIN se.data_pii.scv_session_events_link sessionisation
	ON pages.event_hash = sessionisation.event_hash
	AND sessionisation.event_tstamp >= ' 2025-09-08'
	 INNER JOIN se.data.scv_touch_basic_attributes sessions
	ON sessionisation.touch_id = sessions.touch_id
	AND sessions.touch_start_tstamp >= '2025-09-08'
	 INNER JOIN se.data.scv_touch_marketing_channel channel
	ON sessionisation.touch_id = channel.touch_id
	AND channel.touch_start_tstamp >= '2025-09-08',
	 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
  AND sessions.touch_se_brand = 'SE Brand'
  AND pages.content_viewed_array IS NOT NULL
  -- 8th september is when impressions and interaction events were corrected from
  AND pages.event_tstamp::DATE = '2025-11-04'
  AND viewed_items.value['elements'][0]::VARCHAR IN
	  (
		  'footer media banner'
		  )
;

-- this shows 8.1K
-- adding in se brand -- still shows 8.1K


-- checking the enrichment lateral flatten
WITH
	viewed_events AS (
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			pages.web_page_id,
			viewed_items.value
		FROM se.data_pii.scv_page_screen_enrichment pages,
			 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
		WHERE pages.event_tstamp::DATE = '2025-11-04'
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  (
				  'footer media banner'
				  )
	)
SELECT
	COUNT(*),
	COUNT(DISTINCT viewed_events.event_hash),
	COUNT(DISTINCT web_page_id)
FROM viewed_events
;

-- shows 8.1K and 7.8K unique event hashes, 7.8K unique web page ids;


SELECT
	pages.event_tstamp,
	pages.event_hash,
	pages.web_page_id,
	viewed_items.value,
	viewed_items.value['internalContentName']::VARCHAR AS internal_content_name
FROM se.data_pii.scv_page_screen_enrichment pages,
	 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
WHERE pages.event_tstamp::DATE = CURRENT_DATE - 1
  AND viewed_items.value['elements'][0]::VARCHAR IN
	  (
		  'promotional strapline header banner'
		  )
;


SELECT
	pages.event_tstamp,
	pages.event_hash,
	pages.web_page_id,
	clicked_items.value,
	clicked_items.value['internalContentName']::VARCHAR AS internal_content_name
FROM se.data_pii.scv_page_screen_enrichment pages,
	 LATERAL FLATTEN(INPUT => pages.content_interaction_array, OUTER => TRUE) clicked_items
WHERE pages.event_tstamp::DATE = CURRENT_DATE - 1
  AND clicked_items.value['element_category']::VARCHAR IN
	  (
		  'promotional strapline header banner'
		  )
;

WITH
	top_media_views AS
		(
			SELECT
				pages.event_tstamp,
				pages.event_hash,
				pages.web_page_id,
				viewed_items.value,
				viewed_items.value['internalContentName']::VARCHAR AS internal_content_name
			FROM se.data_pii.scv_page_screen_enrichment pages,
				 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
			WHERE pages.event_tstamp::DATE = CURRENT_DATE - 1
			  AND viewed_items.value['elements'][0]::VARCHAR IN
				  ('top media banner'
					  )
		)

SELECT
	top_media_views.internal_content_name,
	COUNT(*) AS views
FROM top_media_views
GROUP BY ALL
;


SELECT
	pages.event_tstamp,
	pages.event_hash,
	pages.web_page_id,
	clicked_items.value,
	clicked_items.value['internalContentName']::VARCHAR AS internal_content_name
FROM se.data_pii.scv_page_screen_enrichment pages,
	 LATERAL FLATTEN(INPUT => pages.content_interaction_array, OUTER => TRUE) clicked_items
WHERE pages.event_tstamp::DATE = CURRENT_DATE - 1
  AND clicked_items.value['element_category']::VARCHAR IN
	  (
		  'top media banner'
		  )
;



WITH
	flatten_views AS (
		-- explode out content viewed array to get banner impressions
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			se.data.page_url_categorisation(pages.page_url) AS url_categorisation,
			sessions.touch_experience,
			channel.touch_affiliate_territory               AS territory,
			viewed_items.value['elements'][0]::VARCHAR      AS element
		FROM se.data_pii.scv_page_screen_enrichment pages
			 INNER JOIN se.data_pii.scv_session_events_link sessionisation
			ON pages.event_hash = sessionisation.event_hash
			AND sessionisation.event_tstamp >= ' 2025-09-08'
			 INNER JOIN se.data.scv_touch_basic_attributes sessions
			ON sessionisation.touch_id = sessions.touch_id
			AND sessions.touch_start_tstamp >= '2025-09-08'
			 INNER JOIN se.data.scv_touch_marketing_channel channel
			ON sessionisation.touch_id = channel.touch_id
			AND channel.touch_start_tstamp >= '2025-09-08',
			 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
		WHERE pages.content_viewed_array IS NOT NULL
		  AND pages.event_tstamp BETWEEN '2025-09-08' AND CURRENT_DATE
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  (
			   'promotional strapline header banner',
			   'top media banner',
			   'middle media banner',
			   'footer media banner',
			   'two column slot media banner'
				  )
	),
	impressions AS (
		SELECT
			-- aggregate up to common grain, note that impressions are unique per page view, if someone
			-- scrolls a banner in and out of the view port it will fire multiple impressions, however we
			-- only count this once.
			flatten_views.event_tstamp::DATE         AS date,
			flatten_views.url_categorisation,
			flatten_views.element,
			flatten_views.touch_experience,
			flatten_views.territory,
			COUNT(DISTINCT flatten_views.event_hash) AS banner_impressions
		FROM flatten_views
		GROUP BY flatten_views.event_tstamp::DATE,
				 flatten_views.url_categorisation,
				 flatten_views.element,
				 flatten_views.touch_experience,
				 flatten_views.territory
	),
	flatten_clicks AS (
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			se.data.page_url_categorisation(pages.page_url)      AS url_categorisation,
			sessions.touch_experience,
			channel.touch_affiliate_territory                    AS territory,
			interaction_items.value['element_category']::VARCHAR AS element
		FROM se.data_pii.scv_page_screen_enrichment pages
			 INNER JOIN se.data_pii.scv_session_events_link sessionisation
			ON pages.event_hash = sessionisation.event_hash
			AND sessionisation.event_tstamp >= ' 2025-09-08'
			 INNER JOIN se.data.scv_touch_basic_attributes sessions
			ON sessionisation.touch_id = sessions.touch_id
			AND sessions.touch_start_tstamp >= '2025-09-08'
			 INNER JOIN se.data.scv_touch_marketing_channel channel
			ON sessionisation.touch_id = channel.touch_id
			AND channel.touch_start_tstamp >= '2025-09-08',
			 LATERAL FLATTEN(INPUT => pages.content_interaction_array, OUTER => TRUE) interaction_items
-- limit to home page only
		WHERE  pages.event_tstamp BETWEEN '2025-09-08' AND CURRENT_DATE
		  AND pages.content_interaction_array IS NOT NULL
		  AND interaction_items.value['element_category']::VARCHAR IN
			  (
			   'promotional strapline header banner',
			   'top media banner',
			   'middle media banner',
			   'footer media banner',
			   'two column slot media banner'
				  )
	),
	clicks AS (
		SELECT
			-- aggregate up to common grain, note that impressions are unique per page view, if someone
			-- scrolls a banner in and out of the view port it will fire multiple impressions, however we
			-- only count this once.
			flatten_clicks.event_tstamp::DATE         AS date,
			flatten_clicks.url_categorisation,
			flatten_clicks.element,
			flatten_clicks.touch_experience,
			flatten_clicks.territory,
			COUNT(DISTINCT flatten_clicks.event_hash) AS banner_clicks
		FROM flatten_clicks
		GROUP BY flatten_clicks.event_tstamp::DATE,
				 flatten_clicks.url_categorisation,
				 flatten_clicks.element,
				 flatten_clicks.touch_experience,
				 flatten_clicks.territory
	)
SELECT
	impressions.date,
	impressions.url_categorisation,
	impressions.element,
	impressions.banner_impressions,
	impressions.touch_experience,
	impressions.territory,
	clicks.banner_clicks
FROM impressions
LEFT JOIN clicks
	ON impressions.date = clicks.date
	AND impressions.url_categorisation = clicks.url_categorisation
	AND impressions.element = clicks.element
	AND impressions.touch_experience = clicks.touch_experience
	AND impressions.territory = clicks.territory