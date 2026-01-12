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

-- investigating top media banner impressions in relation to homepage page views, theory being that they should be very
-- close to one another because when you load the homepage the top media banner loads straight away

WITH
	viewed_events AS (
		SELECT
			pages.event_tstamp,
			pages.event_hash,
			pages.web_page_id,
			viewed_items.value['elements'][0]::VARCHAR AS element
		FROM se.data_pii.scv_page_screen_enrichment pages,
			 LATERAL FLATTEN(INPUT => pages.content_viewed_array, OUTER => TRUE) viewed_items
-- limit to home page only
		WHERE 1 = 1
-- 		  AND se.data.page_url_categorisation(pages.page_url) = 'home page'
		  AND pages.content_viewed_array IS NOT NULL
		  -- 8th september is when impressions and interaction events were corrected from
		  AND pages.event_tstamp::DATE = '2025-11-17'
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  ('top media banner'
				  )
	)
SELECT
	COUNT(*),
	COUNT(DISTINCT viewed_events.event_hash),
	COUNT(DISTINCT web_page_id)
FROM viewed_events
;

-- 123831 content viewed
-- 48960 unique event hashes

-- when taking homepage filter off, there is a fair amount more

-- 212330 content viewed
-- 76933 unique event hashes

SELECT
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-17'
  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.se_brand = 'SE Brand'
  AND ses.is_server_side_event = FALSE

-- 70400 homepage page views


SELECT
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2025-11-17'
--   AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.se_brand = 'SE Brand'
  AND ses.is_server_side_event = FALSE
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1[0]::VARCHAR LIKE '%top media banner%'


-- found there's a large proportion of events that are firing on urls that aren't homepage therefore looking at some samples

SELECT
	ses.unique_browser_id,
	ses.user_id,
	ses.event_tstamp,
	ses.event_hash,
	ses.event_name,
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
	ses.page_url,
	ses.event_hash
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2025-11-17'
  AND se.data.page_url_categorisation(ses.page_url) IS DISTINCT FROM 'home page'
  AND ses.se_brand = 'SE Brand'
  AND ses.is_server_side_event = FALSE
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1[0]::VARCHAR LIKE '%top media banner%'

-- these appear to be coming from black friday events pages so looks as expected (as these are not homepages)

-- this doesn't explain the query of why homepage views are circa 70K but top media banner content viewed events are only
-- present on circa 50K page views;

WITH
	home_page_pageviews AS (
-- home page page views
		SELECT
			ses.unique_browser_id,
			ses.user_id,
			ses.event_tstamp,
			ses.event_hash,
			ses.event_name,
			ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			ses.page_url,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.event_tstamp::DATE = '2025-11-17'
		  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
		  AND ses.se_brand = 'SE Brand'
		  AND ses.event_name = 'page_view'
		  AND ses.is_server_side_event = FALSE
	)
		,
	top_media_banner_impressions AS (
		SELECT *
		FROM se.data_pii.scv_page_screen_enrichment spse
		WHERE spse.event_tstamp::DATE = '2025-11-17'
		  AND spse.content_viewed_array::VARCHAR LIKE '%top media banner%'
	)
SELECT *
FROM home_page_pageviews
LEFT JOIN top_media_banner_impressions
	ON home_page_pageviews.event_hash = top_media_banner_impressions.event_hash
WHERE top_media_banner_impressions.event_hash IS NULL
;

-- found example where according to above query there are no top media banner views
-- event hash of pageview 765da9da-2748-4dc4-b6ba-e963f9a10dc3
-- web page id 7779ff54-3485-4ab2-b7fe-8193ce52e34f
-- url https://www.secretescapes.com/
-- user id NULL
-- unique browser id: 765da9da-2748-4dc4-b6ba-e963f9a10dc3
-- date '2025-11-17'

SELECT
	ses.unique_browser_id,
	ses.cookie_id,
	ses.device_platform,
	ses.user_id,
	ses.event_tstamp,
	ses.event_hash,
	ses.event_name,
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
	ses.page_url,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'][0]::VARCHAR,
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2025-11-17'
  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.se_brand = 'SE Brand'
  AND ses.is_server_side_event = FALSE
  AND ses.unique_browser_id = '765da9da-2748-4dc4-b6ba-e963f9a10dc3'


-- event hash of pageview 8b9c4b36008436e93086a60b7f8e48099d760a423161d8a617fd0d01e655aaf4
-- web page id 606090eb-822b-44c7-9e78-622cf71756db
-- url https://www.secretescapes.com/
-- user id NULL
-- unique browser id: 9d55f97f-9bf4-40e9-87a3-1c0426a08268
-- date '2025-11-17'

SELECT
	ses.unique_browser_id,
	ses.cookie_id,
	ses.device_platform,
	ses.user_id,
	ses.event_tstamp,
	ses.event_hash,
	ses.event_name,
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
	ses.page_url,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'][0]::VARCHAR,
	ses.useragent
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2025-11-17'
--   AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.se_brand = 'SE Brand'
  AND ses.is_server_side_event = FALSE
  AND (ses.unique_browser_id = 'a261311b-980a-4ee5-a606-160b35c63917'
	OR ses.user_id = '75380693')


SELECT * FROM se.data.se_