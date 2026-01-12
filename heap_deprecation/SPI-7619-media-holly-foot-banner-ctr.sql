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

-- editing flatten_views CTE to sense check footer numbers as these are higher than 2 column banner

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
		WHERE se.data.page_url_categorisation(pages.page_url) = 'home page'
		  AND pages.content_viewed_array IS NOT NULL
		  -- 8th september is when impressions and interaction events were corrected from
		  AND pages.event_tstamp::DATE = '2025-11-17'
		  AND viewed_items.value['elements'][0]::VARCHAR IN
			  ('footer media banner'
				  )
	)
SELECT
	COUNT(*),
	COUNT(DISTINCT viewed_events.event_hash),
	COUNT(DISTINCT web_page_id)
FROM viewed_events
;
;

-- 7.6K viewed items
-- across 7.3 event hashes and web page ids


SELECT
	ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
	ses.page_url,
	ses.event_hash
FROM se.data_pii.scv_event_stream ses,
	 LATERAL FLATTEN(INPUT => ses.contexts_com_secretescapes_content_element_viewed_context_1[0]['elements'], OUTER =>
					 TRUE) viewed_elements
WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
  AND ses.event_tstamp::DATE = '2025-11-17'
--   AND se.data.page_url_categorisation(ses.page_url) = 'home page'
  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND viewed_elements.value = 'footer media banner'


-- based on content viewed events that have url home page there are only 1.1K content viewed events.
-- when removing the homepage url filter it jumps to 7.6K
-- this lends to an assumption that footer content viewed events that don't belong to a homepage are being associated to home pages

-- investigating enrichment to understand why there are so many events for footer on homepage

SELECT *
FROM se.data_pii.scv_page_screen_enrichment pages
WHERE pages.event_tstamp::DATE = '2025-11-17'
  AND se.data.page_url_categorisation(pages.page_url) = 'home page'
  AND pages.content_viewed_array IS NOT NULL
  AND pages.content_viewed_array::VARCHAR LIKE '%footer media banner%'
  AND pages.event_hash = '71c4de41edf8599034a3c49a129d1a83028c6544c270b491b9c6e080f85ec2e5'
;

WITH
	non_homepage_footer_impressions AS (
-- non home page footer views
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
		WHERE ses.se_brand = 'SE Brand'
--   AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE = '2025-11-17'
		  AND se.data.page_url_categorisation(ses.page_url) IS DISTINCT FROM 'home page'
		  AND ses.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
		  AND ses.contexts_com_secretescapes_content_element_viewed_context_1[0]::VARCHAR LIKE '%footer media banner%'
	)
		,
	homepage_page_views AS (

-- homepage page views
		SELECT
			ses.unique_browser_id,
			ses.user_id,
			ses.event_tstamp,
			ses.event_hash,
			ses.event_name,
			ses.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
			ses.page_url,
			ses.event_hash,
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.se_brand = 'SE Brand'
		  AND ses.event_name = 'page_view'
		  AND ses.event_tstamp::DATE = '2025-11-17'
		  AND se.data.page_url_categorisation(ses.page_url) = 'home page'
	)
SELECT *
FROM homepage_page_views
INNER JOIN non_homepage_footer_impressions ON homepage_page_views.web_page_id = non_homepage_footer_impressions.web_page_id
