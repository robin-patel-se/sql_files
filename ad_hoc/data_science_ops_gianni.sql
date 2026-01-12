/*
 Find opportunities where if we apply DS we can generate revenue
 In order to display Will and Alex where insertion of DS can generate revenue

-- how many sessions
-- how much margin by month does our website generate
-- what is the conversion rate


3 Things to start:
  - Search
	-- how many sessions have a search
	-- what margin does a session with a search generate
 	-- what is the conversion rate for sessions with a search
 	-- search click through rate *nice to have*

  - Homepage
	-- how many sessions have see the homepage
	-- what margin does a session with a homepage view generate
 	-- what is the conversion rate for sessions with a homepage view

   - Homepage landing page
	-- how many sessions have see the homepage landing page
	-- what margin does a session with a homepage view as landing page generate
 	-- what is the conversion rate for sessions with a homepage view landing page
 */
USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TRANSIENT TABLE collab.muse.data_science_opportunities_data AS (
	WITH
		search_sessions AS (
			SELECT
				sts.touch_id,
				COUNT(1) AS searches
			FROM se.data.scv_touched_searches sts
			WHERE sts.event_tstamp >= '2023-01-01'
			  AND sts.triggered_by = 'user' -- user only searches
			GROUP BY 1
		),
		converted_sessions AS (
			SELECT
				stt.touch_id,
				COUNT(1)                                           AS bookings,
				SUM(fb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
			FROM se.data.scv_touched_transactions stt
				INNER JOIN se.data.fact_booking fb
						   ON stt.booking_id = fb.booking_id
							   AND fb.booking_status_type IN ('live', 'cancelled')
			WHERE stt.event_tstamp >= '2023-01-01'
			GROUP BY 1
		),
		homepage_in_session AS (
			SELECT DISTINCT
				touch_id
			FROM se.data_pii.scv_event_stream ses
				INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
			WHERE ses.event_tstamp >= '2023-01-01'
			  AND (ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'homepage'
				OR ses.page_urlpath LIKE '%current-sales%'
				OR ses.page_urlpath LIKE '%aktuelle-angebote%'
				OR ses.page_urlpath LIKE '%currentSales'
				OR ses.page_urlpath LIKE '%aanbedingen%' -- NL
				OR ses.page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR ses.page_urlpath LIKE '%nuvaerende-salg%'
				OR ses.page_urlpath LIKE '%aktuella-kampanjer%'
				OR ses.page_urlpath = '/'
				)
		),
		input_session_info AS (
			SELECT DISTINCT
				stba.touch_start_tstamp,
				stmc.touch_affiliate_territory,
				stba.touch_experience,
				stba.touch_id,
				stba.touch_landing_page,
				stba.attributed_user_id,
				cs.touch_id IS NOT NULL                  AS converted_session,
				ss.touch_id IS NOT NULL                  AS session_with_search,
				his.touch_id IS NOT NULL                 AS session_with_homepage_view,
				stba.touch_landing_page LIKE '%current-sales%'
					OR stba.touch_landing_page LIKE '%aktuelle-angebote%'
					OR stba.touch_landing_page LIKE '%currentSales'
					OR stba.touch_landing_page LIKE '%aanbedingen%' -- NL
					OR stba.touch_landing_page LIKE '%offerte-in-corso%' -- IT
					OR stba.touch_landing_page LIKE '%nuvaerende-salg%'
					OR stba.touch_landing_page LIKE '%aktuella-kampanjer%'
					OR stba.touch_landing_pagepath = '/' AS homepage_landing_page,
				COALESCE(ss.searches, 0)                 AS searches,
				COALESCE(cs.bookings, 0)                 AS bookings,
				COALESCE(cs.margin_gbp, 0)               AS margin_gbp
			FROM se.data_pii.scv_touch_basic_attributes stba
				INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
				LEFT JOIN  converted_sessions cs ON stba.touch_id = cs.touch_id
				LEFT JOIN  search_sessions ss ON stba.touch_id = ss.touch_id
				LEFT JOIN  homepage_in_session his ON stba.touch_id = his.touch_id
			WHERE stba.touch_start_tstamp >= '2023-01-01'
			  AND stba.stitched_identity_type = 'se_user_id' -- limiting to only sessions we can associate with a member
		)
	SELECT *
	FROM input_session_info isi
-- removing app because search events not available in app
	WHERE isi.touch_experience NOT LIKE 'native app%'
)
;

DROP TABLE scratch.robinpatel.data_science_opportunities_data
;


SELECT
	DATE_TRUNC(MONTH, isi.touch_start_tstamp)                                AS month,
	isi.touch_affiliate_territory,
	COUNT(DISTINCT isi.touch_id)                                             AS sessions,
	COUNT(DISTINCT isi.attributed_user_id)                                   AS users,
	SUM(IFF(isi.converted_session, 1, 0))                                    AS converted_sessions,
	converted_sessions / sessions                                            AS session_conversion_rate,
	SUM(isi.bookings)                                                        AS bookings,
	SUM(isi.margin_gbp)                                                      AS margin_gbp,

	SUM(IFF(isi.session_with_search, 1, 0))                                  AS sessions_with_search,
	SUM(isi.searches)                                                        AS searches,
	SUM(IFF(isi.session_with_search AND isi.converted_session, 1, 0))        AS sessions_with_search_converted,
	sessions_with_search_converted / NULLIF(sessions_with_search, 0)         AS session_with_search_conversion_rate,
	SUM(IFF(isi.session_with_search, isi.bookings, 0))                       AS sessions_with_search_bookings,
	SUM(IFF(isi.session_with_search, isi.margin_gbp, 0))                     AS sessions_with_search_margin,

	SUM(IFF(isi.session_with_homepage_view, 1, 0))                           AS sessions_with_homepage,
	SUM(IFF(isi.session_with_homepage_view AND isi.converted_session, 1, 0)) AS sessions_with_homepage_converted,
	sessions_with_homepage_converted / NULLIF(sessions_with_homepage, 0)     AS session_with_homepage_conversion_rate,
	SUM(IFF(isi.session_with_homepage_view, isi.bookings, 0))                AS sessions_with_homepage_bookings,
	SUM(IFF(isi.session_with_homepage_view, isi.margin_gbp, 0))              AS sessions_with_homepage_margin,

	SUM(IFF(isi.homepage_landing_page, 1, 0))                                AS sessions_with_homepage_landing,
	SUM(IFF(isi.homepage_landing_page AND isi.converted_session, 1, 0))      AS sessions_with_homepage_landing_converted,
	sessions_with_homepage_landing_converted /
	NULLIF(sessions_with_homepage_landing, 0)                                AS session_with_homepage_landing_conversion_rate,
	SUM(IFF(isi.homepage_landing_page, isi.bookings, 0))                     AS sessions_with_homepage_landing_bookings,
	SUM(IFF(isi.homepage_landing_page, isi.margin_gbp, 0))                   AS sessions_with_homepage_landing_margin

FROM collab.muse.data_science_opportunities_data isi
-- removing app because search events not available in app
WHERE isi.touch_experience NOT LIKE 'native app%'
  AND isi.touch_affiliate_territory IN ('DE', 'UK')
GROUP BY 1, 2
;
